package io.georocket.storage.indexed

import io.georocket.constants.AddressConstants
import io.georocket.index.IndexableChunkCache
import io.georocket.storage.AsyncCursor
import io.georocket.storage.ChunkMeta
import io.georocket.storage.DeleteMeta
import io.georocket.storage.IndexMeta
import io.georocket.storage.Store
import io.georocket.storage.StoreCursor
import io.georocket.tasks.PurgingTask
import io.georocket.tasks.TaskError
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.apache.commons.lang3.StringUtils

import java.security.SecureRandom
import java.time.Instant
import java.util.ArrayDeque
import java.util.Objects
import java.util.Queue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import java.util.function.Function

/**
 * An abstract base class for chunk stores that are backed by an indexer
 * @author Michel Kraemer
 */
abstract class IndexedStore
/**
 * Constructs the chunk store
 * @param vertx the Vert.x instance
 */
(private val vertx: Vertx) : Store {

    override fun add(chunk: String, chunkMeta: ChunkMeta, path: String,
                     indexMeta: IndexMeta, handler: Handler<AsyncResult<Void>>) {
        doAddChunk(chunk, path, indexMeta.correlationId, { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture<Void>(ar.cause()))
            } else {
                // start indexing
                val indexMsg = JsonObject()
                        .put("path", ar.result())
                        .put("meta", chunkMeta.toJsonObject())

                if (indexMeta.correlationId != null) {
                    indexMsg.put("correlationId", indexMeta.correlationId)
                }
                if (indexMeta.filename != null) {
                    indexMsg.put("filename", indexMeta.filename)
                }
                indexMsg.put("timestamp", indexMeta.timestamp)
                if (indexMeta.tags != null) {
                    indexMsg.put("tags", JsonArray(indexMeta.tags))
                }
                if (indexMeta.fallbackCRSString != null) {
                    indexMsg.put("fallbackCRSString", indexMeta.fallbackCRSString)
                }
                if (indexMeta.properties != null) {
                    indexMsg.put("properties", JsonObject(indexMeta.properties))
                }

                // save chunk to cache and then let indexer know about it
                IndexableChunkCache.instance.put(ar.result(), Buffer.buffer(chunk))
                vertx.eventBus().send(AddressConstants.INDEXER_ADD, indexMsg)

                // tell sender that writing was successful
                handler.handle(Future.succeededFuture())
            }
        })
    }

    override fun delete(search: String, path: String, handler: Handler<AsyncResult<Void>>) {
        delete(search, path, null, handler)
    }

    /**
     * Send a message to the task verticle telling it that we are now starting
     * to delete chunks from the store
     * @param correlationId the correlation ID of the purging task
     * @param totalChunks the total number of chunks to purge
     */
    private fun startPurgingTask(correlationId: String?, totalChunks: Int) {
        val purgingTask = PurgingTask(correlationId)
        purgingTask.startTime = Instant.now()
        purgingTask.totalChunks = totalChunks.toLong()
        vertx.eventBus().publish(AddressConstants.TASK_INC,
                JsonObject.mapFrom(purgingTask))
    }

    /**
     * Send a message to the task verticle telling it that we have finished
     * deleting chunks from the store
     * @param correlationId the correlation ID of the purging task
     * @param error an error that occurred during the task execution (may be
     * `null` if everything is OK
     */
    private fun stopPurgingTask(correlationId: String?, error: TaskError?) {
        val purgingTask = PurgingTask(correlationId)
        purgingTask.endTime = Instant.now()
        if (error != null) {
            purgingTask.addError(error)
        }
        vertx.eventBus().publish(AddressConstants.TASK_INC,
                JsonObject.mapFrom(purgingTask))
    }

    /**
     * Send a message to the task verticle telling it that we just deleted
     * the given number of chunks from the store
     * @param correlationId the correlation ID of the purging task
     */
    private fun updatePurgingTask(correlationId: String?, purgedChunks: Int) {
        val purgingTask = PurgingTask(correlationId)
        purgingTask.purgedChunks = purgedChunks.toLong()
        vertx.eventBus().publish(AddressConstants.TASK_INC,
                JsonObject.mapFrom(purgingTask))
    }

    override fun delete(search: String, path: String, deleteMeta: DeleteMeta?,
                        handler: Handler<AsyncResult<Void>>) {
        val correlationId = deleteMeta?.correlationId
        if (correlationId != null) {
            startPurgingTask(correlationId, 0)
        }

        get(search, path) { ar ->
            if (ar.failed()) {
                val cause = ar.cause()
                if (cause is ReplyException) {
                    if (cause.failureCode() == 404) {
                        stopPurgingTask(correlationId, null)
                        handler.handle(Future.succeededFuture())
                        return@get
                    }
                }
                stopPurgingTask(correlationId, TaskError(ar.cause()))
                handler.handle(Future.failedFuture(ar.cause()))
            } else {
                val cursor = ar.result()
                val remaining = AtomicLong(cursor.info.totalHits)
                if (correlationId != null) {
                    startPurgingTask(correlationId, remaining.toInt())
                }

                val paths = ArrayDeque<String>()
                doDelete(cursor, paths, remaining, correlationId, { ddar ->
                    if (correlationId != null) {
                        if (ddar.failed()) {
                            stopPurgingTask(correlationId, TaskError(ddar.cause()))
                        } else {
                            stopPurgingTask(correlationId, null)
                        }
                    }
                    handler.handle(ddar)
                })
            }
        }
    }

    override fun get(search: String, path: String, handler: Handler<AsyncResult<StoreCursor>>) {
        IndexedStoreCursor(vertx, search, path).start(handler)
    }

    override fun scroll(search: String, path: String, size: Int, handler: Handler<AsyncResult<StoreCursor>>) {
        FrameCursor(vertx, search, path, size).start(handler)
    }

    override fun scroll(scrollId: String, handler: Handler<AsyncResult<StoreCursor>>) {
        FrameCursor(vertx, scrollId).start(handler)
    }

    /**
     * Iterate over a cursor and delete all returned chunks from the index
     * and from the store.
     * @param cursor the cursor to iterate over
     * @param paths an empty queue (used internally for recursion)
     * @param remainingChunks holds the remaining number of chunks to delete
     * (used internally for recursion)
     * @param correlationId the correlation ID of the current purging task
     * @param handler will be called when all chunks have been deleted
     */
    private fun doDelete(cursor: StoreCursor, paths: Queue<String>,
                         remainingChunks: AtomicLong, correlationId: String?,
                         handler: (Any) -> Unit) {
        // handle response of bulk delete operation
        val handleBulk = { size ->
            { bulkAr ->
                remainingChunks.getAndAdd((-size)!!.toLong())
                if (correlationId != null) {
                    updatePurgingTask(correlationId, size!!)
                }
                if (bulkAr.failed()) {
                    handler.handle(Future.failedFuture<Void>(bulkAr.cause()))
                } else {
                    doDelete(cursor, paths, remainingChunks, correlationId, handler)
                }
            }
        }

        // while cursor has items ...
        if (cursor.hasNext()) {
            cursor.next { ar ->
                if (ar.failed()) {
                    handler.handle(Future.failedFuture(ar.cause()))
                } else {
                    // add item to queue
                    paths.add(cursor.chunkPath)
                    val size = cursor.info.currentHits

                    if (paths.size >= size) {
                        // if there are enough items in the queue, bulk delete them
                        doDeleteBulk(paths, cursor.info.totalHits,
                                remainingChunks.get(), correlationId, handleBulk.apply(size))
                    } else {
                        // otherwise proceed with next item from cursor
                        doDelete(cursor, paths, remainingChunks, correlationId, handler)
                    }
                }
            }
        } else {
            // cursor does not return any more items
            if (!paths.isEmpty()) {
                // bulk delete the remaining ones
                doDeleteBulk(paths, cursor.info.totalHits,
                        remainingChunks.get(), correlationId, handleBulk.apply(paths.size))
            } else {
                // end operation
                handler.handle(Future.succeededFuture())
            }
        }
    }

    /**
     * Delete all chunks with the given paths from the index and from the store.
     * Remove all items from the given queue.
     * @param paths the queue of paths of chunks to delete (will be empty when
     * the operation has finished)
     * @param totalChunks the total number of paths to delete (including this batch)
     * @param remainingChunks the remaining chunks to delete (including this batch)
     * @param correlationId the correlation ID of the current purging task
     * @param handler will be called when the operation has finished
     */
    private fun doDeleteBulk(paths: Queue<String>, totalChunks: Long,
                             remainingChunks: Long, correlationId: String?, handler: Handler<AsyncResult<Void>>) {
        // delete from index first so the chunks cannot be found anymore
        val jsonPaths = JsonArray()
        paths.forEach(Consumer<String> { jsonPaths.add(it) })
        val indexMsg = JsonObject()
                .put("correlationId", correlationId)
                .put("paths", jsonPaths)
                .put("totalChunks", totalChunks)
                .put("remainingChunks", remainingChunks)
        vertx.eventBus().send<Any>(AddressConstants.INDEXER_DELETE, indexMsg) { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
            } else {
                // now delete all chunks from file system and clear the queue
                doDeleteChunks(paths, handler)
            }
        }
    }

    override fun getAttributeValues(search: String, path: String?, attribute: String,
                                    handler: Handler<AsyncResult<AsyncCursor<String>>>) {
        val template = JsonObject()
                .put("search", search)
                .put("attribute", attribute)
        if (path != null) {
            template.put("path", path)
        }
        IndexedAsyncCursor(Function<Any, String> { Objects.toString(it) },
                AddressConstants.METADATA_GET_ATTRIBUTE_VALUES, vertx, template)
                .start(handler)
    }

    override fun getPropertyValues(search: String, path: String?, property: String,
                                   handler: Handler<AsyncResult<AsyncCursor<String>>>) {
        val template = JsonObject()
                .put("search", search)
                .put("property", property)
        if (path != null) {
            template.put("path", path)
        }
        IndexedAsyncCursor(Function<Any, String> { Objects.toString(it) },
                AddressConstants.METADATA_GET_PROPERTY_VALUES, vertx, template)
                .start(handler)
    }

    override fun setProperties(search: String, path: String?,
                               properties: Map<String, String>, handler: Handler<AsyncResult<Void>>) {
        val msg = JsonObject()
                .put("search", search)
                .put("properties", JsonObject.mapFrom(properties))
        if (path != null) {
            msg.put("path", path)
        }

        send(AddressConstants.METADATA_SET_PROPERTIES, msg, handler)
    }

    override fun removeProperties(search: String, path: String?,
                                  properties: List<String>, handler: Handler<AsyncResult<Void>>) {
        val msg = JsonObject()
                .put("search", search)
                .put("properties", JsonArray(properties))
        if (path != null) {
            msg.put("path", path)
        }

        send(AddressConstants.METADATA_REMOVE_PROPERTIES, msg, handler)
    }

    override fun appendTags(search: String, path: String?, tags: List<String>,
                            handler: Handler<AsyncResult<Void>>) {
        val msg = JsonObject()
                .put("search", search)
                .put("tags", JsonArray(tags))
        if (path != null) {
            msg.put("path", path)
        }

        send(AddressConstants.METADATA_APPEND_TAGS, msg, handler)
    }

    override fun removeTags(search: String, path: String?, tags: List<String>,
                            handler: Handler<AsyncResult<Void>>) {
        val msg = JsonObject()
                .put("search", search)
                .put("tags", JsonArray(tags))
        if (path != null) {
            msg.put("path", path)
        }

        send(AddressConstants.METADATA_REMOVE_TAGS, msg, handler)
    }

    /**
     * Send a message to the specified address and pass null to the handler when
     * the verticle responds
     * @param address the address to send it to
     * @param msg the message to send
     * @param handler the handler which is called when the verticle responds
     */
    private fun send(address: String, msg: Any,
                     handler: Handler<AsyncResult<Void>>) {
        vertx.eventBus().send<Any>(address, msg) { ar -> handler.handle(ar.map { x -> null }) }
    }

    /**
     * Generate or get a unique chunk identifier. The given correlation ID will
     * be used as the new identifier's prefix. The remaining 16 characters will
     * be constructed from the current time and an atomic counter.
     * @param correlationId the correlation ID of the current import process
     * @return chunk identifier
     */
    protected fun generateChunkId(correlationId: String): String {
        val seconds = (System.currentTimeMillis() / 1000).toInt()
        val time = StringUtils.leftPad(Integer.toHexString(seconds),
                Integer.SIZE / java.lang.Byte.SIZE * 2, '0')
        val counter = StringUtils.leftPad(Integer.toHexString(COUNTER.getAndIncrement()),
                Integer.SIZE / java.lang.Byte.SIZE * 2, '0')
        return correlationId + time + counter
    }

    /**
     * Add a chunk to the store
     * @param chunk the chunk to add
     * @param path the chunk's destination path
     * @param handler will be called when the operation has finished
     */
    protected abstract fun doAddChunk(chunk: String, path: String,
                                      correlationId: String, handler: (Any) -> Unit)

    /**
     * Delete all chunks with the given paths from the store. Remove one item
     * from the given queue, delete the chunk, and then call recursively until
     * all items have been removed from the queue. Finally, call the given handler.
     * @param paths the queue of paths of chunks to delete (will be empty when
     * the operation has finished)
     * @param handler will be called when the operation has finished
     */
    protected abstract fun doDeleteChunks(paths: Queue<String>, handler: Handler<AsyncResult<Void>>)

    companion object {
        private val COUNTER = AtomicInteger(SecureRandom().nextInt())
    }
}
