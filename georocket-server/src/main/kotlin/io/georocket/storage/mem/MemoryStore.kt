package io.georocket.storage.mem

import java.io.FileNotFoundException
import java.util.Queue

import io.georocket.storage.ChunkReadStream
import io.georocket.storage.Store
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.shareddata.AsyncMap

/**
 *
 * Stores chunks in memory
 *
 * **Attention: The store is not persisted. Contents will be lost when
 * GeoRocket quits.**
 * @author Michel Kraemer
 */
class MemoryStore
/**
 * Default constructor
 * @param vertx the Vert.x instance
 */
(private val vertx: Vertx) : IndexedStore(vertx), Store {
    private var store: AsyncMap<String, Buffer>? = null

    private fun getStore(handler: Handler<AsyncResult<AsyncMap<String, Buffer>>>) {
        if (store != null) {
            handler.handle(Future.succeededFuture(store))
            return
        }

        val name = javaClass.name + ".STORE"
        vertx.sharedData().getAsyncMap<String, Buffer>(name) { ar ->
            if (ar.succeeded()) {
                store = ar.result()
            }
            handler.handle(ar)
        }
    }

    override fun doAddChunk(chunk: String, path: String?, correlationId: String,
                            handler: Handler<AsyncResult<String>>) {
        var path = path
        if (path == null || path.isEmpty()) {
            path = "/"
        }
        val filename = PathUtils.join(path, generateChunkId(correlationId))

        getStore({ ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture<String>(ar.cause()))
            } else {
                ar.result().put(filename, Buffer.buffer(chunk), { par ->
                    if (par.failed()) {
                        handler.handle(Future.failedFuture<String>(par.cause()))
                    } else {
                        handler.handle(Future.succeededFuture(filename))
                    }
                })
            }
        })
    }

    override fun getOne(path: String, handler: Handler<AsyncResult<ChunkReadStream>>) {
        val finalPath = PathUtils.normalize(path)
        getStore({ ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture<ChunkReadStream>(ar.cause()))
            } else {
                ar.result().get(finalPath, { gar ->
                    if (gar.failed()) {
                        handler.handle(Future.failedFuture<ChunkReadStream>(gar.cause()))
                    } else {
                        val chunk = gar.result()
                        if (chunk == null) {
                            handler.handle(Future.failedFuture(FileNotFoundException(
                                    "Could not find chunk: $finalPath")))
                            return@ar.result().get
                        }
                        handler.handle(Future.succeededFuture(DelegateChunkReadStream(chunk!!)))
                    }
                })
            }
        })
    }

    override fun doDeleteChunks(paths: Queue<String>, handler: Handler<AsyncResult<Void>>) {
        getStore({ ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture<Void>(ar.cause()))
            } else {
                doDeleteChunks(paths, ar.result(), handler)
            }
        })
    }

    private fun doDeleteChunks(paths: Queue<String>, store: AsyncMap<String, Buffer>,
                               handler: Handler<AsyncResult<Void>>) {
        if (paths.isEmpty()) {
            handler.handle(Future.succeededFuture())
            return
        }

        val path = PathUtils.normalize(paths.poll())
        store.remove(path) { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
            } else {
                doDeleteChunks(paths, store, handler)
            }
        }
    }
}
