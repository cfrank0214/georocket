package io.georocket.storage

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.rx.java.ObservableFuture
import io.vertx.rx.java.RxHelper
import io.vertx.rx.java.SingleOnSubscribeAdapter
import rx.Completable
import rx.Single

/**
 * Wraps around [Store] and adds methods to be used with RxJava
 * @author Michel Kraemer
 */
class RxStore
/**
 * Create a new rx-ified store
 * @param delegate the actual store to delegate to
 */
(
        /**
         * @return the actual non-rx-ified store
         */
        val delegate: Store) : Store {

    override fun add(chunk: String, chunkMeta: ChunkMeta, path: String,
                     indexMeta: IndexMeta, handler: Handler<AsyncResult<Void>>) {
        delegate.add(chunk, chunkMeta, path, indexMeta, handler)
    }

    /**
     * Rx version of [.add]
     * @param chunk the chunk to add
     * @param chunkMeta the chunk's metadata
     * @param path the path where the chunk should be stored (may be null)
     * @param indexMeta metadata affecting the way the chunk will be indexed
     * @return a Completable that completes when the operation has finished
     */
    fun rxAdd(chunk: String, chunkMeta: ChunkMeta,
              path: String, indexMeta: IndexMeta): Completable {
        return Single.create(SingleOnSubscribeAdapter<Void> { f -> add(chunk, chunkMeta, path, indexMeta, f) }).toCompletable()
    }

    override fun getOne(path: String, handler: Handler<AsyncResult<ChunkReadStream>>) {
        delegate.getOne(path, handler)
    }

    /**
     * Rx version of [.getOne]
     * @param path the absolute path to the chunk
     * @return a Single that will emit a read stream that can be used to
     * get the chunk's contents
     */
    fun rxGetOne(path: String): Single<ChunkReadStream> {
        val o = RxHelper.observableFuture<ChunkReadStream>()
        getOne(path, o.toHandler())
        return o.toSingle()
    }

    override fun delete(search: String, path: String, handler: Handler<AsyncResult<Void>>) {
        delegate.delete(search, path, handler)
    }

    /**
     * Rx version of [.delete]
     * @param search the search query
     * @param path the path where to search for the chunks (may be null)
     * @return a Completable that completes when the operation has finished
     */
    @Deprecated("Call {@link #rxDelete(String, String, DeleteMeta)}\n" +
            "    instead with a unique {@code correlationId} in the {@link DeleteMeta}\n" +
            "    object so the deletion process can be tracked correctly. This method\n" +
            "    will be removed in GeoRocket 2.0.0.")
    fun rxDelete(search: String, path: String): Completable {
        return Single.create(SingleOnSubscribeAdapter<Void> { f -> delete(search, path, f) }).toCompletable()
    }

    override fun delete(search: String, path: String, deleteMeta: DeleteMeta,
                        handler: Handler<AsyncResult<Void>>) {
        delegate.delete(search, path, deleteMeta, handler)
    }

    /**
     * Rx version of [.delete]
     * @param search the search query
     * @param path the path where to search for the chunks (may be null)
     * @param deleteMeta a metadata object containing additional information
     * about the deletion process
     * @return a Completable that completes when the operation has finished
     */
    fun rxDelete(search: String, path: String, deleteMeta: DeleteMeta): Completable {
        return Single.create(SingleOnSubscribeAdapter<Void> { f -> delete(search, path, deleteMeta, f) }).toCompletable()
    }

    override fun get(search: String, path: String, handler: Handler<AsyncResult<StoreCursor>>) {
        delegate.get(search, path, handler)
    }

    /**
     * Rx version of [.get]
     * @param search the search query
     * @param path the path where to search for the chunks (may be null)
     * @return a Single that emits a cursor that can be used to iterate
     * over all matched chunks
     */
    fun rxGet(search: String, path: String): Single<StoreCursor> {
        val o = RxHelper.observableFuture<StoreCursor>()
        get(search, path, o.toHandler())
        return o.toSingle()
    }

    override fun scroll(search: String, path: String, size: Int, handler: Handler<AsyncResult<StoreCursor>>) {
        delegate.scroll(search, path, size, handler)
    }

    /**
     * Rx version of [.scroll]
     * @param search the search query
     * @param path the path where to search for the chunks (may be null)
     * @param size the number of elements to load
     * @return a Single that emits a cursor that can be used to iterate
     * over all matched chunks
     */
    fun rxScroll(search: String, path: String, size: Int): Single<StoreCursor> {
        val o = RxHelper.observableFuture<StoreCursor>()
        scroll(search, path, size, o.toHandler())
        return o.toSingle()
    }

    override fun scroll(scrollId: String, handler: Handler<AsyncResult<StoreCursor>>) {
        delegate.scroll(scrollId, handler)
    }

    /**
     * Rx version of [.scroll]
     * @param scrollId The scrollId to load the chunks
     * @return a Single that emits a cursor that can be used to iterate
     * over all matched chunks
     */
    fun rxScroll(scrollId: String): Single<StoreCursor> {
        val o = RxHelper.observableFuture<StoreCursor>()
        scroll(scrollId, o.toHandler())
        return o.toSingle()
    }

    override fun getAttributeValues(search: String, path: String, attribute: String,
                                    handler: Handler<AsyncResult<AsyncCursor<String>>>) {
        delegate.getAttributeValues(search, path, attribute, handler)
    }

    /**
     * Rx version of [.getAttributeValues]
     * @param search the search query
     * @param path the path where to search for the values (may be null)
     * @param attribute the name of the attribute
     * @return emits when the values have been retrieved from the store
     */
    fun rxGetAttributeValues(search: String,
                             path: String, attribute: String): Single<AsyncCursor<String>> {
        val o = RxHelper.observableFuture<AsyncCursor<String>>()
        getAttributeValues(search, path, attribute, o.toHandler())
        return o.toSingle()
    }

    override fun getPropertyValues(search: String, path: String, property: String,
                                   handler: Handler<AsyncResult<AsyncCursor<String>>>) {
        delegate.getPropertyValues(search, path, property, handler)
    }

    /**
     * Rx version of [.getPropertyValues]
     * @param search the search query
     * @param path the path where to search for the values (may be null)
     * @param property the name of the property
     * @return emits when the values have been retrieved from the store
     */
    fun rxGetPropertyValues(search: String,
                            path: String, property: String): Single<AsyncCursor<String>> {
        val o = RxHelper.observableFuture<AsyncCursor<String>>()
        getPropertyValues(search, path, property, o.toHandler())
        return o.toSingle()
    }

    override fun setProperties(search: String, path: String,
                               properties: Map<String, String>, handler: Handler<AsyncResult<Void>>) {
        delegate.setProperties(search, path, properties, handler)
    }

    /**
     * Rx version of [.setProperties]
     * @param search the search query
     * @param path the path where to search for the values (may be null)
     * @param properties the list of properties to set
     * @return a Completable that completes when the operation has finished
     */
    fun rxSetProperties(search: String, path: String,
                        properties: Map<String, String>): Completable {
        return Single.create(SingleOnSubscribeAdapter<Void> { f -> setProperties(search, path, properties, f) }).toCompletable()
    }

    override fun removeProperties(search: String, path: String,
                                  properties: List<String>, handler: Handler<AsyncResult<Void>>) {
        delegate.removeProperties(search, path, properties, handler)
    }

    /**
     * Rx version of [.removeProperties]
     * @param search the search query
     * @param path the path where to search for the values (may be null)
     * @param properties the list of properties to remove
     * @return a Completable that completes when the operation has finished
     */
    fun rxRemoveProperties(search: String, path: String,
                           properties: List<String>): Completable {
        return Single.create(SingleOnSubscribeAdapter<Void> { f -> removeProperties(search, path, properties, f) }).toCompletable()
    }

    override fun appendTags(search: String, path: String, tags: List<String>,
                            handler: Handler<AsyncResult<Void>>) {
        delegate.appendTags(search, path, tags, handler)
    }

    /**
     * Rx version of [.appendTags]
     * @param search the search query
     * @param path the path where to search for the values (may be null)
     * @param tags the list of tags to append
     * @return a Completable that completes when the operation has finished
     */
    fun rxAppendTags(search: String, path: String, tags: List<String>): Completable {
        return Single.create(SingleOnSubscribeAdapter<Void> { f -> appendTags(search, path, tags, f) }).toCompletable()
    }

    override fun removeTags(search: String, path: String, tags: List<String>,
                            handler: Handler<AsyncResult<Void>>) {
        delegate.removeTags(search, path, tags, handler)
    }

    /**
     * Rx version of [.removeTags]
     * @param search the search query
     * @param path the path where to search for the values (may be null)
     * @param tags the list of tags to remove
     * @return a Completable that completes when the operation has finished
     */
    fun rxRemoveTags(search: String, path: String, tags: List<String>): Completable {
        return Single.create(SingleOnSubscribeAdapter<Void> { f -> removeTags(search, path, tags, f) }).toCompletable()
    }
}
