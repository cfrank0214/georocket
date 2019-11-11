package io.georocket.http.mocks

import java.util.Queue

import io.georocket.storage.ChunkMeta
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.IndexMeta
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer

/**
 * Mock for the GeoRocket indexed store
 * @author David Gengenbach
 */
class MockStore
/**
 * Standard constructor
 * @param vertx vertx instance
 */
(vertx: Vertx) : IndexedStore(vertx) {

    override fun getOne(path: String, handler: Handler<AsyncResult<ChunkReadStream>>) {
        val chunk = Buffer.buffer(RETURNED_CHUNK)
        handler.handle(Future.succeededFuture(DelegateChunkReadStream(chunk)))
    }

    override fun delete(search: String, path: String, handler: Handler<AsyncResult<Void>>) {
        notImplemented(handler)
    }

    override fun doAddChunk(chunk: String, path: String, correlationId: String,
                            handler: (Any) -> Unit) {
        notImplemented(handler)
    }

    override fun doDeleteChunks(paths: Queue<String>, handler: Handler<AsyncResult<Void>>) {
        notImplemented(handler)
    }

    override fun add(chunk: String, chunkMeta: ChunkMeta, path: String, indexMeta: IndexMeta,
                     handler: Handler<AsyncResult<Void>>) {
        notImplemented(handler)
    }

    private fun <T> notImplemented(handler: Handler<AsyncResult<T>>) {
        handler.handle(Future.failedFuture("NOT IMPLEMENTED"))
    }

    companion object {
        internal val RETURNED_CHUNK = "{\"type\":\"Polygon\"}"
    }
}

