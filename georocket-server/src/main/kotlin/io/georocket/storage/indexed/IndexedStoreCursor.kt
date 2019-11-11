package io.georocket.storage.indexed

import io.georocket.storage.ChunkMeta
import io.georocket.storage.CursorInfo
import io.georocket.storage.StoreCursor
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx

/**
 * Implementation of [StoreCursor] for indexed chunk stores
 * @author Michel Kraemer
 */
class IndexedStoreCursor
/**
 * Create a cursor
 * @param vertx the Vert.x instance
 * @param search the search query
 * @param path the path where to perform the search (may be null if the
 * whole store should be searched)
 */
(
        /**
         * The Vert.x instance
         */
        private val vertx: Vertx,
        /**
         * The search query
         */
        private val search: String,
        /**
         * The path where to perform the search (may be null)
         */
        private val path: String) : StoreCursor {

    /**
     * This cursor use FrameCursor to load the full datastore frame by frame.
     */
    private var currentFrameCursor: StoreCursor? = null

    /**
     * The current read position
     */
    private var pos = -1

    /**
     * The total number of items requested from the store
     */
    private var totalHits: Long? = 0L

    /**
     * The scrollId for elasticsearch
     */
    private var scrollId: String? = null

    /**
     * Starts this cursor
     * @param handler will be called when the cursor has retrieved its first batch
     */
    fun start(handler: Handler<AsyncResult<StoreCursor>>) {
        FrameCursor(vertx, search, path, SIZE).start { h ->
            if (h.succeeded()) {
                handleFrameCursor(h.result())
                handler.handle(Future.succeededFuture(this))
            } else {
                handler.handle(Future.failedFuture(h.cause()))
            }
        }
    }

    private fun handleFrameCursor(framedCursor: StoreCursor) {
        currentFrameCursor = framedCursor
        val info = framedCursor.info
        this.totalHits = info.totalHits
        this.scrollId = info.scrollId
    }

    override fun hasNext(): Boolean {
        return pos + 1 < totalHits
    }

    override fun next(handler: Handler<AsyncResult<ChunkMeta>>) {
        ++pos
        if (pos >= totalHits) {
            handler.handle(Future.failedFuture(IndexOutOfBoundsException(
                    "Cursor is beyond a valid position.")))
        } else if (this.currentFrameCursor!!.hasNext()) {
            this.currentFrameCursor!!.next(handler)
        } else {
            FrameCursor(vertx, scrollId).start { h ->
                if (h.failed()) {
                    handler.handle(Future.failedFuture(h.cause()))
                } else {
                    handleFrameCursor(h.result())
                    this.currentFrameCursor!!.next(handler)
                }
            }
        }
    }

    override fun getChunkPath(): String {
        return this.currentFrameCursor!!.chunkPath
    }

    override fun getInfo(): CursorInfo {
        return this.currentFrameCursor!!.info
    }

    companion object {
        /**
         * The number of items retrieved in one batch
         */
        private val SIZE = 100
    }
}
