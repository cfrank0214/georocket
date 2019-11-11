package io.georocket.storage.indexed

import io.georocket.util.MimeTypeUtils.belongsTo

import io.georocket.constants.AddressConstants
import io.georocket.storage.ChunkMeta
import io.georocket.storage.CursorInfo
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.JsonChunkMeta
import io.georocket.storage.StoreCursor
import io.georocket.storage.XMLChunkMeta
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

/**
 * A cursor to run over a subset of data.
 * @author Andrej Sajenko
 */
class FrameCursor
/**
 * Load the next frame with a scrollId.
 * Use [.getInfo] to get the scrollId.
 * @param vertx vertx instance
 * @param scrollId scrollId to load the next frame
 */
(
        /**
         * The Vert.x instance
         */
        private val vertx: Vertx,
        /**
         * A scroll ID used by Elasticsearch for pagination
         */
        private var scrollId: String?) : StoreCursor {

    /**
     * The current read position in [.ids] and [.metas]
     *
     * INV: `pos + 1 < metas.length`
     */
    private var pos = -1

    /**
     * The search query
     */
    private var search: String = ""

    /**
     * The path where to perform the search (may be null)
     */
    private var path: String? = null

    /**
     * The size of elements to load in the frame.
     *
     * INV: `size == metas.length`
     */
    private var size: Int = 0

    /**
     * The total number of items the store has to offer.
     * If [.size] > totalHits then this frame cursor will
     * never load all items of the store.
     *
     * **INV: `SIZE <= totalHits`**
     */
    private var totalHits: Long = 0

    /**
     * The chunk IDs retrieved in the last batch
     */
    private var ids: Array<String>? = null

    /**
     * Chunk metadata retrieved in the last batch
     */
    private var metas: Array<ChunkMeta>? = null

    /**
     * Load the first frame of chunks.
     * @param vertx vertx instance
     * @param search The search query
     * @param path The search path
     * @param size The number of elements to load in this frame
     */
    constructor(vertx: Vertx, search: String, path: String, size: Int) : this(vertx, null) {
        this.search = search
        this.path = path
        this.size = size
    }

    /**
     * Starts this cursor
     * @param handler will be called when the cursor has retrieved its first batch
     */
    fun start(handler: (Any) -> Unit) {
        val queryMsg = JsonObject()

        if (scrollId != null) {
            queryMsg.put("scrollId", scrollId)
        } else {
            queryMsg
                    .put("size", size)
                    .put("search", search)
            if (path != null) {
                queryMsg.put("path", path)
            }
        }

        vertx.eventBus().send<JsonObject>(AddressConstants.INDEXER_QUERY, queryMsg) { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
            } else {
                handleResponse(ar.result().body())
                handler.handle(Future.succeededFuture(this))
            }
        }
    }

    override fun hasNext(): Boolean {
        return pos + 1 < metas!!.size
    }

    override fun next(handler: Handler<AsyncResult<ChunkMeta>>) {
        vertx.runOnContext { v ->
            ++pos
            if (pos >= metas!!.size) {
                handler.handle(Future.failedFuture(IndexOutOfBoundsException("Cursor out of bound.")))
            } else {
                handler.handle(Future.succeededFuture(metas!![pos]))
            }
        }
    }

    override fun getChunkPath(): String {
        check(pos >= 0) { "You have to call next() first" }
        return ids!![pos]
    }

    override fun getInfo(): CursorInfo {
        return CursorInfo(scrollId, totalHits, metas!!.size)
    }

    /**
     * Handle the response from the indexer.
     * @param body The indexer response body.
     */
    protected fun handleResponse(body: JsonObject) {
        totalHits = body.getLong("totalHits")!!
        scrollId = body.getString("scrollId")
        val hits = body.getJsonArray("hits")
        val count = hits.size()
        ids = arrayOfNulls(count)
        metas = arrayOfNulls(count)
        for (i in 0 until count) {
            val hit = hits.getJsonObject(i)
            ids[i] = hit.getString("id")
            metas[i] = createChunkMeta(hit)
        }
    }

    /**
     * Create a [XMLChunkMeta] object. Sub-classes may override this
     * method to provide their own [XMLChunkMeta] type.
     * @param hit the chunk meta content used to initialize the object
     * @return the created object
     */
    protected fun createChunkMeta(hit: JsonObject): ChunkMeta {
        val mimeType = hit.getString("mimeType", XMLChunkMeta.MIME_TYPE)
        if (belongsTo(mimeType, "application", "xml") || belongsTo(mimeType, "text", "xml")) {
            return XMLChunkMeta(hit)
        } else if (belongsTo(mimeType, "application", "geo+json")) {
            return GeoJsonChunkMeta(hit)
        } else if (belongsTo(mimeType, "application", "json")) {
            return JsonChunkMeta(hit)
        }
        return ChunkMeta(hit)
    }
}
