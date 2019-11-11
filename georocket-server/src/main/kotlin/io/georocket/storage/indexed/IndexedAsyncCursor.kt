package io.georocket.storage.indexed

import io.georocket.storage.AsyncCursor
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Implementation of [AsyncCursor] for [IndexedStore]
 * @author Tim Hellhake
 * @param <T> type of the item
</T> */
class IndexedAsyncCursor<T>
/**
 * Create a cursor
 * @param itemDecoder a function which knows how to decode the items
 * @param address The eventbus address of the item type
 * @param vertx the Vert.x instance
 * @param template a template of the message which should be used to query items
 * @param pageSize the number of items retrieved in one batch
 */
@JvmOverloads constructor(
        /**
         * A function which knows how to decode the items
         */
        private val itemDecoder: Function<Any, T>,
        /**
         * The eventbus address of the item type
         */
        private val address: String,
        /**
         * The Vert.x instance
         */
        private val vertx: Vertx,
        /**
         * A template of the message which should be used to query items
         */
        private val template: JsonObject,
        /**
         * The number of items retrieved in one batch
         */
        private val pageSize: Int = PAGE_SIZE) : AsyncCursor<T> {

    /**
     * The number of items retrieved from the store
     */
    private var count: Long = 0

    /**
     * The current read position in [.items]
     */
    private var pos = -1

    /**
     * The total number of items requested from the store
     */
    private var size: Long = 0

    /**
     * A scroll ID used by Elasticsearch for pagination
     */
    private var scrollId: String? = null

    /**
     * Items retrieved in the last batch
     */
    private var items: List<T>? = null

    /**
     * Starts this cursor
     * @param handler will be called when the cursor has retrieved its first batch
     */
    fun start(handler: Handler<AsyncResult<AsyncCursor<T>>>) {
        val queryMsg = template.copy()
                .put("pageSize", pageSize)
        vertx.eventBus().send<JsonObject>(address, queryMsg) { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
            } else {
                handleResponse(ar.result().body())
                handler.handle(Future.succeededFuture(this))
            }
        }
    }

    /**
     * Handle the response from the verticle and set the items list
     * @param body the response from the indexer
     */
    private fun handleResponse(body: JsonObject) {
        size = body.getLong("totalHits")!!
        scrollId = body.getString("scrollId")
        val hits = body.getJsonArray("hits")
        items = hits.stream()
                .map(itemDecoder)
                .collect<List<T>, Any>(Collectors.toList())
    }

    override fun hasNext(): Boolean {
        return count < size
    }

    override fun next(handler: Handler<AsyncResult<T>>) {
        ++count
        ++pos
        if (pos >= items!!.size) {
            val queryMsg = template.copy()
                    .put("pageSize", pageSize)
                    .put("scrollId", scrollId)
            vertx.eventBus().send<JsonObject>(address, queryMsg) { ar ->
                if (ar.failed()) {
                    handler.handle(Future.failedFuture(ar.cause()))
                } else {
                    handleResponse(ar.result().body())
                    pos = 0
                    handler.handle(Future.succeededFuture(items!![pos]))
                }
            }
        } else {
            handler.handle(Future.succeededFuture(items!![pos]))
        }
    }

    companion object {
        /**
         * The number of items retrieved in one batch
         */
        private val PAGE_SIZE = 100
    }
}
/**
 * Create a cursor
 * @param itemDecoder a function which knows how to decode the items
 * @param address The eventbus address of the item type
 * @param vertx the Vert.x instance
 * @param template a template of the message which should be used to query items
 */
