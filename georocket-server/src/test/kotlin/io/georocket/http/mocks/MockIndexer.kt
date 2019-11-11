package io.georocket.http.mocks

import io.georocket.constants.AddressConstants
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.Vertx
import rx.Subscription

/**
 *
 * Mocks an indexer
 *
 * Consumes messages from [AddressConstants.INDEXER_QUERY] and returns hits.
 * The flow is the following:
 * If no scrollId is given in the request, returns [MockIndexer.HITS_PER_PAGE] items.
 * Also reply the scrollId [MockIndexer.FIRST_RETURNED_SCROLL_ID].
 *
 * If the scrollId [MockIndexer.FIRST_RETURNED_SCROLL_ID] is given, returns ([MockIndexer.TOTAL_HITS]- [MockIndexer.HITS_PER_PAGE]) items.
 * (This number IS smaller than [MockIndexer.HITS_PER_PAGE]!)
 * Also replies the scrollId [MockIndexer.INVALID_SCROLLID].
 *
 * If the scrollId [MockIndexer.INVALID_SCROLLID] is given, returns 0 items.
 * Also replies the scrollId [MockIndexer.INVALID_SCROLLID].
 *
 * So this MockIndexer simulates a query response that has 2 pages: one full and one not-full.
 * @author David Gengenbach
 */
object MockIndexer {
    /**
     * The number of hits per page
     */
    var HITS_PER_PAGE: Long? = 100L

    /**
     * The number of all hits to a given query
     */
    var TOTAL_HITS: Long? = HITS_PER_PAGE!! + 50

    /**
     * The scrollId that gets returned from the indexer after the first query with a "null" scrollId given
     */
    val FIRST_RETURNED_SCROLL_ID = "FIRST_SCROLL_ID"

    /**
     * The scrollId that gets returned when the FIRST_RETURNED_SCROLL_ID or "null" is given
     */
    val INVALID_SCROLLID = "THIS_MOCK_INDEXER_ONLY_HAS_TWO_PAGES_THIS_SCROLLID_IS_INVALID"

    private var indexerQuerySubscription: Subscription? = null

    /**
     * Unsubscribe if an indexer is registered
     */
    fun unsubscribeIndexer() {
        if (indexerQuerySubscription != null && !indexerQuerySubscription!!.isUnsubscribed) {
            indexerQuerySubscription!!.unsubscribe()
        }
        indexerQuerySubscription = null
    }

    /**
     * Start consuming [AddressConstants.INDEXER_QUERY] messages.
     * See the class comments to see the logic of the replied items.
     *
     * Returns "valid" hits that correspond to the items that are returned from the [MockStore].
     *
     * @param vertx vertx instance
     */
    fun mockIndexerQuery(vertx: Vertx) {
        indexerQuerySubscription = vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_QUERY).toObservable()
                .subscribe { msg ->
                    val hits = JsonArray()

                    val givenScrollId = msg.body().getString("scrollId")

                    val numberReturnHits: Long?
                    val returnScrollId: String
                    if (givenScrollId == null) {
                        numberReturnHits = HITS_PER_PAGE
                        returnScrollId = FIRST_RETURNED_SCROLL_ID
                    } else if (givenScrollId == FIRST_RETURNED_SCROLL_ID) {
                        numberReturnHits = TOTAL_HITS!! - HITS_PER_PAGE!!
                        returnScrollId = INVALID_SCROLLID
                    } else {
                        numberReturnHits = 0L
                        returnScrollId = INVALID_SCROLLID
                    }

                    for (i in 0 until numberReturnHits) {
                        hits.add(
                                JsonObject()
                                        .put("mimeType", "application/geo+json")
                                        .put("id", "some_id")
                                        .put("start", 0)
                                        .put("end", MockStore.RETURNED_CHUNK.length)
                                        .put("parents", JsonArray())
                        )
                    }

                    if (INVALID_SCROLLID == givenScrollId) {
                        msg.fail(404, "invalid scroll id")
                    } else {
                        msg.reply(
                                JsonObject()
                                        .put("totalHits", TOTAL_HITS)
                                        .put("scrollId", returnScrollId)
                                        .put("hits", hits)
                        )
                    }
                }
    }

}
