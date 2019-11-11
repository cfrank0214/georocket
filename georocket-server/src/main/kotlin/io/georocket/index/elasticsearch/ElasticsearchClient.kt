package io.georocket.index.elasticsearch

import org.jooq.lambda.tuple.Tuple2

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import rx.Completable
import rx.Single

/**
 * An Elasticsearch client
 * @author Michel Kraemer
 */
interface ElasticsearchClient {

    /**
     * Check if Elasticsearch is running and if it answers to a simple request
     * @return `true` if Elasticsearch is running, `false`
     * otherwise
     */
    val isRunning: Single<Boolean>

    /**
     * Close the client and release all resources
     */
    fun close()

    /**
     * Insert a number of documents in one bulk request
     * @param type the type of the documents to insert
     * @param documents a list of document IDs and actual documents to insert
     * @return the parsed bulk response from the server
     * @see .bulkResponseHasErrors
     * @see .bulkResponseGetErrorMessage
     */
    fun bulkInsert(type: String,
                   documents: List<Tuple2<String, JsonObject>>): Single<JsonObject>

    /**
     * Perform a search and start scrolling over the result documents
     * @param type the type of the documents to search
     * @param query the query to send
     * @param parameters the elasticsearch parameters
     * @param timeout the time after which the returned scroll id becomes invalid
     * @return an object containing the search hits and a scroll id that can
     * be passed to [.continueScroll] to get more results
     */
    fun beginScroll(type: String, query: JsonObject,
                    parameters: JsonObject, timeout: String): Single<JsonObject> {
        return beginScroll(type, query, null, parameters, timeout)
    }

    /**
     * Perform a search and start scrolling over the result documents. You can
     * either specify a `query`, a `postFilter` or both,
     * but one of them is required.
     * @param type the type of the documents to search
     * @param query the query to send (may be `null`, in this case
     * `postFilter` must be set)
     * @param postFilter a filter to apply (may be `null`, in this case
     * `query` must be set)
     * @param parameters the elasticsearch parameters
     * @param timeout the time after which the returned scroll id becomes invalid
     * @return an object containing the search hits and a scroll id that can
     * be passed to [.continueScroll] to get more results
     */
    fun beginScroll(type: String, query: JsonObject,
                    postFilter: JsonObject?, parameters: JsonObject, timeout: String): Single<JsonObject> {
        return beginScroll(type, query, postFilter, null, parameters, timeout)
    }

    /**
     * Perform a search, apply an aggregation, and start scrolling over the
     * result documents. You can either specify a `query`, a
     * `postFilter` or both, but one of them is required.
     * @param type the type of the documents to search
     * @param query the query to send (may be `null`, in this case
     * `postFilter` must be set)
     * @param postFilter a filter to apply (may be `null`, in this case
     * `query` must be set)
     * @param aggregations the aggregations to apply. Can be `null`
     * @param parameters the elasticsearch parameters
     * @param timeout the time after which the returned scroll id becomes invalid
     * @return an object containing the search hits and a scroll id that can
     * be passed to [.continueScroll] to get more results
     */
    fun beginScroll(type: String, query: JsonObject,
                    postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject, timeout: String): Single<JsonObject>

    /**
     * Continue scrolling through search results. Call
     * [.beginScroll] to get a scroll id
     * @param scrollId the scroll id
     * @param timeout the time after which the scroll id becomes invalid
     * @return an object containing new search hits and possibly a new scroll id
     */
    fun continueScroll(scrollId: String, timeout: String): Single<JsonObject>

    /**
     * Perform a search. The result set might not contain all documents. If you
     * want to scroll over all results use
     * [.beginScroll]
     * instead.
     * @param type the type of the documents to search
     * @param query the query to send
     * @param parameters the elasticsearch parameters
     * @return an object containing the search result as returned from Elasticsearch
     */
    fun search(type: String, query: JsonObject, parameters: JsonObject): Single<JsonObject> {
        return search(type, query, null, null, parameters)
    }

    /**
     * Perform a search. The result set might not contain all documents. If you
     * want to scroll over all results use
     * [.beginScroll]
     * instead.
     * @param type the type of the documents to search
     * @param query the query to send (may be `null`)
     * @param postFilter a filter to apply (may be `null`)
     * @param parameters the elasticsearch parameters
     * @return an object containing the search result as returned from Elasticsearch
     */
    fun search(type: String, query: JsonObject,
               postFilter: JsonObject, parameters: JsonObject): Single<JsonObject> {
        return search(type, query, postFilter, null, parameters)
    }

    /**
     * Perform a search. The result set might not contain all documents. If you
     * want to scroll over all results use
     * [.beginScroll]
     * instead.
     * @param type the type of the documents to search
     * @param query the query to send (may be `null`)
     * @param postFilter a filter to apply (may be `null`)
     * @param aggregations the aggregations to apply (may be `null`)
     * @param parameters the elasticsearch parameters
     * @return an object containing the search result as returned from Elasticsearch
     */
    fun search(type: String, query: JsonObject,
               postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject): Single<JsonObject>

    /**
     * Perform a count operation. The result is the number of documents
     * matching the query (without the documents themselves). If no query
     * is given, the total number of documents (of this type) is returned.
     * @param type the type of the documents to count
     * @param query the query to send (may be `null`)
     * @return the number of documents matching the query
     */
    fun count(type: String, query: JsonObject): Single<Long>

    /**
     * Perform an update operation. The update script is applied to all
     * documents that match the post filter.
     * @param type the type of the documents to update
     * @param postFilter a filter to apply (may be `null`)
     * @param script the update script to apply
     * @return an object containing the search result as returned from Elasticsearch
     */
    fun updateByQuery(type: String, postFilter: JsonObject,
                      script: JsonObject): Single<JsonObject>

    /**
     * Delete a number of documents in one bulk request
     * @param type the type of the documents to delete
     * @param ids the IDs of the documents to delete
     * @return the parsed bulk response from the server
     * @see .bulkResponseHasErrors
     * @see .bulkResponseGetErrorMessage
     */
    fun bulkDelete(type: String, ids: JsonArray): Single<JsonObject>

    /**
     * Check if the index exists
     * @return a single emitting `true` if the index exists or
     * `false` otherwise
     */
    fun indexExists(): Single<Boolean>

    /**
     * Check if the type of the index exists
     * @param type the type
     * @return a single emitting `true` if the type of
     * the index exists or `false` otherwise
     */
    fun typeExists(type: String): Single<Boolean>

    /**
     * Create the index
     * @return a single emitting `true` if the index creation
     * was acknowledged by Elasticsearch, `false` otherwise
     */
    fun createIndex(): Single<Boolean>

    /**
     * Create the index with settings.
     * @param settings the settings to set for the index.
     * @return a single emitting `true` if the index creation
     * was acknowledged by Elasticsearch, `false` otherwise
     */
    fun createIndex(settings: JsonObject): Single<Boolean>

    /**
     * Convenience method that makes sure the index exists. It first calls
     * [.indexExists] and then [.createIndex] if the index does
     * not exist yet.
     * @return a single that will emit a single item when the index has
     * been created or if it already exists
     */
    fun ensureIndex(): Completable

    /**
     * Add mapping for the given type
     * @param type the type
     * @param mapping the mapping to set for the type
     * @return a single emitting `true` if the operation
     * was acknowledged by Elasticsearch, `false` otherwise
     */
    fun putMapping(type: String, mapping: JsonObject): Single<Boolean>

    /**
     * Convenience method that makes sure the given mapping exists. It first calls
     * [.typeExists] and then [.putMapping]
     * if the mapping does not exist yet.
     * @param type the target type for the mapping
     * @param mapping the mapping to set for the type
     * @return a single that will emit a single item when the mapping has
     * been created or if it already exists
     */
    fun ensureMapping(type: String, mapping: JsonObject): Completable

    /**
     * Get mapping for the given type
     * @param type the type
     * @return the parsed mapping response from the server
     */
    fun getMapping(type: String): Single<JsonObject>

    /**
     * Get mapping for the given type
     * @param type the type
     * @param field the field
     * @return the parsed mapping response from the server
     */
    fun getMapping(type: String, field: String): Single<JsonObject>

    /**
     * Check if the given bulk response contains errors
     * @param response the bulk response
     * @return `true` if the response has errors, `false`
     * otherwise
     * @see .bulkInsert
     * @see .bulkDelete
     * @see .bulkResponseGetErrorMessage
     */
    fun bulkResponseHasErrors(response: JsonObject): Boolean {
        return response.getBoolean("errors", false)!!
    }

    /**
     * Builds an error message from a bulk response containing errors
     * @param response the response containing errors
     * @return the error message or null if the bulk response does not contain
     * errors
     * @see .bulkInsert
     * @see .bulkDelete
     * @see .bulkResponseHasErrors
     */
    fun bulkResponseGetErrorMessage(response: JsonObject): String? {
        if (!bulkResponseHasErrors(response)) {
            return null
        }

        val res = StringBuilder()
        res.append("Errors in bulk operation:")
        val items = response.getJsonArray("items", JsonArray())
        for (o in items) {
            val jo = o as JsonObject
            for (key in jo.fieldNames()) {
                val op = jo.getJsonObject(key)
                if (bulkResponseItemHasErrors(op)) {
                    res.append(bulkResponseItemGetErrorMessage(op))
                }
            }
        }
        return res.toString()
    }

    /**
     * Check if an item of a bulk response has an error
     * @param item the item
     * @return `true` if the item has an error, `false`
     * otherwise
     */
    fun bulkResponseItemHasErrors(item: JsonObject): Boolean {
        return item.getJsonObject("error") != null
    }

    /**
     * Builds an error message from a bulk response item
     * @param item the item
     * @return the error message or null if the bulk response item does not
     * contain an error
     */
    fun bulkResponseItemGetErrorMessage(item: JsonObject): String? {
        if (!bulkResponseItemHasErrors(item)) {
            return null
        }

        val res = StringBuilder()
        val error = item.getJsonObject("error")
        if (error != null) {
            val id = item.getString("_id")
            val type = error.getString("type")
            val reason = error.getString("reason")
            res.append("\n[id: [")
            res.append(id)
            res.append("], type: [")
            res.append(type)
            res.append("], reason: [")
            res.append(reason)
            res.append("]]")
        }

        return res.toString()
    }
}
