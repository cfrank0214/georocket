package io.georocket.index.elasticsearch

import org.jooq.lambda.tuple.Tuple2

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import rx.Completable
import rx.Single

/**
 * A client for embedded Elasticsearch instances. Will shut down the embedded
 * instance when the client is closed.
 * @author Michel Kraemer
 */
class EmbeddedElasticsearchClient
/**
 * Wrap around an existing [ElasticsearchClient] instance
 * @param delegate the client to wrap around
 * @param runner the Elasticsearch instance to stop when the client is closed
 */
(private val delegate: ElasticsearchClient,
 private val runner: ElasticsearchRunner) : ElasticsearchClient {

    override val isRunning: Single<Boolean>
        get() = delegate.isRunning

    override fun close() {
        delegate.close()
        runner.stop()
    }

    override fun bulkInsert(type: String,
                            documents: List<Tuple2<String, JsonObject>>): Single<JsonObject> {
        return delegate.bulkInsert(type, documents)
    }

    override fun beginScroll(type: String, query: JsonObject,
                             postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject, timeout: String): Single<JsonObject> {
        return delegate.beginScroll(type, query, postFilter, aggregations, parameters, timeout)
    }

    override fun continueScroll(scrollId: String, timeout: String): Single<JsonObject> {
        return delegate.continueScroll(scrollId, timeout)
    }

    override fun search(type: String, query: JsonObject,
                        postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject): Single<JsonObject> {
        return delegate.search(type, query, postFilter, aggregations, parameters)
    }

    override fun count(type: String, query: JsonObject): Single<Long> {
        return delegate.count(type, query)
    }

    override fun updateByQuery(type: String, postFilter: JsonObject,
                               script: JsonObject): Single<JsonObject> {
        return delegate.updateByQuery(type, postFilter, script)
    }

    override fun bulkDelete(type: String, ids: JsonArray): Single<JsonObject> {
        return delegate.bulkDelete(type, ids)
    }

    override fun indexExists(): Single<Boolean> {
        return delegate.indexExists()
    }

    override fun typeExists(type: String): Single<Boolean> {
        return delegate.typeExists(type)
    }

    override fun createIndex(): Single<Boolean> {
        return delegate.createIndex()
    }

    override fun createIndex(settings: JsonObject): Single<Boolean> {
        return delegate.createIndex(settings)
    }

    override fun ensureIndex(): Completable {
        return delegate.ensureIndex()
    }

    override fun putMapping(type: String, mapping: JsonObject): Single<Boolean> {
        return delegate.putMapping(type, mapping)
    }

    override fun getMapping(type: String): Single<JsonObject> {
        return delegate.getMapping(type)
    }

    override fun getMapping(type: String, field: String): Single<JsonObject> {
        return delegate.getMapping(type, field)
    }

    override fun ensureMapping(type: String, mapping: JsonObject): Completable {
        return delegate.ensureMapping(type, mapping)
    }
}
