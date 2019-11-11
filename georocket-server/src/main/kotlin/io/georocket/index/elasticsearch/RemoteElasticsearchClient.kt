package io.georocket.index.elasticsearch

import io.georocket.util.HttpException
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import org.apache.commons.collections.CollectionUtils
import org.jooq.lambda.tuple.Tuple2
import rx.Completable
import rx.Single

import java.net.URI
import java.time.Duration
import java.util.ArrayList

/**
 * An Elasticsearch client using the HTTP API
 * @author Michel Kraemer
 */
class RemoteElasticsearchClient
/**
 * Connect to an Elasticsearch instance
 * @param hosts the hosts to connect to
 * @param index the index to query against
 * @param autoUpdateHostsInterval interval of a periodic timer that
 * automatically updates the list of hosts to connect to (may be `null`
 * if the list of hosts should never be updated)
 * @param compressRequestBodies `true` if bodies of HTTP requests
 * should be compressed with GZIP
 * @param vertx a Vert.x instance
 */
(hosts: List<URI>,
 /**
  * The index to query against
  */
 private val index: String,
 autoUpdateHostsInterval: Duration?, compressRequestBodies: Boolean,
 /**
  * The vertx instance
  */
 private val vertx: Vertx) : ElasticsearchClient {

    /**
     * The HTTP client used to talk to Elasticsearch
     */
    private val client: LoadBalancingHttpClient

    /**
     * The ID of the periodic timer that automatically updates the hosts
     * to connect to
     */
    private var autoUpdateHostsTimerId: Long = -1

    override val isRunning: Single<Boolean>
        get() = client.performRequestNoRetry(HttpMethod.HEAD, "/", null)
                .map { v -> true }.onErrorReturn { t -> false }

    init {
        client = LoadBalancingHttpClient(vertx, compressRequestBodies)
        client.hosts = hosts

        if (autoUpdateHostsInterval != null) {
            autoUpdateHostsTimerId = vertx.setPeriodic(
                    autoUpdateHostsInterval.toMillis()) { id -> updateHosts() }
        }
    }

    override fun close() {
        client.close()
        if (autoUpdateHostsTimerId != -1) {
            vertx.cancelTimer(autoUpdateHostsTimerId)
        }
    }

    /**
     * Asynchronously update the list of Elasticsearch hosts
     */
    private fun updateHosts() {
        client.performRequest("/_nodes/http").subscribe({ response ->
            val nodes = response.getJsonObject("nodes")

            val hosts = ArrayList<URI>()
            for (nodeId in nodes.fieldNames()) {
                val node = nodes.getJsonObject(nodeId)
                val http = node.getJsonObject("http")
                        ?: // ignore this host. it does not have HTTP enabled
                        continue

                val publishAddress = http.getString("publish_address")
                        ?: // ignore this host. it does not have a publish address
                        continue

                val uri = URI.create("http://$publishAddress")
                hosts.add(uri)
            }

            if (hosts.isEmpty()) {
                log.warn("Retrieved empty list of hosts from Elasticsearch")
            } else {
                if (!CollectionUtils.isEqualCollection(hosts, client.hosts)) {
                    log.info("Updated list of Elasticsearch hosts: $hosts")
                }

                client.hosts = hosts
            }
        }, { err -> log.error("Could not update list of Elasticsearch hosts", err) }
        )
    }

    override fun bulkInsert(type: String,
                            documents: List<Tuple2<String, JsonObject>>): Single<JsonObject> {
        val uri = "/$index/$type/_bulk"

        // prepare the whole body now because it's much faster to send
        // it at once instead of using HTTP chunked mode.
        val body = Buffer.buffer()
        for (e in documents) {
            val id = e.v1
            val source = e.v2.toBuffer()
            val subject = JsonObject().put("_id", id)
            body.appendString("{\"index\":")
                    .appendBuffer(subject.toBuffer())
                    .appendString("}\n")
                    .appendBuffer(source)
                    .appendString("\n")
        }

        return client.performRequest(HttpMethod.POST, uri, body)
    }

    override fun beginScroll(type: String, query: JsonObject?,
                             postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject, timeout: String): Single<JsonObject> {
        var uri = "/$index/$type/_search"
        uri += "?scroll=$timeout"

        val source = JsonObject()
        parameters.forEach { entry -> source.put(entry.key, entry.value) }

        if (query != null) {
            source.put("query", query)
        }
        if (postFilter != null) {
            source.put("post_filter", postFilter)
        }
        if (aggregations != null) {
            source.put("aggs", aggregations)
        }

        // sort by doc (fastest way to scroll)
        source.put("sort", JsonArray().add("_doc"))

        return client.performRequest(HttpMethod.GET, uri, source.toBuffer())
    }

    override fun continueScroll(scrollId: String, timeout: String): Single<JsonObject> {
        val uri = "/_search/scroll"

        val source = JsonObject()
        source.put("scroll", timeout)
        source.put("scroll_id", scrollId)

        return client.performRequest(HttpMethod.GET, uri, source.toBuffer())
    }

    override fun search(type: String, query: JsonObject?,
                        postFilter: JsonObject?, aggregations: JsonObject?, parameters: JsonObject): Single<JsonObject> {
        val uri = "/$index/$type/_search"

        val source = JsonObject()
        parameters.forEach { entry -> source.put(entry.key, entry.value) }

        if (query != null) {
            source.put("query", query)
        }
        if (postFilter != null) {
            source.put("post_filter", postFilter)
        }
        if (aggregations != null) {
            source.put("aggs", aggregations)
        }

        return client.performRequest(HttpMethod.GET, uri, source.toBuffer())
    }

    override fun count(type: String, query: JsonObject?): Single<Long> {
        val uri = "/$index/$type/_count"

        val source = JsonObject()
        if (query != null) {
            source.put("query", query)
        }

        return client.performRequest(HttpMethod.GET, uri, source.toBuffer())
                .flatMap { sr ->
                    val l = sr.getLong("count")
                    if (l == null) {
                        return@client.performRequest(HttpMethod.GET, uri, source.toBuffer())
                                .flatMap Single . error < Long >(NoStackTraceThrowable(
                                "Could not count documents"))
                    }
                    Single.just(l)
                }
    }

    override fun updateByQuery(type: String, postFilter: JsonObject?,
                               script: JsonObject?): Single<JsonObject> {
        val uri = "/$index/$type/_update_by_query"

        val source = JsonObject()
        if (postFilter != null) {
            source.put("post_filter", postFilter)
        }
        if (script != null) {
            source.put("script", script)
        }

        return client.performRequest(HttpMethod.POST, uri, source.toBuffer())
    }

    override fun bulkDelete(type: String, ids: JsonArray): Single<JsonObject> {
        val uri = "/$index/$type/_bulk"

        // prepare the whole body now because it's much faster to send
        // it at once instead of using HTTP chunked mode.
        val body = Buffer.buffer()
        for (i in 0 until ids.size()) {
            val id = ids.getString(i)
            val subject = JsonObject().put("_id", id)
            body.appendString("{\"delete\":")
                    .appendBuffer(subject.toBuffer())
                    .appendString("}\n")
        }

        return client.performRequest(HttpMethod.POST, uri, body)
    }

    override fun indexExists(): Single<Boolean> {
        return exists("/$index")
    }

    override fun typeExists(type: String): Single<Boolean> {
        return exists("/$index/_mapping/$type")
    }

    /**
     * Check if the given URI exists by sending an empty request
     * @param uri uri to check
     * @return an observable emitting `true` if the request
     * was successful or `false` otherwise
     */
    private fun exists(uri: String): Single<Boolean> {
        return client.performRequest(HttpMethod.HEAD, uri)
                .map { o -> true }
                .onErrorResumeNext { t ->
                    if (t is HttpException && t.statusCode == 404) {
                        return@client.performRequest(HttpMethod.HEAD, uri)
                                .map(o -> true)
                        .onErrorResumeNext Single . just < Boolean >(false)
                    }
                    Single.error(t)
                }
    }

    override fun createIndex(): Single<Boolean> {
        return createIndex(null)
    }

    override fun createIndex(settings: JsonObject?): Single<Boolean> {
        val uri = "/$index"

        val body = if (settings == null)
            null
        else
            JsonObject().put("settings", settings).toBuffer()

        return client.performRequest(HttpMethod.PUT, uri, body)
                .map { res -> res.getBoolean("acknowledged", true) }
    }

    /**
     * Ensure the Elasticsearch index exists
     * @return an observable that will emit a single item when the index has
     * been created or if it already exists
     */
    override fun ensureIndex(): Completable {
        // check if the index exists
        return indexExists().flatMapCompletable { exists ->
            if (exists!!) {
                return@indexExists ().flatMapCompletable Completable . complete ()
            } else {
                // index does not exist. create it.
                return@indexExists ().flatMapCompletable createIndex ().flatMapCompletable({ ack ->
                    if (ack!!) {
                        return@createIndex ().flatMapCompletable Completable . complete ()
                    }
                    Completable.error(NoStackTraceThrowable("Index creation " + "was not acknowledged by Elasticsearch"))
                })
            }
        }
    }

    override fun putMapping(type: String, mapping: JsonObject): Single<Boolean> {
        val uri = "/$index/_mapping/$type"
        return client.performRequest(HttpMethod.PUT, uri, mapping.toBuffer())
                .map { res -> res.getBoolean("acknowledged", true) }
    }

    /**
     * Ensure the Elasticsearch mapping exists
     * @param type the target type for the mapping
     * @return an observable that will emit a single item when the mapping has
     * been created or if it already exists
     */
    override fun ensureMapping(type: String, mapping: JsonObject): Completable {
        // check if the target type exists
        return typeExists(type).flatMapCompletable { exists ->
            if (exists!!) {
                return@typeExists type.flatMapCompletable Completable . complete ()
            } else {
                // target type does not exist. create the mapping.
                return@typeExists type.flatMapCompletable putMapping type, mapping).flatMapCompletable({ ack ->
                    if (ack!!) {
                        return@putMapping type, mapping).flatMapCompletable Completable.complete()
                    }
                    Completable.error(NoStackTraceThrowable("Mapping creation " + "was not acknowledged by Elasticsearch"))
                })
            }
        }
    }

    override fun getMapping(type: String): Single<JsonObject> {
        return getMapping(type, null)
    }

    override fun getMapping(type: String, field: String?): Single<JsonObject> {
        var uri = "/$index/_mapping/$type"
        if (field != null) {
            uri += "/field/$field"
        }
        return client.performRequest(HttpMethod.GET, uri)
    }

    companion object {
        private val log = LoggerFactory.getLogger(RemoteElasticsearchClient::class.java)
    }
}
