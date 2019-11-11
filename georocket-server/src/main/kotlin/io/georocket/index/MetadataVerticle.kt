package io.georocket.index

import com.google.common.collect.ImmutableList
import com.google.common.io.Resources
import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.index.elasticsearch.ElasticsearchClient
import io.georocket.index.elasticsearch.ElasticsearchClientFactory
import io.georocket.index.generic.DefaultMetaIndexerFactory
import io.georocket.query.DefaultQueryCompiler
import io.georocket.util.FilteredServiceLoader
import io.georocket.util.MapUtils
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.rxjava.core.AbstractVerticle
import rx.Completable
import rx.Single

import java.io.FileNotFoundException
import java.io.IOException
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.HashMap
import java.util.Objects
import java.util.concurrent.TimeoutException
import java.util.function.Function
import java.util.stream.Collectors

import io.georocket.util.ThrowableHelper.throwableToCode
import io.georocket.util.ThrowableHelper.throwableToMessage

/**
 * Generic methods for handling chunk metadata
 * @author Tim hellhake
 */
class MetadataVerticle : AbstractVerticle() {

    /**
     * The Elasticsearch client
     */
    private var client: ElasticsearchClient? = null

    /**
     * Compiles search strings to Elasticsearch documents
     */
    private var queryCompiler: DefaultQueryCompiler? = null

    /**
     * A list of [IndexerFactory] objects
     */
    private var indexerFactories: List<IndexerFactory>? = null

    override fun start(startFuture: Future<Void>) {
        // load and copy all indexer factories now and not lazily to avoid
        // concurrent modifications to the service loader's internal cache
        indexerFactories = ImmutableList.copyOf(FilteredServiceLoader.load(IndexerFactory::class.java))
        queryCompiler = createQueryCompiler()
        queryCompiler!!.setQueryCompilers(indexerFactories)

        ElasticsearchClientFactory(vertx).createElasticsearchClient(INDEX_NAME)
                .doOnSuccess { es -> client = es }
                .flatMapCompletable { v -> client!!.ensureIndex() }
                .andThen(Completable.defer { ensureMapping() })
                .subscribe({
                    registerMessageConsumers()
                    startFuture.complete()
                }, Action1<Throwable> { startFuture.fail(it) })
    }

    override fun stop() {
        client!!.close()
    }

    private fun createQueryCompiler(): DefaultQueryCompiler {
        val config = vertx.orCreateContext.config()
        val cls = config.getString(ConfigConstants.QUERY_COMPILER_CLASS,
                DefaultQueryCompiler::class.java.name)
        try {
            return Class.forName(cls).newInstance() as DefaultQueryCompiler
        } catch (e: ReflectiveOperationException) {
            throw RuntimeException("Could not create a DefaultQueryCompiler", e)
        }

    }

    private fun ensureMapping(): Completable {
        // merge mappings from all indexers
        val mappings = HashMap<String, Any>()
        indexerFactories!!.stream().filter { f -> f is DefaultMetaIndexerFactory }
                .forEach { factory -> MapUtils.deepMerge(mappings, factory.mapping) }
        indexerFactories!!.stream().filter { f -> f !is DefaultMetaIndexerFactory }
                .forEach { factory -> MapUtils.deepMerge(mappings, factory.mapping) }

        return client!!.putMapping(TYPE_NAME, JsonObject(mappings)).toCompletable()
    }

    /**
     * Register all message consumers for this verticle
     */
    private fun registerMessageConsumers() {
        register(AddressConstants.METADATA_GET_ATTRIBUTE_VALUES, Function<JsonObject, Single<JsonObject>> { this.onGetAttributeValues(it) })
        register(AddressConstants.METADATA_GET_PROPERTY_VALUES, Function<JsonObject, Single<JsonObject>> { this.onGetPropertyValues(it) })
        registerCompletable<Any>(AddressConstants.METADATA_SET_PROPERTIES, Function { this.onSetProperties(it) })
        registerCompletable<Any>(AddressConstants.METADATA_REMOVE_PROPERTIES, Function { this.onRemoveProperties(it) })
        registerCompletable<Any>(AddressConstants.METADATA_APPEND_TAGS, Function { this.onAppendTags(it) })
        registerCompletable<Any>(AddressConstants.METADATA_REMOVE_TAGS, Function { this.onRemoveTags(it) })
    }

    private fun <T> registerCompletable(address: String, mapper: Function<JsonObject, Completable>) {
        register<Int>(address, { obj -> mapper.apply(obj).toSingleDefault(0) })
    }

    private fun <T> register(address: String, mapper: Function<JsonObject, Single<T>>) {
        vertx.eventBus().consumer<JsonObject>(address)
                .toObservable()
                .subscribe { msg ->
                    mapper.apply(msg.body()).subscribe(Action1<T> { msg.reply(it) }, { err ->
                        log.error("Could not perform query", err)
                        msg.fail(throwableToCode(err), throwableToMessage(err, ""))
                    })
                }
    }

    private fun onGetAttributeValues(body: JsonObject): Single<JsonObject> {
        return onGetMap(body, "genAttrs", body.getString("attribute"))
    }

    private fun onGetPropertyValues(body: JsonObject): Single<JsonObject> {
        return onGetMap(body, "props", body.getString("property"))
    }

    private fun onGetMap(body: JsonObject, map: String, key: String): Single<JsonObject> {
        return executeQuery(body, "$map.$key")
                .map { result ->
                    val hits = result.getJsonObject("hits")

                    val resultHits = hits.getJsonArray("hits").stream()
                            .map { JsonObject::class.java.cast(it) }
                            .map { hit -> hit.getJsonObject("_source") }
                            .flatMap<Entry<String, Any>> { source -> source.getJsonObject(map, JsonObject()).stream() }
                            .filter { pair -> pair.key == key }
                            .map(Function<Entry<String, Any>, Any> { it.value })
                            .map { String::class.java.cast(it) }
                            .collect<List<String>, Any>(Collectors.toList())

                    JsonObject()
                            .put("hits", JsonArray(resultHits))
                            .put("totalHits", hits.getLong("total"))
                            .put("scrollId", result.getString("_scroll_id"))
                }
    }

    private fun executeQuery(body: JsonObject, keyExists: String): Single<JsonObject> {
        val search = body.getString("search")
        val path = body.getString("path")
        val scrollId = body.getString("scrollId")
        val parameters = JsonObject()
                .put("size", body.getInteger("pageSize", 100))
        val timeout = "1m" // one minute

        return if (scrollId == null) {
            try {
                // Execute a new search. Use a post_filter because we only want to get
                // a yes/no answer and no scoring (i.e. we only want to get matching
                // documents and not those that likely match). For the difference between
                // query and post_filter see the Elasticsearch documentation.
                val postFilter = queryCompiler!!.compileQuery(search, path, keyExists)
                client!!.beginScroll(TYPE_NAME, null, postFilter, parameters, timeout)
            } catch (t: Throwable) {
                Single.error(t)
            }

        } else {
            // continue searching
            client!!.continueScroll(scrollId, timeout)
        }
    }

    /**
     * Set properties of a list of chunks
     * @param body the message containing the search, path and the properties
     * @return a Completable that will complete when the properties have been set
     * successfully
     */
    private fun onSetProperties(body: JsonObject): Completable {
        val list = body.getJsonObject("properties")
        val params = JsonObject().put("properties", list)
        return updateMetadata(body, "set_properties.txt", params)
    }

    /**
     * Remove properties of a list of chunks
     * @param body the message containing the search, path and the properties
     * @return a Completable that will complete when the properties have been
     * deleted successfully
     */
    private fun onRemoveProperties(body: JsonObject): Completable {
        val list = body.getJsonArray("properties")
        val params = JsonObject().put("properties", list)
        return updateMetadata(body, "remove_properties.txt", params)
    }

    /**
     * Append tags to a list of chunks
     * @param body the message containing the search, path and the tags
     * @return a Completable that will complete when the tags have been set
     * successfully
     */
    private fun onAppendTags(body: JsonObject): Completable {
        val list = body.getJsonArray("tags")
        val params = JsonObject().put("tags", list)
        return updateMetadata(body, "append_tags.txt", params)
    }

    /**
     * Remove tags of a list of chunks
     * @param body the message containing the search, path and the tags
     * @return a Completable that will complete when the tags have been set
     * successfully
     */
    private fun onRemoveTags(body: JsonObject): Completable {
        val list = body.getJsonArray("tags")
        val params = JsonObject().put("tags", list)
        return updateMetadata(body, "remove_tags.txt", params)
    }

    /**
     * Update the meta data of existing chunks in the index. The chunks are
     * specified by a search query.
     * @param body the message containing the search and path
     * @param scriptName the name of the painscript file
     * @param params the parameters for the painscript
     * @return a Completable that will complete when the chunks have been updated
     * successfully
     */
    private fun updateMetadata(body: JsonObject, scriptName: String,
                               params: JsonObject): Completable {
        val search = body.getString("search", "")
        val path = body.getString("path", "")
        val postFilter = queryCompiler!!.compileQuery(search, path)

        val updateScript = JsonObject()
                .put("lang", "painless")

        try {
            updateScript.put("params", params)

            val url = javaClass.getResource(scriptName)
                    ?: throw FileNotFoundException("Script $scriptName does not exist")
            val script = Resources.toString(url, StandardCharsets.UTF_8)
            updateScript.put("inline", script)
            return updateDocuments(postFilter, updateScript)
        } catch (e: IOException) {
            return Completable.error(e)
        }

    }

    /**
     * Update a document using a painless script
     * @param postFilter the filter to select the documents
     * @param updateScript the script which should be applied to the documents
     * @return a Completable that completes if the update is successful or fails
     * if an error occurs
     */
    private fun updateDocuments(postFilter: JsonObject, updateScript: JsonObject): Completable {
        return client!!.updateByQuery(TYPE_NAME, postFilter, updateScript)
                .flatMapCompletable { sr ->
                    if (sr.getBoolean("timed_out", true)!!) {
                        return@client.updateByQuery(TYPE_NAME, postFilter, updateScript)
                                .flatMapCompletable Completable . error TimeoutException()
                    }
                    Completable.complete()
                }
    }

    companion object {
        private val log = LoggerFactory.getLogger(IndexerVerticle::class.java)

        /**
         * Elasticsearch index
         */
        private val INDEX_NAME = "georocket"

        /**
         * Type of documents stored in the Elasticsearch index
         */
        private val TYPE_NAME = "object"
    }
}
