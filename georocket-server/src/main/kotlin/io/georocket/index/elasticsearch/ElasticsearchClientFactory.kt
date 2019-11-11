package io.georocket.index.elasticsearch

import io.georocket.constants.ConfigConstants
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.rxjava.core.Vertx
import org.apache.commons.io.IOUtils
import rx.Single

import java.io.IOException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.stream.Collectors

/**
 * A factory for [ElasticsearchClient] instances. Either starts an
 * Elasticsearch instance or connects to an external one - depending on the
 * configuration.
 * @author Michel Kraemer
 */
class ElasticsearchClientFactory
/**
 * Construct the factory
 * @param vertx the current Vert.x instance
 */
(private val vertx: Vertx) {

    /**
     * Create an Elasticsearch client. Either start an Elasticsearch instance or
     * connect to an external one - depending on the configuration.
     * @param indexName the name of the index the Elasticsearch client will
     * operate on
     * @return a single emitting an Elasticsearch client and runner
     */
    fun createElasticsearchClient(indexName: String): Single<ElasticsearchClient> {
        val config = vertx.orCreateContext.config()

        val embedded = config.getBoolean(ConfigConstants.INDEX_ELASTICSEARCH_EMBEDDED, true)!!

        if (config.containsKey(ConfigConstants.INDEX_ELASTICSEARCH_HOST)) {
            log.warn("The configuration item `" + ConfigConstants.INDEX_ELASTICSEARCH_HOST
                    + "' is deprecated and will be removed in a future release. Please use `"
                    + ConfigConstants.INDEX_ELASTICSEARCH_HOSTS + "' instead.")
        }
        if (config.containsKey(ConfigConstants.INDEX_ELASTICSEARCH_PORT)) {
            log.warn("The configuration item `" + ConfigConstants.INDEX_ELASTICSEARCH_PORT
                    + "' is deprecated and will be removed in a future release. Please use `"
                    + ConfigConstants.INDEX_ELASTICSEARCH_HOSTS + "' instead.")
        }

        var hosts: JsonArray? = config.getJsonArray(ConfigConstants.INDEX_ELASTICSEARCH_HOSTS)
        if (hosts == null || hosts.isEmpty) {
            val host = config.getString(ConfigConstants.INDEX_ELASTICSEARCH_HOST, "localhost")
            val port = config.getInteger(ConfigConstants.INDEX_ELASTICSEARCH_PORT, 9200)!!
            log.warn("Configuration item `" + ConfigConstants.INDEX_ELASTICSEARCH_HOSTS
                    + "' not set. Using " + host + ":" + port)
            hosts = JsonArray().add("$host:$port")
        }

        if (embedded && hosts!!.size() > 1) {
            log.warn("There are more than one Elasticsearch hosts configured in `"
                    + ConfigConstants.INDEX_ELASTICSEARCH_HOSTS + "' but embedded mode is "
                    + "enabled. Only the first host will be considered.")
            hosts = JsonArray().add(hosts.getString(0))
        }

        val uris = hosts!!.stream()
                .map { h -> URI.create("http://$h") }
                .collect<List<URI>, Any>(Collectors.toList())

        val autoUpdateHostsIntervalSeconds = config.getLong(
                ConfigConstants.INDEX_ELASTICSEARCH_AUTO_UPDATE_HOSTS_INTERVAL_SECONDS, -1L)!!
        var autoUpdateHostsInterval: Duration? = null
        if (!embedded && autoUpdateHostsIntervalSeconds > 0) {
            autoUpdateHostsInterval = Duration.ofSeconds(autoUpdateHostsIntervalSeconds)
        }

        val compressRequestBodies = config.getBoolean(
                ConfigConstants.INDEX_ELASTICSEARCH_COMPRESS_REQUEST_BODIES, false)!!

        val client = RemoteElasticsearchClient(uris, indexName,
                autoUpdateHostsInterval, compressRequestBodies, vertx.delegate)

        return if (!embedded) {
            // just return the client
            Single.just(client)
        } else client.isRunning.flatMap { running ->
            if (running!!) {
                // we don't have to start Elasticsearch again
                return@client.isRunning().flatMap Single . just < ElasticsearchClient >(client)
            }

            val home = config.getString(ConfigConstants.HOME)

            val defaultElasticsearchDownloadUrl: String
            try {
                defaultElasticsearchDownloadUrl = IOUtils.toString(javaClass.getResource(
                        "/elasticsearch_download_url.txt"), StandardCharsets.UTF_8)
            } catch (e: IOException) {
                return@client.isRunning().flatMap Single . error < ElasticsearchClient >(e)
            }

            val elasticsearchDownloadUrl = config.getString(
                    ConfigConstants.INDEX_ELASTICSEARCH_DOWNLOAD_URL, defaultElasticsearchDownloadUrl)

            val pattern = Pattern.compile("-([0-9]\\.[0-9]\\.[0-9])\\.zip$")
            val matcher = pattern.matcher(elasticsearchDownloadUrl)
            if (!matcher.find()) {
                return@client.isRunning().flatMap Single . error < ElasticsearchClient >(NoStackTraceThrowable("Could not extract "
                        + "version number from Elasticsearch download URL: "
                        + elasticsearchDownloadUrl))
            }
            val elasticsearchVersion = matcher.group(1)

            val elasticsearchInstallPath = config.getString(
                    ConfigConstants.INDEX_ELASTICSEARCH_INSTALL_PATH,
                    "$home/elasticsearch/$elasticsearchVersion")

            val host = uris[0].host
            val port = uris[0].port

            // install Elasticsearch, start it and then create the client
            val installer = ElasticsearchInstaller(vertx)
            val runner = ElasticsearchRunner(vertx)
            installer.download(elasticsearchDownloadUrl, elasticsearchInstallPath)
                    .flatMapCompletable { path -> runner.runElasticsearch(host, port, path) }
                    .andThen(runner.waitUntilElasticsearchRunning(client))
                    .toSingle { EmbeddedElasticsearchClient(client, runner) }
        }

    }

    companion object {
        private val log = LoggerFactory.getLogger(ElasticsearchClientFactory::class.java)
    }
}
