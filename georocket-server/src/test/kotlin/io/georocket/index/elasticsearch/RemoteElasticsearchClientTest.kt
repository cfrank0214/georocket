package io.georocket.index.elasticsearch

import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.jooq.lambda.tuple.Tuple
import org.jooq.lambda.tuple.Tuple2
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import java.net.URI
import java.time.Duration
import java.util.ArrayList
import java.util.Arrays
import java.util.Collections
import java.util.HashSet

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.equalToJson
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.head
import com.github.tomakehurst.wiremock.client.WireMock.headRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.put
import com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options

/**
 * Test for [RemoteElasticsearchClient]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class RemoteElasticsearchClientTest {

    private var client: ElasticsearchClient? = null

    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    /**
     * Run a mock HTTP server
     */
    @Rule
    var wireMockRule1 = WireMockRule(options().dynamicPort(), false)

    /**
     * Run another mock HTTP server
     */
    @Rule
    var wireMockRule2 = WireMockRule(options().dynamicPort(), false)

    /**
     * Create the Elasticsearch client
     */
    @Before
    fun setUp() {
        val hosts = listOf(URI.create("http://localhost:" + wireMockRule1.port()))
        client = RemoteElasticsearchClient(hosts, INDEX, null, false, rule.vertx())
    }

    /**
     * Close the Elasticsearch client
     */
    @After
    fun tearDown() {
        client!!.close()
    }

    /**
     * Test if the [ElasticsearchClient.indexExists] method returns
     * `false` if the index does not exist
     * @param context the test context
     */
    @Test
    fun indexExistsFalse(context: TestContext) {
        wireMockRule1.stubFor(head(urlEqualTo("/$INDEX"))
                .willReturn(aResponse()
                        .withStatus(404)))

        val async = context.async()
        client!!.indexExists().subscribe({ f ->
            context.assertFalse(f!!)
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the [ElasticsearchClient.typeExists] method returns
     * `false` if the index does not exist
     * @param context the test context
     */
    @Test
    fun typeIndexExistsFalse(context: TestContext) {
        wireMockRule1.stubFor(head(urlEqualTo("/$INDEX/$TYPE"))
                .willReturn(aResponse()
                        .withStatus(404)))

        val async = context.async()
        client!!.typeExists(TYPE).subscribe({ f ->
            context.assertFalse(f!!)
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the [ElasticsearchClient.indexExists] method returns
     * `true` if the index exists
     * @param context the test context
     */
    @Test
    fun indexExistsTrue(context: TestContext) {
        wireMockRule1.stubFor(head(urlEqualTo("/$INDEX"))
                .willReturn(aResponse()
                        .withStatus(200)))

        val async = context.async()
        client!!.indexExists().subscribe({ f ->
            context.assertTrue(f!!)
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the [ElasticsearchClient.indexExists] method returns
     * `true` if the index exists
     * @param context the test context
     */
    @Test
    fun typeIndexExistsTrue(context: TestContext) {
        wireMockRule1.stubFor(head(urlEqualTo("/$INDEX/_mapping/$TYPE"))
                .willReturn(aResponse()
                        .withStatus(200)))

        val async = context.async()
        client!!.typeExists(TYPE).subscribe({ f ->
            context.assertTrue(f!!)
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the client can create a mapping for an index
     * @param context the test context
     */
    @Test
    fun putMapping(context: TestContext) {
        wireMockRule1.stubFor(put(urlEqualTo("/$INDEX/_mapping/$TYPE"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(ACKNOWLEDGED.encode())))

        val mappings = JsonObject()
                .put("properties", JsonObject()
                        .put("name", JsonObject()
                                .put("type", "text")))

        val async = context.async()
        client!!.putMapping(TYPE, mappings).subscribe({ ack ->
            wireMockRule1.verify(putRequestedFor(urlEqualTo("/$INDEX/_mapping/$TYPE"))
                    .withRequestBody(equalToJson("{\"properties\":{\"name\":{\"type\":\"text\"}}}}}")))
            context.assertTrue(ack!!)
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the client can create an index
     * @param context the test context
     */
    @Test
    fun createIndex(context: TestContext) {
        val settings = wireMockRule1.stubFor(put(urlEqualTo("/$INDEX"))
                .withRequestBody(equalTo(""))
                .willReturn(aResponse()
                        .withBody(ACKNOWLEDGED.encode())
                        .withStatus(200)))

        val async = context.async()
        client!!.createIndex().subscribe({ ok ->
            context.assertTrue(ok!!)
            wireMockRule1.verify(putRequestedFor(settings.request.urlMatcher))
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the client can create an index with settings
     * @param context the test context
     */
    @Test
    fun createIndexWithSettings(context: TestContext) {
        val settings = wireMockRule1.stubFor(put(urlEqualTo("/$INDEX"))
                .withRequestBody(equalToJson(SETTINGS_WRAPPER.encode()))
                .willReturn(aResponse()
                        .withBody(ACKNOWLEDGED.encode())
                        .withStatus(200)))

        val async = context.async()
        client!!.createIndex(SETTINGS).subscribe({ ok ->
            context.assertTrue(ok!!)
            wireMockRule1.verify(putRequestedFor(settings.request.urlMatcher))
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the client can insert multiple documents in one request
     * @param context the test context
     */
    @Test
    fun bulkInsert(context: TestContext) {
        val url = "/$INDEX/$TYPE/_bulk"

        wireMockRule1.stubFor(post(urlEqualTo(url))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{}")))

        val documents = ArrayList<Tuple2<String, JsonObject>>()
        documents.add(Tuple.tuple("A", JsonObject().put("name", "Elvis")))
        documents.add(Tuple.tuple("B", JsonObject().put("name", "Max")))

        val async = context.async()
        client!!.bulkInsert(TYPE, documents).subscribe({ res ->
            wireMockRule1.verify(postRequestedFor(urlEqualTo(url))
                    .withRequestBody(equalToJson("{\"index\":{\"_id\":\"A\"}}\n" +
                            "{\"name\":\"Elvis\"}\n" +
                            "{\"index\":{\"_id\":\"B\"}}\n" +
                            "{\"name\":\"Max\"}\n")))
            context.assertEquals(0, res.size())
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Check if [ElasticsearchClient.isRunning] returns false
     * if it is not running
     * @param context the test context
     */
    @Test
    fun isRunningFalse(context: TestContext) {
        val async = context.async()
        client!!.isRunning.subscribe({ r ->
            context.assertFalse(r!!)
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Check if [ElasticsearchClient.isRunning] returns true
     * if it is running
     * @param context the test context
     */
    @Test
    fun isRunning(context: TestContext) {
        wireMockRule1.stubFor(head(urlEqualTo("/"))
                .willReturn(aResponse()
                        .withStatus(200)))

        val async = context.async()
        client!!.isRunning.subscribe({ r ->
            context.assertTrue(r!!)
            wireMockRule1.verify(headRequestedFor(urlEqualTo("/")))
            async.complete()
        }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test the [ElasticsearchClient.bulkResponseHasErrors] method
     * @param context the test context
     */
    @Test
    fun bulkResponseHasErrors(context: TestContext) {
        context.assertTrue(client!!.bulkResponseHasErrors(BULK_RESPONSE_ERROR))
        context.assertFalse(client!!.bulkResponseHasErrors(JsonObject()))
    }

    /**
     * Test [ElasticsearchClient.bulkResponseGetErrorMessage]
     * produces an error message
     * @param context the test context
     */
    @Test
    fun bulkResponseGetErrorMessage(context: TestContext) {
        context.assertNull(client!!.bulkResponseGetErrorMessage(JsonObject()))
        val expected = ("Errors in bulk operation:\n"
                + "[id: [B], type: [mapper_parsing_exception], "
                + "reason: [Field name [na.me] cannot contain '.']]\n"
                + "[id: [C], type: [mapper_parsing_exception], "
                + "reason: [Field name [nam.e] cannot contain '.']]")
        context.assertEquals(expected,
                client!!.bulkResponseGetErrorMessage(BULK_RESPONSE_ERROR))
    }

    /**
     * Test if the client can connect to multiple hosts
     * @param context the test context
     */
    @Test
    fun multipleHosts(context: TestContext) {
        val hosts = Arrays.asList(URI.create("http://localhost:" + wireMockRule1.port()),
                URI.create("http://localhost:" + wireMockRule2.port()))
        client = RemoteElasticsearchClient(hosts, INDEX, null, false, rule.vertx())

        wireMockRule1.stubFor(head(urlEqualTo("/$INDEX"))
                .willReturn(aResponse()
                        .withStatus(200)))
        wireMockRule2.stubFor(head(urlEqualTo("/$INDEX"))
                .willReturn(aResponse()
                        .withStatus(404)))

        val async = context.async()
        client!!.indexExists()
                .doOnSuccess(Action1<Boolean> { context.assertTrue(it) })
                .flatMap { v -> client!!.indexExists() }
                .doOnSuccess(Action1<Boolean> { context.assertFalse(it) })
                .subscribe({ v -> async.complete() }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the client can auto-detect multiple hosts
     * @param context the test context
     */
    @Test
    fun autoDetectHosts(context: TestContext) {
        val hosts = Arrays.asList(URI.create("http://localhost:" + wireMockRule1.port()))
        client!!.close()
        client = RemoteElasticsearchClient(hosts, INDEX, Duration.ofMillis(222),
                false, rule.vertx())

        val nodes = JsonObject()
                .put("nodes", JsonObject()
                        .put("A", JsonObject()
                                .put("http", JsonObject()
                                        .put("publish_address", "localhost:" + wireMockRule1.port())))
                        .put("B", JsonObject()
                                .put("http", JsonObject()
                                        .put("publish_address", "localhost:" + wireMockRule2.port()))))


        wireMockRule1.stubFor(get(urlEqualTo("/_nodes/http"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(nodes.encode())))
        wireMockRule2.stubFor(get(urlEqualTo("/_nodes/http"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(nodes.encode())))

        wireMockRule1.stubFor(head(urlEqualTo("/$INDEX"))
                .willReturn(aResponse()
                        .withStatus(200)))
        wireMockRule2.stubFor(head(urlEqualTo("/$INDEX"))
                .willReturn(aResponse()
                        .withStatus(404)))

        val expected = HashSet<Boolean>()
        expected.add(java.lang.Boolean.TRUE)
        expected.add(java.lang.Boolean.FALSE)
        val async = context.async(expected.size)
        rule.vertx().setPeriodic(100) { id ->
            client!!.indexExists()
                    .subscribe({ e ->
                        if (expected.remove(e)) {
                            if (expected.isEmpty()) {
                                rule.vertx().cancelTimer(id!!)
                            }
                            async.countDown()
                        }
                    }, Action1<Throwable> { context.fail(it) })
        }
    }

    companion object {
        private val INDEX = "myindex"
        private val TYPE = "mytype"

        private val BULK_RESPONSE_ERROR = JsonObject()
                .put("errors", true)
                .put("items", JsonArray()
                        .add(JsonObject()
                                .put("index", JsonObject()
                                        .put("_id", "A")
                                        .put("status", 200)))
                        .add(JsonObject()
                                .put("index", JsonObject()
                                        .put("_id", "B")
                                        .put("status", 400)
                                        .put("error", JsonObject()
                                                .put("type", "mapper_parsing_exception")
                                                .put("reason", "Field name [na.me] cannot contain '.'"))))
                        .add(JsonObject()
                                .put("index", JsonObject()
                                        .put("_id", "C")
                                        .put("status", 400)
                                        .put("error", JsonObject()
                                                .put("type", "mapper_parsing_exception")
                                                .put("reason", "Field name [nam.e] cannot contain '.'")))))

        private val SETTINGS = JsonObject()
                .put("index", JsonObject()
                        .put("number_of_shards", 3)
                        .put("number_of_replicas", 3))
        private val SETTINGS_WRAPPER = JsonObject()
                .put("settings", SETTINGS)

        private val ACKNOWLEDGED = JsonObject()
                .put("acknowledged", true)
    }
}
