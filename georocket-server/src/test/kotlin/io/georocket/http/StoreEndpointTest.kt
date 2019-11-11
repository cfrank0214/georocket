package io.georocket.http

import io.georocket.NetUtils
import io.georocket.constants.ConfigConstants
import io.georocket.http.mocks.MockIndexer
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpClientResponse
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.Router
import io.vertx.rx.java.RxHelper
import io.vertx.rxjava.core.Vertx
import org.junit.After
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import rx.Observable

import java.util.LinkedList

/**
 * Test class for [StoreEndpoint]
 * @author David Gengenbach, Andrej Sajenko
 */
@RunWith(VertxUnitRunner::class)
class StoreEndpointTest {

    /**
     * Uninitialize the unit test
     * @param context the test context
     */
    @After
    fun teardown(context: TestContext) {
        MockIndexer.unsubscribeIndexer()
    }

    /**
     * Tests that a scroll request can be done.
     * @param context Test context
     */
    @Test
    fun testScrolling(context: TestContext) {
        val async = context.async()
        MockIndexer.mockIndexerQuery(vertx!!)

        doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY&scroll=true&size=100", true, true) { response ->
            val si = MockIndexer.FIRST_RETURNED_SCROLL_ID + ":" + MockIndexer.FIRST_RETURNED_SCROLL_ID
            context.assertEquals(si, response.getHeader(HeaderConstants.SCROLL_ID))
            checkGeoJsonResponse(response, context) { returned ->
                checkGeoJsonSize(context, response, returned, MockIndexer.HITS_PER_PAGE, true, "The size of the returned elements on the first page should be the page size.")
                async.complete()
            }
        }
    }

    /**
     * Tests whether a scroll can be continued with a given scrollId.
     * @param context Test context
     */
    @Test
    fun testScrollingWithGivenScrollId(context: TestContext) {
        val async = context.async()
        MockIndexer.mockIndexerQuery(vertx!!)
        val si = MockIndexer.FIRST_RETURNED_SCROLL_ID + ":" + MockIndexer.FIRST_RETURNED_SCROLL_ID
        doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY&scroll=true&scrollId=$si", true, true) { response ->
            val isi = MockIndexer.INVALID_SCROLLID + ":" + MockIndexer.INVALID_SCROLLID
            context.assertEquals(isi, response.getHeader(HeaderConstants.SCROLL_ID), "The second scrollId should be invalid if there a no elements left.")
            checkGeoJsonResponse(response, context) { returned ->
                checkGeoJsonSize(context, response, returned, MockIndexer.TOTAL_HITS!! - MockIndexer.HITS_PER_PAGE!!, true, "The size of the returned elements on the second page should be (TOTAL_HITS - HITS_PER_PAGE)")
                async.complete()
            }
        }
    }

    /**
     * Tests what happens when an invalid scrollId is returned.
     * @param context Test context
     */
    @Test
    fun testScrollingWithInvalidScrollId(context: TestContext) {
        val async = context.async()
        MockIndexer.mockIndexerQuery(vertx!!)

        doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY&scroll=true&scrollId=" + MockIndexer.INVALID_SCROLLID, checkHeaders = false, checkScrollIdHeaderPresent = false, handler = { response ->
            context.assertEquals(404, response.statusCode(), "Giving an invalid scrollId should return 404.")
            async.complete()
        })
    }

    /**
     * Tests that a normal query returns all the elements.
     * @param context Test context
     */
    @Test
    fun testNormalGet(context: TestContext) {
        val async = context.async()
        MockIndexer.mockIndexerQuery(vertx!!)
        doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY&scroll=false", false, checkScrollIdHeaderPresent = false) { response ->
            checkGeoJsonResponse(response, context) { returned ->
                checkGeoJsonSize(context, response, returned, MockIndexer.TOTAL_HITS, false, "The size of the returned elements on a normal query should be TOTAL_HITS")
                async.complete()
            }
        }
    }

    /**
     * Tests that a normal query returns all the elements.
     * @param context Test context
     */
    @Test
    fun testNormalGetWithoutScrollParameterGiven(context: TestContext) {
        val async = context.async()
        MockIndexer.mockIndexerQuery(vertx!!)
        doScrolledStorepointRequest(context, "/?search=DUMMY_QUERY", false, false) { response ->
            checkGeoJsonResponse(response, context) { returned ->
                checkGeoJsonSize(context, response, returned, MockIndexer.TOTAL_HITS, false, "The size of the returned elements on a normal query should be TOTAL_HITS")
                async.complete()
            }
        }
    }

    private fun checkGeoJsonSize(context: TestContext, response: HttpClientResponse, returned: JsonObject, expectedSize: Long?, checkScrollHeaders: Boolean, msg: String?) {
        context.assertEquals(expectedSize, returned.getJsonArray("geometries").size(), msg
                ?: "Response GeoJson had not the expected size!")
        if (checkScrollHeaders) {
            context.assertEquals(MockIndexer.TOTAL_HITS!!.toString(), response.getHeader(HeaderConstants.TOTAL_HITS))
            context.assertEquals(expectedSize!!.toString(), response.getHeader(HeaderConstants.HITS))
        }
    }

    private fun checkGeoJsonResponse(response: HttpClientResponse, context: TestContext, handler: (Any) -> Unit) {
        response.bodyHandler { body ->
            val returned = body.toJsonObject()
            context.assertNotNull(returned)
            context.assertTrue(returned.containsKey("geometries"))
            handler.handle(returned)
        }
    }

    /**
     * Checks for scroll-specific headers that are returned from the server are present or not.
     * @param response client response
     * @param context context
     * @param checkScrollIdHeaderPresent should the test check the scroll id
     */
    private fun checkScrollingResponsePresent(response: HttpClientResponse, context: TestContext, checkScrollIdHeaderPresent: Boolean) {
        val neededHeaders = LinkedList<String>()
        neededHeaders.add(HeaderConstants.TOTAL_HITS)
        neededHeaders.add(HeaderConstants.HITS)

        if (checkScrollIdHeaderPresent) {
            neededHeaders.add(HeaderConstants.SCROLL_ID)
        }

        for (header in neededHeaders) {
            context.assertNotNull(response.getHeader(header), "$header header not set")
        }
    }

    /**
     * Performs request against the server and checks for the scroll headers.
     * Fails when the headers are not present or an error occured during the request.
     *
     * @param context Test context
     * @param url url
     * @param checkHeaders should the test check the headers
     * @param checkScrollIdHeaderPresent should the test check the scroll id
     * @param handler response handler
     */
    private fun doScrolledStorepointRequest(context: TestContext, url: String, checkHeaders: Boolean?,
                                            checkScrollIdHeaderPresent: Boolean?, handler: (Any) -> Unit) {
        val client = createHttpClient()
        val request = client.get(url) { response ->
            if (checkHeaders!!) {
                checkScrollingResponsePresent(response, context, checkScrollIdHeaderPresent!!)
            }
            handler.handle(response)
        }
        request.exceptionHandler { x -> context.fail("Exception during query.") }
        request.end()
    }

    private object HeaderConstants {
        internal val SCROLL_ID = "X-Scroll-Id"
        internal val TOTAL_HITS = "X-Total-Hits"
        internal val HITS = "X-Hits"
    }

    companion object {
        private var vertx: Vertx? = null

        private var vertxCore: io.vertx.core.Vertx? = null

        /**
         * Removes the warnings about blocked threads.
         * Otherwise vertx would log a lot of warnings, because the startup takes some time.
         */
        private val vertxOptions = VertxOptions().setBlockedThreadCheckInterval(999999L)

        /**
         * Run the test on a Vert.x test context
         */
        @ClassRule
        var rule = RunTestOnContext(vertxOptions)

        /**
         * Starts a MockServer verticle with a StoreEndpoint to test against
         * @param context the test context
         */
        @BeforeClass
        fun setupServer(context: TestContext) {
            val async = context.async()
            vertx = Vertx(rule.vertx())
            vertxCore = vertx!!.delegate

            setConfig(vertx!!.orCreateContext.config())
            setupMockEndpoint().subscribe { x -> async.complete() }
        }

        /**
         * Creates a StoreEndpoint router
         * @return Router
         */
        private val storeEndpointRouter: Router
            get() {
                val router = Router.router(vertxCore)
                val storeEndpoint = StoreEndpoint()
                router.mountSubRouter("/", storeEndpoint.createRouter(vertxCore))
                return router
            }

        /**
         * Creates a HttpClient to do requests against the server. No SSL is used.
         * @return a client that's preconfigured for requests to the server.
         */
        private fun createHttpClient(): HttpClient {
            val options = HttpClientOptions()
                    .setDefaultHost(vertx!!.orCreateContext.config().getString(ConfigConstants.HOST))
                    .setDefaultPort(vertx!!.orCreateContext.config().getInteger(ConfigConstants.PORT)!!)
                    .setSsl(false)
            return vertxCore!!.createHttpClient(options)
        }

        private fun setupMockEndpoint(): Observable<HttpServer> {
            val config = vertx!!.orCreateContext.config()
            val host = config.getString(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST)
            val port = config.getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT)!!

            val serverOptions = HttpServerOptions().setCompressionSupported(true)
            val server = vertxCore!!.createHttpServer(serverOptions)

            val observable = RxHelper.observableFuture<HttpServer>()
            server.requestHandler { storeEndpointRouter.accept(it) }.listen(port, host, observable.toHandler())
            return observable
        }

        private fun setConfig(config: JsonObject) {
            // Use mock store
            config.put(ConfigConstants.STORAGE_CLASS, "io.georocket.http.mocks.MockStore")
            config.put(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST)
            config.put(ConfigConstants.PORT, NetUtils.findPort())
        }
    }
}
