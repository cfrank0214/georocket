package io.georocket.index.elasticsearch

import com.github.tomakehurst.wiremock.junit.WireMockRule
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import java.net.URI
import java.util.Arrays
import java.util.Collections

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.absent
import com.github.tomakehurst.wiremock.client.WireMock.binaryEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options

/**
 * Tests for [LoadBalancingHttpClient]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class LoadBalancingHttpClientTest {
    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    /**
     * Mock server 1
     */
    @Rule
    var wireMockRule1 = WireMockRule(options().dynamicPort())

    /**
     * Mock server 2
     */
    @Rule
    var wireMockRule2 = WireMockRule(options().dynamicPort())

    /**
     * Mock server 3
     */
    @Rule
    var wireMockRule3 = WireMockRule(options().dynamicPort())

    /**
     * Mock server 4
     */
    @Rule
    var wireMockRule4 = WireMockRule(options().dynamicPort())

    private val expected1 = JsonObject().put("name", "D'Artagnan")
    private val expected2 = JsonObject().put("name", "Athos")
    private val expected3 = JsonObject().put("name", "Porthos")
    private val expected4 = JsonObject().put("name", "Aramis")

    /**
     * The client under test
     */
    private var client: LoadBalancingHttpClient? = null

    @Before
    fun setUp() {
        client = LoadBalancingHttpClient(rule.vertx())

        wireMockRule1.stubFor(get(urlEqualTo("/"))
                .willReturn(aResponse()
                        .withBody(expected1.encode())))
        wireMockRule2.stubFor(get(urlEqualTo("/"))
                .willReturn(aResponse()
                        .withBody(expected2.encode())))
        wireMockRule3.stubFor(get(urlEqualTo("/"))
                .willReturn(aResponse()
                        .withBody(expected3.encode())))
        wireMockRule4.stubFor(get(urlEqualTo("/"))
                .willReturn(aResponse()
                        .withBody(expected4.encode())))
    }

    @After
    fun tearDown() {
        client!!.close()
    }

    /**
     * Test if four requests can be sent to four hosts
     */
    @Test
    fun fourHostsFourRequests(ctx: TestContext) {
        client!!.setHosts(Arrays.asList(
                URI.create("http://localhost:" + wireMockRule1.port()),
                URI.create("http://localhost:" + wireMockRule2.port()),
                URI.create("http://localhost:" + wireMockRule3.port()),
                URI.create("http://localhost:" + wireMockRule4.port())))

        val async = ctx.async()
        client!!.performRequest("/")
                .doOnSuccess { o -> ctx.assertEquals(expected1, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected2, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected3, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected4, o) }
                .subscribe { o -> async.complete() }
    }

    /**
     * Test if six requests can be sent to four hosts
     */
    @Test
    fun fourHostsSixRequests(ctx: TestContext) {
        client!!.setHosts(Arrays.asList(
                URI.create("http://localhost:" + wireMockRule1.port()),
                URI.create("http://localhost:" + wireMockRule2.port()),
                URI.create("http://localhost:" + wireMockRule3.port()),
                URI.create("http://localhost:" + wireMockRule4.port())))

        val async = ctx.async()
        client!!.performRequest("/")
                .doOnSuccess { o -> ctx.assertEquals(expected1, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected2, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected3, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected4, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected1, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected2, o) }
                .subscribe { o -> async.complete() }
    }

    /**
     * Test if five requests can be sent to three hosts in a pseudo-random order
     */
    @Test
    fun anotherOrder(ctx: TestContext) {
        client!!.setHosts(Arrays.asList(
                URI.create("http://localhost:" + wireMockRule4.port()),
                URI.create("http://localhost:" + wireMockRule1.port()),
                URI.create("http://localhost:" + wireMockRule3.port())))

        val async = ctx.async()
        client!!.performRequest("/")
                .doOnSuccess { o -> ctx.assertEquals(expected4, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected1, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected3, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected4, o) }
                .flatMap { v -> client!!.performRequest("/") }
                .doOnSuccess { o -> ctx.assertEquals(expected1, o) }
                .subscribe { o -> async.complete() }
    }

    /**
     * Test what happens if the first host is unreachable
     */
    @Test
    fun retrySecondHost(ctx: TestContext) {
        client!!.setDefaultOptions(HttpClientOptions().setConnectTimeout(500))
        client!!.setHosts(Arrays.asList(
                URI.create("http://192.0.2.0:80"),
                URI.create("http://localhost:" + wireMockRule2.port())))

        val async = ctx.async()
        client!!.performRequest("/")
                .doOnSuccess { o -> ctx.assertEquals(expected2, o) }
                .subscribe { o -> async.complete() }
    }

    /**
     * Test if request bodies can be compressed
     */
    @Test
    fun compressRequestBodies(ctx: TestContext) {
        client!!.close()
        client = LoadBalancingHttpClient(rule.vertx(), true)

        val body = Buffer.buffer()
        for (i in 0..149) {
            body.appendString("Hello world")
        }

        wireMockRule1.stubFor(post(urlEqualTo("/"))
                .withHeader("Content-Encoding", equalTo("gzip"))
                .withRequestBody(binaryEqualTo(body.bytes))
                .willReturn(aResponse()
                        .withBody(expected1.encode())))

        client!!.setHosts(listOf(URI.create("http://localhost:" + wireMockRule1.port())))

        val async = ctx.async()
        client!!.performRequest(HttpMethod.POST, "/", body)
                .doOnSuccess { o -> ctx.assertEquals(expected1, o) }
                .subscribe { o -> async.complete() }
    }

    /**
     * Make sure request bodies that are too small are not compressed
     */
    @Test
    fun compressRequestBodiesMessageTooSmall(ctx: TestContext) {
        client!!.close()
        client = LoadBalancingHttpClient(rule.vertx(), true)

        val body = Buffer.buffer("Hello World")

        wireMockRule1.stubFor(post(urlEqualTo("/"))
                .withHeader("Content-Encoding", absent())
                .withRequestBody(binaryEqualTo(body.bytes))
                .willReturn(aResponse()
                        .withBody(expected1.encode())))

        client!!.setHosts(listOf(URI.create("http://localhost:" + wireMockRule1.port())))

        val async = ctx.async()
        client!!.performRequest(HttpMethod.POST, "/", body)
                .doOnSuccess { o -> ctx.assertEquals(expected1, o) }
                .subscribe { o -> async.complete() }
    }
}
