package io.georocket.storage.s3

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.configureFor
import com.github.tomakehurst.wiremock.client.WireMock.delete
import com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.put
import com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options

import org.junit.After
import org.junit.Before
import org.junit.Rule

import com.github.tomakehurst.wiremock.junit.WireMockRule

import io.georocket.constants.ConfigConstants
import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext

/**
 * Test [S3Store]
 * @author Andrej Sajenko
 */
class S3StoreTest : StorageTest() {

    /**
     * The http mock test rule
     */
    @Rule
    var wireMockRule = WireMockRule(options().dynamicPort())

    private object Http {

        val CONTENT_TYPE = "Content-Type"
        val SERVER = "Server"
        val CONNECTION = "Connection"
        val CONTENT_LENGTH = "Content-Length"

        object Types {
            val XML = "application/xml"
        }

        object Codes {
            val OK = 200
            val NO_CONTENT = 204
        }
    }

    /**
     * Set up test dependencies.
     */
    @Before
    fun setUp() {
        wireMockRule.start()
        configureFor("localhost", wireMockRule.port())

        // Mock http request for getOne
        wireMockRule.stubFor(
                // Request
                get(urlPathEqualTo(pathWithLeadingSlash(S3_BUCKET, StorageTest.ID)))
                        .willReturn(aResponse()
                                .withStatus(Http.Codes.OK)

                                .withHeader(Http.CONTENT_LENGTH, StorageTest.CHUNK_CONTENT.length.toString())
                                .withHeader(Http.CONTENT_TYPE, Http.Types.XML)
                                .withHeader(Http.CONNECTION, "close")
                                .withHeader(Http.SERVER, "AmazonS3")

                                .withBody(StorageTest.CHUNK_CONTENT)
                        )
        )

        // Mock http request for add without tempFolder
        wireMockRule.stubFor(
                put(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, ".*")))
                        .withHeader(Http.CONTENT_LENGTH, equalTo(StorageTest.CHUNK_CONTENT.length.toString()))

                        .withRequestBody(equalTo(StorageTest.CHUNK_CONTENT))

                        .willReturn(aResponse()
                                .withStatus(Http.Codes.OK)
                        )
        )

        wireMockRule.stubFor(
                put(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, StorageTest.TEST_FOLDER, ".*")))
                        .withHeader(Http.CONTENT_LENGTH, equalTo(StorageTest.CHUNK_CONTENT.length.toString()))

                        .withRequestBody(equalTo(StorageTest.CHUNK_CONTENT))

                        .willReturn(aResponse()
                                .withStatus(Http.Codes.OK)
                        )
        )

        val listItems = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                "  <Name>" + S3_BUCKET + "</Name>\n" +
                "  <KeyCount>1</KeyCount>\n" +
                "  <MaxKeys>3</MaxKeys>\n" +
                "  <IsTruncated>false</IsTruncated>\n" +
                "  <Contents>\n" +
                "    <Key>ExampleObject1.txt</Key>\n" +
                "    <LastModified>2013-09-17T18:07:53.000Z</LastModified>\n" +
                "    <ETag>&quot;599bab3ed2c697f1d26842727561fd94&quot;</ETag>\n" +
                "    <Size>857</Size>\n" +
                "    <StorageClass>STANDARD</StorageClass>\n" +
                "  </Contents>\n" +
                "  <Contents>\n" +
                "    <Key>ExampleObject2.txt</Key>\n" +
                "    <LastModified>2013-09-17T18:07:53.000Z</LastModified>\n" +
                "    <ETag>&quot;599bab3ed2c697f1d26842727561fd20&quot;</ETag>\n" +
                "    <Size>233</Size>\n" +
                "    <StorageClass>STANDARD</StorageClass>\n" +
                "  </Contents>\n" +
                "  <Contents>\n" +
                "    <Key>ExampleObject3.txt</Key>\n" +
                "    <LastModified>2013-09-17T18:07:53.000Z</LastModified>\n" +
                "    <ETag>&quot;599bab3ed2c697f1d26842727561fd30&quot;</ETag>\n" +
                "    <Size>412</Size>\n" +
                "    <StorageClass>STANDARD</StorageClass>\n" +
                "  </Contents>\n" +
                "</ListBucketResult>"

        wireMockRule.stubFor(
                get(urlMatching(pathWithLeadingSlash(S3_BUCKET) + "/\\?list-type=2.*"))

                        .willReturn(aResponse()
                                .withStatus(Http.Codes.OK)
                                .withHeader("Content-Type", "application/xml")
                                .withHeader("Content-Length", listItems.length.toString())

                                .withBody(listItems)
                        )
        )

        // Mock http request for delete without tempFolder
        wireMockRule.stubFor(
                delete(urlPathEqualTo(pathWithLeadingSlash(S3_BUCKET, StorageTest.ID)))

                        .willReturn(aResponse()
                                .withStatus(Http.Codes.NO_CONTENT)
                        )
        )

        // Mock http request for delete with tempFolder
        wireMockRule.stubFor(
                delete(urlPathEqualTo(pathWithLeadingSlash(S3_BUCKET, StorageTest.TEST_FOLDER, StorageTest.ID)))

                        .willReturn(aResponse()
                                .withStatus(Http.Codes.NO_CONTENT)
                        )
        )
    }

    /**
     * Stop WireMock
     */
    @After
    fun tearDown() {
        wireMockRule.stop()
    }

    private fun configureVertx(vertx: Vertx) {
        val config = vertx.orCreateContext.config()

        config.put(ConfigConstants.STORAGE_S3_ACCESS_KEY, S3_ACCESS_KEY)
        config.put(ConfigConstants.STORAGE_S3_SECRET_KEY, S3_SECRET_KEY)
        config.put(ConfigConstants.STORAGE_S3_HOST, S3_HOST)
        config.put(ConfigConstants.STORAGE_S3_PORT, wireMockRule.port())
        config.put(ConfigConstants.STORAGE_S3_BUCKET, S3_BUCKET)
        config.put(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, S3_PATH_STYLE_ACCESS)
    }

    override fun createStore(vertx: Vertx): Store {
        configureVertx(vertx)
        return S3Store(vertx)
    }

    override fun prepareData(context: TestContext, vertx: Vertx, path: String?,
                             handler: Handler<AsyncResult<String>>) {
        handler.handle(Future.succeededFuture(PathUtils.join(path, StorageTest.ID)))
    }

    override fun validateAfterStoreAdd(context: TestContext, vertx: Vertx,
                                       path: String?, handler: Handler<AsyncResult<Void>>) {
        verify(putRequestedFor(urlPathMatching(pathWithLeadingSlash(S3_BUCKET, path, ".*"))))
        handler.handle(Future.succeededFuture())
    }

    override fun validateAfterStoreDelete(context: TestContext, vertx: Vertx,
                                          path: String, handler: Handler<AsyncResult<Void>>) {
        verify(deleteRequestedFor(urlPathEqualTo(pathWithLeadingSlash(S3_BUCKET, path))))
        handler.handle(Future.succeededFuture())
    }

    companion object {
        private val S3_ACCESS_KEY = "640ab2bae07bedc4c163f679a746f7ab7fb5d1fa"
        private val S3_SECRET_KEY = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
        private val S3_HOST = "localhost"
        private val S3_BUCKET = "testbucket"
        private val S3_PATH_STYLE_ACCESS = true

        private fun pathWithLeadingSlash(vararg paths: String): String {
            return "/" + PathUtils.join(*paths)
        }
    }
}
