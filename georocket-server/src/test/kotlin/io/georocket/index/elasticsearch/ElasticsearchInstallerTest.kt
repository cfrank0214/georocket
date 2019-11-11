package io.georocket.index.elasticsearch

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.configureFor
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options

import java.io.File
import java.io.IOException
import java.net.URL

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SystemUtils
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith

import com.github.tomakehurst.wiremock.http.Fault
import com.github.tomakehurst.wiremock.junit.WireMockRule

import io.vertx.core.Vertx
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner

/**
 * Test [ElasticsearchInstaller]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class ElasticsearchInstallerTest {
    private val ZIP_NAME = "elasticsearch-dummy.zip"

    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    /**
     * Create a temporary folder
     */
    @Rule
    var folder = TemporaryFolder()

    /**
     * Run a mock HTTP server
     */
    @Rule
    var wireMockRule = WireMockRule(options().dynamicPort())

    /**
     * The URL pointing to the elasticsearch dummy zip file
     */
    private var downloadUrl: String? = null

    /**
     * Set up the unit tests
     */
    @Before
    fun setUp() {
        configureFor("localhost", wireMockRule.port())
        downloadUrl = "http://localhost:" + wireMockRule.port() + "/" + ZIP_NAME
    }

    /**
     * Test if Elasticsearch is downloaded correctly
     * @param context the test context
     * @throws IOException if the temporary folder for download could not be created
     */
    @Test
    @Throws(IOException::class)
    fun download(context: TestContext) {
        val zipFile = this.javaClass.getResource(ZIP_NAME)
        val zipFileContents = IOUtils.toByteArray(zipFile)

        stubFor(get(urlEqualTo("/$ZIP_NAME"))
                .willReturn(aResponse()
                        .withBody(zipFileContents)))

        val vertx = rule.vertx()
        val async = context.async()
        val dest = folder.newFolder()
        dest.delete()
        ElasticsearchInstaller(io.vertx.rxjava.core.Vertx(vertx))
                .download(downloadUrl!!, dest.absolutePath)
                .subscribe({ path ->
                    verify(getRequestedFor(urlEqualTo("/$ZIP_NAME")))
                    context.assertEquals(dest.absolutePath, path)
                    context.assertTrue(dest.exists())
                    context.assertTrue(File(dest, "bin/elasticsearch").exists())
                    if (!SystemUtils.IS_OS_WINDOWS) {
                        context.assertTrue(File(dest, "bin/elasticsearch").canExecute())
                    }
                    async.complete()
                }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if a server error is handled correctly
     * @param context the test context
     * @throws IOException if the temporary folder for download could not be created
     */
    @Test
    @Throws(IOException::class)
    fun download500(context: TestContext) {
        stubFor(get(urlEqualTo("/$ZIP_NAME"))
                .willReturn(aResponse()
                        .withStatus(500)))

        val vertx = rule.vertx()
        val async = context.async()
        val dest = folder.newFolder()
        dest.delete()
        ElasticsearchInstaller(io.vertx.rxjava.core.Vertx(vertx))
                .download(downloadUrl!!, dest.absolutePath)
                .subscribe({ path -> context.fail("Download is expected to fail") },
                        { err ->
                            verify(getRequestedFor(urlEqualTo("/$ZIP_NAME")))
                            context.assertFalse(dest.exists())
                            async.complete()
                        })
    }

    /**
     * Test if the installer fails if the returned archive is invalid
     * @param context the test context
     * @throws IOException if the temporary folder for download could not be created
     */
    @Test
    @Throws(IOException::class)
    fun downloadInvalidArchive(context: TestContext) {
        stubFor(get(urlEqualTo("/$ZIP_NAME"))
                .willReturn(aResponse()
                        .withBody("foobar")))

        val vertx = rule.vertx()
        val async = context.async()
        val dest = folder.newFolder()
        dest.delete()
        ElasticsearchInstaller(io.vertx.rxjava.core.Vertx(vertx))
                .download(downloadUrl!!, dest.absolutePath)
                .subscribe({ path -> context.fail("Download is expected to fail") },
                        { err ->
                            verify(getRequestedFor(urlEqualTo("/$ZIP_NAME")))
                            context.assertFalse(dest.exists())
                            async.complete()
                        })
    }

    /**
     * Test if the installer fails if it receives random data and the stream is
     * closed in between
     * @param context the test context
     * @throws IOException if the temporary folder for download could not be created
     */
    @Test
    @Throws(IOException::class)
    fun downloadRandomAndClose(context: TestContext) {
        stubFor(get(urlEqualTo("/$ZIP_NAME"))
                .willReturn(aResponse()
                        .withFault(Fault.RANDOM_DATA_THEN_CLOSE)))

        val vertx = rule.vertx()
        val async = context.async()
        val dest = folder.newFolder()
        dest.delete()
        ElasticsearchInstaller(io.vertx.rxjava.core.Vertx(vertx))
                .download(downloadUrl!!, dest.absolutePath)
                .subscribe({ path -> context.fail("Download is expected to fail") },
                        { err ->
                            verify(getRequestedFor(urlEqualTo("/$ZIP_NAME")))
                            context.assertFalse(dest.exists())
                            async.complete()
                        })
    }
}
