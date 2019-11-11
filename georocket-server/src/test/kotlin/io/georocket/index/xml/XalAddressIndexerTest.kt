package io.georocket.index.xml

import com.google.common.collect.ImmutableMap
import io.georocket.util.XMLParserTransformer
import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import rx.Observable

import java.io.IOException
import java.io.InputStream
import java.util.Scanner

/**
 * Tests [XalAddressIndexer]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class XalAddressIndexerTest {
    /**
     * Indexes the given XML file and checks if the result matches the
     * expected properties map
     * @param expected the expected properties map
     * @param xmlFile the XML file to parse
     * @param context the current test context
     * @throws IOException if the JSON file could not be read
     */
    @Throws(IOException::class)
    private fun assertIndexed(expected: Map<String, Any>, xmlFile: String,
                              context: TestContext) {
        var json: String
        javaClass.getResourceAsStream(xmlFile).use { `is` ->
            Scanner(`is`, "UTF-8").use { scanner ->
                scanner.useDelimiter("\\A")
                json = scanner.next()
            }
        }

        val indexer = XalAddressIndexer()
        val expectedMap = ImmutableMap.of<String, Any>("address", expected)

        val async = context.async()
        Observable.just(Buffer.buffer(json))
                .compose<XMLStreamEvent>(XMLParserTransformer())
                .doOnNext(Action1<XMLStreamEvent> { indexer.onEvent(it) })
                .last()
                .subscribe({ r ->
                    context.assertEquals(expectedMap, indexer.result)
                    async.complete()
                }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if an XML file containing a XAL address can be indexed
     * @param context the current test context
     * @throws IOException if the XML file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun feature(context: TestContext) {
        val expected = ImmutableMap.of<String, Any>(
                "Country", "Germany",
                "Locality", "Darmstadt",
                "Street", "Fraunhoferstra\u00DFe",
                "Number", "5"
        )
        assertIndexed(expected, "xal_simple_address.xml", context)
    }
}
