package io.georocket.output.xml

import io.georocket.storage.ChunkReadStream
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
import io.georocket.util.io.BufferWriteStream
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.apache.commons.lang3.tuple.Pair
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import rx.Observable

import java.util.Collections

/**
 * Test [XMLMerger]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class XMLMergerTest {

    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    private fun doMerge(context: TestContext, chunks: Observable<Buffer>,
                        metas: Observable<XMLChunkMeta>, xmlContents: String, optimistic: Boolean,
                        expected: Class<out Throwable>? = null) {
        val m = XMLMerger(optimistic)
        val bws = BufferWriteStream()
        val async = context.async()
        val s: Observable<XMLChunkMeta>
        if (optimistic) {
            s = metas
        } else {
            s = metas.flatMapSingle { meta -> m.init(meta).toSingleDefault(meta) }
        }
        s.toList()
                .flatMap { l ->
                    chunks.map<DelegateChunkReadStream>(Func1<Buffer, DelegateChunkReadStream> { DelegateChunkReadStream(it) })
                            .zipWith<XMLChunkMeta, Pair<ChunkReadStream, XMLChunkMeta>>(l, Func2<DelegateChunkReadStream, XMLChunkMeta, Pair<ChunkReadStream, XMLChunkMeta>> { left, right -> Pair.of(left, right) })
                }
                .flatMapCompletable { p -> m.merge(p.left, p.right, bws) }
                .toCompletable()
                .subscribe({
                    if (expected != null) {
                        context.fail("Expected: " + expected.name)
                    } else {
                        m.finish(bws)
                        context.assertEquals(XMLHEADER + xmlContents, bws.buffer.toString("utf-8"))
                    }
                    async.complete()
                }, { e ->
                    if (e.javaClass != expected) {
                        context.fail(e)
                    }
                    async.complete()
                })
    }

    /**
     * Test if simple chunks can be merged
     * @param context the Vert.x test context
     */
    @Test
    fun simple(context: TestContext) {
        val chunk1 = Buffer.buffer("$XMLHEADER<root><test chunk=\"1\"></test></root>")
        val chunk2 = Buffer.buffer("$XMLHEADER<root><test chunk=\"2\"></test></root>")
        val cm = XMLChunkMeta(listOf(XMLStartElement("root")),
                XMLHEADER.length + 6, chunk1.length() - 7)
        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm, cm),
                "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>", false)
    }

    private fun mergeNamespaces(context: TestContext, optimistic: Boolean,
                                expected: Class<out Throwable>?) {
        val root1 = XMLStartElement(null, "CityModel",
                arrayOf("", "gml", "gen", XSI),
                arrayOf(NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_GENERICS, NS_SCHEMA_INSTANCE),
                arrayOf(XSI),
                arrayOf(SCHEMA_LOCATION),
                arrayOf(NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION))
        val root2 = XMLStartElement(null, "CityModel",
                arrayOf("", "gml", "bldg", XSI),
                arrayOf(NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_BUILDING, NS_SCHEMA_INSTANCE),
                arrayOf(XSI),
                arrayOf(SCHEMA_LOCATION),
                arrayOf(NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION))

        val contents1 = "<cityObjectMember><gen:GenericCityObject></gen:GenericCityObject></cityObjectMember>"
        val chunk1 = Buffer.buffer(XMLHEADER + root1 + contents1 + "</" + root1.name + ">")
        val contents2 = "<cityObjectMember><bldg:Building></bldg:Building></cityObjectMember>"
        val chunk2 = Buffer.buffer(XMLHEADER + root2 + contents2 + "</" + root2.name + ">")

        val cm1 = XMLChunkMeta(listOf(root1),
                XMLHEADER.length + root1.toString().length,
                chunk1.length() - root1.name.length - 3)
        val cm2 = XMLChunkMeta(listOf(root2),
                XMLHEADER.length + root2.toString().length,
                chunk2.length() - root2.name.length - 3)

        val expectedRoot = XMLStartElement(null, "CityModel",
                arrayOf("", "gml", "gen", XSI, "bldg"),
                arrayOf(NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_GENERICS, NS_SCHEMA_INSTANCE, NS_CITYGML_1_0_BUILDING),
                arrayOf(XSI),
                arrayOf(SCHEMA_LOCATION),
                arrayOf("$NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION $NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION"))

        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
                expectedRoot.toString() + contents1 + contents2 + "</" + expectedRoot.name + ">",
                optimistic, expected)
    }

    /**
     * Test if chunks with different namespaces can be merged
     * @param context the Vert.x test context
     */
    @Test
    fun mergeNamespaces(context: TestContext) {
        mergeNamespaces(context, false, null)
    }

    /**
     * Make sure chunks with different namespaces cannot be merged in
     * optimistic mode
     * @param context the Vert.x test context
     */
    @Test
    fun mergeNamespacesOptimistic(context: TestContext) {
        mergeNamespaces(context, true, IllegalStateException::class.java)
    }

    /**
     * Test if chunks with the same namespaces can be merged in optimistic mode
     * @param context the Vert.x test context
     */
    @Test
    fun mergeOptimistic(context: TestContext) {
        val root1 = XMLStartElement(null, "CityModel",
                arrayOf("", "gml", "gen", XSI),
                arrayOf(NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_GENERICS, NS_SCHEMA_INSTANCE),
                arrayOf(XSI),
                arrayOf(SCHEMA_LOCATION),
                arrayOf(NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION))

        val contents1 = "<cityObjectMember><gen:GenericCityObject></gen:GenericCityObject></cityObjectMember>"
        val chunk1 = Buffer.buffer(XMLHEADER + root1 + contents1 + "</" + root1.name + ">")
        val contents2 = "<cityObjectMember><gen:Building></gen:Building></cityObjectMember>"
        val chunk2 = Buffer.buffer(XMLHEADER + root1 + contents2 + "</" + root1.name + ">")

        val cm1 = XMLChunkMeta(listOf(root1),
                XMLHEADER.length + root1.toString().length,
                chunk1.length() - root1.name.length - 3)
        val cm2 = XMLChunkMeta(listOf(root1),
                XMLHEADER.length + root1.toString().length,
                chunk2.length() - root1.name.length - 3)

        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
                root1.toString() + contents1 + contents2 + "</" + root1.name + ">", true)
    }

    companion object {
        private val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"

        private val XSI = "xsi"
        private val SCHEMA_LOCATION = "schemaLocation"

        private val NS_CITYGML_1_0 = "http://www.opengis.net/citygml/1.0"
        private val NS_CITYGML_1_0_BUILDING = "http://www.opengis.net/citygml/building/1.0"
        private val NS_CITYGML_1_0_BUILDING_URL = "http://schemas.opengis.net/citygml/building/1.0/building.xsd"
        private val NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION =
                "$NS_CITYGML_1_0_BUILDING $NS_CITYGML_1_0_BUILDING_URL"
        private val NS_CITYGML_1_0_GENERICS = "http://www.opengis.net/citygml/generics/1.0"
        private val NS_CITYGML_1_0_GENERICS_URL = "http://schemas.opengis.net/citygml/generics/1.0/generics.xsd"
        private val NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION =
                "$NS_CITYGML_1_0_GENERICS $NS_CITYGML_1_0_GENERICS_URL"
        private val NS_GML = "http://www.opengis.net/gml"
        private val NS_SCHEMA_INSTANCE = "http://www.w3.org/2001/XMLSchema-instance"
    }
}
