package io.georocket.input.geojson

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull

import java.io.IOException
import java.io.InputStream
import java.util.ArrayList

import org.apache.commons.io.IOUtils
import org.jooq.lambda.tuple.Tuple
import org.jooq.lambda.tuple.Tuple2
import org.junit.Test

import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.util.JsonParserTransformer
import io.georocket.util.StringWindow
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import rx.Observable

/**
 * Test for [GeoJsonSplitter]
 * @author Michel Kraemer
 */
class GeoJsonSplitterTest {
    @Throws(IOException::class)
    private fun getFileSize(file: String): Long {
        GeoJsonSplitterTest::class.java.getResourceAsStream(file).use { `is` -> return IOUtils.skip(`is`, java.lang.Long.MAX_VALUE) }
    }

    @Throws(IOException::class)
    private fun split(file: String): List<Tuple2<GeoJsonChunkMeta, JsonObject>> {
        val json = IOUtils.toByteArray(GeoJsonSplitterTest::class.java.getResource(file))
        val chunks = ArrayList<Tuple2<GeoJsonChunkMeta, JsonObject>>()
        val window = StringWindow()
        val splitter = GeoJsonSplitter(window)
        Observable.just(json)
                .map<Buffer>(Func1<ByteArray, Buffer> { Buffer.buffer(it) })
                .doOnNext(Action1<Buffer> { window.append(it) })
                .compose<JsonStreamEvent>(JsonParserTransformer())
                .flatMap<Result<JsonChunkMeta>>(Func1<JsonStreamEvent, Observable<out Result<JsonChunkMeta>>> { splitter.onEventObservable(it) })
                .toBlocking()
                .forEach { result ->
                    val o = JsonObject(result.chunk)
                    chunks.add(Tuple.tuple(result.meta as GeoJsonChunkMeta, o))
                }
        return chunks
    }

    /**
     * Test if a Feature can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun feature() {
        val filename = "feature.json"
        val size = getFileSize(filename)

        val chunks = split(filename)
        assertEquals(1, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertNull(m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(size, m1.end.toLong())
        assertEquals("Feature", m1.type)

        val o1 = t1.v2
        assertEquals("Feature", o1.getString("type"))
        assertEquals("Fraunhofer IGD", o1.getJsonObject("properties").getString("name"))
    }

    /**
     * Test if a Feature with a greek property can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun greek() {
        val filename = "greek.json"
        val size = getFileSize(filename)

        val chunks = split(filename)
        assertEquals(1, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertNull(m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(size, m1.end.toLong())
        assertEquals("Feature", m1.type)

        val o1 = t1.v2
        assertEquals("Feature", o1.getString("type"))
        assertEquals("\u03a1\u039f\u0394\u0391\u039a\u0399\u039d\u0399\u0395\u03a3 " + "\u039c\u0395\u03a4\u0391\u03a0\u039f\u0399\u0397\u03a3\u0397\u03a3",
                o1.getJsonObject("properties").getString("name"))
    }

    /**
     * Test if a FeatureCollection can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun featureCollection() {
        val filename = "featurecollection.json"
        val chunks = split(filename)
        assertEquals(2, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertEquals("features", m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(307, m1.end.toLong())
        assertEquals("Feature", m1.type)

        val o1 = t1.v2
        assertEquals("Feature", o1.getString("type"))
        assertEquals("Fraunhofer IGD", o1.getJsonObject("properties").getString("name"))

        val t2 = chunks[1]

        val m2 = t2.v1
        assertEquals("features", m2.parentFieldName)
        assertEquals(0, m2.start.toLong())
        assertEquals(305, m2.end.toLong())
        assertEquals("Feature", m2.type)

        val o2 = t2.v2
        assertEquals("Feature", o2.getString("type"))
        assertEquals("Darmstadtium", o2.getJsonObject("properties").getString("name"))
    }

    /**
     * Test if a GeometryCollection can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun geometryCollection() {
        val filename = "geometrycollection.json"
        val chunks = split(filename)
        assertEquals(2, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertEquals("geometries", m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(132, m1.end.toLong())
        assertEquals("Point", m1.type)

        val o1 = t1.v2
        assertEquals("Point", o1.getString("type"))
        assertEquals(8.6599, o1.getJsonArray("coordinates").getDouble(0)!!.toDouble(), 0.00001)

        val t2 = chunks[1]

        val m2 = t2.v1
        assertEquals("geometries", m2.parentFieldName)
        assertEquals(0, m2.start.toLong())
        assertEquals(132, m2.end.toLong())
        assertEquals("Point", m2.type)

        val o2 = t2.v2
        assertEquals("Point", o2.getString("type"))
        assertEquals(8.6576, o2.getJsonArray("coordinates").getDouble(0)!!.toDouble(), 0.00001)
    }

    /**
     * Test if a LineString can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun lineString() {
        val filename = "linestring.json"
        val size = getFileSize(filename)

        val chunks = split(filename)
        assertEquals(1, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertNull(m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(size, m1.end.toLong())
        assertEquals("LineString", m1.type)

        val o1 = t1.v2
        assertEquals("LineString", o1.getString("type"))
        assertEquals(13, o1.getJsonArray("coordinates").size().toLong())
    }

    /**
     * Test if a MultiLineString can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun muliLineString() {
        val filename = "multilinestring.json"
        val size = getFileSize(filename)

        val chunks = split(filename)
        assertEquals(1, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertNull(m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(size, m1.end.toLong())
        assertEquals("MultiLineString", m1.type)

        val o1 = t1.v2
        assertEquals("MultiLineString", o1.getString("type"))
        assertEquals(3, o1.getJsonArray("coordinates").size().toLong())
    }

    /**
     * Test if a MultiPoint can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun multiPoint() {
        val filename = "multipoint.json"
        val size = getFileSize(filename)

        val chunks = split(filename)
        assertEquals(1, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertNull(m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(size, m1.end.toLong())
        assertEquals("MultiPoint", m1.type)

        val o1 = t1.v2
        assertEquals("MultiPoint", o1.getString("type"))
        assertEquals(2, o1.getJsonArray("coordinates").size().toLong())
    }

    /**
     * Test if a MultiPolygon can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun multiPolygon() {
        val filename = "multipolygon.json"
        val size = getFileSize(filename)

        val chunks = split(filename)
        assertEquals(1, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertNull(m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(size, m1.end.toLong())
        assertEquals("MultiPolygon", m1.type)

        val o1 = t1.v2
        assertEquals("MultiPolygon", o1.getString("type"))
        assertEquals(1, o1.getJsonArray("coordinates").size().toLong())
        assertEquals(1, o1.getJsonArray("coordinates").getJsonArray(0).size().toLong())
        assertEquals(13, o1.getJsonArray("coordinates").getJsonArray(0)
                .getJsonArray(0).size().toLong())
    }

    /**
     * Test if a Point can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun point() {
        val filename = "point.json"
        val size = getFileSize(filename)

        val chunks = split(filename)
        assertEquals(1, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertNull(m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(size, m1.end.toLong())
        assertEquals("Point", m1.type)

        val o1 = t1.v2
        assertEquals("Point", o1.getString("type"))
        assertEquals(2, o1.getJsonArray("coordinates").size().toLong())
    }

    /**
     * Test if a Polygon can be split correctly
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun polygon() {
        val filename = "polygon.json"
        val size = getFileSize(filename)

        val chunks = split(filename)
        assertEquals(1, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertNull(m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(size, m1.end.toLong())
        assertEquals("Polygon", m1.type)

        val o1 = t1.v2
        assertEquals("Polygon", o1.getString("type"))
        assertEquals(1, o1.getJsonArray("coordinates").size().toLong())
        assertEquals(13, o1.getJsonArray("coordinates").getJsonArray(0).size().toLong())
    }

    /**
     * Test if extra attributes not part of the standard are ignored
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun extraAttributes() {
        val filename = "featurecollectionext.json"
        val chunks = split(filename)
        assertEquals(2, chunks.size.toLong())

        val t1 = chunks[0]

        val m1 = t1.v1
        assertEquals("features", m1.parentFieldName)
        assertEquals(0, m1.start.toLong())
        assertEquals(307, m1.end.toLong())
        assertEquals("Feature", m1.type)

        val o1 = t1.v2
        assertEquals("Feature", o1.getString("type"))
        assertEquals("Fraunhofer IGD", o1.getJsonObject("properties").getString("name"))

        val t2 = chunks[1]

        val m2 = t2.v1
        assertEquals("features", m2.parentFieldName)
        assertEquals(0, m2.start.toLong())
        assertEquals(305, m2.end.toLong())
        assertEquals("Feature", m2.type)

        val o2 = t2.v2
        assertEquals("Feature", o2.getString("type"))
        assertEquals("Darmstadtium", o2.getJsonObject("properties").getString("name"))
    }

    /**
     * Make sure the splitter doesn't find any chunks in an empty feature collection
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun emptyFeatureCollection() {
        val filename = "featurecollectionempty.json"
        val chunks = split(filename)
        assertEquals(0, chunks.size.toLong())
    }

    /**
     * Make sure the splitter doesn't find any chunks in an empty geometry collection
     * @throws IOException if the test file could not be read
     */
    @Test
    @Throws(IOException::class)
    fun emptyGeometryCollection() {
        val filename = "geometrycollectionempty.json"
        val chunks = split(filename)
        assertEquals(0, chunks.size.toLong())
    }
}
