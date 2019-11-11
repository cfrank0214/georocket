package io.georocket.index.generic

import org.junit.Assert.assertEquals

import org.geotools.referencing.CRS
import org.junit.Test

import io.georocket.index.Indexer
import io.georocket.query.QueryCompiler.MatchPriority
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Test the [BoundingBoxIndexerFactory]
 * @author Tim Hellhake
 */
class BoundingBoxIndexerFactoryTest {
    private class BoundingBoxIndexerFactoryImpl : BoundingBoxIndexerFactory() {
        override fun createIndexer(): Indexer? {
            return null
        }
    }

    /**
     * Test if the factory returns NONE for invalid queries
     */
    @Test
    fun testInvalid() {
        val factory = BoundingBoxIndexerFactoryImpl()

        assertEquals(MatchPriority.NONE, factory.getQueryPriority(""))
        assertEquals(MatchPriority.NONE, factory.getQueryPriority("42"))
    }

    /**
     * Test if the factory compiles simple queries
     */
    @Test
    fun testQuery() {
        val point = "3477534.683,5605739.857"
        val query = "$point,$point"
        val factory = BoundingBoxIndexerFactoryImpl()

        assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query))
        val destination = doubleArrayOf(3477534.683, 5605739.857, 3477534.683, 5605739.857)
        testQuery(factory.compileQuery(query)!!, destination)
    }

    /**
     * Test if the factory compiles EPSG queries
     */
    @Test
    fun testEPSG() {
        val point = "3477534.683,5605739.857"
        val query = "EPSG:31467:$point,$point"

        val factory = BoundingBoxIndexerFactoryImpl()
        assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query))
        assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query.toLowerCase()))
        val destination = doubleArrayOf(8.681739535269804, 50.58691850210496, 8.681739535269804, 50.58691850210496)
        testQuery(factory.compileQuery(query)!!, destination)
        testQuery(factory.compileQuery(query.toLowerCase())!!, destination)
    }

    /**
     * Test if the factory uses the configured default CRS code
     */
    @Test
    fun testEPSGDefault() {
        val point = "3477534.683,5605739.857"
        val query = "$point,$point"

        val factory = BoundingBoxIndexerFactoryImpl()
        factory.setDefaultCrs("EPSG:31467")
        assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query))
        assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query.toLowerCase()))
        val destination = doubleArrayOf(8.681739535269804, 50.58691850210496, 8.681739535269804, 50.58691850210496)
        testQuery(factory.compileQuery(query)!!, destination)
        testQuery(factory.compileQuery(query.toLowerCase())!!, destination)
    }

    /**
     * Test if CRS codes in queries have priority over the configured default CRS
     */
    @Test
    fun testEPSGDefaultQueryOverride() {
        val point = "3477534.683,5605739.857"
        val query = "EPSG:31467:$point,$point"

        val factory = BoundingBoxIndexerFactoryImpl()
        factory.setDefaultCrs("invalid string")
        assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query))
        val destination = doubleArrayOf(8.681739535269804, 50.58691850210496, 8.681739535269804, 50.58691850210496)
        testQuery(factory.compileQuery(query)!!, destination)
    }

    /**
     * Test if the factory uses the configured default CRS WKT
     * @throws Exception if the test fails
     */
    @Test
    @Throws(Exception::class)
    fun testWKTDefault() {
        val wkt = CRS.decode("epsg:31467").toWKT()
        val point = "3477534.683,5605739.857"
        val query = "$point,$point"

        val factory = BoundingBoxIndexerFactoryImpl()
        factory.setDefaultCrs(wkt)
        val destination = doubleArrayOf(8.681739535269804, 50.58691850210496, 8.681739535269804, 50.58691850210496)
        testQuery(factory.compileQuery(query)!!, destination)
    }

    /**
     * Test if query contains correct coordinates
     * @param jsonQuery the query
     * @param destination the expected coordinates
     */
    private fun testQuery(jsonQuery: JsonObject, destination: DoubleArray) {
        val coordinates = jsonQuery
                .getJsonObject("geo_shape")
                .getJsonObject("bbox")
                .getJsonObject("shape")
                .getJsonArray("coordinates")

        val first = coordinates.getJsonArray(0)
        assertEquals(destination[0], first.getDouble(0)!!, 0.0001)
        assertEquals(destination[1], first.getDouble(1)!!, 0.0001)
        val second = coordinates.getJsonArray(1)
        assertEquals(destination[2], second.getDouble(0)!!, 0.0001)
        assertEquals(destination[3], second.getDouble(1)!!, 0.0001)
    }
}
