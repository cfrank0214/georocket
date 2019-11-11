package io.georocket.query

import org.junit.Assert.assertEquals

import java.io.IOException
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.ArrayList

import org.apache.commons.io.IOUtils
import org.junit.Test

import io.georocket.index.generic.DefaultMetaIndexerFactory
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Test [DefaultQueryCompiler]
 * @author Michel Kraemer
 */
class DefaultQueryCompilerTest {
    private fun expectFixture(fixture: String) {
        val u = this.javaClass.getResource("fixtures/$fixture.json")
        val fixtureStr: String
        try {
            fixtureStr = IOUtils.toString(u, StandardCharsets.UTF_8)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }

        val fixtureObj = JsonObject(fixtureStr)
        val query = fixtureObj.getString("query")
        val expected = fixtureObj.getJsonObject("expected")
        val queryCompilersArr = fixtureObj.getJsonArray("queryCompilers", JsonArray())
        queryCompilersArr.add(DefaultMetaIndexerFactory::class.java.name)
        val queryCompilers = ArrayList<QueryCompiler>()
        try {
            for (o in queryCompilersArr) {
                queryCompilers.add(Class.forName(o.toString()).newInstance() as QueryCompiler)
            }
        } catch (e: ReflectiveOperationException) {
            throw RuntimeException(e)
        }

        val compiler = DefaultQueryCompiler()
        compiler.setQueryCompilers(queryCompilers)
        val compiledQuery = compiler.compileQuery(query)
        if (expected != compiledQuery) {
            println(Json.encodePrettily(compiledQuery))
        }
        assertEquals(expected, compiledQuery)
    }

    /**
     * Test query with a single string
     */
    @Test
    fun string() {
        expectFixture("string")
    }

    /**
     * Test if two strings are implicitly combined using logical OR
     */
    @Test
    fun implicitOr() {
        expectFixture("implicit_or")
    }

    /**
     * Test query with a bounding box
     */
    @Test
    fun boundingBox() {
        expectFixture("bounding_box")
    }

    /**
     * Test query with a bounding box and a string
     */
    @Test
    fun boundingBoxOrString() {
        expectFixture("bounding_box_or_string")
    }

    /**
     * Test query with logical AND
     */
    @Test
    fun and() {
        expectFixture("and")
    }

    /**
     * Test query with key-value pair and operator: equal
     */
    @Test
    fun eq() {
        expectFixture("eq")
    }

    /**
     * Test query with key-value pair and operator: greater than
     */
    @Test
    fun gt() {
        expectFixture("gt")
    }

    /**
     * Test query with key-value pair and operator: greater than or equal
     */
    @Test
    fun gte() {
        expectFixture("gte")
    }

    /**
     * Test query with key-value pair and operator: less than
     */
    @Test
    fun lt() {
        expectFixture("lt")
    }

    /**
     * Test query with key-value pair and operator: less than or equal
     */
    @Test
    fun lte() {
        expectFixture("lte")
    }

    /**
     * Test query with logical NOT
     */
    @Test
    operator fun not() {
        expectFixture("not")
    }

    /**
     * Test query with logical NOT and nested EQ
     */
    @Test
    fun notEq() {
        expectFixture("not_eq")
    }

    /**
     * Test complex query
     */
    @Test
    fun complex() {
        expectFixture("complex")
    }
}
