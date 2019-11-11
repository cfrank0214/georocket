package io.georocket.query.parser

import org.junit.Assert.assertEquals
import org.junit.Assert.fail

import java.io.IOException
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.ArrayDeque
import java.util.Deque

import org.antlr.v4.runtime.ANTLRInputStream
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.apache.commons.io.IOUtils
import org.junit.Test

import io.georocket.query.parser.QueryParser.AndContext
import io.georocket.query.parser.QueryParser.EqContext
import io.georocket.query.parser.QueryParser.GtContext
import io.georocket.query.parser.QueryParser.GteContext
import io.georocket.query.parser.QueryParser.KeyvalueContext
import io.georocket.query.parser.QueryParser.LtContext
import io.georocket.query.parser.QueryParser.LteContext
import io.georocket.query.parser.QueryParser.NotContext
import io.georocket.query.parser.QueryParser.OrContext
import io.georocket.query.parser.QueryParser.QueryContext
import io.georocket.query.parser.QueryParser.StringContext
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Test [QueryParser]
 * @author Michel Kraemer
 */
class QueryParserTest {
    /**
     * Convert a parse tree to a JsonObject
     */
    private class ToJsonTreeListener internal constructor() : QueryBaseListener() {
        internal var tree: Deque<JsonObject> = ArrayDeque()

        init {
            push(QUERY)
        }

        private fun push(type: String, text: String? = null) {
            val obj = JsonObject().put(TYPE, type)
            if (text != null) {
                obj.put(TEXT, text)
            }
            if (!tree.isEmpty()) {
                var children: JsonArray? = tree.peek().getJsonArray(CHILDREN)
                if (children == null) {
                    children = JsonArray()
                    tree.peek().put(CHILDREN, children)
                }
                children.add(obj)
            }
            tree.push(obj)
        }

        fun enterOr(ctx: OrContext) {
            push(OR)
        }

        fun exitOr(ctx: OrContext) {
            tree.pop()
        }

        fun enterAnd(ctx: AndContext) {
            push(AND)
        }

        fun exitAnd(ctx: AndContext) {
            tree.pop()
        }

        fun enterNot(ctx: NotContext) {
            push(NOT)
        }

        fun exitNot(ctx: NotContext) {
            tree.pop()
        }

        fun enterEq(ctx: EqContext) {
            push(EQ)
        }

        fun exitEq(ctx: EqContext) {
            tree.pop()
        }

        fun enterGt(ctx: GtContext) {
            push(GT)
        }

        fun exitGt(ctx: GtContext) {
            tree.pop()
        }

        fun enterGte(ctx: GteContext) {
            push(GTE)
        }

        fun exitGte(ctx: GteContext) {
            tree.pop()
        }

        fun enterLt(ctx: LtContext) {
            push(LT)
        }

        fun exitLt(ctx: LtContext) {
            tree.pop()
        }

        fun enterLte(ctx: LteContext) {
            push(LTE)
        }

        fun exitLte(ctx: LteContext) {
            tree.pop()
        }

        fun enterKeyvalue(ctx: KeyvalueContext) {
            push(KEYVALUE)
        }

        fun exitKeyvalue(ctx: KeyvalueContext) {
            tree.pop()
        }

        fun enterString(ctx: StringContext) {
            push(STRING, ctx.getText())
        }

        fun exitString(ctx: StringContext) {
            tree.pop()
        }

        fun visitErrorNode(node: ErrorNode) {
            fail()
        }

        companion object {

            internal val TYPE = "type"
            internal val TEXT = "text"
            internal val QUERY = "query"
            internal val STRING = "string"
            internal val OR = "or"
            internal val AND = "and"
            internal val NOT = "not"
            internal val EQ = "eq"
            internal val LT = "lt"
            internal val LTE = "lte"
            internal val GT = "gt"
            internal val GTE = "gte"
            internal val KEYVALUE = "keyvalue"
            internal val CHILDREN = "children"
        }
    }

    /**
     * Load a fixture, parse and check the result
     * @param fixture the name of the fixture to load (without path and extension)
     */
    private fun expectFixture(fixture: String) {
        // load file
        val u = this.javaClass.getResource("fixtures/$fixture.json")
        val fixtureStr: String
        try {
            fixtureStr = IOUtils.toString(u, StandardCharsets.UTF_8)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }

        // get query and expected tree
        val fixtureObj = JsonObject(fixtureStr)
        val query = fixtureObj.getString("query")
        val expected = fixtureObj.getJsonObject("expected")

        // parse query
        val lexer = QueryLexer(ANTLRInputStream(query.trim { it <= ' ' }))
        val tokens = CommonTokenStream(lexer)
        val parser = QueryParser(tokens)
        val ctx = parser.query()
        val listener = ToJsonTreeListener()
        ParseTreeWalker.DEFAULT.walk(listener, ctx)

        // assert tree
        assertEquals(1, listener.tree.size.toLong())
        val root = listener.tree.pop()
        assertEquals(expected, root)
    }

    /**
     * Query with a single string
     */
    @Test
    fun string() {
        expectFixture("string")
    }

    /**
     * Query with two strings
     */
    @Test
    fun strings() {
        expectFixture("strings")
    }

    /**
     * EQuals
     */
    @Test
    fun eq() {
        expectFixture("eq")
    }

    /**
     * Greater than (GT)
     */
    @Test
    fun gt() {
        expectFixture("gt")
    }

    /**
     * Greater than or equal (GTE)
     */
    @Test
    fun gte() {
        expectFixture("gte")
    }

    /**
     * Less than (LT)
     */
    @Test
    fun lt() {
        expectFixture("lt")
    }

    /**
     * Less than or equal (LTE)
     */
    @Test
    fun lte() {
        expectFixture("lte")
    }

    /**
     * Explicit OR
     */
    @Test
    fun or() {
        expectFixture("or")
    }

    /**
     * Logical AND
     */
    @Test
    fun and() {
        expectFixture("and")
    }

    /**
     * Logical NOT
     */
    @Test
    operator fun not() {
        expectFixture("not")
    }

    /**
     * Logical NOT with nested EQ
     */
    @Test
    fun notEq() {
        expectFixture("not_eq")
    }

    /**
     * Query with a double-quoted string
     */
    @Test
    fun doubleQuotedString() {
        expectFixture("double_quoted_string")
    }

    /**
     * Query with a single-quoted string
     */
    @Test
    fun singleQuotedString() {
        expectFixture("single_quoted_string")
    }

    /**
     * Query with a quoted OR
     */
    @Test
    fun quotedOr() {
        expectFixture("quoted_or")
    }
}
