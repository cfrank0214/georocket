package io.georocket.util

import org.junit.Assert.assertEquals

import java.util.Arrays

import org.junit.Test

/**
 * Test [QuotedStringSplitter]
 * @author Michel Kraemer
 */
class QuotedStringSplitterTest {
    /**
     * Split a simple string
     */
    @Test
    fun noQuotes() {
        assertEquals(Arrays.asList("hello", "world"),
                QuotedStringSplitter.split("hello world"))
    }

    /**
     * Test trimming
     */
    @Test
    fun manySpaces() {
        assertEquals(Arrays.asList("hello", "world"),
                QuotedStringSplitter.split("  hello    world "))
    }

    /**
     * Test double quotes
     */
    @Test
    fun doubleQuotes() {
        assertEquals(Arrays.asList("hello world", "test"),
                QuotedStringSplitter.split("\"hello world\" test"))
    }

    /**
     * Test double quoted and trimming
     */
    @Test
    fun doubleQuotesWithSpaces() {
        assertEquals(Arrays.asList(" hello   world", "test"),
                QuotedStringSplitter.split("\" hello   world\" test"))
    }

    /**
     * Test single quotes
     */
    @Test
    fun singleQuotes() {
        assertEquals(Arrays.asList("hello world", "test"),
                QuotedStringSplitter.split("'hello world' test"))
    }

    /**
     * Test single quotes and trimming
     */
    @Test
    fun singleQuotesWithSpaces() {
        assertEquals(Arrays.asList(" hello   world", "test"),
                QuotedStringSplitter.split("' hello   world' test"))
    }

    /**
     * Test if a single single-quote is included
     */
    @Test
    fun singleSingleQuote() {
        assertEquals(Arrays.asList("that's", "cool"),
                QuotedStringSplitter.split("that's cool"))
    }

    /**
     * Test mixed single and double-quotes
     */
    @Test
    fun mixed() {
        assertEquals(Arrays.asList("'s g'", "s' ", "\"s", "s\""),
                QuotedStringSplitter.split("\"'s g'\" \"s' \" '\"s' 's\"'"))
    }

    /**
     * Test escaping
     */
    @Test
    fun escaped() {
        assertEquals(Arrays.asList("a'b", "a\"b", "\n"),
                QuotedStringSplitter.split("'a\\'b' \"a\\\"b\" \\n"))
    }
}
