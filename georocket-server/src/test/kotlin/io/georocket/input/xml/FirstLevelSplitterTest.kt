package io.georocket.input.xml

import org.junit.Assert.assertEquals

import java.nio.charset.StandardCharsets
import java.util.ArrayList
import java.util.Arrays

import org.junit.Test

import com.fasterxml.aalto.AsyncByteArrayFeeder
import com.fasterxml.aalto.AsyncXMLInputFactory
import com.fasterxml.aalto.AsyncXMLStreamReader
import com.fasterxml.aalto.stax.InputFactoryImpl

import io.georocket.input.Splitter.Result
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.Window
import io.georocket.util.XMLStartElement
import io.georocket.util.XMLStreamEvent
import io.vertx.core.buffer.Buffer

/**
 * Test the [FirstLevelSplitter]
 * @author Michel Kraemer
 */
class FirstLevelSplitterTest {

    /**
     * Use the [FirstLevelSplitter] and split an XML string
     * @param xml the XML string
     * @return the chunks created by the splitter
     * @throws Exception if the XML string could not be parsed
     */
    @Throws(Exception::class)
    private fun split(xml: String): List<Result<XMLChunkMeta>> {
        val window = Window()
        window.append(Buffer.buffer(xml))
        val xmlInputFactory = InputFactoryImpl()
        val reader = xmlInputFactory.createAsyncForByteArray()
        val xmlBytes = xml.toByteArray(StandardCharsets.UTF_8)
        reader.inputFeeder.feedInput(xmlBytes, 0, xmlBytes.size)
        val splitter = FirstLevelSplitter(window)
        val chunks = ArrayList<Result<XMLChunkMeta>>()
        while (reader.hasNext()) {
            val event = reader.next()
            if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
                reader.close()
                continue
            }
            val pos = reader.location.characterOffset
            val chunk = splitter.onEvent(
                    XMLStreamEvent(event, pos, reader))
            if (chunk != null) {
                chunks.add(chunk)
            }
        }
        return chunks
    }

    /**
     * Test if an XML string with one chunk can be split
     * @throws Exception if an error has occurred
     */
    @Test
    @Throws(Exception::class)
    fun oneChunk() {
        val xml = "$XMLHEADER<root>\n<object><child></child></object>\n</root>"
        val chunks = split(xml)
        assertEquals(1, chunks.size.toLong())
        val chunk = chunks[0]
        val meta = XMLChunkMeta(Arrays.asList(XMLStartElement("root")),
                XMLHEADER.length + 7, xml.length - 8)
        assertEquals(meta, chunk.meta)
        assertEquals(xml, chunk.chunk)
    }

    /**
     * Test if an XML string with tow chunks can be split
     * @throws Exception if an error has occurred
     */
    @Test
    @Throws(Exception::class)
    fun twoChunks() {
        val xml = (XMLHEADER + "<root><object><child></child></object>"
                + "<object><child2></child2></object></root>")
        val chunks = split(xml)
        assertEquals(2, chunks.size.toLong())
        val chunk1 = chunks[0]
        val chunk2 = chunks[1]
        val parents = Arrays.asList(XMLStartElement("root"))
        val meta1 = XMLChunkMeta(parents,
                XMLHEADER.length + 7, XMLHEADER.length + 7 + 32)
        val meta2 = XMLChunkMeta(parents,
                XMLHEADER.length + 7, XMLHEADER.length + 7 + 34)
        assertEquals(meta1, chunk1.meta)
        assertEquals(meta2, chunk2.meta)
        assertEquals("$XMLHEADER<root>\n<object><child></child></object>\n</root>", chunk1.chunk)
        assertEquals("$XMLHEADER<root>\n<object><child2></child2></object>\n</root>", chunk2.chunk)
    }

    /**
     * Test if an XML string with two chunks and a namespace can be split
     * @throws Exception if an error has occurred
     */
    @Test
    @Throws(Exception::class)
    fun namespace() {
        val root = "<root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\">"
        val xml = (XMLHEADER + root + "<p:object><p:child></p:child></p:object>"
                + "<p:object><child2></child2></p:object></root>")
        val chunks = split(xml)
        assertEquals(2, chunks.size.toLong())
        val chunk1 = chunks[0]
        val chunk2 = chunks[1]
        val parents = Arrays.asList(XMLStartElement(null, "root",
                arrayOf("", "p"), arrayOf("http://example.com", "http://example.com")))
        val meta1 = XMLChunkMeta(parents,
                XMLHEADER.length + root.length + 1, XMLHEADER.length + root.length + 1 + 40)
        val meta2 = XMLChunkMeta(parents,
                XMLHEADER.length + root.length + 1, XMLHEADER.length + root.length + 1 + 38)
        assertEquals(meta1, chunk1.meta)
        assertEquals(meta2, chunk2.meta)
        assertEquals("$XMLHEADER$root\n<p:object><p:child></p:child></p:object>\n</root>", chunk1.chunk)
        assertEquals("$XMLHEADER$root\n<p:object><child2></child2></p:object>\n</root>", chunk2.chunk)
    }

    /**
     * Test if an XML string with two chunks and a attributes can be split
     * @throws Exception if an error has occurred
     */
    @Test
    @Throws(Exception::class)
    fun attributes() {
        val root = "<root key=\"value\" key2=\"value2\">"
        val xml = (XMLHEADER + root + "<object ok=\"ov\"><child></child></object>"
                + "<object><child2></child2></object></root>")
        val chunks = split(xml)
        assertEquals(2, chunks.size.toLong())
        val chunk1 = chunks[0]
        val chunk2 = chunks[1]
        val parents = Arrays.asList(XMLStartElement(null, "root",
                arrayOf("", ""), arrayOf("key", "key2"), arrayOf("value", "value2")))
        val meta1 = XMLChunkMeta(parents,
                XMLHEADER.length + root.length + 1, XMLHEADER.length + root.length + 1 + 40)
        val meta2 = XMLChunkMeta(parents,
                XMLHEADER.length + root.length + 1, XMLHEADER.length + root.length + 1 + 34)
        assertEquals(meta1, chunk1.meta)
        assertEquals(meta2, chunk2.meta)
        assertEquals("$XMLHEADER$root\n<object ok=\"ov\"><child></child></object>\n</root>", chunk1.chunk)
        assertEquals("$XMLHEADER$root\n<object><child2></child2></object>\n</root>", chunk2.chunk)
    }

    /**
     * Test if an XML string with two chunks, a namespace and attributes can be split
     * @throws Exception if an error has occurred
     */
    @Test
    @Throws(Exception::class)
    fun full() {
        val root = "<root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\" key=\"value\" key2=\"value2\">"
        val xml = (XMLHEADER + root + "<p:object ok=\"ov\"><p:child></p:child></p:object>"
                + "<p:object><child2></child2></p:object></root>")
        val chunks = split(xml)
        assertEquals(2, chunks.size.toLong())
        val chunk1 = chunks[0]
        val chunk2 = chunks[1]
        val parents = Arrays.asList(XMLStartElement(null, "root",
                arrayOf("", "p"), arrayOf("http://example.com", "http://example.com"),
                arrayOf("", ""), arrayOf("key", "key2"), arrayOf("value", "value2")))
        val meta1 = XMLChunkMeta(parents,
                XMLHEADER.length + root.length + 1, XMLHEADER.length + root.length + 1 + 48)
        val meta2 = XMLChunkMeta(parents,
                XMLHEADER.length + root.length + 1, XMLHEADER.length + root.length + 1 + 38)
        assertEquals(meta1, chunk1.meta)
        assertEquals(meta2, chunk2.meta)
        assertEquals("$XMLHEADER$root\n<p:object ok=\"ov\"><p:child></p:child></p:object>\n</root>", chunk1.chunk)
        assertEquals("$XMLHEADER$root\n<p:object><child2></child2></p:object>\n</root>", chunk2.chunk)
    }

    /**
     * Test if an XML string with an UTF8 character can be split
     * @throws Exception if an error has occurred
     */
    @Test
    @Throws(Exception::class)
    fun utf8() {
        val xml = "$XMLHEADER<root>\n<object><child name=\"\u2248\"></child></object>\n</root>"
        val chunks = split(xml)
        assertEquals(1, chunks.size.toLong())
        val chunk = chunks[0]
        val meta = XMLChunkMeta(Arrays.asList(XMLStartElement("root")),
                XMLHEADER.length + 7, xml.toByteArray(StandardCharsets.UTF_8).size - 8)
        assertEquals(meta, chunk.meta)
        assertEquals(xml, chunk.chunk)
    }

    companion object {
        private val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
    }
}
