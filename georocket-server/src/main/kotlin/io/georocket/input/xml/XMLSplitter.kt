package io.georocket.input.xml

import java.nio.charset.StandardCharsets
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Deque

import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent

import com.google.common.base.Utf8
import io.georocket.input.Splitter
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.Window
import io.georocket.util.XMLStartElement
import io.georocket.util.XMLStreamEvent

/**
 * Abstract base class for splitters that split XML streams
 * @author Michel Kraemer
 */
abstract class XMLSplitter
/**
 * Create splitter
 * @param window a buffer for incoming data
 */
(
        /**
         * A buffer for incoming data
         */
        private val window: Window) : Splitter<XMLStreamEvent, XMLChunkMeta> {
    /**
     * A marked position. See [.mark]
     */
    private var mark = -1

    /**
     * A stack keeping all encountered start elements
     */
    private val startElements = ArrayDeque<XMLStartElement>()

    /**
     * @return true if a position is marked currently
     */
    protected val isMarked: Boolean
        get() = mark >= 0

    override fun onEvent(event: XMLStreamEvent): Splitter.Result<XMLChunkMeta>? {
        val chunk = onXMLEvent(event)
        if (!isMarked) {
            if (event.event == XMLEvent.START_ELEMENT) {
                startElements.push(makeXMLStartElement(event.xmlReader))
            } else if (event.event == XMLEvent.END_ELEMENT) {
                startElements.pop()
            }
        }
        return chunk
    }

    /**
     * Creates an [XMLStartElement] from the current parser state
     * @param xmlReader the XML parser
     * @return the [XMLStartElement]
     */
    private fun makeXMLStartElement(xmlReader: XMLStreamReader): XMLStartElement {
        // copy namespaces (if there are any)
        val nc = xmlReader.namespaceCount
        var namespacePrefixes: Array<String>? = null
        var namespaceUris: Array<String>? = null
        if (nc > 0) {
            namespacePrefixes = arrayOfNulls(nc)
            namespaceUris = arrayOfNulls(nc)
            for (i in 0 until nc) {
                namespacePrefixes[i] = xmlReader.getNamespacePrefix(i)
                namespaceUris[i] = xmlReader.getNamespaceURI(i)
            }
        }

        // copy attributes (if there are any)
        val ac = xmlReader.attributeCount
        var attributePrefixes: Array<String>? = null
        var attributeLocalNames: Array<String>? = null
        var attributeValues: Array<String>? = null
        if (ac > 0) {
            attributePrefixes = arrayOfNulls(ac)
            attributeLocalNames = arrayOfNulls(ac)
            attributeValues = arrayOfNulls(ac)
            for (i in 0 until ac) {
                attributePrefixes[i] = xmlReader.getAttributePrefix(i)
                attributeLocalNames[i] = xmlReader.getAttributeLocalName(i)
                attributeValues[i] = xmlReader.getAttributeValue(i)
            }
        }

        // make element
        return XMLStartElement(xmlReader.prefix,
                xmlReader.localName, namespacePrefixes, namespaceUris,
                attributePrefixes, attributeLocalNames, attributeValues)
    }

    /**
     * Mark a position
     * @param pos the position to mark
     */
    protected fun mark(pos: Int) {
        mark = pos
    }

    /**
     * Create a new chunk starting from the marked position and ending on the
     * given position. Reset the mark afterwards and advance the window to the
     * end position. Return a [io.georocket.input.Splitter.Result] object
     * with the new chunk and its metadata.
     * @param pos the end position
     * @return the [io.georocket.input.Splitter.Result] object
     */
    protected fun makeResult(pos: Int): Splitter.Result<XMLChunkMeta> {
        val sb = StringBuilder()
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n")

        // append the full stack of start elements (backwards)
        val chunkParents = ArrayList<XMLStartElement>()
        startElements.descendingIterator().forEachRemaining { e ->
            chunkParents.add(e)
            sb.append(e)
            sb.append("\n")
        }

        // get chunk start in bytes
        val chunkStart = Utf8.encodedLength(sb)

        // append current element
        val bytes = window.getBytes(mark, pos)
        sb.append(String(bytes, StandardCharsets.UTF_8))
        window.advanceTo(pos)
        mark = -1

        // get chunk end in bytes
        val chunkEnd = chunkStart + bytes.size

        // append the full stack of end elements
        startElements.iterator().forEachRemaining { e -> sb.append("\n</").append(e.name).append(">") }

        val meta = XMLChunkMeta(chunkParents, chunkStart, chunkEnd)
        return Splitter.Result(sb.toString(), meta)
    }

    /**
     * Will be called on every XML event
     * @param event the XML event
     * @return a new [io.georocket.input.Splitter.Result] object (containing
     * chunk and metadata) or `null` if no result was produced
     */
    protected abstract fun onXMLEvent(event: XMLStreamEvent): Splitter.Result<XMLChunkMeta>
}
