package io.georocket.util

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.util.zip.GZIPInputStream

import org.apache.commons.io.input.BOMInputStream

/**
 * Utility methods for mime types
 * @author Andrej Sajenko
 * @author Michel Kraemer
 */
object MimeTypeUtils {
    /**
     * Mime type for XML
     */
    val XML = "application/xml"

    /**
     * Mime type for JSON
     */
    val JSON = "application/json"

    /**
     *
     * Check if the given mime type belongs to another one.
     *
     * Examples:
     *
     *  * belongsTo("application/gml+xml", "application", "xml") == true
     *  * belongsTo("application/exp+xml", "application", "xml") == true
     *  * belongsTo("application/xml", "application", "xml") == true
     *  * belongsTo("application/exp+xml", "text", "xml") == false
     *  * belongsTo("application/exp+xml", "application", "json") == false
     *
     * @param mimeType the mime type
     * @param otherType the general type of the other mime type
     * @param otherStructuredSyntaxSuffix the structured syntax suffix of the
     * other subtype (subtype = example+structuredSyntaxSuffix)
     * @return true if the mime type belongs to the other one
     */
    fun belongsTo(mimeType: String, otherType: String,
                  otherStructuredSyntaxSuffix: String): Boolean {
        val mediaParts = mimeType.split("/".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        if (mediaParts.size != 2) {
            return false
        }

        val type = mediaParts[0]
        val subtype = mediaParts[1]

        if (type != otherType) {
            return false
        }

        if (subtype == otherStructuredSyntaxSuffix) {
            return true
        }

        val subtypeParts = subtype.split("\\+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        if (subtypeParts.size != 2) {
            return false
        }

        val structuredSyntaxSuffix = subtypeParts[1]
        return structuredSyntaxSuffix == otherStructuredSyntaxSuffix
    }

    /**
     * Read the first bytes of the given file and try to determine the file
     * format. Read up to 100 KB before giving up.
     * @param f the file to read
     * @param gzip true if the file is compressed with GZIP
     * @return the file format (or `null` if the format
     * could not be determined)
     * @throws IOException if the input stream could not be read
     */
    @Throws(IOException::class)
    @JvmOverloads
    fun detect(f: File, gzip: Boolean = false): String? {
        if (!f.exists()) {
            return null
        }
        var `is`: InputStream? = null
        try {
            `is` = FileInputStream(f)
            if (gzip) {
                `is` = GZIPInputStream(`is`)
            }
            BufferedInputStream(BOMInputStream(`is`)).use { bis -> return determineFileFormat(bis) }
        } finally {
            `is`?.close()
        }
    }

    /**
     * Read the first bytes of the given input stream and try to
     * determine the file format. Reset the input stream to the position
     * it had when the method was called. Read up to 100 KB before
     * giving up.
     * @param bis a buffered input stream that supports the mark and reset
     * methods
     * @return the file format (or `null` if the format
     * could not be determined)
     * @throws IOException if the input stream could not be read
     */
    @Throws(IOException::class)
    private fun determineFileFormat(bis: BufferedInputStream): String? {
        var len = 1024 * 100

        bis.mark(len)
        try {
            while (true) {
                val c = bis.read()
                --len
                if (c < 0 || len < 2) {
                    return null
                }

                if (!Character.isWhitespace(c)) {
                    if (c == '['.toInt() || c == '{'.toInt()) {
                        return JSON
                    } else if (c == '<'.toInt()) {
                        return XML
                    }
                    return null
                }
            }
        } finally {
            bis.reset()
        }
    }
}
/**
 * Read the first bytes of the given file and try to determine the file
 * format. Read up to 100 KB before giving up.
 * @param f the file to read
 * @return the file format (or `null` if the format
 * could not be determined)
 * @throws IOException if the input stream could not be read
 */
