package io.georocket.util

import java.util.ArrayList
import java.util.regex.Matcher
import java.util.regex.Pattern

import org.apache.commons.text.StringEscapeUtils

/**
 * Splits strings around whitespace characters. Takes care of double-quotes
 * and single-quotes. Trims and unescapes the results.
 * @author Michel Kraemer
 */
object QuotedStringSplitter {
    private val pattern = Pattern.compile("\"((\\\\\"|[^\"])*)\"|'((\\\\'|[^'])*)'|(\\S+)")

    /**
     *
     * Splits strings around whitespace characters.
     *
     * Example:
     * <pre>
     * input string: "Hello World"
     * output:       ["Hello", "World"]
    </pre> *
     *
     *
     * Takes care of double-quotes and single-quotes.
     *
     * Example:
     * <pre>
     * input string: "\"Hello World\" 'cool'"
     * output:       ["Hello World", "cool"]
    </pre> *
     *
     *
     * Trims and unescapes the results.
     *
     * Example:
     * <pre>
     * input string: "  Hello   \"Wo\\\"rld\"  "
     * output:       ["Hello", "Wo\"rld"]
    </pre> *
     *
     * @param str the string to split
     * @return the parts
     */
    fun split(str: String): List<String> {
        val m = pattern.matcher(str)
        val result = ArrayList<String>()
        while (m.find()) {
            val r: String
            if (m.group(1) != null) {
                r = m.group(1)
            } else if (m.group(3) != null) {
                r = m.group(3)
            } else {
                r = m.group()
            }
            result.add(StringEscapeUtils.unescapeJava(r))
        }
        return result
    }
}
