package io.georocket.index.xml

import com.google.common.collect.ImmutableMap
import io.georocket.util.XMLStreamEvent

import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent
import java.util.HashMap

/**
 *
 * Indexes XAL 2.0 addresses. Currently it supports the following structure:
 *
 * <pre>
 * &lt;xal:AddressDetails&gt;
 * &lt;xal:Country&gt;
 * &lt;xal:CountryName&gt;My Country&lt;/xal:CountryName&gt;
 * &lt;xal:Locality Type="Town"&gt;
 * &lt;xal:LocalityName&gt;My City&lt;/xal:LocalityName&gt;
 * &lt;xal:Thoroughfare Type="Street"&gt;
 * &lt;xal:ThoroughfareName&gt;My Street&lt;/xal:ThoroughfareName&gt;
 * &lt;xal:ThoroughfareNumber&gt;1&lt;/xal:ThoroughfareNumber&gt;
 * &lt;/xal:Thoroughfare&gt;
 * &lt;/xal:Locality&gt;
 * &lt;/xal:Country&gt;
 * &lt;/xal:AddressDetails&gt;
</pre> *
 *
 *
 * The following attributes will be extracted from this structure:
 *
 * <pre>
 * {
 * "Country": "My Country",
 * "Locality": "My City",
 * "Street": "My Street",
 * "Number": "1"
 * }
</pre> *
 *
 *
 * Note that XAL specifies a lot more attributes that are not supported at
 * the moment but will probably be in future versions.
 *
 * @author Michel Kraemer
 */
class XalAddressIndexer : XMLIndexer {

    private var currentKey: String? = null
    private val result = HashMap<String, String>()

    override fun onEvent(event: XMLStreamEvent) {
        val e = event.event
        if (e == XMLEvent.START_ELEMENT) {
            val reader = event.xmlReader
            if (NS_XAL == reader.namespaceURI) {
                when (reader.localName) {
                    "CountryName" -> currentKey = "Country"

                    "LocalityName" -> currentKey = "Locality"

                    "ThoroughfareName" -> currentKey = "Street"

                    "ThoroughfareNumber" -> currentKey = "Number"
                }
            }
        } else if (e == XMLEvent.END_ELEMENT) {
            currentKey = null
        } else if (e == XMLEvent.CHARACTERS && currentKey != null) {
            var value: String? = event.xmlReader.text
            if (value != null) {
                value = value.trim { it <= ' ' }
                if (!value.isEmpty()) {
                    result[currentKey] = value
                }
            }
        }
    }

    override fun getResult(): Map<String, Any> {
        return ImmutableMap.of<String, Any>("address", result)
    }

    companion object {
        private val NS_XAL = "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0"
    }
}
