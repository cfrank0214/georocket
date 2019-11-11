package io.georocket.util

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer

import java.io.IOException
import java.time.Instant
import java.time.format.DateTimeFormatter

/**
 * A JSON deserializer that converts ISO-8601 instant strings to [Instant]
 * objects. Vert.x is already able to convert [Instant] objects to Strings
 * while serializing JSON objects but not the other way round.
 * @author Michel Kraemer
 */
class InstantDeserializer : JsonDeserializer<Instant>() {
    @Throws(IOException::class)
    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): Instant {
        return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(jp.text))
    }
}
