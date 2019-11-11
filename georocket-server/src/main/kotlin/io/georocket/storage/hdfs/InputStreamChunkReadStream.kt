package io.georocket.storage.hdfs

import java.io.InputStream

import io.georocket.storage.ChunkReadStream
import io.georocket.util.io.InputStreamReadStream
import io.vertx.core.Vertx

/**
 * A chunk read stream delegating to an input stream
 * @author Michel Kraemer
 */
class InputStreamChunkReadStream
/**
 * Constructs a new read stream
 * @param is the input stream containing the chunk
 * @param size the chunk size
 * @param vertx the Vert.x instance
 */
(`is`: InputStream, private val size: Long, vertx: Vertx) : InputStreamReadStream(`is`, vertx), ChunkReadStream {

    override fun getSize(): Long {
        return size
    }
}
