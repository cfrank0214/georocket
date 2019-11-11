package io.georocket.storage.file

import io.georocket.storage.ChunkReadStream
import io.georocket.util.io.DelegateReadStream
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.AsyncFile

/**
 * A read stream for chunks
 * @author Michel Kraemer
 */
class FileChunkReadStream
/**
 * Constructs a new read stream
 * @param size the chunk's size
 * @param delegate the underlying read stream
 */
(private val size: Long, private val file: AsyncFile) : DelegateReadStream<Buffer>(file), ChunkReadStream {

    /**
     * @return the chunk's size
     */
    override fun getSize(): Long {
        return size
    }

    override fun close(handler: Handler<AsyncResult<Void>>) {
        file.close(handler)
    }
}
