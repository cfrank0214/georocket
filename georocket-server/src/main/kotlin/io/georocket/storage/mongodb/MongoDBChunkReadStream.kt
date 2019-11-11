package io.georocket.storage.mongodb

import java.nio.ByteBuffer

import com.mongodb.client.gridfs.GridFSDownloadStream

import io.georocket.storage.ChunkReadStream
import io.vertx.core.AsyncResult
import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.logging.LoggerFactory
import io.vertx.core.streams.ReadStream

/**
 * A read stream for chunks stored in MongoDB
 * @author Michel Kraemer
 */
class MongoDBChunkReadStream
/**
 * Constructs a new read stream
 * @param is the input stream containing the chunk
 * @param size the chunk's size
 * @param readBufferSize the size of the buffer used to read
 * from the input stream. This value should match the MongoDB GridFS
 * chunk size. If it doesn't unexpected read errors might occur.
 * @param context the Vert.x context
 */
(
        /**
         * The input stream containing the chunk
         */
        private val `is`: GridFSDownloadStream,
        /**
         * The chunk's size
         */
        private val size: Long,
        /**
         * The size of the buffer used to read from the input stream.
         * This value should match the MongoDB GridFS chunk size. If it
         * doesn't unexpected read errors might occur.
         */
        private val readBufferSize: Int,
        /**
         * The current Vert.x context
         */
        private val context: Context) : ChunkReadStream {

    /**
     * True if the stream is closed
     */
    private var closed: Boolean = false

    /**
     * True if the stream is paused
     */
    private var paused: Boolean = false

    /**
     * True if a read operation is currently in progress
     */
    private var readInProgress: Boolean = false

    /**
     * A handler that will be called when new data has
     * been read from the input stream
     */
    private var handler: Handler<Buffer>? = null

    /**
     * A handler that will be called when the whole chunk
     * has been read
     */
    private var endHandler: Handler<Void>? = null

    /**
     * A handler that will be called when an exception has
     * occurred while reading from the input stream
     */
    private var exceptionHandler: Handler<Throwable>? = null

    /**
     * Perform sanity checks
     */
    private fun check() {
        check(!closed) { "Read stream is closed" }
    }

    /**
     * Perform asynchronous read and call handlers accordingly
     */
    private fun doRead() {
        if (!readInProgress) {
            readInProgress = true
            val buff = Buffer.buffer(readBufferSize)
            val bb = ByteBuffer.allocate(readBufferSize)
            doRead(buff, bb, { ar ->
                if (ar.succeeded()) {
                    readInProgress = false
                    val buffer = ar.result()
                    if (buffer.length() == 0) {
                        // empty buffer represents end of file
                        handleEnd()
                    } else {
                        handleData(buffer)
                        if (!paused && handler != null) {
                            doRead()
                        }
                    }
                } else {
                    handleException(ar.cause())
                }
            })
        }
    }

    private fun doRead(writeBuff: Buffer, buff: ByteBuffer,
                       handler: (Any) -> Unit) {
        `is`.read(buff) { bytesRead, t ->
            if (t != null) {
                context.runOnContext { v -> handler.handle(Future.failedFuture(t)) }
            } else {
                if (bytesRead == -1 || !buff.hasRemaining()) {
                    // end of file or end of buffer
                    context.runOnContext { v ->
                        buff.flip()
                        writeBuff.setBytes(0, buff)
                        handler.handle(Future.succeededFuture(writeBuff))
                    }
                } else {
                    // read more bytes
                    doRead(writeBuff, buff, handler)
                }
            }
        }
    }

    /**
     * Will be called when data has been read from the stream
     * @param buffer the buffer containing the data read
     */
    private fun handleData(buffer: Buffer) {
        if (handler != null) {
            handler!!.handle(buffer)
        }
    }

    /**
     * Will be called when the end of the stream has been reached
     */
    private fun handleEnd() {
        if (endHandler != null) {
            endHandler!!.handle(null)
        }
    }

    /**
     * Will be called when an error has occurred while reading
     * @param t the error
     */
    private fun handleException(t: Throwable) {
        if (exceptionHandler != null && t is Exception) {
            exceptionHandler!!.handle(t)
        } else {
            log.error("Unhandled exception", t)
        }
    }

    override fun getSize(): Long {
        return size
    }

    override fun exceptionHandler(handler: Handler<Throwable>): ReadStream<Buffer> {
        check()
        exceptionHandler = handler
        return this
    }

    override fun handler(handler: Handler<Buffer>?): ReadStream<Buffer> {
        check()
        this.handler = handler
        if (handler != null && !paused && !closed) {
            doRead()
        }
        return this
    }

    override fun pause(): ReadStream<Buffer> {
        check()
        paused = true
        return this
    }

    override fun resume(): ReadStream<Buffer> {
        check()
        if (paused && !closed) {
            paused = false
            if (handler != null) {
                doRead()
            }
        }
        return this
    }

    override fun fetch(amount: Long): ReadStream<Buffer> {
        return resume()
    }

    override fun endHandler(endHandler: Handler<Void>): ReadStream<Buffer> {
        check()
        this.endHandler = endHandler
        return this
    }

    override fun close(handler: Handler<GridFSDownloadStream<Void>>?) {
        check()
        closed = true
        `is`.close { r, t ->
            val res = Future.future<Void>()
            if (t != null) {
                res.fail(t)
            } else {
                res.complete(r)
            }
            if (handler != null) {
                context.runOnContext { v -> handler.handle(res) }
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(MongoDBChunkReadStream::class.java)
    }
}
