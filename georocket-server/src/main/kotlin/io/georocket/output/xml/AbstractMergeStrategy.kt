package io.georocket.output.xml

import io.georocket.storage.ChunkReadStream
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import rx.Completable

/**
 * Abstract base class for XML merge strategies
 * @author Michel Kraemer
 */
abstract class AbstractMergeStrategy : MergeStrategy {

    /**
     * The XML parent elements
     */
    override var parents: List<XMLStartElement>? = null

    /**
     * True if the header has already been written in
     * [.merge]
     */
    /**
     * @return true if the header has already been write to the output stream
     */
    protected var isHeaderWritten = false
        private set

    /**
     * Merge the parent elements of a given chunk into the current parent
     * elements. Perform no checks.
     * @param meta the chunk metadata containing the parents to merge
     * @return a Completable that will complete when the parents have been merged
     */
    protected abstract fun mergeParents(meta: XMLChunkMeta): Completable

    override fun init(meta: XMLChunkMeta): Completable {
        return canMerge(meta)
                .flatMapCompletable { b ->
                    if (b!!) {
                        return@canMerge meta
                                .flatMapCompletable mergeParents meta
                    }
                    Completable.error(IllegalArgumentException(
                            "Chunk cannot be merged with this strategy"))
                }
    }

    /**
     * Write the XML header and the parent elements
     * @param out the output stream to write to
     */
    private fun writeHeader(out: WriteStream<Buffer>) {
        out.write(Buffer.buffer(XMLHEADER))
        parents!!.forEach { e -> out.write(Buffer.buffer(e.toString())) }
    }

    override fun merge(chunk: ChunkReadStream, meta: XMLChunkMeta,
                       out: WriteStream<Buffer>): Completable {
        return canMerge(meta)
                .flatMapCompletable { b ->
                    if (!b) {
                        return@canMerge meta
                                .flatMapCompletable Completable . error IllegalStateException(
                                "Chunk cannot be merged with this strategy")
                    }
                    if (!isHeaderWritten) {
                        writeHeader(out)
                        isHeaderWritten = true
                    }
                    writeChunk(chunk, meta, out)
                }
    }

    override fun finish(out: WriteStream<Buffer>) {
        // close all parent elements
        for (i in parents!!.indices.reversed()) {
            val e = parents!![i]
            out.write(Buffer.buffer("</" + e.name + ">"))
        }
    }

    companion object {
        /**
         * The default XML header written by the merger
         */
        val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
    }
}
