package io.georocket.output.xml

import io.georocket.output.Merger
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.XMLChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import rx.Completable

/**
 * Merges XML chunks using various strategies to create a valid XML document
 * @author Michel Kraemer
 */
class XMLMerger
/**
 * Creates a new merger
 * @param optimistic `true` if chunks should be merged optimistically
 * without prior initialization
 */
(
        /**
         * `true` if chunks should be merged optimistically without
         * prior initialization
         */
        private val optimistic: Boolean) : Merger<XMLChunkMeta> {
    /**
     * The merger strategy determined by [.init]
     */
    private var strategy: MergeStrategy? = null

    /**
     * `true` if [.merge]
     * has been called at least once
     */
    private var mergeStarted = false

    /**
     * @return the next merge strategy (depending on the current one) or
     * `null` if there is no other strategy available.
     */
    private fun nextStrategy(): MergeStrategy? {
        if (strategy == null) {
            return AllSameStrategy()
        } else if (strategy is AllSameStrategy) {
            return MergeNamespacesStrategy()
        }
        return null
    }

    override fun init(meta: XMLChunkMeta): Completable {
        if (mergeStarted) {
            return Completable.error(IllegalStateException("You cannot " + "initialize the merger anymore after merging has begun"))
        }

        if (strategy == null) {
            strategy = nextStrategy()
        }

        return strategy!!.canMerge(meta)
                .flatMapCompletable { canMerge ->
                    if (canMerge!!) {
                        // current strategy is able to handle the chunk
                        return@strategy.canMerge(meta)
                                .flatMapCompletable strategy !!. init meta
                    }

                    // current strategy cannot merge the chunk. select next one and retry.
                    val ns = nextStrategy()
                    if (ns == null) {
                        return@strategy.canMerge(meta)
                                .flatMapCompletable Completable . error UnsupportedOperationException(
                                "Cannot merge chunks. No valid strategy available.")
                    }
                    ns!!.parents = strategy!!.parents
                    strategy = ns
                    init(meta)
                }
    }

    override fun merge(chunk: ChunkReadStream, meta: XMLChunkMeta,
                       out: WriteStream<Buffer>): Completable {
        mergeStarted = true
        var c = Completable.complete()
        if (strategy == null) {
            if (optimistic) {
                strategy = AllSameStrategy()
                c = strategy!!.init(meta)
            } else {
                return Completable.error(IllegalStateException(
                        "You must call init() at least once"))
            }
        }
        return c.andThen(strategy!!.merge(chunk, meta, out))
    }

    override fun finish(out: WriteStream<Buffer>) {
        strategy!!.finish(out)
    }
}
