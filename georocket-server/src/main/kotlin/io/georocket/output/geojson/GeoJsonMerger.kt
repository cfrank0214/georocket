package io.georocket.output.geojson

import io.georocket.output.Merger
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.GeoJsonChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import rx.Completable

/**
 * Merges chunks to valid GeoJSON documents
 * @author Michel Kraemer
 */
class GeoJsonMerger
/**
 * Create a new merger
 * @param optimistic `true` if chunks should be merged optimistically
 * without prior initialization. In this mode, the merger will always return
 * FeatureCollections.
 */
(optimistic: Boolean) : Merger<GeoJsonChunkMeta> {
    // CHECKSTYLE:ON

    /**
     * `true` if [.merge]
     * has been called at least once
     */
    private var mergeStarted = false

    /**
     * True if the header has already been written in
     * [.merge]
     */
    private var headerWritten = false

    /**
     * The GeoJSON object type the merged result should have
     */
    private var mergedType = NOT_SPECIFIED

    init {
        if (optimistic) {
            mergedType = FEATURE_COLLECTION
        }
    }

    /**
     * Write the header
     * @param out the output stream to write to
     */
    private fun writeHeader(out: WriteStream<Buffer>) {
        if (mergedType == FEATURE_COLLECTION) {
            out.write(Buffer.buffer("{\"type\":\"FeatureCollection\",\"features\":["))
        } else if (mergedType == GEOMETRY_COLLECTION) {
            out.write(Buffer.buffer("{\"type\":\"GeometryCollection\",\"geometries\":["))
        }
    }

    override fun init(meta: GeoJsonChunkMeta): Completable {
        if (mergeStarted) {
            return Completable.error(IllegalStateException("You cannot " + "initialize the merger anymore after merging has begun"))
        }

        if (mergedType == FEATURE_COLLECTION) {
            // shortcut: we don't need to analyse the other chunks anymore,
            // we already reached the most generic type
            return Completable.complete()
        }

        // calculate the type of the merged document
        if ("Feature" == meta.type) {
            mergedType = TRANSITIONS[mergedType][0]
        } else {
            mergedType = TRANSITIONS[mergedType][1]
        }

        return Completable.complete()
    }

    override fun merge(chunk: ChunkReadStream, meta: GeoJsonChunkMeta,
                       out: WriteStream<Buffer>): Completable {
        mergeStarted = true

        if (!headerWritten) {
            writeHeader(out)
            headerWritten = true
        } else {
            if (mergedType == FEATURE_COLLECTION || mergedType == GEOMETRY_COLLECTION) {
                out.write(Buffer.buffer(","))
            } else {
                return Completable.error(IllegalStateException(
                        "Trying to merge two or more chunks but the merger has only been " + "initialized with one chunk."))
            }
        }

        // check if we have to wrap a geometry into a feature
        val wrap = mergedType == FEATURE_COLLECTION && "Feature" != meta.type
        if (wrap) {
            out.write(Buffer.buffer("{\"type\":\"Feature\",\"geometry\":"))
        }

        return writeChunk(chunk, meta, out)
                .doOnCompleted {
                    if (wrap) {
                        out.write(Buffer.buffer("}"))
                    }
                }
    }

    override fun finish(out: WriteStream<Buffer>) {
        if (mergedType == FEATURE_COLLECTION || mergedType == GEOMETRY_COLLECTION) {
            out.write(Buffer.buffer("]}"))
        }
    }

    companion object {
        private val NOT_SPECIFIED = 0
        private val GEOMETRY = 1
        private val GEOMETRY_COLLECTION = 2
        private val FEATURE = 3
        private val FEATURE_COLLECTION = 4

        // CHECKSTYLE:OFF
        private val TRANSITIONS = arrayOf(
                /*                          FEATURE            | GEOMETRY            */
                /* NOT_SPECIFIED       */ intArrayOf(FEATURE, GEOMETRY),
                /* GEOMETRY            */ intArrayOf(FEATURE_COLLECTION, GEOMETRY_COLLECTION),
                /* GEOMETRY_COLLECTION */ intArrayOf(FEATURE_COLLECTION, GEOMETRY_COLLECTION),
                /* FEATURE             */ intArrayOf(FEATURE_COLLECTION, FEATURE_COLLECTION),
                /* FEATURE_COLLECTION  */ intArrayOf(FEATURE_COLLECTION, FEATURE_COLLECTION))
    }
}
