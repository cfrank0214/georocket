package io.georocket.output

import io.georocket.output.geojson.GeoJsonMerger
import io.georocket.output.xml.XMLMerger
import io.georocket.storage.ChunkMeta
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.XMLChunkMeta
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import rx.Completable

/**
 *
 * A merger that either delegates to [XMLMerger] or
 * [GeoJsonMerger] depending on the types of the chunks to merge.
 *
 * For the time being the merger can only merge chunks of the same type.
 * In the future it may create an archive (e.g. a ZIP or a TAR file) containing
 * chunks of mixed types.
 * @author Michel Kraemer
 */
class MultiMerger
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
        private val optimistic: Boolean) : Merger<ChunkMeta> {
    private var xmlMerger: XMLMerger? = null
    private var geoJsonMerger: GeoJsonMerger? = null

    private fun ensureMerger(meta: ChunkMeta): Completable {
        if (meta is XMLChunkMeta) {
            if (xmlMerger == null) {
                if (geoJsonMerger != null) {
                    return Completable.error(IllegalStateException("Cannot merge " + "XML chunk into a GeoJSON document."))
                }
                xmlMerger = XMLMerger(optimistic)
            }
            return Completable.complete()
        } else if (meta is GeoJsonChunkMeta) {
            if (geoJsonMerger == null) {
                if (xmlMerger != null) {
                    return Completable.error(IllegalStateException("Cannot merge " + "GeoJSON chunk into an XML document."))
                }
                geoJsonMerger = GeoJsonMerger(optimistic)
            }
            return Completable.complete()
        }
        return Completable.error(IllegalStateException("Cannot merge "
                + "chunk of type " + meta.mimeType))
    }

    override fun init(meta: ChunkMeta): Completable {
        return ensureMerger(meta)
                .andThen(Completable.defer Completable@{
                    if (meta is XMLChunkMeta) {
                        return@Completable.defer xmlMerger !!. init meta
                    }
                    geoJsonMerger!!.init(meta as GeoJsonChunkMeta)
                })
    }

    override fun merge(chunk: ChunkReadStream, meta: ChunkMeta,
                       out: WriteStream<Buffer>): Completable {
        return ensureMerger(meta)
                .andThen(Completable.defer {
                    if (meta is XMLChunkMeta) {
                        return@Completable.defer xmlMerger !!. merge chunk, meta as XMLChunkMeta, out)
                    }
                    geoJsonMerger!!.merge(chunk, meta as GeoJsonChunkMeta, out)
                })
    }

    override fun finish(out: WriteStream<Buffer>) {
        if (xmlMerger != null) {
            xmlMerger!!.finish(out)
        }
        if (geoJsonMerger != null) {
            geoJsonMerger!!.finish(out)
        }
    }
}
