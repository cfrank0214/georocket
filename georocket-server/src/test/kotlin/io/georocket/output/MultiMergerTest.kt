package io.georocket.output

import java.util.Arrays

import org.apache.commons.lang3.tuple.Pair
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import io.georocket.storage.ChunkMeta
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
import io.georocket.util.io.BufferWriteStream
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import rx.Observable

/**
 * Tests for [MultiMerger]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class MultiMergerTest {

    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    private fun doMerge(context: TestContext, chunks: Observable<Buffer>,
                        metas: Observable<ChunkMeta>, jsonContents: String) {
        val m = MultiMerger(false)
        val bws = BufferWriteStream()
        val async = context.async()
        metas
                .flatMapSingle { meta -> m.init(meta).toSingleDefault(meta) }
                .toList()
                .flatMap { l ->
                    chunks.map<DelegateChunkReadStream>(Func1<Buffer, DelegateChunkReadStream> { DelegateChunkReadStream(it) })
                            .zipWith<ChunkMeta, Pair<ChunkReadStream, ChunkMeta>>(l, Func2<DelegateChunkReadStream, ChunkMeta, Pair<ChunkReadStream, ChunkMeta>> { left, right -> Pair.of(left, right) })
                }
                .flatMapCompletable { p -> m.merge(p.left, p.right, bws) }
                .toCompletable()
                .subscribe({
                    m.finish(bws)
                    context.assertEquals(jsonContents, bws.buffer.toString("utf-8"))
                    async.complete()
                }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if one GeoJSON geometry is rendered directly
     * @param context the Vert.x test context
     */
    @Test
    fun geoJsonOneGeometry(context: TestContext) {
        val strChunk1 = "{\"type\":\"Polygon\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
        doMerge(context, Observable.just(chunk1), Observable.just(cm1), strChunk1)
    }

    /**
     * Test if two GeoJSON features can be merged to a feature collection
     * @param context the Vert.x test context
     */
    @Test
    fun geoJsonTwoFeatures(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
                "{\"type\":\"FeatureCollection\",\"features\":[$strChunk1,$strChunk2]}")
    }

    /**
     * Test if simple XML chunks can be merged
     * @param context the Vert.x test context
     */
    @Test
    fun xmlSimple(context: TestContext) {
        val chunk1 = Buffer.buffer("$XMLHEADER<root><test chunk=\"1\"></test></root>")
        val chunk2 = Buffer.buffer("$XMLHEADER<root><test chunk=\"2\"></test></root>")
        val cm = XMLChunkMeta(Arrays.asList(XMLStartElement("root")),
                XMLHEADER.length + 6, chunk1.length() - 7)
        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm, cm),
                "$XMLHEADER<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>")
    }

    /**
     * Test if the merger fails if chunks with a different type should be merged
     * @param context the Vert.x test context
     */
    @Test
    fun mixedInit(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "$XMLHEADER<root><test chunk=\"2\"></test></root>"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)

        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = XMLChunkMeta(Arrays.asList(XMLStartElement("root")),
                XMLHEADER.length + 6, chunk2.length() - 7)

        val m = MultiMerger(false)
        val async = context.async()
        m.init(cm1)
                .andThen(m.init(cm2))
                .subscribe(Action0 { context.fail() }, { err ->
                    context.assertTrue(err is IllegalStateException)
                    async.complete()
                })
    }

    /**
     * Test if the merger fails if chunks with a different type should be merged
     * @param context the Vert.x test context
     */
    @Test
    fun mixedMerge(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "$XMLHEADER<root><test chunk=\"2\"></test></root>"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)

        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = XMLChunkMeta(Arrays.asList(XMLStartElement("root")),
                XMLHEADER.length + 6, chunk2.length() - 7)

        val m = MultiMerger(false)
        val bws = BufferWriteStream()
        val async = context.async()
        m.init(cm1)
                .andThen(m.merge(DelegateChunkReadStream(chunk1), cm1, bws))
                .andThen(m.merge(DelegateChunkReadStream(chunk2), cm2, bws))
                .subscribe(Action0 { context.fail() }, { err ->
                    context.assertTrue(err is IllegalStateException)
                    async.complete()
                })
    }

    companion object {
        private val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
    }
}
