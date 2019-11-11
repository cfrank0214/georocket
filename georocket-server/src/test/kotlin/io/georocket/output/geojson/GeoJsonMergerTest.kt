package io.georocket.output.geojson

import io.georocket.storage.ChunkReadStream
import io.georocket.storage.GeoJsonChunkMeta
import io.georocket.util.io.BufferWriteStream
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.apache.commons.lang3.tuple.Pair
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import rx.Observable

/**
 * Test [GeoJsonMerger]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class GeoJsonMergerTest {
    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    private fun doMerge(context: TestContext, chunks: Observable<Buffer>,
                        metas: Observable<GeoJsonChunkMeta>, jsonContents: String, optimistic: Boolean = false) {
        val m = GeoJsonMerger(optimistic)
        val bws = BufferWriteStream()
        val async = context.async()
        val s: Observable<GeoJsonChunkMeta>
        if (optimistic) {
            s = metas
        } else {
            s = metas.flatMapSingle { meta -> m.init(meta).toSingleDefault(meta) }
        }
        s.toList()
                .flatMap { l ->
                    chunks.map<DelegateChunkReadStream>(Func1<Buffer, DelegateChunkReadStream> { DelegateChunkReadStream(it) })
                            .zipWith<GeoJsonChunkMeta, Pair<ChunkReadStream, GeoJsonChunkMeta>>(l, Func2<DelegateChunkReadStream, GeoJsonChunkMeta, Pair<ChunkReadStream, GeoJsonChunkMeta>> { left, right -> Pair.of(left, right) })
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
     * Test if one geometry is rendered directly
     * @param context the Vert.x test context
     */
    @Test
    fun oneGeometry(context: TestContext) {
        val strChunk1 = "{\"type\":\"Polygon\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
        doMerge(context, Observable.just(chunk1), Observable.just(cm1), strChunk1)
    }

    /**
     * Test if one geometry can be merged in optimistic mode
     * @param context the Vert.x test context
     */
    @Test
    fun oneGeometryOptimistic(context: TestContext) {
        val strChunk1 = "{\"type\":\"Polygon\"}"
        val expected = "{\"type\":\"FeatureCollection\",\"features\":" +
                "[{\"type\":\"Feature\",\"geometry\":" + strChunk1 + "}]}"
        val chunk1 = Buffer.buffer(strChunk1)
        val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
        doMerge(context, Observable.just(chunk1), Observable.just(cm1), expected, true)
    }

    /**
     * Test if one feature is rendered directly
     * @param context the Vert.x test context
     */
    @Test
    fun oneFeature(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        doMerge(context, Observable.just(chunk1), Observable.just(cm1), strChunk1)
    }

    /**
     * Test if one feature can be merged in optimistic mode
     * @param context the Vert.x test context
     */
    @Test
    fun oneFeatureOptimistic(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val expected = "{\"type\":\"FeatureCollection\",\"features\":[$strChunk1]}"
        val chunk1 = Buffer.buffer(strChunk1)
        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        doMerge(context, Observable.just(chunk1), Observable.just(cm1), expected, true)
    }

    /**
     * Test if two geometries can be merged to a geometry collection
     * @param context the Vert.x test context
     */
    @Test
    fun twoGeometries(context: TestContext) {
        val strChunk1 = "{\"type\":\"Polygon\"}"
        val strChunk2 = "{\"type\":\"Point\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length())
        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
                "{\"type\":\"GeometryCollection\",\"geometries\":[$strChunk1,$strChunk2]}")
    }

    /**
     * Test if two geometries can be merged in optimistic mode
     * @param context the Vert.x test context
     */
    @Test
    fun twoGeometriesOptimistic(context: TestContext) {
        val strChunk1 = "{\"type\":\"Polygon\"}"
        val strChunk2 = "{\"type\":\"Point\"}"
        val expected = "{\"type\":\"FeatureCollection\",\"features\":" +
                "[{\"type\":\"Feature\",\"geometry\":" + strChunk1 + "}," +
                "{\"type\":\"Feature\",\"geometry\":" + strChunk2 + "}]}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length())
        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2), expected, true)
    }

    /**
     * Test if three geometries can be merged to a geometry collection
     * @param context the Vert.x test context
     */
    @Test
    fun threeGeometries(context: TestContext) {
        val strChunk1 = "{\"type\":\"Polygon\"}"
        val strChunk2 = "{\"type\":\"Point\"}"
        val strChunk3 = "{\"type\":\"MultiPoint\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val chunk3 = Buffer.buffer(strChunk3)
        val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length())
        val cm3 = GeoJsonChunkMeta("MultiPoint", "geometries", 0, chunk3.length())
        doMerge(context, Observable.just(chunk1, chunk2, chunk3), Observable.just(cm1, cm2, cm3),
                "{\"type\":\"GeometryCollection\",\"geometries\":[" + strChunk1 + "," +
                        strChunk2 + "," + strChunk3 + "]}")
    }

    /**
     * Test if two features can be merged to a feature collection
     * @param context the Vert.x test context
     */
    @Test
    fun twoFeatures(context: TestContext) {
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
     * Test if two features can be merged in optimistic mode
     * @param context the Vert.x test context
     */
    @Test
    fun twoFeaturesOptimistic(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
                "{\"type\":\"FeatureCollection\",\"features\":[$strChunk1,$strChunk2]}", true)
    }

    /**
     * Test if three features can be merged to a feature collection
     * @param context the Vert.x test context
     */
    @Test
    fun threeFeatures(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}"
        val strChunk3 = "{\"type\":\"Feature\",\"geometry\":[]}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val chunk3 = Buffer.buffer(strChunk3)
        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
        val cm3 = GeoJsonChunkMeta("Feature", "features", 0, chunk3.length())
        doMerge(context, Observable.just(chunk1, chunk2, chunk3), Observable.just(cm1, cm2, cm3),
                "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 + "," +
                        strChunk2 + "," + strChunk3 + "]}")
    }

    /**
     * Test if two geometries and a feature can be merged to a feature collection
     * @param context the Vert.x test context
     */
    @Test
    fun geometryAndFeature(context: TestContext) {
        val strChunk1 = "{\"type\":\"Polygon\"}"
        val strChunk2 = "{\"type\":\"Feature\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
                "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":" + strChunk1 + "}," +
                        strChunk2 + "]}")
    }

    /**
     * Test if two geometries and a feature can be merged to a feature collection
     * @param context the Vert.x test context
     */
    @Test
    fun featureAndGeometry(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "{\"type\":\"Polygon\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk2.length())
        doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
                "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 +
                        ",{\"type\":\"Feature\",\"geometry\":" + strChunk2 + "}]}")
    }

    /**
     * Test if two geometries and a feature can be merged to a feature collection
     * @param context the Vert.x test context
     */
    @Test
    fun twoGeometriesAndAFeature(context: TestContext) {
        val strChunk1 = "{\"type\":\"Polygon\"}"
        val strChunk2 = "{\"type\":\"Point\"}"
        val strChunk3 = "{\"type\":\"Feature\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val chunk3 = Buffer.buffer(strChunk3)
        val cm1 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length())
        val cm3 = GeoJsonChunkMeta("Feature", "features", 0, chunk3.length())
        doMerge(context, Observable.just(chunk1, chunk2, chunk3), Observable.just(cm1, cm2, cm3),
                "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":" + strChunk1 + "}," +
                        "{\"type\":\"Feature\",\"geometry\":" + strChunk2 + "}," + strChunk3 + "]}")
    }

    /**
     * Test if two geometries and a feature can be merged to a feature collection
     * @param context the Vert.x test context
     */
    @Test
    fun twoFeaturesAndAGeometry(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}"
        val strChunk3 = "{\"type\":\"Point\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val chunk3 = Buffer.buffer(strChunk3)
        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
        val cm3 = GeoJsonChunkMeta("Point", "geometries", 0, chunk3.length())
        doMerge(context, Observable.just(chunk1, chunk2, chunk3), Observable.just(cm1, cm2, cm3),
                "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 + "," + strChunk2 +
                        ",{\"type\":\"Feature\",\"geometry\":" + strChunk3 + "}]}")
    }

    /**
     * Test if the merger fails if [GeoJsonMerger.init] has
     * not been called often enough
     * @param context the Vert.x test context
     */
    @Test
    fun notEnoughInits(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)

        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())

        val m = GeoJsonMerger(false)
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

    /**
     * Test if the merger succeeds if [GeoJsonMerger.init] has
     * not been called just often enough
     * @param context the Vert.x test context
     */
    @Test
    fun enoughInits(context: TestContext) {
        val strChunk1 = "{\"type\":\"Feature\"}"
        val strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}"
        val strChunk3 = "{\"type\":\"Polygon\"}"
        val chunk1 = Buffer.buffer(strChunk1)
        val chunk2 = Buffer.buffer(strChunk2)
        val chunk3 = Buffer.buffer(strChunk3)

        val cm1 = GeoJsonChunkMeta("Feature", "features", 0, chunk1.length())
        val cm2 = GeoJsonChunkMeta("Feature", "features", 0, chunk2.length())
        val cm3 = GeoJsonChunkMeta("Polygon", "geometries", 0, chunk2.length())

        val jsonContents = "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 +
                "," + strChunk2 + ",{\"type\":\"Feature\",\"geometry\":" + strChunk3 + "}]}"

        val m = GeoJsonMerger(false)
        val bws = BufferWriteStream()
        val async = context.async()
        m.init(cm1)
                .andThen(m.init(cm2))
                .andThen(m.merge(DelegateChunkReadStream(chunk1), cm1, bws))
                .andThen(m.merge(DelegateChunkReadStream(chunk2), cm2, bws))
                .andThen(m.merge(DelegateChunkReadStream(chunk3), cm3, bws))
                .subscribe({
                    m.finish(bws)
                    context.assertEquals(jsonContents, bws.buffer.toString("utf-8"))
                    async.complete()
                }, Action1<Throwable> { context.fail(it) })
    }
}
