package io.georocket.index

import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Test [IndexableChunkCache]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class IndexableChunkCacheTest {
    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    /**
     * Test if a chunk is removed from the cache once it has been retrieved
     * @param ctx the current test context
     */
    @Test
    operator fun get(ctx: TestContext) {
        val c = IndexableChunkCache()
        val path = "path"
        val chunk = Buffer.buffer("CHUNK")
        c.put(path, chunk)
        ctx.assertEquals(1L, c.numberOfChunks)
        ctx.assertEquals(chunk.length().toLong(), c.getSize())
        ctx.assertEquals(chunk, c[path])
        ctx.assertEquals(0L, c.numberOfChunks)
        ctx.assertEquals(0L, c.getSize())
        ctx.assertNull(c[path])
    }

    /**
     * Test if a chunk is removed from the cache after a certain amount of seconds
     * @param ctx the current test context
     */
    @Test
    fun timeout(ctx: TestContext) {
        val c = IndexableChunkCache(1024, 1)
        val path = "path"
        val chunk = Buffer.buffer("CHUNK")
        c.put(path, chunk)
        val async = ctx.async()
        rule.vertx().setTimer(1100) { l ->
            // We need to call the get() method at least 64 times so the internal
            // Guava cache completely evicts the expired item and calls our removal
            // listener that updates the cache size.
            // See com.google.common.cache.LocalCache#DRAIN_THRESHOLD
            for (i in 0..63) {
                ctx.assertNull(c[path])
            }
            ctx.assertEquals(0L, c.getSize())
            async.complete()
        }
    }

    /**
     * Test if a chunk is removed from the cache if it exceeds the maximum size
     * @param ctx the current test context
     */
    @Test
    fun maxSize(ctx: TestContext) {
        val c = IndexableChunkCache(1024, 60)
        val path1 = "path1"
        val path2 = "path2"
        val sb = StringBuilder()
        for (i in 0..1022) {
            sb.append(('a'.toInt() + i % 26).toChar())
        }
        val chunk1 = Buffer.buffer(sb.toString() + "1")
        val chunk2 = Buffer.buffer(sb.toString() + "2")
        c.put(path1, chunk1)
        c.put(path2, chunk2)
        ctx.assertEquals(chunk1.length().toLong(), c.getSize())
        ctx.assertEquals(1L, c.numberOfChunks)
        ctx.assertNull(c[path2])
        ctx.assertEquals(chunk1.length().toLong(), c.getSize())
        ctx.assertEquals(1L, c.numberOfChunks)
        ctx.assertEquals(chunk1, c[path1])
        ctx.assertEquals(0L, c.getSize())
        ctx.assertEquals(0L, c.numberOfChunks)
    }

    /**
     * Test if a second chunk can be added after the first has been evicted due to
     * timeout if the sizes of both chunks would exceed the cache's maximum size
     * @param ctx the current test context
     */
    @Test
    fun maxSizeAndTimeout(ctx: TestContext) {
        val c = IndexableChunkCache(1024, 1)
        val path1 = "path1"
        val path2 = "path2"
        val sb = StringBuilder()
        for (i in 0..1022) {
            sb.append(('a'.toInt() + i % 26).toChar())
        }
        val chunk1 = Buffer.buffer(sb.toString())
        val chunk2 = Buffer.buffer(sb.toString() + "2")
        c.put(path1, chunk1)
        val async = ctx.async()
        rule.vertx().setTimer(1100) { l ->
            ctx.assertNull(c[path1])
            c.put(path2, chunk2)
            ctx.assertEquals(chunk2.length().toLong(), c.getSize())
            ctx.assertEquals(1L, c.numberOfChunks)
            ctx.assertEquals(chunk2, c[path2])
            ctx.assertEquals(0L, c.getSize())
            ctx.assertEquals(0L, c.numberOfChunks)
            async.complete()
        }
    }
}
