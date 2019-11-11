package io.georocket.output.xml

import java.util.Arrays

import org.junit.Test
import org.junit.runner.RunWith

import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
import io.georocket.util.io.BufferWriteStream
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner

/**
 * Test [AllSameStrategy]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class AllSameStrategyTest {

    private val chunk1 = Buffer.buffer("$XMLHEADER<root><test chunk=\"1\"></test></root>")
    private val chunk2 = Buffer.buffer("$XMLHEADER<root><test chunk=\"2\"></test></root>")
    private val cm = XMLChunkMeta(Arrays.asList(XMLStartElement("root")),
            XMLHEADER.length + 6, chunk1.length() - 7)

    /**
     * Test a simple merge
     * @param context the test context
     */
    @Test
    fun simple(context: TestContext) {
        val async = context.async()
        val strategy = AllSameStrategy()
        val bws = BufferWriteStream()
        strategy.init(cm)
                .andThen(strategy.init(cm))
                .andThen(strategy.merge(DelegateChunkReadStream(chunk1), cm, bws))
                .andThen(strategy.merge(DelegateChunkReadStream(chunk2), cm, bws))
                .doOnCompleted { strategy.finish(bws) }
                .subscribe({
                    context.assertEquals("$XMLHEADER<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>",
                            bws.buffer.toString("utf-8"))
                    async.complete()
                }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if chunks that have not been passed to the initalize method can be merged
     * @param context the test context
     */
    @Test
    fun mergeUninitialized(context: TestContext) {
        val async = context.async()
        val strategy = AllSameStrategy()
        val bws = BufferWriteStream()
        strategy.init(cm)
                // skip second init
                .andThen(strategy.merge(DelegateChunkReadStream(chunk1), cm, bws))
                .andThen(strategy.merge(DelegateChunkReadStream(chunk2), cm, bws))
                .doOnCompleted { strategy.finish(bws) }
                .subscribe({
                    context.assertEquals("$XMLHEADER<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>",
                            bws.buffer.toString("utf-8"))
                    async.complete()
                }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if canMerge works correctly
     * @param context the test context
     */
    @Test
    fun canMerge(context: TestContext) {
        val cm2 = XMLChunkMeta(Arrays.asList(XMLStartElement("other")), 10, 20)
        val cm3 = XMLChunkMeta(Arrays.asList(XMLStartElement("pre", "root")), 10, 20)
        val cm4 = XMLChunkMeta(Arrays.asList(XMLStartElement(null, "root",
                arrayOf(""), arrayOf("uri"))), 10, 20)

        val async = context.async()
        val strategy = AllSameStrategy()
        strategy.canMerge(cm)
                .doOnSuccess(Action1<Boolean> { context.assertTrue(it) })
                .flatMapCompletable { v -> strategy.init(cm) }
                .andThen(strategy.canMerge(cm))
                .doOnSuccess(Action1<Boolean> { context.assertTrue(it) })
                .flatMap { v -> strategy.canMerge(cm2) }
                .doOnSuccess(Action1<Boolean> { context.assertFalse(it) })
                .flatMap { v -> strategy.canMerge(cm3) }
                .doOnSuccess(Action1<Boolean> { context.assertFalse(it) })
                .flatMap { v -> strategy.canMerge(cm4) }
                .doOnSuccess(Action1<Boolean> { context.assertFalse(it) })
                .subscribe({ v -> async.complete() }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Test if the merge method fails if it is called with an unexpected chunk
     * @param context the test context
     */
    @Test
    fun mergeFail(context: TestContext) {
        val cm2 = XMLChunkMeta(Arrays.asList(XMLStartElement("other")), 10, 20)

        val async = context.async()
        val strategy = AllSameStrategy()
        val bws = BufferWriteStream()
        strategy.init(cm)
                .andThen(strategy.merge(DelegateChunkReadStream(chunk2), cm2, bws))
                .subscribe(Action0 { context.fail() }, { err ->
                    context.assertTrue(err is IllegalStateException)
                    async.complete()
                })
    }

    companion object {
        private val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
    }
}
