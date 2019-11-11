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
 * Test [MergeNamespacesStrategy]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class MergeNamespacesStrategyTest {

    /**
     * Test a simple merge
     * @param context the test context
     */
    @Test
    fun simple(context: TestContext) {
        val async = context.async()
        val strategy = MergeNamespacesStrategy()
        val bws = BufferWriteStream()
        strategy.init(META1)
                .andThen(strategy.init(META2))
                .andThen(strategy.merge(DelegateChunkReadStream(CHUNK1), META1, bws))
                .andThen(strategy.merge(DelegateChunkReadStream(CHUNK2), META2, bws))
                .doOnCompleted { strategy.finish(bws) }
                .subscribe({
                    context.assertEquals(XMLHEADER + EXPECTEDROOT + CONTENTS1 + CONTENTS2 +
                            "</" + EXPECTEDROOT.name + ">", bws.buffer.toString("utf-8"))
                    async.complete()
                }, Action1<Throwable> { context.fail(it) })
    }

    /**
     * Make sure that chunks that have not been passed to the initalize method cannot be merged
     * @param context the test context
     */
    @Test
    fun mergeUninitialized(context: TestContext) {
        val async = context.async()
        val strategy = MergeNamespacesStrategy()
        val bws = BufferWriteStream()
        strategy.init(META1)
                // skip second init
                .andThen(strategy.merge(DelegateChunkReadStream(CHUNK1), META1, bws))
                .andThen(strategy.merge(DelegateChunkReadStream(CHUNK2), META2, bws))
                .subscribe({ context.fail() }, { err ->
                    context.assertTrue(err is IllegalStateException)
                    async.complete()
                })
    }

    companion object {
        private val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"

        private val ROOT1 = XMLStartElement(null, "root",
                arrayOf("", "ns1", "xsi"),
                arrayOf("uri0", "uri1", "http://www.w3.org/2001/XMLSchema-instance"),
                arrayOf("xsi", "ns1"),
                arrayOf("schemaLocation", "attr1"),
                arrayOf("uri0 location0 uri1 location1", "value1"))
        private val ROOT2 = XMLStartElement(null, "root",
                arrayOf("", "ns2", "xsi"),
                arrayOf("uri0", "uri2", "http://www.w3.org/2001/XMLSchema-instance"),
                arrayOf("xsi", "ns2"),
                arrayOf("schemaLocation", "attr2"),
                arrayOf("uri0 location0 uri2 location2", "value2"))
        private val EXPECTEDROOT = XMLStartElement(null, "root",
                arrayOf("", "ns1", "xsi", "ns2"),
                arrayOf("uri0", "uri1", "http://www.w3.org/2001/XMLSchema-instance", "uri2"),
                arrayOf("xsi", "ns1", "ns2"),
                arrayOf("schemaLocation", "attr1", "attr2"),
                arrayOf("uri0 location0 uri1 location1 uri2 location2", "value1", "value2"))

        private val CONTENTS1 = "<elem><ns1:child1></ns1:child1></elem>"
        private val CHUNK1 = Buffer.buffer(XMLHEADER + ROOT1 + CONTENTS1 + "</" + ROOT1.name + ">")
        private val CONTENTS2 = "<elem><ns2:child2></ns2:child2></elem>"
        private val CHUNK2 = Buffer.buffer(XMLHEADER + ROOT2 + CONTENTS2 + "</" + ROOT2.name + ">")

        private val META1 = XMLChunkMeta(Arrays.asList(ROOT1),
                XMLHEADER.length + ROOT1.toString().length,
                CHUNK1.length() - ROOT1.name.length - 3)
        private val META2 = XMLChunkMeta(Arrays.asList(ROOT2),
                XMLHEADER.length + ROOT2.toString().length,
                CHUNK2.length() - ROOT2.name.length - 3)
    }
}
