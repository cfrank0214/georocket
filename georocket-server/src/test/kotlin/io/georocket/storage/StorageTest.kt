package io.georocket.storage

import java.util.Arrays

import org.bson.types.ObjectId
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import com.google.common.collect.ImmutableMap

import io.georocket.constants.AddressConstants
import io.georocket.util.PathUtils
import io.georocket.util.XMLStartElement
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner

/**
 *
 * Abstract test implementation for a [Store]
 *
 *
 * This class defines test methods for the store interface and should be
 * used as base class for all concrete Store tests.
 *
 *
 * A concrete store test implement only the data preparation and some
 * validation methods which have access to the storage system.
 *
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner::class)
abstract class StorageTest {
    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    /**
     * Create the store under test
     * @param vertx A Vert.x instance for one test
     * @return A Store
     */
    protected abstract fun createStore(vertx: Vertx): Store

    /**
     *
     * Prepare test data for (every) test. Will be called during every test.
     *
     * Heads up: use the protected attributes as test data!
     * Call context.fail(...) if the data preparation failed!
     * @param context The current test context.
     * @param vertx A Vert.x instance for one test.
     * @param path The path for the data (may be null).
     * @param handler will be called when the test data has been prepared
     */
    protected abstract fun prepareData(context: TestContext, vertx: Vertx,
                                       path: String?, handler: Handler<AsyncResult<String>>)

    /**
     *
     * Validate the add method. Will be called after the store added data.
     *
     * Heads up: look on the protected attributes of this class to know which
     * data were used for the store add method. These will be used for the
     * [Store.add] method.
     * Use context.assert ... and context.fail to validate the test.
     * @param context The current test context.
     * @param vertx A Vert.x instance of one test.
     * @param path The path where the data was created (may be null if not used
     * for [.prepareData])
     * @param handler will be called when the validation has finished
     */
    protected abstract fun validateAfterStoreAdd(context: TestContext, vertx: Vertx,
                                                 path: String?, handler: Handler<AsyncResult<Void>>)

    /**
     *
     * Validate the delete method of a test. Will be called after the store
     * deleted data.
     *
     * Heads up: look on the protected attributes and your
     * [.prepareData] implementation
     * to know which data you have deleted with the
     * [Store.delete] method.
     * Use context.assert ... and context.fail to validate the test.
     * @param context The current test context.
     * @param vertx A Vert.x instance of one test.
     * @param path The path where the data were created (may be null if not used
     * for [.prepareData])
     * @param handler will be called when the validation has finished
     */
    protected abstract fun validateAfterStoreDelete(context: TestContext,
                                                    vertx: Vertx, path: String, handler: Handler<AsyncResult<Void>>)

    private fun mockIndexerQuery(vertx: Vertx, context: TestContext, async: Async, path: String?) {
        vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_QUERY).handler { request ->
            val msg = request.body()

            context.assertTrue(msg.containsKey("size"))
            context.assertTrue(msg.containsKey("search"))

            val indexSearch = msg.getString("search")
            context.assertEquals(SEARCH, indexSearch)

            request.reply(createIndexerQueryReply(path))

            async.complete()
        }
    }

    /**
     * Call [.testAdd] with null as path.
     * @param context Test context
     */
    @Test
    fun testAddWithoutSubfolder(context: TestContext) {
        testAdd(context, null)
    }

    /**
     * Call [.testAdd] with a path.
     * @param context Test context
     */
    @Test
    fun testAddWithSubfolder(context: TestContext) {
        testAdd(context, TEST_FOLDER)
    }

    /**
     * Call [.testDelete] with null as path.
     * @param context Test context
     */
    @Test
    fun testDeleteWithoutSubfolder(context: TestContext) {
        testDelete(context, null)
    }

    /**
     * Call [.testDelete] with a path.
     * @param context Test context
     */
    @Test
    fun testDeleteWithSubfolder(context: TestContext) {
        testDelete(context, TEST_FOLDER)
    }

    /**
     * Call [.testGet] with null as path.
     * @param context Test context
     */
    @Test
    fun testGetWithoutSubfolder(context: TestContext) {
        testGet(context, null)
    }

    /**
     * Call [.testGet] with a path.
     * @param context Test context
     */
    @Test
    fun testGetWithSubfolder(context: TestContext) {
        testGet(context, TEST_FOLDER)
    }

    /**
     * Call [.testGetOne] with null as path.
     * @param context Test context
     */
    @Test
    fun testGetOneWithoutFolder(context: TestContext) {
        testGetOne(context, null)
    }

    /**
     * Apply the [Store.delete] with a
     * non-existing path and expects a success (no exceptions or failure codes).
     * @param context Test context
     */
    @Test
    fun testDeleteNonExistingEntity(context: TestContext) {
        val vertx = rule.vertx()
        val async = context.async()
        val asyncIndexerQuery = context.async()

        val store = createStore(vertx)

        // register add
        vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_ADD).handler { h -> context.fail("Indexer should not be notified for a add event after " + "Store::delete was called!") }

        // register delete
        vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_DELETE).handler { r -> context.fail("INDEXER_DELETE should not be notified if no file was found.") }

        // register query
        vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_QUERY).handler { r ->
            r.fail(404, "NOT FOUND")
            asyncIndexerQuery.complete()
        }

        store.delete(SEARCH, "NOT_EXISTING_PATH", context.asyncAssertSuccess { h -> async.complete() })
    }

    /**
     * Apply the [Store.delete] with a
     * existing path but non existing entity and expects a success (no exceptions or failure codes).
     * @param context Test context
     */
    @Test
    fun testDeleteNonExistingEntityWithPath(context: TestContext) {
        val vertx = rule.vertx()
        val asyncIndexerQuery = context.async()
        val asyncIndexerDelete = context.async()
        val asyncDelete = context.async()

        val store = createStore(vertx)

        // register add
        vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_ADD).handler { h -> context.fail("Indexer should not be notified for a add event after" + "Store::delete was called!") }

        // register delete
        vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_DELETE).handler { req ->
            val msg = req.body()
            context.assertTrue(msg.containsKey("paths"))

            val paths = msg.getJsonArray("paths")
            context.assertEquals(1, paths.size())

            val notifiedPath = paths.getString(0)
            context.assertEquals(PATH_TO_NON_EXISTING_ENTITY, notifiedPath)

            req.reply(null) // Value is not used in Store

            asyncIndexerDelete.complete()
        }

        // register query
        // the null argument depend on the static PATH_TO_NON_EXISTING_ENTITY attribute value
        mockIndexerQuery(vertx, context, asyncIndexerQuery, null)

        store.delete(SEARCH, null, context.asyncAssertSuccess { h -> asyncDelete.complete() })
    }

    /**
     *
     * Add test data to a storage and retrieve the data with the
     * [Store.getOne] method to compare them.
     *
     * Uses [.prepareData]
     * @param context Test context
     * @param path The path where to look for data (may be null)
     */
    fun testGetOne(context: TestContext, path: String?) {
        val vertx = rule.vertx()
        val async = context.async()

        prepareData(context, vertx, path, context.asyncAssertSuccess { resultPath ->
            val store = createStore(vertx)
            store.getOne(ID, context.asyncAssertSuccess { h ->
                h.handler { buffer ->
                    val receivedChunk = String(buffer.bytes)
                    context.assertEquals(CHUNK_CONTENT, receivedChunk)
                }.endHandler { end -> async.complete() }
            })
        })
    }

    /**
     * Add test data and compare the data with the stored one
     * @param context Test context
     * @param path Path where to add data (may be null)
     */
    fun testAdd(context: TestContext, path: String?) {
        val vertx = rule.vertx()
        val asyncIndexerAdd = context.async()
        val asyncAdd = context.async()

        val store = createStore(vertx)

        vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_ADD).handler { h ->
            val index = h.body()

            context.assertEquals(META.toJsonObject(), index.getJsonObject("meta"))
            context.assertEquals(JsonArray(TAGS), index.getJsonArray("tags"))
            context.assertEquals(JsonObject(PROPERTIES), index.getJsonObject("properties"))
            context.assertEquals(FALLBACK_CRS_STRING, index.getString("fallbackCRSString"))

            asyncIndexerAdd.complete()
        }

        // register delete
        vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_DELETE).handler { h -> context.fail("Indexer should not be notified for a delete event after" + "Store::add was called!") }

        // register query
        vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_QUERY).handler { h -> context.fail("Indexer should not be notified for a query event after" + "Store::add was called!") }

        val indexMeta = IndexMeta(IMPORT_ID, ID, TIMESTAMP, TAGS, PROPERTIES, FALLBACK_CRS_STRING)
        store.add(CHUNK_CONTENT, META, path, indexMeta, context.asyncAssertSuccess { err -> validateAfterStoreAdd(context, vertx, path, context.asyncAssertSuccess { v -> asyncAdd.complete() }) })
    }

    /**
     * Add test data and try to delete them with the
     * [Store.delete] method, then check the
     * storage for any data
     * @param context Test context
     * @param path Path where the data can be found (may be null)
     */
    fun testDelete(context: TestContext, path: String?) {
        val vertx = rule.vertx()
        val asyncIndexerQuery = context.async()
        val asyncIndexerDelete = context.async()
        val asyncDelete = context.async()

        prepareData(context, vertx, path, context.asyncAssertSuccess { resultPath ->
            val store = createStore(vertx)

            // register add
            vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_ADD).handler { h -> context.fail("Indexer should not be notified for a add event after" + "Store::delete was called!") }

            // register delete
            vertx.eventBus().consumer<JsonObject>(AddressConstants.INDEXER_DELETE).handler { req ->
                val msg = req.body()
                context.assertTrue(msg.containsKey("paths"))

                val paths = msg.getJsonArray("paths")
                context.assertEquals(1, paths.size())

                val notifiedPath = paths.getString(0)
                context.assertEquals(resultPath, notifiedPath)

                req.reply(null) // Value is not used in Store

                asyncIndexerDelete.complete()
            }

            // register query
            mockIndexerQuery(vertx, context, asyncIndexerQuery, path)

            store.delete(SEARCH, path, context.asyncAssertSuccess { h -> validateAfterStoreDelete(context, vertx, resultPath, context.asyncAssertSuccess { v -> asyncDelete.complete() }) })
        })
    }

    /**
     * Add test data with meta data and try to retrieve them with the
     * [Store.get] method
     * @param context Test context.
     * @param path The path where the data can be found (may be null).
     */
    fun testGet(context: TestContext, path: String?) {
        val vertx = rule.vertx()
        val asyncQuery = context.async()
        val asyncGet = context.async()

        // register query
        mockIndexerQuery(vertx, context, asyncQuery, path)

        // register delete
        vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_DELETE).handler { h -> context.fail("Indexer should not be notified for a delete event after" + "Store::get was called!") }

        // register query
        vertx.eventBus().consumer<Any>(AddressConstants.INDEXER_ADD).handler { h -> context.fail("Indexer should not be notified for an add event after" + "Store::get was called!") }

        prepareData(context, vertx, path, context.asyncAssertSuccess { resultPath ->
            val store = createStore(vertx)

            store.get(SEARCH, resultPath) { ar ->
                val cursor = ar.result()
                context.assertTrue(cursor.hasNext())

                cursor.next { h ->
                    val meta = h.result()

                    context.assertEquals(END, meta.getEnd())
                    context.assertEquals(START, meta.getStart())

                    val fileName = cursor.chunkPath

                    context.assertEquals(PathUtils.join(path, ID), fileName)

                    asyncGet.complete()
                }
            }
        })
    }

    companion object {

        /**
         * Test data: tempFolder name which is used to call the test with a folder
         */
        protected val TEST_FOLDER = "testFolder"

        /**
         * Test data: content of a chunk
         */
        protected val CHUNK_CONTENT = "<b>This is a chunk content</b>"

        /**
         * Test data: search for a Store (value is irrelevant for the test, because
         * this test do not use the Indexer)
         */
        protected val SEARCH = "irrelevant but necessary value"

        /**
         * Test data: version 1.0 XML standalone header
         */
        protected val XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"

        /**
         * Test data: a valid xml with header
         */
        protected val XML = "$XML_HEADER<root>\n<object><child></child></object>\n</root>"

        /**
         * Test data: metadata for a chunk
         */
        protected val META = XMLChunkMeta(Arrays.asList(XMLStartElement("root")),
                XML_HEADER.length + 7, XML.length - 8)

        /**
         * Test data: fallback CRS for chunk indexing
         */
        protected val FALLBACK_CRS_STRING = "EPSG:25832"

        /**
         * Test data: the import id of a file import
         */
        protected val IMPORT_ID = "Af023dasd3"

        /**
         * Test data: the timestamp for an import
         */
        protected val TIMESTAMP = System.currentTimeMillis()

        /**
         * Test data: a sample tag list for an Store::add method
         */
        protected val TAGS = Arrays.asList("a", "b", "c")

        /**
         * Test data: a sample property map for an Store::add method
         */
        protected val PROPERTIES: Map<String, Any> = ImmutableMap.of<String, Any>("k1", "v1", "k2", "v2", "k3", "v3")

        /**
         * Test data: a randomly generated id for all tests.
         */
        protected val ID = ObjectId().toString()

        /**
         * Test data: path to a non existing entity
         */
        protected val PATH_TO_NON_EXISTING_ENTITY = ID

        /**
         * Test data: the parents of one hit
         */
        protected val PARENTS = JsonArray()

        /**
         * Test data: start of a hit
         */
        protected val START = 0

        /**
         * Test data: end of a hit
         */
        protected val END = 5

        /**
         * Test data: Total amount of hits
         */
        protected val TOTAL_HITS = 1L

        /**
         * Test data: scroll id
         */
        protected val SCROLL_ID = "0"

        /**
         * Create a JsonObject to simulate a reply from an indexer
         * @param path The path which is used as prefix of the id (may be null)
         * @return A reply msg
         */
        protected fun createIndexerQueryReply(path: String?): JsonObject {
            var path = path
            if (path != null && !path.isEmpty()) {
                path = PathUtils.join(path, ID)
            } else {
                path = ID
            }

            val hits = JsonArray()
            val hit = JsonObject()
                    .put("parents", PARENTS)
                    .put("start", START)
                    .put("end", END)
                    .put("id", path)

            hits.add(hit)

            return JsonObject()
                    .put("totalHits", TOTAL_HITS)
                    .put("scrollId", SCROLL_ID)
                    .put("hits", hits)
        }
    }
}
