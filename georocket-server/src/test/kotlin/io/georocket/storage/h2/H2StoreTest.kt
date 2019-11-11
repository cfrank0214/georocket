package io.georocket.storage.h2

import java.io.IOException

import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.rules.TemporaryFolder

import io.georocket.constants.ConfigConstants
import io.georocket.storage.StorageTest
import io.georocket.util.PathUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext

/**
 * Test [H2Store]
 * @author Michel Kraemer
 */
class H2StoreTest : StorageTest() {
    /**
     * Create a temporary tempFolder
     */
    @Rule
    var tempFolder = TemporaryFolder()

    private var path: String? = null
    private var store: H2Store? = null

    /**
     * Set up the test
     * @throws IOException if a temporary file could not be created
     */
    @Before
    @Throws(IOException::class)
    fun setUp() {
        path = tempFolder.newFile().absolutePath
    }

    /**
     * Release test resources
     */
    @After
    fun tearDown() {
        if (store != null) {
            store!!.close()
            store = null
        }
    }

    private fun configureVertx(vertx: Vertx) {
        val config = vertx.orCreateContext.config()
        config.put(ConfigConstants.STORAGE_H2_PATH, path)
    }

    override fun createStore(vertx: Vertx): H2Store {
        if (store == null) {
            configureVertx(vertx)
            store = H2Store(vertx)
        }
        return store
    }

    override fun prepareData(context: TestContext, vertx: Vertx, path: String?, handler: Handler<AsyncResult<String>>) {
        val p = PathUtils.join(path, StorageTest.ID)
        createStore(vertx).getMap()[p] = StorageTest.CHUNK_CONTENT
        handler.handle(Future.succeededFuture(p))
    }

    override fun validateAfterStoreAdd(context: TestContext, vertx: Vertx, path: String?,
                                       handler: Handler<AsyncResult<Void>>) {
        context.assertEquals(1, createStore(vertx).getMap().size)
        handler.handle(Future.succeededFuture())
    }

    override fun validateAfterStoreDelete(context: TestContext, vertx: Vertx, path: String,
                                          handler: Handler<AsyncResult<Void>>) {
        context.assertEquals(0, createStore(vertx).getMap().size)
        handler.handle(Future.succeededFuture())
    }
}
