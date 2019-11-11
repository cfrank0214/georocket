package io.georocket.storage.mem

import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.TestContext

/**
 * Test [MemoryStore]
 * @author Michel Kraemer
 */
class MemoryStoreTest : StorageTest() {
    override fun createStore(vertx: Vertx): Store {
        return MemoryStore(vertx)
    }

    private fun getAsyncMap(vertx: Vertx,
                            handler: (Any) -> Unit) {
        val name = MemoryStore::class.java.name + ".STORE"
        vertx.sharedData().getAsyncMap(name, handler)
    }

    override fun prepareData(context: TestContext, vertx: Vertx, path: String?,
                             handler: Handler<AsyncResult<String>>) {
        getAsyncMap(vertx) { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture<String>(ar.cause()))
                return@getAsyncMap
            }
            val am = ar.result()
            val p = PathUtils.join(path, StorageTest.ID)
            am.put(p, Buffer.buffer(StorageTest.CHUNK_CONTENT), { par ->
                if (par.failed()) {
                    handler.handle(Future.failedFuture<String>(par.cause()))
                } else {
                    handler.handle(Future.succeededFuture(p))
                }
            })
        }
    }

    private fun assertSize(context: TestContext, vertx: Vertx, expectedSize: Int,
                           handler: Handler<AsyncResult<Void>>) {
        getAsyncMap(vertx) { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture<Void>(ar.cause()))
                return@getAsyncMap
            }
            val am = ar.result()
            am.size({ sar ->
                if (sar.failed()) {
                    handler.handle(Future.failedFuture<Void>(sar.cause()))
                } else {
                    context.assertEquals(expectedSize, sar.result())
                    handler.handle(Future.succeededFuture())
                }
            })
        }
    }

    override fun validateAfterStoreAdd(context: TestContext,
                                       vertx: Vertx, path: String?, handler: Handler<AsyncResult<Void>>) {
        assertSize(context, vertx, 1, handler)
    }

    override fun validateAfterStoreDelete(context: TestContext,
                                          vertx: Vertx, path: String, handler: Handler<AsyncResult<Void>>) {
        assertSize(context, vertx, 0, handler)
    }
}
