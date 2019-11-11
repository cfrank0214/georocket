package io.georocket.storage.mongodb

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.charset.StandardCharsets

import org.junit.After
import org.junit.AfterClass
import org.junit.BeforeClass

import com.google.common.collect.Iterables
import com.mongodb.MongoClient
import com.mongodb.client.MongoDatabase
import com.mongodb.client.gridfs.GridFSBucket
import com.mongodb.client.gridfs.GridFSBuckets
import com.mongodb.client.gridfs.GridFSFindIterable
import com.mongodb.client.gridfs.model.GridFSFile

import io.georocket.constants.ConfigConstants
import io.georocket.storage.StorageTest
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext

/**
 * Test [MongoDBStore]
 * @author Andrej Sajenko
 */
class MongoDBStoreTest : StorageTest() {

    /**
     * Uninitialize tests
     */
    @After
    fun tearDown() {
        MongoClient(mongoConnector!!.serverAddress).use { client ->
            val db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME)
            db.drop()
        }
    }

    private fun configureVertx(vertx: Vertx) {
        val config = vertx.orCreateContext.config()

        config.put(ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING,
                "mongodb://" + mongoConnector!!.serverAddress.host + ":" +
                        mongoConnector!!.serverAddress.port)
        config.put(ConfigConstants.STORAGE_MONGODB_DATABASE,
                MongoDBTestConnector.MONGODB_DBNAME)
    }

    override fun createStore(vertx: Vertx): Store {
        configureVertx(vertx)
        return MongoDBStore(vertx)
    }

    override fun prepareData(context: TestContext, vertx: Vertx, path: String?,
                             handler: Handler<AsyncResult<String>>) {
        val filename = PathUtils.join(path, StorageTest.ID)
        vertx.executeBlocking({ f ->
            MongoClient(mongoConnector!!.serverAddress).use { client ->
                val db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME)
                val gridFS = GridFSBuckets.create(db)
                val contents = StorageTest.CHUNK_CONTENT.toByteArray(StandardCharsets.UTF_8)
                gridFS.uploadFromStream(filename, ByteArrayInputStream(contents))
                f.complete(filename)
            }
        }, handler)
    }

    override fun validateAfterStoreAdd(context: TestContext, vertx: Vertx,
                                       path: String?, handler: Handler<AsyncResult<Void>>) {
        vertx.executeBlocking({ f ->
            MongoClient(mongoConnector!!.serverAddress).use { client ->
                val db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME)
                val gridFS = GridFSBuckets.create(db)

                val files = gridFS.find()

                val file = files.first()
                val baos = ByteArrayOutputStream()
                gridFS.downloadToStream(file!!.filename, baos)
                val contents = String(baos.toByteArray(), StandardCharsets.UTF_8)
                context.assertEquals(StorageTest.CHUNK_CONTENT, contents)
            }
            f.complete()
        }, handler)
    }

    override fun validateAfterStoreDelete(context: TestContext, vertx: Vertx,
                                          path: String, handler: Handler<AsyncResult<Void>>) {
        vertx.executeBlocking({ f ->
            MongoClient(mongoConnector!!.serverAddress).use { client ->
                val db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME)
                val gridFS = GridFSBuckets.create(db)

                val files = gridFS.find()
                context.assertTrue(Iterables.isEmpty(files))
            }
            f.complete()
        }, handler)
    }

    companion object {
        private var mongoConnector: MongoDBTestConnector? = null

        /**
         * Set up test dependencies.
         * @throws IOException if the MongoDB instance could not be started
         */
        @BeforeClass
        @Throws(IOException::class)
        fun setUpClass() {
            mongoConnector = MongoDBTestConnector()
        }

        /**
         * Uninitialize tests
         */
        @AfterClass
        fun tearDownClass() {
            mongoConnector!!.stop()
            mongoConnector = null
        }
    }
}
