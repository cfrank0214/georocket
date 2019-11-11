package io.georocket.storage.mongodb

import java.io.IOException
import java.util.Arrays

import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import com.mongodb.MongoClient
import com.mongodb.async.client.MongoClientSettings
import com.mongodb.async.client.MongoClients
import com.mongodb.async.client.gridfs.GridFSDownloadStream
import com.mongodb.client.MongoDatabase
import com.mongodb.client.gridfs.GridFSBucket
import com.mongodb.client.gridfs.GridFSBuckets
import com.mongodb.client.gridfs.GridFSUploadStream
import com.mongodb.connection.ClusterSettings

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner

/**
 * Test [MongoDBChunkReadStream]
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner::class)
class MongoDBChunkReadStreamTest {
    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    var rule = RunTestOnContext()

    /**
     * Create a file in GridFS with the given filename and write
     * some random data to it.
     * @param filename the name of the file to create
     * @param size the number of random bytes to write
     * @param vertx the Vert.x instance
     * @param handler a handler that will be called when the file
     * has been written
     */
    private fun prepareData(filename: String, size: Int, vertx: Vertx,
                            handler: Handler<AsyncResult<String>>) {
        vertx.executeBlocking({ f ->
            MongoClient(mongoConnector!!.serverAddress).use { client ->
                val db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME)
                val gridFS = GridFSBuckets.create(db)
                gridFS.openUploadStream(filename).use { os ->
                    for (i in 0 until size) {
                        os.write((i and 0xFF).toByte().toInt())
                    }
                }
            }
            f.complete(filename)
        }, handler)
    }

    /**
     * Connect to MongoDB and get the GridFS chunk size
     * @param vertx the Vert.x instance
     * @param handler a handler that will be called with the chunk size
     */
    private fun getChunkSize(vertx: Vertx, handler: Handler<AsyncResult<Int>>) {
        vertx.executeBlocking({ f ->
            MongoClient(mongoConnector!!.serverAddress).use { client ->
                val db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME)
                val gridFS = GridFSBuckets.create(db)
                f.complete(gridFS.chunkSizeBytes)
            }
        }, handler)
    }

    /**
     * Create an asynchronous MongoDB client
     * @return the client
     */
    private fun createAsyncClient(): com.mongodb.async.client.MongoClient {
        val clusterSettings = ClusterSettings.builder()
                .hosts(Arrays.asList<ServerAddress>(mongoConnector!!.serverAddress))
                .build()
        val settings = MongoClientSettings.builder()
                .clusterSettings(clusterSettings).build()
        return MongoClients.create(settings)
    }

    /**
     * Test if a tiny file can be read
     * @param context the test context
     */
    @Test
    fun tiny(context: TestContext) {
        val vertx = rule.vertx()
        getChunkSize(vertx, context.asyncAssertSuccess { cs -> doRead(2, cs!!, vertx, context) })
    }

    /**
     * Test if a small file can be read
     * @param context the test context
     */
    @Test
    fun small(context: TestContext) {
        val vertx = rule.vertx()
        getChunkSize(vertx, context.asyncAssertSuccess { cs -> doRead(1024, cs!!, vertx, context) })
    }

    /**
     * Test if a file can be read whose size equals the
     * default GridFS chunk size
     * @param context the test context
     */
    @Test
    fun defaultChunkSize(context: TestContext) {
        val vertx = rule.vertx()
        getChunkSize(vertx, context.asyncAssertSuccess { cs -> doRead(cs!!, cs, vertx, context) })
    }

    /**
     * Test if a medium-sized file can be read
     * @param context the test context
     */
    @Test
    fun medium(context: TestContext) {
        val vertx = rule.vertx()
        getChunkSize(vertx, context.asyncAssertSuccess { cs -> doRead(cs!! * 3 / 2, cs, vertx, context) })
    }

    /**
     * Test if a large file can be read
     * @param context the test context
     */
    @Test
    fun large(context: TestContext) {
        val vertx = rule.vertx()
        getChunkSize(vertx, context.asyncAssertSuccess { cs -> doRead(cs!! * 10 + 100, cs, vertx, context) })
    }

    /**
     * The actual test method. Creates a temporary file with random contents. Writes
     * `size` bytes to it and reads it again through
     * [MongoDBChunkReadStream]. Finally, checks if the file has been read correctly.
     * @param size the number of bytes to write/read
     * @param chunkSize the GridFS chunk size
     * @param vertx the Vert.x instance
     * @param context the current test context
     */
    private fun doRead(size: Int, chunkSize: Int, vertx: Vertx, context: TestContext) {
        val async = context.async()

        // create a test file in GridFS
        prepareData("test_$size.bin", size, vertx, context.asyncAssertSuccess { filename ->
            // connect to GridFS
            val client = createAsyncClient()
            val db = client.getDatabase(MongoDBTestConnector.MONGODB_DBNAME)
            val gridfs = com.mongodb.async.client.gridfs.GridFSBuckets.create(db)

            // open the test file
            val `is` = gridfs.openDownloadStream(filename)
            val rs = MongoDBChunkReadStream(`is`, size.toLong(), chunkSize,
                    vertx.orCreateContext)

            // read from the test file
            rs.exceptionHandler { context.fail(it) }

            val pos = intArrayOf(0)

            rs.endHandler { v ->
                // the file has been completely read
                rs.close()
                context.assertEquals(size, pos[0])
                async.complete()
            }

            rs.handler { buf ->
                // check number of read bytes
                if (size - pos[0] > chunkSize) {
                    context.assertEquals(chunkSize, buf.length())
                } else {
                    context.assertEquals(size - pos[0], buf.length())
                }

                // check file contents
                for (i in pos[0] until pos[0] + buf.length()) {
                    context.assertEquals((i and 0xFF).toByte(), buf.getByte(i - pos[0]))
                }

                pos[0] += buf.length()
            }
        })
    }

    companion object {

        private var mongoConnector: MongoDBTestConnector? = null

        /**
         * Set up test dependencies
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
