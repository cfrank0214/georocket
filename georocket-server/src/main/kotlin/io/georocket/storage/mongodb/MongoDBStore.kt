package io.georocket.storage.mongodb

import java.nio.charset.StandardCharsets
import java.util.Queue

import org.bson.Document

import com.google.common.base.Preconditions
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.mongodb.client.gridfs.AsyncInputStream
import com.mongodb.client.gridfs.GridFSBucket
import com.mongodb.client.gridfs.GridFSBuckets
import com.mongodb.client.gridfs.GridFSDownloadStream
import com.mongodb.client.gridfs.GridFSFindIterable
import com.mongodb.client.gridfs.helpers.AsyncStreamHelper

import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

/**
 * Stores chunks in MongoDB
 * @author Michel Kraemer
 */
class MongoDBStore
/**
 * Constructs a new store
 * @param vertx the Vert.x instance
 */
(vertx: Vertx) : IndexedStore(vertx) {
    private val context: Context
    private val connectionString: String
    private val databaseName: String

    private var mongoClient: MongoClient? = null
    private var database: MongoDatabase? = null
    private var gridfs: GridFSBucket? = null

    /**
     * Get or create the MongoDB database
     * @return the MongoDB client
     */
    private val db: MongoDatabase?
        get() {
            if (database == null) {
                database = getMongoClient().getDatabase(databaseName)
            }
            return database
        }

    /**
     * Get or create the MongoDB GridFS instance
     * @return the MongoDB client
     */
    private val gridFS: GridFSBucket?
        get() {
            if (gridfs == null) {
                gridfs = GridFSBuckets.create(db)
            }
            return gridfs
        }

    init {
        context = vertx.orCreateContext

        val config = context.config()

        connectionString = config.getString(ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING)
        Preconditions.checkNotNull(connectionString, "Missing configuration item \"" +
                ConfigConstants.STORAGE_MONGODB_CONNECTION_STRING + "\"")

        databaseName = config.getString(ConfigConstants.STORAGE_MONGODB_DATABASE)
        Preconditions.checkNotNull(connectionString, "Missing configuration item \"" +
                ConfigConstants.STORAGE_MONGODB_DATABASE + "\"")
    }

    /**
     * Get or create the MongoDB client
     * @return the MongoDB client
     */
    private fun getMongoClient(): MongoClient {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(connectionString)
        }
        return mongoClient
    }

    override fun getOne(path: String, handler: Handler<AsyncResult<ChunkReadStream>>) {
        val downloadStream = gridFS.openDownloadStream(PathUtils.normalize(path))
        downloadStream.getGridFSFile { file, t ->
            context.runOnContext { v ->
                if (t != null) {
                    handler.handle(Future.failedFuture(t))
                } else {
                    val length = file.length
                    val chunkSize = file.chunkSize
                    handler.handle(Future.succeededFuture(MongoDBChunkReadStream(
                            downloadStream, length, chunkSize, context)))
                }
            }
        }
    }

    override fun doAddChunk(chunk: String, path: String?, correlationId: String,
                            handler: Handler<AsyncResult<String>>) {
        var path = path
        if (path == null || path.isEmpty()) {
            path = "/"
        }

        // generate new file name
        val id = generateChunkId(correlationId)
        val filename = PathUtils.join(path, id)

        val bytes = chunk.toByteArray(StandardCharsets.UTF_8)
        val `is` = AsyncStreamHelper.toAsyncInputStream(bytes)
        gridFS.uploadFromStream(filename, `is`) { oid, t ->
            context.runOnContext { v ->
                if (t != null) {
                    handler.handle(Future.failedFuture(t))
                } else {
                    handler.handle(Future.succeededFuture(filename))
                }
            }
        }
    }

    override fun doDeleteChunks(paths: Queue<String>, handler: Handler<AsyncResult<Void>>) {
        if (paths.isEmpty()) {
            handler.handle(Future.succeededFuture())
            return
        }

        val path = PathUtils.normalize(paths.poll())
        val gridFS = gridFS
        val i = gridFS.find(Document("filename", path))
        i.first { file, t ->
            if (t != null) {
                context.runOnContext { v -> handler.handle(Future.failedFuture(t)) }
            } else {
                if (file == null) {
                    // file does not exist
                    context.runOnContext { v -> doDeleteChunks(paths, handler) }
                    return@i.first
                }
                gridFS.delete(file!!.objectId) { r, t2 ->
                    context.runOnContext { v ->
                        if (t2 != null) {
                            handler.handle(Future.failedFuture(t2))
                        } else {
                            doDeleteChunks(paths, handler)
                        }
                    }
                }
            }
        }
    }
}
