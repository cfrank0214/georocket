package io.georocket.storage.hdfs

import java.io.IOException
import java.io.InputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.util.Queue

import org.apache.commons.lang3.tuple.Pair
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.google.common.base.Preconditions

import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

/**
 * Stores chunks on HDFS
 * @author Michel Kraemer
 */
abstract class HDFSStore
/**
 * Constructs a new store
 * @param vertx the Vert.x instance
 */
(private val vertx: Vertx) : IndexedStore(vertx) {
    private val configuration: Configuration
    private val root: String
    private var fs: FileSystem? = null

    init {

        val config = vertx.orCreateContext.config()

        root = config.getString(ConfigConstants.STORAGE_HDFS_PATH)
        Preconditions.checkNotNull(root, "Missing configuration item \"" +
                ConfigConstants.STORAGE_HDFS_PATH + "\"")

        val defaultFS = config.getString(ConfigConstants.STORAGE_HDFS_DEFAULT_FS)
        Preconditions.checkNotNull(defaultFS, "Missing configuration item \"" +
                ConfigConstants.STORAGE_HDFS_DEFAULT_FS + "\"")

        configuration = Configuration()
        configuration.set("fs.defaultFS", defaultFS)
    }

    /**
     * Get or create the HDFS file system
     * Note: this method must be synchronized because we're accessing the
     * [.fs] field and we're calling this method from a worker thread.
     * @return the MongoDB client
     * @throws IOException if the file system instance could not be created
     */
    @Synchronized
    @Throws(IOException::class)
    private fun getFS(): FileSystem? {
        if (fs == null) {
            fs = FileSystem.get(configuration)
        }
        return fs
    }

    override fun getOne(path: String, handler: Handler<AsyncResult<ChunkReadStream>>) {
        vertx.executeBlocking<Pair<Long, InputStream>>({ f ->
            try {
                val p = Path(PathUtils.join(root, path))
                val size: Long
                val `is`: FSDataInputStream
                synchronized(this@HDFSStore) {
                    val fs = getFS()
                    val status = fs?.getFileStatus(p)
                    size = status.len
                    `is` = fs.open(p)
                }
                f.complete(Pair.of(size, `is`))
            } catch (e: IOException) {
                f.fail(e)
            }
        }, { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
            } else {
                val p = ar.result()
                handler.handle(Future.succeededFuture(
                        InputStreamChunkReadStream(p.value, p.key, vertx)))
            }
        })
    }

    /**
     * Create a new file on HDFS
     * @param filename the file name
     * @return an output stream that you can use to write the new file
     * @throws IOException if the file cannot be created
     */
    @Synchronized
    @Throws(IOException::class)
    private fun createFile(filename: String): FSDataOutputStream {
        return getFS().create(Path(PathUtils.join(root, filename)), false)
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

        vertx.executeBlocking({ f ->
            try {
                createFile(filename).use { os -> OutputStreamWriter(os, StandardCharsets.UTF_8).use { writer -> writer.write(chunk) } }
            } catch (e: IOException) {
                f.fail(e)
                return@vertx.executeBlocking
            }

            f.complete(filename)
        }, handler)
    }

    override fun doDeleteChunks(paths: Queue<String>, handler: Handler<AsyncResult<Void>>) {
        if (paths.isEmpty()) {
            handler.handle(Future.succeededFuture())
            return
        }

        val path = PathUtils.join(root, paths.poll())
        vertx.executeBlocking({ f ->
            try {
                synchronized(this@HDFSStore) {
                    val fs = getFS()
                    val hdfsPath = Path(path)

                    if (fs.exists(hdfsPath)) {
                        fs.delete(hdfsPath, false)
                    }
                }
            } catch (e: IOException) {
                f.fail(e)
                return@vertx.executeBlocking
            }

            f.complete()
        }, handler)
    }
}
