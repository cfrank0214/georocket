package io.georocket.storage.h2

import java.io.FileNotFoundException
import java.util.Queue
import java.util.concurrent.atomic.AtomicReference

import org.h2.mvstore.MVStore

import com.google.common.base.Preconditions

import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.georocket.util.io.DelegateChunkReadStream
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject

/**
 * Stores chunks on a H2 database
 * @author Michel Kraemer
 */
abstract class H2Store
/**
 * Constructs a new store
 * @param vertx the Vert.x instance
 */
(vertx: Vertx) : IndexedStore(vertx) {
    /**
     * The path to the H2 database file
     */
    private val path: String

    /**
     * True if the H2 database should compress chunks using the LZF algorithm.
     * This can save a lot of disk space but will slow down read and write
     * operations slightly.
     */
    private val compress: Boolean

    /**
     * The name of the MVMap within the H2 database.
     */
    private val mapName: String

    /**
     * The underlying H2 MVMap. Use [.getMap] to retrieve this field's
     * value.
     */
    private var map: Map<String, String>? = null

    /**
     * Get or create the H2 MVStore
     * @return the MVStore
     */
    protected val mvStore: MVStore?
        get() {
            var result: MVStore? = mvstore.get()
            if (result == null) {
                synchronized(mvstore) {
                    var builder: MVStore.Builder = MVStore.Builder()
                            .fileName(path)

                    if (compress) {
                        builder = builder.compress()
                    }

                    result = builder.open()
                    mvstore.set(result)
                }
            }
            return result
        }

    init {

        val config = vertx.orCreateContext.config()

        path = config.getString(ConfigConstants.STORAGE_H2_PATH)
        Preconditions.checkNotNull(path, "Missing configuration item \"" +
                ConfigConstants.STORAGE_H2_PATH + "\"")

        compress = config.getBoolean(ConfigConstants.STORAGE_H2_COMPRESS, false)!!
        mapName = config.getString(ConfigConstants.STORAGE_H2_MAP_NAME, "georocket")
    }

    /**
     * Release all resources and close this store
     */
    fun close() {
        val s = mvstore.getAndSet(null)
        s?.close()
        map = null
    }

    /**
     * Get or create the H2 MVMap
     * @return the MVMap
     */
    fun getMap(): Map<String, String>? {
        if (map == null) {
            map = mvStore!!.openMap(mapName)
        }
        return map
    }

    override fun getOne(path: String, handler: Handler<AsyncResult<ChunkReadStream>>) {
        val finalPath = PathUtils.normalize(path)
        val chunkStr = getMap()?.get(finalPath)
        if (chunkStr == null) {
            handler.handle(Future.failedFuture(FileNotFoundException(
                    "Could not find chunk: $finalPath")))
            return
        }

        val chunk = Buffer.buffer(chunkStr)
        handler.handle(Future.succeededFuture(DelegateChunkReadStream(chunk)))
    }

    override fun doAddChunk(chunk: String, path: String?, correlationId: String,
                            handler: Handler<AsyncResult<String>>) {
        var path = path
        if (path == null || path.isEmpty()) {
            path = "/"
        }

        val filename = PathUtils.join(path, generateChunkId(correlationId))
        getMap()[filename] = chunk
        handler.handle(Future.succeededFuture(filename))
    }

    override fun doDeleteChunks(paths: Queue<String>,
                                handler: Handler<AsyncResult<Void>>) {
        while (!paths.isEmpty()) {
            val path = PathUtils.normalize(paths.poll())
            getMap().remove(path)
        }
        handler.handle(Future.succeededFuture())
    }

    companion object {

        /**
         * The underlying H2 MVStore. Use [.getMVStore] to retrieve this
         * field's value.
         */
        private val mvstore = AtomicReference<MVStore>()
    }
}
