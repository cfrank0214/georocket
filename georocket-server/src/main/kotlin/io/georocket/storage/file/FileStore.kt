package io.georocket.storage.file

import java.io.FileNotFoundException
import java.nio.file.Paths
import java.util.Queue

import com.google.common.base.Preconditions

import io.georocket.constants.ConfigConstants
import io.georocket.storage.ChunkReadStream
import io.georocket.storage.indexed.IndexedStore
import io.georocket.util.PathUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.AsyncFile
import io.vertx.core.file.FileProps
import io.vertx.core.file.FileSystem
import io.vertx.core.file.OpenOptions
import io.vertx.rx.java.ObservableFuture
import io.vertx.rx.java.RxHelper
import rx.Observable

/**
 * Stores chunks on the file system
 * @author Michel Kraemer
 */
class FileStore
/**
 * Default constructor
 * @param vertx the Vert.x instance
 */
(
        /**
         * The vertx container
         */
        private val vertx: Vertx) : IndexedStore(vertx) {
    /**
     * The folder where the chunks should be saved
     */
    private val root: String

    init {

        val storagePath = vertx.orCreateContext.config().getString(
                ConfigConstants.STORAGE_FILE_PATH)
        Preconditions.checkNotNull(storagePath, "Missing configuration item \"" +
                ConfigConstants.STORAGE_FILE_PATH + "\"")

        this.root = Paths.get(storagePath, "file").toString()
    }

    override fun doAddChunk(chunk: String, path: String?,
                            correlationId: String, handler: Handler<AsyncResult<String>>) {
        var path = path
        if (path == null || path.isEmpty()) {
            path = "/"
        }
        val dir = Paths.get(root, path).toString()
        val finalPath = path

        // create storage folder
        vertx.fileSystem().mkdirs(dir) { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
                return@vertx.fileSystem().mkdirs
            }

            // generate new file name
            val filename = generateChunkId(correlationId)

            // open new file
            val fs = vertx.fileSystem()
            fs.open(Paths.get(dir, filename).toString(), OpenOptions()) { openar ->
                if (openar.failed()) {
                    handler.handle(Future.failedFuture(openar.cause()))
                    return@fs.open
                }

                // write contents to file
                val f = openar.result()
                val buf = Buffer.buffer(chunk)
                f.write(buf, 0) { writear ->
                    f.close()
                    if (writear.failed()) {
                        handler.handle(Future.failedFuture(writear.cause()))
                    } else {
                        val result = PathUtils.join(finalPath, filename)
                        handler.handle(Future.succeededFuture(result))
                    }
                }
            }
        }
    }

    override fun getOne(path: String, handler: Handler<AsyncResult<ChunkReadStream>>) {
        val absolutePath = Paths.get(root, path).toString()

        // check if chunk exists
        val fs = vertx.fileSystem()
        val observable = RxHelper.observableFuture<Boolean>()
        fs.exists(absolutePath, observable.toHandler())
        observable
                .flatMap { exists ->
                    if (!exists) {
                        return@observable
                                .flatMap Observable . error < Boolean >(FileNotFoundException("Could not find chunk: $path"))
                    }
                    Observable.just(exists)
                }
                .flatMap { exists ->
                    // get chunk's size
                    val propsObservable = RxHelper.observableFuture<FileProps>()
                    fs.props(absolutePath, propsObservable.toHandler())
                    propsObservable
                }
                .map { props -> props.size() }
                .flatMap { size ->
                    // open chunk
                    val openObservable = RxHelper.observableFuture<AsyncFile>()
                    val openOptions = OpenOptions().setCreate(false).setWrite(false)
                    fs.open(absolutePath, openOptions, openObservable.toHandler())
                    openObservable.map { f -> FileChunkReadStream(size!!, f) }
                }
                .subscribe({ readStream ->
                    // send chunk to peer
                    handler.handle(Future.succeededFuture(readStream))
                }, { err -> handler.handle(Future.failedFuture(err)) })
    }

    override fun doDeleteChunks(paths: Queue<String>, handler: Handler<AsyncResult<Void>>) {
        if (paths.isEmpty()) {
            handler.handle(Future.succeededFuture())
            return
        }

        val path = paths.poll()
        val fs = vertx.fileSystem()
        val absolutePath = Paths.get(root, path).toString()

        fs.exists(absolutePath) { existAr ->
            if (existAr.failed()) {
                handler.handle(Future.failedFuture(existAr.cause()))
            } else {
                if (existAr.result()) {
                    fs.delete(absolutePath) { deleteAr ->
                        if (deleteAr.failed()) {
                            handler.handle(Future.failedFuture(deleteAr.cause()))
                        } else {
                            doDeleteChunks(paths, handler)
                        }
                    }
                } else {
                    doDeleteChunks(paths, handler)
                }
            }
        }

    }
}
