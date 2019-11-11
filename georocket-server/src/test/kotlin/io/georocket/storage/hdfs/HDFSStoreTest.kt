package io.georocket.storage.hdfs

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.junit.Before
import org.junit.Rule
import org.junit.rules.TemporaryFolder

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
 * Test [HDFSStore]
 * @author Andrej Sajenko
 */
class HDFSStoreTest : StorageTest() {
    /**
     * Create a temporary folder
     */
    @Rule
    var tempFolder = TemporaryFolder()

    private var hdfsLocalRoot: String? = null
    private var hdfsAdress: String? = null

    /**
     * Set up test dependencies.
     */
    @Before
    fun setUp() {
        hdfsLocalRoot = tempFolder.root.absolutePath
        hdfsAdress = "file://" + hdfsLocalRoot!!
    }

    private fun configureVertx(vertx: Vertx) {
        val config = vertx.orCreateContext.config()

        // prevent exception -> http://stackoverflow.com/questions/19840056/failed-to-detect-a-valid-hadoop-home-directory
        System.setProperty("hadoop.home.dir", "/")

        config.put(ConfigConstants.STORAGE_HDFS_PATH, hdfsLocalRoot)
        config.put(ConfigConstants.STORAGE_HDFS_DEFAULT_FS, hdfsAdress)
    }

    override fun createStore(vertx: Vertx): Store {
        configureVertx(vertx)
        return HDFSStore(vertx)
    }

    override fun prepareData(context: TestContext, vertx: Vertx, path: String?,
                             handler: Handler<AsyncResult<String>>) {
        vertx.executeBlocking({ f ->
            val destinationFolder = if (path == null || path.isEmpty())
                hdfsLocalRoot
            else
                PathUtils.join(hdfsLocalRoot, path)
            val filePath = Paths.get(destinationFolder!!, StorageTest.ID)
            try {
                Files.createDirectories(Paths.get(destinationFolder))
                Files.write(filePath, StorageTest.CHUNK_CONTENT.toByteArray())
                f.complete(filePath.toString().replace(hdfsLocalRoot!! + "/", ""))
            } catch (ex: IOException) {
                f.fail(ex)
            }
        }, handler)
    }

    override fun validateAfterStoreAdd(context: TestContext, vertx: Vertx,
                                       path: String?, handler: Handler<AsyncResult<Void>>) {
        vertx.executeBlocking({ f ->
            val fileDestination = if (path == null || path.isEmpty())
                hdfsLocalRoot
            else
                PathUtils.join(hdfsLocalRoot, path)

            val folder = File(fileDestination!!)
            context.assertTrue(folder.exists())

            val files = folder.listFiles()
            context.assertNotNull(files)
            context.assertEquals(2, files!!.size)

            // Hadoop client creates two files, one starts with a point '.' and ends
            // with the extension ".crc". The other file contains the needed content.
            val file = if (files[0].path.endsWith(".crc")) files[1] else files[0]

            val lines: List<String>
            try {
                lines = Files.readAllLines(file.toPath())
            } catch (ex: IOException) {
                f.fail(ex)
                return@vertx.executeBlocking
            }

            context.assertFalse(lines.isEmpty())
            val firstLine = lines[0]
            context.assertEquals(StorageTest.CHUNK_CONTENT, firstLine)

            f.complete()
        }, handler)
    }

    override fun validateAfterStoreDelete(context: TestContext,
                                          vertx: Vertx, path: String, handler: Handler<AsyncResult<Void>>) {
        vertx.executeBlocking({ f ->
            context.assertFalse(Files.exists(Paths.get(path)))
            f.complete()
        }, handler)
    }
}
