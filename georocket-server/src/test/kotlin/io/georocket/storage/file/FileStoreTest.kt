package io.georocket.storage.file

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
import io.vertx.ext.unit.TestContext

/**
 * Test [FileStore]
 * @author Andrej Sajenko
 */
class FileStoreTest : StorageTest() {
    /**
     * Create a temporary tempFolder
     */
    @Rule
    var tempFolder = TemporaryFolder()

    private var fileStoreRoot: String? = null
    private var fileDestination: String? = null

    /**
     * Set up test dependencies.
     */
    @Before
    fun setUp() {
        fileStoreRoot = tempFolder.root.absolutePath
        fileDestination = PathUtils.join(fileStoreRoot, "file")
    }

    private fun configureVertx(vertx: Vertx) {
        vertx.orCreateContext.config().put(ConfigConstants.STORAGE_FILE_PATH, fileStoreRoot)
    }

    override fun createStore(vertx: Vertx): Store {
        configureVertx(vertx)
        return FileStore(vertx)
    }

    override fun prepareData(context: TestContext, vertx: Vertx, subPath: String?,
                             handler: Handler<AsyncResult<String>>) {
        vertx.executeBlocking({ f ->
            val destinationFolder = if (subPath == null || subPath.isEmpty())
                fileDestination
            else
                PathUtils.join(fileDestination, subPath)
            val filePath = Paths.get(destinationFolder!!.toString(), StorageTest.ID)
            try {
                Files.createDirectories(Paths.get(destinationFolder))
                Files.write(filePath, StorageTest.CHUNK_CONTENT.toByteArray())
                f.complete(filePath.toString().replace(fileDestination!! + "/", ""))
            } catch (ex: IOException) {
                f.fail(ex)
            }
        }, handler)
    }

    override fun validateAfterStoreAdd(context: TestContext, vertx: Vertx,
                                       path: String?, handler: Handler<AsyncResult<Void>>) {
        vertx.executeBlocking({ f ->
            val destinationFolder = if (path == null || path.isEmpty())
                fileDestination
            else
                PathUtils.join(fileDestination, path)

            val folder = File(destinationFolder!!)
            context.assertTrue(folder.exists())

            val files = folder.listFiles()
            context.assertNotNull(files)
            context.assertEquals(1, files!!.size)
            val file = files[0]

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
