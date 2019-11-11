package io.georocket.index.elasticsearch

import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.Enumeration
import java.util.zip.ZipEntry
import java.util.zip.ZipFile

import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SystemUtils

import io.georocket.util.HttpException
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.file.OpenOptions
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.rx.java.ObservableFuture
import io.vertx.rx.java.RxHelper
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.core.file.AsyncFile
import io.vertx.rxjava.core.file.FileSystem
import io.vertx.rxjava.core.http.HttpClient
import io.vertx.rxjava.core.http.HttpClientRequest
import rx.Completable
import rx.Single

/**
 * Download and extract Elasticsearch to a given location if it does not exist
 * there yet
 * @author Michel Kraemer
 */
class ElasticsearchInstaller
/**
 * Create a new Elasticsearch installer
 * @param vertx a Vert.x instance
 */
(private val vertx: Vertx) {

    /**
     * Download and extract Elasticsearch to a given location. If the destination
     * exists already this method does nothing.
     * @param downloadUrl the URL to the ZIP file containing Elasticsearch
     * @param destPath the path to the destination folder where the downloaded ZIP
     * file should be extracted to
     * @param strip `true` if the first path element of all items in the
     * ZIP file should be stripped away.
     * @return a single emitting the path to the extracted Elasticsearch
     */
    @JvmOverloads
    fun download(downloadUrl: String, destPath: String,
                 strip: Boolean = true): Single<String> {
        val fs = vertx.fileSystem()
        return fs.rxExists(destPath)
                .flatMap { exists ->
                    if (exists!!) {
                        return@fs.rxExists(destPath)
                                .flatMap Single . just < String >(destPath)
                    }
                    downloadNoCheck(downloadUrl, destPath, strip)
                }
    }

    /**
     * Download and extract Elasticsearch to a given location no matter if the
     * destination folder exists or not.
     * @param downloadUrl the URL to the ZIP file containing Elasticsearch
     * @param destPath the path to the destination folder where the downloaded ZIP
     * file should be extracted to
     * @param strip `true` if the first path element of all items in the
     * ZIP file should be stripped away.
     * @return emitting the path to the extracted Elasticsearch
     */
    private fun downloadNoCheck(downloadUrl: String, destPath: String,
                                strip: Boolean): Single<String> {
        log.info("Downloading Elasticsearch ...")
        log.info("Source: $downloadUrl")
        log.info("Dest: $destPath")

        // download the archive, extract it and finally delete it
        return downloadArchive(downloadUrl)
                .flatMap { archivePath ->
                    extractArchive(archivePath, destPath, strip)
                            .doAfterTerminate {
                                val fs = vertx.fileSystem()
                                fs.deleteBlocking(archivePath)
                            }
                }
    }

    /**
     * Download the ZIP file containing Elasticsearch to a temporary location
     * @param downloadUrl the URL to the ZIP file
     * @return a single emitting the location of the temporary file
     */
    private fun downloadArchive(downloadUrl: String): Single<String> {
        // create temporary file
        val archiveFile: File
        try {
            archiveFile = File.createTempFile("elasticsearch", "tmp")
        } catch (e: IOException) {
            return Single.error(e)
        }

        // open temporary file
        val fs = vertx.fileSystem()
        val archivePath = archiveFile.absolutePath
        val openOptions = OpenOptions()
                .setCreate(true)
                .setWrite(true)
                .setTruncateExisting(true)
        return fs.rxOpen(archivePath, openOptions)
                .flatMap { file ->
                    doDownload(downloadUrl, file)
                            .doAfterTerminate {
                                file.flush()
                                file.close()
                            }
                            .toSingleDefault(archivePath)
                }
    }

    /**
     * Download a file
     * @param downloadUrl the URL to download from
     * @param dest the destination file
     * @return a Completable that will complete once the file has been downloaded
     */
    private fun doDownload(downloadUrl: String, dest: AsyncFile): Completable {
        val observable = RxHelper.observableFuture<Void>()
        val handler = observable.toHandler()

        val options = HttpClientOptions()
        if (downloadUrl.startsWith("https")) {
            options.isSsl = true
        }
        val client = vertx.createHttpClient(options)
        val req = client.getAbs(downloadUrl)

        req.exceptionHandler { t -> handler.handle(Future.failedFuture(t)) }

        req.handler { res ->
            if (res.statusCode() != 200) {
                handler.handle(Future.failedFuture(HttpException(res.statusCode(),
                        res.statusMessage())))
                return@req.handler
            }

            // get content-length
            val length: Int
            val read = intArrayOf(0)
            val lastOutput = intArrayOf(0)
            val contentLength = res.getHeader("Content-Length")
            if (contentLength != null) {
                length = Integer.parseInt(contentLength)
            } else {
                length = 0
            }

            res.exceptionHandler { t -> handler.handle(Future.failedFuture(t)) }

            // download file contents, log progress
            res.handler { buf ->
                read[0] += buf.length()
                if (lastOutput[0] == 0 || read[0] - lastOutput[0] > 1024 * 2048) {
                    logProgress(length, read[0])
                    lastOutput[0] = read[0]
                }
                dest.write(buf)
            }

            res.endHandler { v ->
                logProgress(length, read[0])
                handler.handle(Future.succeededFuture())
            }
        }

        req.end()

        return observable.toCompletable()
    }

    /**
     * Log progress of a file download
     * @param length the length of the file (may be 0 if unknown)
     * @param read the number of bytes downloaded so far
     */
    private fun logProgress(length: Int, read: Int) {
        if (length > 0) {
            log.info("Downloaded " + read + "/" + length + " bytes (" +
                    Math.round(read * 100.0 / length) + "%)")
        } else {
            log.info("Downloaded $read bytes")
        }
    }

    /**
     * Extract the Elasticsearch ZIP archive to a destination path
     * @param archivePath the path to the ZIP file
     * @param destPath the destination path
     * @param strip `true` if the first path element of all items in the
     * ZIP file should be stripped away.
     * @return emitting the path to the extracted contents (i.e.
     * `destPath`)
     */
    private fun extractArchive(archivePath: String, destPath: String,
                               strip: Boolean): Single<String> {
        val observable = RxHelper.observableFuture<String>()
        val handler = observable.toHandler()

        // extract archive asynchronously
        vertx.executeBlocking<Any>({ f ->
            val archiveFile = File(archivePath)
            val destFile = File(destPath)
            destFile.mkdirs()
            try {
                extractZip(archiveFile, destFile, strip)
                f.complete()
            } catch (e: IOException) {
                FileUtils.deleteQuietly(destFile)
                f.fail(e)
            }
        }, { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
            } else {
                handler.handle(Future.succeededFuture(destPath))
            }
        })

        // set executable permissions for Elasticsearch binary
        return observable.doOnNext { path ->
            if (!SystemUtils.IS_OS_WINDOWS) {
                log.info("Set executable permissions for \"bin/elasticsearch\"")
                val archiveFile = File(path)
                val executable = File(archiveFile, "bin/elasticsearch")
                executable.setExecutable(true)
            }
        }.toSingle()
    }

    /**
     * Extract a ZIP file to a destination directory. This method is blocking!
     * @param file the ZIP file to extract
     * @param destDir the destination directory
     * @param strip `true` if the first path element of all items in the
     * ZIP file should be stripped away.
     * @throws IOException if an I/O error occurred while extracting
     */
    @Throws(IOException::class)
    private fun extractZip(file: File, destDir: File, strip: Boolean) {
        log.info("Extracting archive ... 0%")

        // open ZIP file
        ZipFile(file).use { zipFile ->
            val entries = zipFile.entries()
            var count = 0

            // extract all elements
            while (entries.hasMoreElements()) {
                val entry = entries.nextElement()
                val name: String

                // strip first path element
                if (strip) {
                    name = entry.name.substring(FilenameUtils.separatorsToUnix(
                            entry.name).indexOf('/') + 1)
                } else {
                    name = entry.name
                }

                val dest = File(destDir, name)
                if (entry.isDirectory) {
                    // create directories
                    dest.mkdirs()
                } else {
                    // extract files
                    dest.parentFile.mkdirs()
                    zipFile.getInputStream(entry).use { `in` -> FileOutputStream(dest).use { out -> IOUtils.copy(`in`, out) } }
                }

                // log progress
                count++
                val p = Math.round(count * 100.0 / zipFile.size())
                if (p % 10 == 0L) {
                    log.info("Extracting archive ... $p%")
                }
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ElasticsearchInstaller::class.java)
    }
}
/**
 * Download and extract Elasticsearch to a given location. If the destination
 * exists already this method does nothing.
 * @param downloadUrl the URL to the ZIP file containing Elasticsearch
 * @param destPath the path to the destination folder where the downloaded ZIP
 * file should be extracted to
 * @return a single emitting the path to the extracted Elasticsearch
 */
