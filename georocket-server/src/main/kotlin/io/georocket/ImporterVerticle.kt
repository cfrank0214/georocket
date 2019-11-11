package io.georocket

import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.index.xml.XMLCRSIndexer
import io.georocket.input.Splitter.Result
import io.georocket.input.geojson.GeoJsonSplitter
import io.georocket.input.xml.FirstLevelSplitter
import io.georocket.input.xml.XMLSplitter
import io.georocket.storage.*
import io.georocket.tasks.ImportingTask
import io.georocket.tasks.TaskError
import io.georocket.util.*
import io.georocket.util.io.RxGzipReadStream
import io.vertx.core.file.OpenOptions
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.buffer.Buffer
import io.vertx.rxjava.core.eventbus.Message
import io.vertx.rxjava.core.file.AsyncFile
import io.vertx.rxjava.core.file.FileSystem
import io.vertx.rxjava.core.streams.ReadStream
import rx.Completable
import rx.Observable
import rx.Single

import java.time.Instant
import java.util.HashSet
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer
import java.util.stream.Collectors
import java.util.stream.Stream

import io.georocket.util.MimeTypeUtils.belongsTo
import io.vertx.core.Handler

/**
 * Imports file in the background
 * @author Michel Kraemer
 */
class ImporterVerticle : AbstractVerticle() {

    protected var store: RxStore = null
    private var incoming: String? = null
    private var paused: Boolean = false
    private val filesBeingImported = HashSet<AsyncFile>()

    override fun start() {
        log.info("Launching importer ...")

        store = RxStore(StoreFactory.createStore(getVertx()))
        val storagePath = config().getString(ConfigConstants.STORAGE_FILE_PATH)
        incoming = "$storagePath/incoming"

        vertx.eventBus().localConsumer<JsonObject>(AddressConstants.IMPORTER_IMPORT)
                .toObservable()
                .onBackpressureBuffer() // unlimited buffer
                .flatMapCompletable({ msg ->
                    // call onImport() but ignore errors. onImport() will handle errors for us.
                    onImport(msg).onErrorComplete()
                }, false, MAX_PARALLEL_IMPORTS)
                .subscribe({ v ->
                    // ignore
                }, { err ->
                    // This is bad. It will unsubscribe the consumer from the eventbus!
                    // Should never happen anyhow. If it does, something else has
                    // completely gone wrong.
                    log.fatal("Could not import file", err)
                })

        vertx.eventBus().localConsumer<Boolean>(AddressConstants.IMPORTER_PAUSE, Handler<Message<Boolean>> { this.onPause(it) })
    }

    /**
     * Receives a name of a file to import
     * @param msg the event bus message containing the filename
     * @return a Completable that will complete when the file has been imported
     */
    protected fun onImport(msg: Message<JsonObject>): Completable {
        val body = msg.body()
        val filename = body.getString("filename")
        val filepath = "$incoming/$filename"
        val layer = body.getString("layer", "/")
        val contentType = body.getString("contentType")
        val correlationId = body.getString("correlationId")
        val fallbackCRSString = body.getString("fallbackCRSString")
        val contentEncoding = body.getString("contentEncoding")

        // get tags
        val tagsArr = body.getJsonArray("tags")
        val tags = tagsArr?.stream()?.flatMap { o ->
            if (o != null)
                Stream.of(o.toString())
            else
                Stream.of()
        }?.collect<List<String>, Any>(Collectors.toList())

        // get properties
        val propertiesObj = body.getJsonObject("properties")
        val properties = propertiesObj?.map

        // generate timestamp for this import
        val timestamp = System.currentTimeMillis()

        log.info("Importing [$correlationId] to layer '$layer'")

        val fs = vertx.fileSystem()
        val openOptions = OpenOptions().setCreate(false).setWrite(false)
        return fs.rxOpen(filepath, openOptions)
                .flatMap { f ->
                    filesBeingImported.add(f)
                    importFile(contentType, f, correlationId, filename, timestamp,
                            layer, tags, properties, fallbackCRSString, contentEncoding)
                            .doAfterTerminate {
                                // delete file from 'incoming' folder
                                log.debug("Deleting $filepath from incoming folder")
                                filesBeingImported.remove(f)
                                f.rxClose()
                                        .flatMap { v -> fs.rxDelete(filepath) }
                                        .subscribe({ v -> }, { err -> log.error("Could not delete file from 'incoming' folder", err) })
                            }
                }
                .doOnSuccess { chunkCount ->
                    val duration = System.currentTimeMillis() - timestamp
                    log.info("Finished importing [" + correlationId + "] with " + chunkCount +
                            " chunks to layer '" + layer + "' after " + duration + " ms")
                }
                .doOnError { err ->
                    val duration = System.currentTimeMillis() - timestamp
                    log.error("Failed to import [" + correlationId + "] to layer '" +
                            layer + "' after " + duration + " ms", err)
                }
                .toCompletable()
    }

    /**
     * Import a file from the given read stream into the store. Inspect the file's
     * content type and forward to the correct import method.
     * @param contentType the file's content type
     * @param f the file to import
     * @param correlationId a unique identifier for this import process
     * @param filename the name of the file currently being imported
     * @param timestamp denotes when the import process has started
     * @param layer the layer where the file should be stored (may be null)
     * @param tags the list of tags to attach to the file (may be null)
     * @param properties the map of properties to attach to the file (may be null)
     * @param fallbackCRSString the CRS which should be used if the imported
     * file does not specify one (may be `null`)
     * @param contentEncoding the content encoding of the file to be
     * imported (e.g. "gzip"). May be `null`.
     * @return a single that will emit with the number if chunks imported
     * when the file has been imported
     */
    protected fun importFile(contentType: String, f: ReadStream<Buffer>,
                             correlationId: String, filename: String, timestamp: Long, layer: String,
                             tags: List<String>?, properties: Map<String, Any>?, fallbackCRSString: String,
                             contentEncoding: String?): Single<Int> {
        var f = f
        if ("gzip" == contentEncoding) {
            log.debug("Importing file compressed with GZIP")
            f = RxGzipReadStream(f)
        } else if (contentEncoding != null && !contentEncoding.isEmpty()) {
            log.warn("Unknown content encoding: `$contentEncoding'. Trying anyway.")
        }

        // let the task verticle know that we're now importing
        val startTask = ImportingTask(correlationId)
        startTask.startTime = Instant.now()
        vertx.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(startTask))

        val result: Observable<Int>
        if (belongsTo(contentType, "application", "xml") || belongsTo(contentType, "text", "xml")) {
            result = importXML(f, correlationId, filename, timestamp, layer, tags,
                    properties, fallbackCRSString)
        } else if (belongsTo(contentType, "application", "json")) {
            result = importJSON(f, correlationId, filename, timestamp, layer, tags, properties)
        } else {
            result = Observable.error(NoStackTraceThrowable(String.format(
                    "Received an unexpected content type '%s' while trying to import " + "file '%s'", contentType, filename)))
        }

        val onFinish = { t ->
            // let the task verticle know that the import process has finished
            val endTask = ImportingTask(correlationId)
            endTask.endTime = Instant.now()
            if (t != null) {
                endTask.addError(TaskError(t!!))
            }
            vertx.eventBus().publish(AddressConstants.TASK_INC,
                    JsonObject.mapFrom(endTask))
        }

        return result.window(100)
                .flatMap<Int>(Func1<Observable<Int>, Observable<out Int>> { it.count() })
                .doOnNext { n ->
                    // let the task verticle know that we imported n chunks
                    val currentTask = ImportingTask(correlationId)
                    currentTask.importedChunks = n
                    vertx.eventBus().publish(AddressConstants.TASK_INC,
                            JsonObject.mapFrom(currentTask))
                }
                .reduce(0, { a, b -> a!! + b!! })
                .toSingle()
                .doOnError(Action1<Throwable> { onFinish.accept(it) })
                .doOnSuccess { i -> onFinish.accept(null) }
    }

    /**
     * Imports an XML file from the given input stream into the store
     * @param f the XML file to read
     * @param correlationId a unique identifier for this import process
     * @param filename the name of the file currently being imported
     * @param timestamp denotes when the import process has started
     * @param layer the layer where the file should be stored (may be null)
     * @param tags the list of tags to attach to the file (may be null)
     * @param properties the map of properties to attach to the file (may be null)
     * @param fallbackCRSString the CRS which should be used if the imported
     * file does not specify one (may be `null`)
     * @return an observable that will emit the number 1 when a chunk has been imported
     */
    protected fun importXML(f: ReadStream<Buffer>, correlationId: String,
                            filename: String, timestamp: Long, layer: String, tags: List<String>?,
                            properties: Map<String, Any>?, fallbackCRSString: String): Observable<Int> {
        val bomFilter = UTF8BomFilter()
        val window = Window()
        val splitter = FirstLevelSplitter(window)
        val processing = AtomicInteger(0)
        val crsIndexer = XMLCRSIndexer()
        return f.toObservable()
                .map<Buffer>(Func1<Buffer, Buffer> { it.getDelegate() })
                .map<Buffer>(Func1<Buffer, Buffer> { bomFilter.filter(it) })
                .doOnNext(Action1<Buffer> { window.append(it) })
                .compose<XMLStreamEvent>(XMLParserTransformer())
                .doOnNext { e ->
                    // save the first CRS found in the file
                    if (crsIndexer.crs == null) {
                        crsIndexer.onEvent(e)
                    }
                }
                .flatMap<Result<XMLChunkMeta>>(Func1<XMLStreamEvent, Observable<out Result<XMLChunkMeta>>> { splitter.onEventObservable(it) })
                .flatMapSingle { result ->
                    var crsString = fallbackCRSString
                    if (crsIndexer.crs != null) {
                        crsString = crsIndexer.crs
                    }
                    val indexMeta = IndexMeta(correlationId, filename,
                            timestamp, tags, properties, crsString)
                    addToStoreWithPause(result, layer, indexMeta, f, processing)
                            .toSingleDefault(1)
                }
    }

    /**
     * Imports a JSON file from the given input stream into the store
     * @param f the JSON file to read
     * @param correlationId a unique identifier for this import process
     * @param filename the name of the file currently being imported
     * @param timestamp denotes when the import process has started
     * @param layer the layer where the file should be stored (may be null)
     * @param tags the list of tags to attach to the file (may be null)
     * @param properties the map of properties to attach to the file (may be null)
     * @return an observable that will emit the number 1 when a chunk has been imported
     */
    protected fun importJSON(f: ReadStream<Buffer>, correlationId: String,
                             filename: String, timestamp: Long, layer: String, tags: List<String>?, properties: Map<String, Any>?): Observable<Int> {
        val bomFilter = UTF8BomFilter()
        val window = StringWindow()
        val splitter = GeoJsonSplitter(window)
        val processing = AtomicInteger(0)
        return f.toObservable()
                .map<Buffer>(Func1<Buffer, Buffer> { it.getDelegate() })
                .map<Buffer>(Func1<Buffer, Buffer> { bomFilter.filter(it) })
                .doOnNext(Action1<Buffer> { window.append(it) })
                .compose<JsonStreamEvent>(JsonParserTransformer())
                .flatMap<Result<JsonChunkMeta>>(Func1<JsonStreamEvent, Observable<out Result<JsonChunkMeta>>> { splitter.onEventObservable(it) })
                .flatMapSingle { result ->
                    val indexMeta = IndexMeta(correlationId, filename,
                            timestamp, tags, properties, null)
                    addToStoreWithPause(result, layer, indexMeta, f, processing)
                            .toSingleDefault(1)
                }
    }

    /**
     * Handle a pause message
     * @param msg the message
     */
    private fun onPause(msg: Message<Boolean>) {
        val paused = msg.body()
        if (paused == null || !paused) {
            if (this.paused) {
                log.info("Resuming import")
                this.paused = false
                for (f in filesBeingImported) {
                    f.resume()
                }
            }
        } else {
            if (!this.paused) {
                log.info("Pausing import")
                this.paused = true
                for (f in filesBeingImported) {
                    f.pause()
                }
            }
        }
    }

    /**
     * Add a chunk to the store. Pause the given read stream before adding and
     * increase the given counter. Decrease the counter after the chunk has been
     * written and only resume the read stream if the counter is `0`.
     * This is necessary because the writing to the store may take longer than
     * reading. We need to pause reading so the store is not overloaded (i.e.
     * we handle back-pressure here).
     * @param chunk the chunk to write
     * @param layer the layer the chunk should be added to (may be null)
     * @param indexMeta metadata specifying how the chunk should be indexed
     * @param f the read stream to pause while writing
     * @param processing an AtomicInteger keeping the number of chunks currently
     * being written (should be initialized to `0` the first time this
     * method is called)
     * @return a Completable that will complete when the operation has finished
     */
    private fun addToStoreWithPause(chunk: Result<out ChunkMeta>,
                                    layer: String, indexMeta: IndexMeta, f: ReadStream<Buffer>, processing: AtomicInteger): Completable {
        // pause stream while chunk is being written
        f.pause()

        // count number of chunks being written
        processing.incrementAndGet()

        return addToStore(chunk.chunk, chunk.meta, layer, indexMeta)
                .doOnCompleted {
                    // resume stream only after all chunks from the current
                    // buffer have been stored
                    if (processing.decrementAndGet() == 0 && !paused) {
                        f.resume()
                    }
                }
    }

    /**
     * Add a chunk to the store. Retry operation several times before failing.
     * @param chunk the chunk to add
     * @param meta the chunk's metadata
     * @param layer the layer the chunk should be added to (may be null)
     * @param indexMeta metadata specifying how the chunk should be indexed
     * @return a Completable that will complete when the operation has finished
     */
    protected fun addToStore(chunk: String, meta: ChunkMeta,
                             layer: String, indexMeta: IndexMeta): Completable {
        return Completable.defer { store.rxAdd(chunk, meta, layer, indexMeta) }
                .retryWhen(RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log))
    }

    companion object {
        private val log = LoggerFactory.getLogger(ImporterVerticle::class.java)

        private val MAX_RETRIES = 5
        private val RETRY_INTERVAL = 1000
        private val MAX_PARALLEL_IMPORTS = 1
    }
}
