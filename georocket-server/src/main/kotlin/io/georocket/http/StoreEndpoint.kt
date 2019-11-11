package io.georocket.http

import com.google.common.base.Splitter
import io.georocket.ServerAPIException
import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.georocket.output.Merger
import io.georocket.output.MultiMerger
import io.georocket.storage.ChunkMeta
import io.georocket.storage.DeleteMeta
import io.georocket.storage.RxAsyncCursor
import io.georocket.storage.RxStore
import io.georocket.storage.RxStoreCursor
import io.georocket.storage.StoreCursor
import io.georocket.storage.StoreFactory
import io.georocket.tasks.ReceivingTask
import io.georocket.tasks.TaskError
import io.georocket.util.HttpException
import io.georocket.util.MimeTypeUtils
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.file.AsyncFile
import io.vertx.core.file.FileSystem
import io.vertx.core.file.OpenOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.core.streams.Pump
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.rx.java.ObservableFuture
import io.vertx.rx.java.RxHelper
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.tuple.Pair
import org.apache.commons.text.StringEscapeUtils
import org.apache.http.ParseException
import org.apache.http.entity.ContentType
import org.bson.types.ObjectId
import rx.Completable
import rx.Observable
import rx.Single

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.time.Instant
import java.util.Arrays
import java.util.HashMap
import java.util.regex.Pattern

import io.georocket.http.Endpoint.fail
import io.georocket.http.Endpoint.getEndpointPath

/**
 * An HTTP endpoint handling requests related to the GeoRocket data store
 * @author Michel Kraemer
 */
class StoreEndpoint : Endpoint {

    private var vertx: Vertx? = null
    private var store: RxStore? = null
    private var storagePath: String? = null

    override fun getMountPoint(): String {
        return "/store"
    }

    override fun createRouter(vertx: Vertx): Router {
        this.vertx = vertx

        store = RxStore(StoreFactory.createStore(vertx))
        storagePath = vertx.orCreateContext.config()
                .getString(ConfigConstants.STORAGE_FILE_PATH)

        val router = Router.router(vertx)
        router.get("/*").handler { this.onGet(it) }
        router.put("/*").handler { this.onPut(it) }
        router.post("/*").handler { this.onPost(it) }
        router.delete("/*").handler { this.onDelete(it) }

        return router
    }

    /**
     * Create a new merger
     * @param ctx routing context
     * @param optimisticMerging `true` if optimistic merging is enabled
     * @return the new merger instance
     */
    protected fun createMerger(ctx: RoutingContext,
                               optimisticMerging: Boolean): Merger<ChunkMeta> {
        return MultiMerger(optimisticMerging)
    }

    /**
     * Initialize the given merger. Perform a search using the given search string
     * and pass all chunk metadata retrieved to the merger.
     * @param merger the merger to initialize
     * @param data data to use for the initialization
     * @return a Completable that will complete when the merger has been
     * initialized with all results
     */
    private fun initializeMerger(merger: Merger<ChunkMeta>, data: Single<StoreCursor>): Completable {
        return data
                .map<RxStoreCursor>(Func1<StoreCursor, RxStoreCursor> { RxStoreCursor(it) })
                .flatMapObservable<Pair<ChunkMeta, String>>(Func1<RxStoreCursor, Observable<out Pair<ChunkMeta, String>>> { it.toObservable() })
                .map<ChunkMeta>(Func1<Pair<ChunkMeta, String>, ChunkMeta> { it.getLeft() })
                .flatMapCompletable(Func1<ChunkMeta, Completable> { merger.init(it) })
                .toCompletable()
    }

    /**
     * Perform a search and merge all retrieved chunks using the given merger
     * @param merger the merger
     * @param data Data to merge into the response
     * @param out the response to write the merged chunks to
     * @param trailersAllowed `true` if the HTTP client accepts trailers
     * @return a single that will emit one item when all chunks have been merged
     */
    private fun doMerge(merger: Merger<ChunkMeta>, data: Single<StoreCursor>,
                        out: HttpServerResponse, trailersAllowed: Boolean): Completable {
        return data
                .map<RxStoreCursor>(Func1<StoreCursor, RxStoreCursor> { RxStoreCursor(it) })
                .flatMapObservable<Pair<ChunkMeta, String>>(Func1<RxStoreCursor, Observable<out Pair<ChunkMeta, String>>> { it.toObservable() })
                .flatMapSingle({ p ->
                    store!!.rxGetOne(p.right)
                            .flatMap { crs ->
                                merger.merge(crs, p.left, out)
                                        .toSingleDefault(Pair.of(1L, 0L)) // left: count, right: not_accepted
                                        .onErrorResumeNext { t ->
                                            if (t is IllegalStateException) {
                                                // Chunk cannot be merged. maybe it's a new one that has
                                                // been added after the merger was initialized. Just
                                                // ignore it, but emit a warning later
                                                return@merger.merge(crs, p.left, out)
                                                        .toSingleDefault(Pair.of(1L, 0L)) // left: count, right: not_accepted
                                                        .onErrorResumeNext Single . just < Pair < Long, Long>>(Pair.of<Long, Long>(0L, 1L))
                                            }
                                            Single.error(t)
                                        }
                                        .doAfterTerminate {
                                            // don't forget to close the chunk!
                                            crs.close()
                                        }
                            }
                }, false, 1 /* write only one chunk concurrently to the output stream */)
                .defaultIfEmpty(Pair.of(0L, 0L))
                .reduce { p1, p2 ->
                    Pair.of(p1.left + p2.left,
                            p1.right + p2.right)
                }
                .flatMapCompletable { p ->
                    val count = p.left
                    val notaccepted = p.right
                    if (notaccepted > 0) {
                        log.warn("Could not merge " + notaccepted + " chunks "
                                + "because the merger did not accept them. Most likely "
                                + "these are new chunks that were added while "
                                + "merging was in progress or those that were ignored "
                                + "during optimistic merging. If this worries you, "
                                + "just repeat the request.")
                    }
                    if (trailersAllowed) {
                        out.putTrailer(TRAILER_UNMERGED_CHUNKS, notaccepted.toString())
                    }
                    if (count > 0) {
                        merger.finish(out)
                        return@data
                                .map(RxStoreCursor::new)
                                .flatMapObservable(RxStoreCursor::toObservable)
                                .flatMapSingle(p -> store.rxGetOne(p.getRight())
                        .flatMap(crs -> merger.merge(crs, p.getLeft(), out)
                        .toSingleDefault(Pair.of(1L, 0L)) // left: count, right: not_accepted
                                .onErrorResumeNext(t -> {
                            return if (t instanceof IllegalStateException) {
                                // Chunk cannot be merged. maybe it's a new one that has
                                // been added after the merger was initialized. Just
                                // ignore it, but emit a warning later
                                Single.just(Pair.of(0L, 1L));
                            } else Single.error(t)
                        })
                        .doAfterTerminate(() -> {
                            // don't forget to close the chunk!
                            crs.close();
                        })), false, 1 /* write only one chunk concurrently to the output stream */)
                        .defaultIfEmpty(Pair.of(0L, 0L))
                                .reduce(p1, p2) -> Pair.of(p1.getLeft()+p2.getLeft(),
                        p1.getRight() + p2.getRight()))
                        .flatMapCompletable Completable . complete ()
                    } else {
                        return@data
                                .map(RxStoreCursor::new)
                                .flatMapObservable(RxStoreCursor::toObservable)
                                .flatMapSingle(p -> store.rxGetOne(p.getRight())
                        .flatMap(crs -> merger.merge(crs, p.getLeft(), out)
                        .toSingleDefault(Pair.of(1L, 0L)) // left: count, right: not_accepted
                                .onErrorResumeNext(t -> {
                            return if (t instanceof IllegalStateException) {
                                // Chunk cannot be merged. maybe it's a new one that has
                                // been added after the merger was initialized. Just
                                // ignore it, but emit a warning later
                                Single.just(Pair.of(0L, 1L));
                            } else Single.error(t)
                        })
                        .doAfterTerminate(() -> {
                            // don't forget to close the chunk!
                            crs.close();
                        })), false, 1 /* write only one chunk concurrently to the output stream */)
                        .defaultIfEmpty(Pair.of(0L, 0L))
                                .reduce(p1, p2) -> Pair.of(p1.getLeft()+p2.getLeft(),
                        p1.getRight() + p2.getRight()))
                        .flatMapCompletable Completable . error FileNotFoundException("Not Found")
                    }
                }
                .toCompletable()
    }

    /**
     * Read the context, select the right StoreCursor and set the response header.
     * For the first call to this method within a request the `preview`
     * parameter must equal `true` and for the second one (if there
     * is one) the parameter must equal `false`. This method must not
     * be called more than two times within a request.
     * @param context the routing context
     * @param preview `true` if the cursor should be used to generate
     * a preview or to initialize the merger
     * @return a Single providing a StoreCursor
     */
    protected fun prepareCursor(context: RoutingContext, preview: Boolean): Single<StoreCursor> {
        val request = context.request()
        val response = context.response()

        val scroll = request.getParam("scroll")
        val scrollIdParam = request.getParam("scrollId")
        val scrolling = BooleanUtils.toBoolean(scroll) || scrollIdParam != null

        // if we're generating a preview, split the scrollId param at ':' and
        // use the first part. Otherwise use the second one.
        val scrollId: String?
        if (scrollIdParam != null) {
            val scrollIdParts = scrollIdParam.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            if (preview) {
                scrollId = scrollIdParts[0]
            } else {
                scrollId = scrollIdParts[1]
            }
        } else {
            scrollId = null
        }

        val path = getEndpointPath(context)
        val search = request.getParam("search")
        val strSize = request.getParam("size")
        val size = if (strSize == null) 100 else Integer.parseInt(strSize)

        return Single.defer<StoreCursor> {
            if (scrolling) {
                if (scrollId == null) {
                    return@Single.defer store !!. rxScroll search, path, size)
                } else {
                    return@Single.defer store !!. rxScroll scrollId
                }
            } else {
                return@Single.defer store !!. rxGet search, path)
            }
        }.doOnSuccess { cursor ->
            if (scrolling) {
                // create a new scrollId consisting of the one used for the preview and
                // the other one used for the real query
                var newScrollId = cursor.info.scrollId
                if (!preview) {
                    var oldScrollId: String? = response.headers().get("X-Scroll-Id")
                    if (isOptimisticMerging(request)) {
                        oldScrollId = "0"
                    } else checkNotNull(oldScrollId) {
                        "A preview must be generated " +
                                "before the actual request can be made. This usually happens " +
                                "when the merger is initialized."
                    }
                    newScrollId = "$oldScrollId:$newScrollId"
                }
                response
                        .putHeader("X-Scroll-Id", newScrollId)
                        .putHeader("X-Total-Hits", cursor.info.totalHits.toString())
                        .putHeader("X-Hits", cursor.info.currentHits.toString())
            }
        }
    }

    /**
     * Handles the HTTP GET request for a bunch of chunks
     * @param context the routing context
     */
    protected fun onGet(context: RoutingContext) {
        val request = context.request()
        val response = context.response()

        val path = getEndpointPath(context)
        val search = request.getParam("search")
        val property = request.getParam("property")
        val attribute = request.getParam("attribute")

        if (property != null && attribute != null) {
            response
                    .setStatusCode(400)
                    .end("You can only get the values of a property or an attribute, but not both")
        } else if (property != null) {
            getPropertyValues(search, path, property, response)
        } else if (attribute != null) {
            getAttributeValues(search, path, attribute, response)
        } else {
            getChunks(context)
        }
    }

    /**
     * Checks if optimistic merging is enabled
     * @param request the HTTP request
     * @return `true` if optimistic is enabled, `false` otherwise
     */
    private fun isOptimisticMerging(request: HttpServerRequest): Boolean {
        return BooleanUtils.toBoolean(request.getParam("optimisticMerging"))
    }

    /**
     * Checks if the client accepts an HTTP trailer
     * @param request the HTTP request
     * @return `true` if the client accepts a trailer, `false` otherwise
     */
    private fun isTrailerAccepted(request: HttpServerRequest): Boolean {
        val te = request.getHeader("TE")
        return te != null && te.toLowerCase().contains("trailers")
    }

    /**
     * Retrieve all chunks matching the specified query and path
     * @param context the routing context
     */
    private fun getChunks(context: RoutingContext) {
        val request = context.request()
        val response = context.response()

        // Our responses must always be chunked because we cannot calculate
        // the exact content-length beforehand. We perform two searches, one to
        // initialize the merger and one to do the actual merge. The problem is
        // that the result set may change between these two searches and so we
        // cannot calculate the content-length just from looking at the result
        // from the first search.
        response.isChunked = true

        val optimisticMerging = isOptimisticMerging(request)
        val isTrailerAccepted = isTrailerAccepted(request)

        if (isTrailerAccepted) {
            response.putHeader("Trailer", TRAILER_UNMERGED_CHUNKS)
        }

        // perform two searches: first initialize the merger and then
        // merge all retrieved chunks
        val merger = createMerger(context, optimisticMerging)

        val c: Completable
        if (optimisticMerging) {
            // skip initialization if optimistic merging is enabled
            c = Completable.complete()
        } else {
            c = initializeMerger(merger, prepareCursor(context, true))
        }

        c.andThen(Completable.defer {
            doMerge(merger, prepareCursor(context, false),
                    response, isTrailerAccepted)
        })
                .subscribe(Action0 { response.end() }, { err ->
                    if (err !is FileNotFoundException) {
                        log.error("Could not perform query", err)
                    }
                    fail(response, err)
                })
    }

    /**
     * Get all values for the specified attribute
     * @param search the search query
     * @param path the path
     * @param attribute the name of the attribute
     * @param response the http response
     */
    private fun getAttributeValues(search: String, path: String, attribute: String,
                                   response: HttpServerResponse) {
        val first = arrayOf(true)
        response.isChunked = true
        response.write("[")

        store!!.rxGetAttributeValues(search, path, attribute)
                .flatMapObservable { x -> RxAsyncCursor(x).toObservable() }
                .subscribe(
                        { x ->
                            if (first[0]) {
                                first[0] = false
                            } else {
                                response.write(",")
                            }
                            response.write("\"" + StringEscapeUtils.escapeJson(x) + "\"")
                        },
                        { err -> fail(response, err) },
                        {
                            response
                                    .write("]")
                                    .setStatusCode(200)
                                    .end()
                        })
    }

    /**
     * Get all values for the specified property
     * @param search the search query
     * @param path the path
     * @param property the name of the property
     * @param response the http response
     */
    private fun getPropertyValues(search: String, path: String, property: String,
                                  response: HttpServerResponse) {
        val first = arrayOf(true)
        response.isChunked = true
        response.write("[")

        store!!.rxGetPropertyValues(search, path, property)
                .flatMapObservable { x -> RxAsyncCursor(x).toObservable() }
                .subscribe(
                        { x ->
                            if (first[0]) {
                                first[0] = false
                            } else {
                                response.write(",")
                            }
                            response.write("\"" + StringEscapeUtils.escapeJson(x) + "\"")
                        },
                        { err -> fail(response, err) },
                        {
                            response
                                    .write("]")
                                    .setStatusCode(200)
                                    .end()
                        })
    }

    /**
     * Try to detect the content type of a file
     * @param filepath the absolute path to the file to analyse
     * @param gzip true if the file is compressed with GZIP
     * @return an observable emitting either the detected content type or an error
     * if the content type could not be detected or the file could not be read
     */
    private fun detectContentType(filepath: String, gzip: Boolean): Observable<String> {
        val result = RxHelper.observableFuture<String>()
        val resultHandler = result.toHandler()

        vertx!!.executeBlocking<String>({ f ->
            try {
                var mimeType = MimeTypeUtils.detect(File(filepath), gzip)
                if (mimeType == null) {
                    log.warn("Could not detect file type for " + filepath + ". Using "
                            + "application/octet-stream.")
                    mimeType = "application/octet-stream"
                }
                f.complete(mimeType)
            } catch (e: IOException) {
                f.fail(e)
            }
        }, { ar ->
            if (ar.failed()) {
                resultHandler.handle(Future.failedFuture(ar.cause()))
            } else {
                val ct = ar.result()
                if (ct != null) {
                    resultHandler.handle(Future.succeededFuture(ar.result()))
                } else {
                    resultHandler.handle(Future.failedFuture(HttpException(215)))
                }
            }
        })

        return result
    }

    /**
     * Handles the HTTP POST request
     * @param context the routing context
     */
    private fun onPost(context: RoutingContext) {
        val request = context.request()
        request.pause()

        val layer = getEndpointPath(context)
        val tagsStr = request.getParam("tags")
        val propertiesStr = request.getParam("props")
        val fallbackCRSString = request.getParam("fallbackCRS")

        val tags = if (StringUtils.isNotEmpty(tagsStr))
            Splitter.on(',')
                    .trimResults().splitToList(tagsStr)
        else
            null

        val properties = HashMap<String, Any>()
        if (StringUtils.isNotEmpty(propertiesStr)) {
            val regex = "(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":")
            val parts = propertiesStr.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            for (part in parts) {
                part = part.trim { it <= ' ' }
                val property = part.split(regex.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                if (property.size != 2) {
                    request.response()
                            .setStatusCode(400)
                            .end("Invalid property syntax: $part")
                    return
                }
                val key = StringEscapeUtils.unescapeJava(property[0].trim { it <= ' ' })
                val value = StringEscapeUtils.unescapeJava(property[1].trim { it <= ' ' })
                properties[key] = value
            }
        }

        // get temporary filename
        val incoming = storagePath!! + "/incoming"
        val filename = ObjectId().toString()
        val filepath = "$incoming/$filename"

        val correlationId = ObjectId().toString()
        val startTime = System.currentTimeMillis()
        this.onReceivingFileStarted(correlationId, startTime)

        // create directory for incoming files
        val fs = vertx!!.fileSystem()
        val observable = RxHelper.observableFuture<Void>()
        fs.mkdirs(incoming, observable.toHandler())
        observable
                .flatMap { v ->
                    // create temporary file
                    val openObservable = RxHelper.observableFuture<AsyncFile>()
                    fs.open(filepath, OpenOptions(), openObservable.toHandler())
                    openObservable
                }
                .flatMap { f ->
                    // write request body into temporary file
                    val pumpObservable = RxHelper.observableFuture<Void>()
                    val pumpHandler = pumpObservable.toHandler()
                    Pump.pump<Buffer>(request, f).start()
                    val errHandler = { t: Throwable ->
                        request.endHandler(null)
                        f.close()
                        pumpHandler.handle(Future.failedFuture(t))
                    }
                    f.exceptionHandler(errHandler)
                    request.exceptionHandler(errHandler)
                    request.endHandler { v -> f.close { v2 -> pumpHandler.handle(Future.succeededFuture()) } }
                    request.resume()
                    pumpObservable
                }
                .flatMap { v ->
                    val contentTypeHeader = request.getHeader("Content-Type")
                    var mimeType: String? = null

                    try {
                        val contentType = ContentType.parse(contentTypeHeader)
                        mimeType = contentType.mimeType
                    } catch (ex: ParseException) {
                        // mimeType already null
                    } catch (ex: IllegalArgumentException) {
                    }

                    val contentEncoding = request.getHeader("Content-Encoding")
                    var gzip = false
                    if ("gzip" == contentEncoding) {
                        gzip = true
                    }

                    // detect content type of file to import
                    if (mimeType == null || mimeType.trim { it <= ' ' }.isEmpty() ||
                            mimeType == "application/octet-stream" ||
                            mimeType == "application/x-www-form-urlencoded") {
                        // fallback: if the client has not sent a Content-Type or if it's
                        // a generic one, then try to guess it
                        log.debug("Mime type '" + mimeType + "' is invalid or generic. "
                                + "Trying to guess the right type.")
                        return@observable
                                .flatMap(v -> {
                            // create temporary file
                            ObservableFuture<AsyncFile> openObservable = RxHelper . observableFuture ();
                            fs.open(filepath, new OpenOptions (), openObservable.toHandler());
                            return openObservable;
                        })
                        .flatMap(f -> {
                            // write request body into temporary file
                            ObservableFuture<Void> pumpObservable = RxHelper . observableFuture ();
                            Handler<AsyncResult<Void>> pumpHandler = pumpObservable . toHandler ();
                            Pump.pump(request, f).start();
                            Handler<Throwable> errHandler =(Throwable t) -> {
                            request.endHandler(null);
                            f.close();
                            pumpHandler.handle(Future.failedFuture(t));
                        };
                            f.exceptionHandler(errHandler);
                            request.exceptionHandler(errHandler);
                            request.endHandler(v -> f.close(v2 -> pumpHandler.handle(Future.succeededFuture())));
                            request.resume();
                            return pumpObservable;
                        })
                        .flatMap detectContentType filepath, gzip).doOnNext({ guessedType -> log.info("Guessed mime type '$guessedType'.") })
                    }

                    Observable.just<String>(mimeType)
                }
                .subscribe({ detectedContentType ->
                    val duration = System.currentTimeMillis() - startTime
                    this.onReceivingFileFinished(correlationId, duration, null)

                    val contentEncoding = request.getHeader("Content-Encoding")

                    // run importer
                    val msg = JsonObject()
                            .put("filename", filename)
                            .put("layer", layer)
                            .put("contentType", detectedContentType)
                            .put("correlationId", correlationId)
                            .put("contentEncoding", contentEncoding)

                    if (tags != null) {
                        msg.put("tags", JsonArray(tags))
                    }

                    if (!properties.isEmpty()) {
                        msg.put("properties", JsonObject(properties))
                    }

                    if (fallbackCRSString != null) {
                        msg.put("fallbackCRSString", fallbackCRSString)
                    }

                    request.response()
                            .setStatusCode(202) // Accepted
                            .putHeader("X-Correlation-Id", correlationId)
                            .setStatusMessage("Accepted file - importing in progress")
                            .end()

                    // run importer
                    vertx!!.eventBus().send(AddressConstants.IMPORTER_IMPORT, msg)
                }, { err ->
                    val duration = System.currentTimeMillis() - startTime
                    this.onReceivingFileFinished(correlationId, duration, err)
                    fail(request.response(), err)
                    err.printStackTrace()
                    fs.delete(filepath) { ar -> }
                })
    }

    private fun onReceivingFileStarted(correlationId: String, startTime: Long) {
        log.info("Receiving file [$correlationId]")
        val task = ReceivingTask(correlationId)
        task.startTime = Instant.now()
        vertx!!.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(task))
    }

    private fun onReceivingFileFinished(correlationId: String, duration: Long,
                                        error: Throwable?) {
        if (error == null) {
            log.info(String.format("Finished receiving file [%s] after %d ms",
                    correlationId, duration))
        } else {
            log.error(String.format("Failed receiving file [%s] after %d ms",
                    correlationId, duration), error)
        }
        val task = ReceivingTask(correlationId)
        task.endTime = Instant.now()
        if (error != null) {
            task.addError(TaskError(error))
        }
        vertx!!.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(task))
    }

    /**
     * Handles the HTTP DELETE request
     * @param context the routing context
     */
    private fun onDelete(context: RoutingContext) {
        val path = getEndpointPath(context)
        val response = context.response()
        val request = context.request()
        val search = request.getParam("search")
        val properties = request.getParam("properties")
        val tags = request.getParam("tags")

        if (StringUtils.isNotEmpty(properties) && StringUtils.isNotEmpty(tags)) {
            response
                    .setStatusCode(400)
                    .end("You can only delete properties or tags, but not both")
        } else if (StringUtils.isNotEmpty(properties)) {
            removeProperties(search, path, properties, response)
        } else if (StringUtils.isNotEmpty(tags)) {
            removeTags(search, path, tags, response)
        } else {
            val strAsync = request.getParam("async")
            val async = BooleanUtils.toBoolean(strAsync)
            deleteChunks(search, path, async, response)
        }
    }

    /**
     * Remove properties
     * @param search the search query
     * @param path the path
     * @param properties the properties to remove
     * @param response the http response
     */
    private fun removeProperties(search: String, path: String, properties: String,
                                 response: HttpServerResponse) {
        val list = Arrays.asList(*properties.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
        store!!.removeProperties(search, path, list) { ar ->
            if (ar.succeeded()) {
                response
                        .setStatusCode(204)
                        .end()
            } else {
                fail(response, ar.cause())
            }
        }
    }

    /**
     * Remove tags
     * @param search the search query
     * @param path the path
     * @param tags the tags to remove
     * @param response the http response
     */
    private fun removeTags(search: String, path: String, tags: String?,
                           response: HttpServerResponse) {
        if (tags != null) {
            val list = Arrays.asList(*tags.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
            store!!.removeTags(search, path, list) { ar ->
                if (ar.succeeded()) {
                    response
                            .setStatusCode(204)
                            .end()
                } else {
                    fail(response, ar.cause())
                }
            }
        }
    }

    /**
     * Delete chunks
     * @param search the search query
     * @param path the path
     * @param async `true` if the operation should be performed asynchronously
     * @param response the http response
     */
    private fun deleteChunks(search: String, path: String, async: Boolean,
                             response: HttpServerResponse) {
        val correlationId = ObjectId().toString()
        val deleteMeta = DeleteMeta(correlationId)
        if (async) {
            store!!.rxDelete(search, path, deleteMeta)
                    .subscribe({ }, { err -> log.error("Could not delete chunks", err) })
            response
                    .setStatusCode(202) // Accepted
                    .putHeader("X-Correlation-Id", correlationId)
                    .end()
        } else {
            store!!.rxDelete(search, path, deleteMeta)
                    .subscribe({
                        response
                                .setStatusCode(204) // No Content
                                .putHeader("X-Correlation-Id", correlationId)
                                .end()
                    }, { err ->
                        log.error("Could not delete chunks", err)
                        fail(response, err)
                    })
        }
    }

    /**
     * Handles the HTTP PUT request
     * @param context the routing context
     */
    private fun onPut(context: RoutingContext) {
        val path = getEndpointPath(context)
        val response = context.response()
        val request = context.request()
        val search = request.getParam("search")
        val properties = request.getParam("properties")
        val tags = request.getParam("tags")

        if (StringUtils.isNotEmpty(properties) || StringUtils.isNotEmpty(tags)) {
            var single = Completable.complete()

            if (StringUtils.isNotEmpty(properties)) {
                single = setProperties(search, path, properties)
            }

            if (StringUtils.isNotEmpty(tags)) {
                single = appendTags(search, path, tags)
            }

            single.subscribe(
                    {
                        response
                                .setStatusCode(204)
                                .end()
                    },
                    { err -> fail(response, err) }
            )
        } else {
            response
                    .setStatusCode(405)
                    .end("Only properties and tags can be modified")
        }
    }

    /**
     * Set properties
     * @param search the search query
     * @param path the path
     * @param properties the properties to set
     * @return a Completable that completes when the properties have been set
     */
    private fun setProperties(search: String, path: String, properties: String): Completable {
        return Single.just(properties)
                .map { x -> x.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray() }
                .map<List<String>>(Func1<Array<String>, List<String>> { Arrays.asList(it) })
                .flatMap<Map<String, String>> { x ->
                    try {
                        return@Single.just(properties)
                                .map(x -> x.split(","))
                        .map(Arrays::asList)
                                .flatMap Single . just < Map < String, String>>(parseProperties(x))
                    } catch (e: ServerAPIException) {
                        return@Single.just(properties)
                                .map(x -> x.split(","))
                        .map(Arrays::asList)
                                .flatMap Single . error < Map < String, String>>(e)
                    }
                }
                .flatMapCompletable { map -> store!!.rxSetProperties(search, path, map) }
    }

    /**
     * Append tags
     * @param search the search query
     * @param path the path
     * @param tags the tags to append
     * @return a Completable that completes when the tags have been appended
     */
    private fun appendTags(search: String, path: String, tags: String): Completable {
        return Single.just(tags)
                .map { x -> x.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray() }
                .map<List<String>>(Func1<Array<String>, List<String>> { Arrays.asList(it) })
                .flatMapCompletable { tagList -> store!!.rxAppendTags(search, path, tagList) }
    }

    companion object {
        private val log = LoggerFactory.getLogger(StoreEndpoint::class.java)

        /**
         *
         * Name of the HTTP trailer that tells the client how many chunks could not
         * be merged. Possible reasons for unmerged chunks are:
         *
         *  * New chunks were added to the store while merging was in progress.
         *  * Optimistic merging was enabled and some chunks could not be merged.
         *
         *
         * The trailer will contain the number of chunks that could not be
         * merged. The client can decide whether to repeat the request to fetch
         * the missing chunks (e.g. with optimistic merging disabled) or not.
         */
        private val TRAILER_UNMERGED_CHUNKS = "GeoRocket-Unmerged-Chunks"

        /**
         * Parse list of properties in the form key:value
         * @param updates the list of properties
         * @return a json object with the property keys as object keys and the property
         * values as corresponding object values
         * @throws ServerAPIException if the syntax is not valid
         */
        @Throws(ServerAPIException::class)
        private fun parseProperties(updates: List<String>): Map<String, String> {
            val props = HashMap<String, String>()
            val regex = "(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":")

            for (part in updates) {
                part = part.trim { it <= ' ' }
                val property = part.split(regex.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                if (property.size != 2) {
                    throw ServerAPIException(
                            ServerAPIException.INVALID_PROPERTY_SYNTAX_ERROR,
                            "Invalid property syntax: $part")
                }
                val key = StringEscapeUtils.unescapeJava(property[0].trim { it <= ' ' })
                val value = StringEscapeUtils.unescapeJava(property[1].trim { it <= ' ' })
                props[key] = value
            }

            return props
        }
    }
}
