package io.georocket.index.elasticsearch

import io.georocket.util.HttpException
import io.georocket.util.RxUtils
import io.georocket.util.io.GzipWriteStream
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpClientRequest
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.rx.java.ObservableFuture
import io.vertx.rx.java.RxHelper
import rx.Observable
import rx.Single

import java.net.URI
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedHashSet

/**
 * An HTTP client that can perform requests against multiple hosts using
 * round robin
 * @author Michel Kraemer
 */
class LoadBalancingHttpClient
/**
 * Constructs a new load-balancing HTTP client
 * @param vertx the current Vert.x instance
 * @param compressRequestBodies `true` if bodies of HTTP requests
 * should be compressed with GZIP
 */
@JvmOverloads constructor(private val vertx: Vertx, private val compressRequestBodies: Boolean = false) {
    private var currentHost = -1
    private var hosts: MutableList<URI> = ArrayList()
    private val hostsToClients = HashMap<URI, HttpClient>()

    private var defaultOptions = HttpClientOptions()
            .setKeepAlive(true)
            .setTryUseCompression(true)


    /**
     * Set the hosts to communicate with
     * @param hosts the hosts
     */
    fun setHosts(hosts: List<URI>) {
        val uniqueHosts = LinkedHashSet(hosts)

        this.hosts = ArrayList(uniqueHosts)
        if (currentHost >= 0) {
            currentHost = currentHost % this.hosts.size
        }

        // close clients of all removed hosts
        val i = hostsToClients.entries.iterator()
        while (i.hasNext()) {
            val e = i.next()
            if (!uniqueHosts.contains(e.key)) {
                e.value.close()
                i.remove()
            }
        }

        // add clients for all new hosts
        for (u in uniqueHosts) {
            if (!hostsToClients.containsKey(u)) {
                hostsToClients[u] = createClient(u)
            }
        }
    }

    /**
     * Get a copy of the list of hosts to communicate with
     * @return a copy of the list of hosts
     */
    fun getHosts(): List<URI> {
        return ArrayList(hosts)
    }

    /**
     * Set default HTTP client options. Must be called before [.setHosts].
     * @param options the options
     */
    fun setDefaultOptions(options: HttpClientOptions) {
        defaultOptions = options
    }

    /**
     * Create an HttpClient for the given host
     * @param u the host
     * @return the HttpClient
     */
    private fun createClient(u: URI): HttpClient {
        val clientOptions = HttpClientOptions(defaultOptions)
                .setDefaultHost(u.host)
                .setDefaultPort(u.port)
        return vertx.createHttpClient(clientOptions)
    }

    /**
     * Get the next available HTTP client
     * @return the client
     */
    private fun nextClient(): HttpClient {
        currentHost = (currentHost + 1) % hosts.size
        val u = hosts[currentHost]
        return hostsToClients[u]
    }

    /**
     * Perform an HTTP request and convert the response to a JSON object
     * @param req the request to perform
     * @param body the body to send in the request (may be `null`)
     * @return an observable emitting the parsed response body (may be
     * `null` if no body was received)
     */
    private fun performRequest(req: HttpClientRequest, body: Buffer?): Single<JsonObject> {
        val observable = RxHelper.observableFuture<JsonObject>()
        val handler = observable.toHandler()

        req.exceptionHandler { t -> handler.handle(Future.failedFuture(t)) }

        req.handler { res ->
            val code = res.statusCode()
            if (code == 200) {
                val buf = Buffer.buffer()
                res.handler { buf.appendBuffer(it) }
                res.endHandler { v ->
                    if (buf.length() > 0) {
                        handler.handle(Future.succeededFuture(buf.toJsonObject()))
                    } else {
                        handler.handle(Future.succeededFuture())
                    }
                }
            } else {
                val buf = Buffer.buffer()
                res.handler { buf.appendBuffer(it) }
                res.endHandler { v ->
                    handler.handle(Future.failedFuture(
                            HttpException(code, buf.toString())))
                }
            }
        }

        if (body != null) {
            req.putHeader("Accept", "application/json")
            req.putHeader("Content-Type", "application/json")
            if (compressRequestBodies && body.length() >= MIN_COMPRESSED_BODY_SIZE) {
                req.isChunked = true
                req.putHeader("Content-Encoding", "gzip")
                val gws = GzipWriteStream(req)
                gws.end(body)
            } else {
                req.isChunked = false
                req.putHeader("Content-Length", body.length().toString())
                req.end(body)
            }
        } else {
            req.end()
        }

        return observable.toSingle()
    }

    /**
     * Perform an HTTP request and convert the response to a JSON object
     * @param uri the request URI
     * @return a single emitting the parsed response body (may be
     * `null` if no body was received)
     */
    fun performRequest(uri: String): Single<JsonObject> {
        return performRequest(HttpMethod.GET, uri)
    }

    /**
     * Perform an HTTP request and convert the response to a JSON object
     * @param method the HTTP method
     * @param uri the request URI
     * @param body the body to send in the request (may be `null`)
     * @return a single emitting the parsed response body (may be
     * `null` if no body was received)
     */
    @JvmOverloads
    fun performRequest(method: HttpMethod, uri: String, body: Buffer? = null): Single<JsonObject> {
        return performRequestNoRetry(method, uri, body).retryWhen { errors ->
            val o = errors.flatMap { error ->
                if (error is HttpException) {
                    // immediately forward HTTP errors, don't retry
                    return@errors.flatMap Observable . error < Throwable >(error)
                }
                Observable.just(error)
            }
            RxUtils.makeRetry(5, 1000, log).call(o)
        }
    }

    /**
     * Perform an HTTP request and convert the response to a JSON object. Select
     * any host and do not retry on failure.
     * @param method the HTTP method
     * @param uri the request URI
     * @param body the body to send in the request (may be `null`)
     * @return a single emitting the parsed response body (may be
     * `null` if no body was received)
     */
    fun performRequestNoRetry(method: HttpMethod, uri: String, body: Buffer?): Single<JsonObject> {
        return Single.defer {
            val client = nextClient()
            val req = client.request(method, uri)
            performRequest(req, body)
        }
    }

    /**
     * Closes this client and all underlying clients
     */
    fun close() {
        hosts.clear()
        for (client in hostsToClients.values) {
            client.close()
        }
        hostsToClients.clear()
        currentHost = -1
    }

    companion object {
        private val log = LoggerFactory.getLogger(LoadBalancingHttpClient::class.java)

        /**
         * The minimum number of bytes a body must have for it to be compressed with
         * GZIP when posting to the server. The value is smaller than the default
         * MTU (1500 bytes) to reduce compression overhead for very small bodies that
         * fit into one TCP packet.
         * @see .compressRequestBodies
         */
        private val MIN_COMPRESSED_BODY_SIZE = 1400
    }
}
/**
 * Constructs a new load-balancing HTTP client
 * @param vertx the current Vert.x instance
 */
/**
 * Perform an HTTP request and convert the response to a JSON object
 * @param method the HTTP method
 * @param uri the request URI
 * @return a single emitting the parsed response body (may be
 * `null` if no body was received)
 */
