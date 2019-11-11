package io.georocket.storage.s3

import java.net.URL
import java.util.Date
import java.util.Queue

import com.amazonaws.ClientConfiguration
import com.amazonaws.ClientConfigurationFactory
import com.amazonaws.HttpMethod
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.internal.ServiceUtils
import com.amazonaws.util.AwsHostNameUtils
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
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpClientRequest
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

/**
 * Stores chunks on Amazon S3
 * @author Michel Kraemer
 */
abstract class S3Store
/**
 * Constructs a new store
 * @param vertx the Vert.x instance
 */
(private val vertx: Vertx) : IndexedStore(vertx) {
    private var s3Client: AmazonS3? = null
    private val accessKey: String
    private val secretKey: String
    private val host: String
    private val port: Int
    private val bucket: String
    private val pathStyleAccess: Boolean
    private val forceSignatureV2: Boolean
    private val requestExpirySeconds: Int
    private val client: HttpClient

    init {

        val config = vertx.orCreateContext.config()

        accessKey = config.getString(ConfigConstants.STORAGE_S3_ACCESS_KEY)
        Preconditions.checkNotNull(accessKey, "Missing configuration item \"" +
                ConfigConstants.STORAGE_S3_ACCESS_KEY + "\"")

        secretKey = config.getString(ConfigConstants.STORAGE_S3_SECRET_KEY)
        Preconditions.checkNotNull(secretKey, "Missing configuration item \"" +
                ConfigConstants.STORAGE_S3_SECRET_KEY + "\"")

        host = config.getString(ConfigConstants.STORAGE_S3_HOST)
        Preconditions.checkNotNull(host, "Missing configuration item \"" +
                ConfigConstants.STORAGE_S3_HOST + "\"")

        port = config.getInteger(ConfigConstants.STORAGE_S3_PORT, 80)!!

        bucket = config.getString(ConfigConstants.STORAGE_S3_BUCKET)
        Preconditions.checkNotNull(bucket, "Missing configuration item \"" +
                ConfigConstants.STORAGE_S3_BUCKET + "\"")

        pathStyleAccess = config.getBoolean(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, true)!!
        forceSignatureV2 = config.getBoolean(ConfigConstants.STORAGE_S3_FORCE_SIGNATURE_V2, false)!!
        requestExpirySeconds = config.getInteger(ConfigConstants.STORAGE_S3_REQUEST_EXPIRY_SECONDS, 600)!!

        val options = HttpClientOptions()
        options.defaultHost = host
        options.defaultPort = port
        client = vertx.createHttpClient(options)
    }

    /**
     * Get or initialize the S3 client.
     * Note: this method must be synchronized because we're accessing the
     * [.s3Client] field and we're calling this method from a worker thread.
     * @return the S3 client
     */
    @Synchronized
    private fun getS3Client(): AmazonS3? {
        if (s3Client == null) {
            val credentials = BasicAWSCredentials(accessKey, secretKey)

            var builder = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(AWSStaticCredentialsProvider(credentials))

            if (forceSignatureV2) {
                val configFactory = ClientConfigurationFactory()
                val config = configFactory.config
                config.signerOverride = "S3SignerType"
                builder = builder.withClientConfiguration(config)
            }

            val endpoint = "http://$host:$port"
            var clientRegion: String? = null
            if (!ServiceUtils.isS3USStandardEndpoint(endpoint)) {
                clientRegion = AwsHostNameUtils.parseRegion(host,
                        AmazonS3Client.S3_SERVICE_NAME)
            }

            builder = builder.withEndpointConfiguration(EndpointConfiguration(
                    endpoint, clientRegion))
            builder = builder.withPathStyleAccessEnabled(pathStyleAccess)

            s3Client = builder.build()
        }
        return s3Client
    }

    /**
     * Generate a pre-signed URL that can be used to make an HTTP request.
     * Note: this method must be synchronized because we're accessing the
     * [.s3Client] field and we're calling this method from a worker thread.
     * @param key the key of the S3 object to query
     * @param method the HTTP method that will be used in the request
     * @return the presigned URL
     */
    @Synchronized
    private fun generatePresignedUrl(key: String, method: HttpMethod): URL {
        val expiry = Date(System.currentTimeMillis() + 1000 * requestExpirySeconds)
        return getS3Client().generatePresignedUrl(bucket, key, expiry, method)
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
        val key = PathUtils.removeLeadingSlash(filename)

        vertx.executeBlocking<URL>({ f -> f.complete(generatePresignedUrl(key, HttpMethod.PUT)) }, { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
                return@vertx.< URL > executeBlocking
            }

            val u = ar.result()
            log.debug("PUT $u")

            val chunkBuf = Buffer.buffer(chunk)
            val request = client.put(u.file)

            request.putHeader("Host", u.host)
            request.putHeader("Content-Length", chunkBuf.length().toString())

            request.exceptionHandler { t -> handler.handle(Future.failedFuture(t)) }

            request.handler { response ->
                val errorBody = Buffer.buffer()
                if (response.statusCode() != 200) {
                    response.handler { errorBody.appendBuffer(it) }
                }
                response.endHandler { v ->
                    if (response.statusCode() == 200) {
                        handler.handle(Future.succeededFuture(filename))
                    } else {
                        log.error(errorBody)
                        handler.handle(Future.failedFuture(response.statusMessage()))
                    }
                }
            }

            request.end(chunkBuf)
        })
    }

    override fun getOne(path: String, handler: Handler<AsyncResult<ChunkReadStream>>) {
        val key = PathUtils.removeLeadingSlash(PathUtils.normalize(path))
        vertx.executeBlocking<URL>({ f -> f.complete(generatePresignedUrl(key, HttpMethod.GET)) }, { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
                return@vertx.< URL > executeBlocking
            }

            val u = ar.result()
            log.debug("GET $u")

            val request = client.get(ar.result().file)
            request.putHeader("Host", u.host)

            request.exceptionHandler { t -> handler.handle(Future.failedFuture(t)) }

            request.handler { response ->
                if (response.statusCode() == 200) {
                    val contentLength = response.getHeader("Content-Length")
                    val chunkSize = java.lang.Long.parseLong(contentLength)
                    handler.handle(Future.succeededFuture(DelegateChunkReadStream(chunkSize, response)))
                } else {
                    val errorBody = Buffer.buffer()
                    response.handler { errorBody.appendBuffer(it) }
                    response.endHandler { v ->
                        log.error(errorBody)
                        handler.handle(Future.failedFuture(response.statusMessage()))
                    }
                }
            }

            request.end()
        })
    }

    override fun doDeleteChunks(paths: Queue<String>, handler: Handler<AsyncResult<Void>>) {
        if (paths.isEmpty()) {
            handler.handle(Future.succeededFuture())
            return
        }

        val key = PathUtils.removeLeadingSlash(PathUtils.normalize(paths.poll()))
        vertx.executeBlocking<URL>({ f -> f.complete(generatePresignedUrl(key, HttpMethod.DELETE)) }, { ar ->
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()))
                return@vertx.< URL > executeBlocking
            }

            val u = ar.result()
            log.debug("DELETE $u")

            val request = client.delete(ar.result().file)
            request.putHeader("Host", u.host)

            request.exceptionHandler { t -> handler.handle(Future.failedFuture(t)) }

            request.handler { response ->
                val errorBody = Buffer.buffer()
                if (response.statusCode() != 204) {
                    response.handler { errorBody.appendBuffer(it) }
                }
                response.endHandler { v ->
                    when (response.statusCode()) {
                        204, 404 -> doDeleteChunks(paths, handler)
                        else -> {
                            log.error(errorBody)
                            handler.handle(Future.failedFuture(response.statusMessage()))
                        }
                    }
                }
            }

            request.end()
        })
    }

    companion object {
        private val log = LoggerFactory.getLogger(S3Store::class.java)
    }
}
