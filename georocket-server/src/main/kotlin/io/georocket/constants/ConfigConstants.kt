package io.georocket.constants

import io.georocket.config.ConfigKeysProvider
import io.georocket.util.FilteredServiceLoader

import java.util.ArrayList
import java.util.Arrays
import java.util.stream.Collectors

/**
 * Configuration constants
 * @author Michel Kraemer
 */
object ConfigConstants {
    val HOME = "georocket.home"
    val HOST = "georocket.host"
    val PORT = "georocket.port"

    val HTTP_COMPRESS = "georocket.http.compress"
    val HTTP_SSL = "georocket.http.ssl"
    val HTTP_CERT_PATH = "georocket.http.certPath"
    val HTTP_KEY_PATH = "georocket.http.keyPath"
    val HTTP_ALPN = "georocket.http.alpn"

    val HTTP_CORS_ENABLE = "georocket.http.cors.enable"
    val HTTP_CORS_ALLOW_ORIGIN = "georocket.http.cors.allowOrigin"
    val HTTP_CORS_ALLOW_CREDENTIALS = "georocket.http.cors.allowCredentials"
    val HTTP_CORS_ALLOW_HEADERS = "georocket.http.cors.allowHeaders"
    val HTTP_CORS_ALLOW_METHODS = "georocket.http.cors.allowMethods"
    val HTTP_CORS_EXPOSE_HEADERS = "georocket.http.cors.exposeHeaders"
    val HTTP_CORS_MAX_AGE = "georocket.http.cors.maxAge"

    val LOG_CONFIG = "georocket.logConfig"

    val STORAGE_CLASS = "georocket.storage.class"
    val STORAGE_H2_PATH = "georocket.storage.h2.path"
    val STORAGE_H2_COMPRESS = "georocket.storage.h2.compress"
    val STORAGE_H2_MAP_NAME = "georocket.storage.h2.mapName" // undocumented
    val STORAGE_FILE_PATH = "georocket.storage.file.path"
    val STORAGE_HDFS_DEFAULT_FS = "georocket.storage.hdfs.defaultFS"
    val STORAGE_HDFS_PATH = "georocket.storage.hdfs.path"
    val STORAGE_MONGODB_CONNECTION_STRING = "georocket.storage.mongodb.connectionString"
    val STORAGE_MONGODB_DATABASE = "georocket.storage.mongodb.database"
    val STORAGE_S3_ACCESS_KEY = "georocket.storage.s3.accessKey"
    val STORAGE_S3_SECRET_KEY = "georocket.storage.s3.secretKey"
    val STORAGE_S3_HOST = "georocket.storage.s3.host"
    val STORAGE_S3_PORT = "georocket.storage.s3.port"
    val STORAGE_S3_BUCKET = "georocket.storage.s3.bucket"
    val STORAGE_S3_PATH_STYLE_ACCESS = "georocket.storage.s3.pathStyleAccess"
    val STORAGE_S3_FORCE_SIGNATURE_V2 = "georocket.storage.s3.forceSignatureV2"
    val STORAGE_S3_REQUEST_EXPIRY_SECONDS = "georocket.storage.s3.requestExpirySeconds"

    val INDEX_MAX_BULK_SIZE = "georocket.index.maxBulkSize"
    val INDEX_MAX_PARALLEL_INSERTS = "georocket.index.maxParallelInserts"
    val INDEX_MAX_QUEUED_CHUNKS = "georocket.index.maxQueuedChunks"
    val INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE = "georocket.index.indexableChunkCache.maxSize"
    val INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS = "georocket.index.indexableChunkCache.maxTimeSeconds"
    val INDEX_ELASTICSEARCH_EMBEDDED = "georocket.index.elasticsearch.embedded"
    val INDEX_ELASTICSEARCH_HOST = "georocket.index.elasticsearch.host"
    val INDEX_ELASTICSEARCH_PORT = "georocket.index.elasticsearch.port"
    val INDEX_ELASTICSEARCH_HOSTS = "georocket.index.elasticsearch.hosts"
    val INDEX_ELASTICSEARCH_AUTO_UPDATE_HOSTS_INTERVAL_SECONDS = "georocket.index.elasticsearch.autoUpdateHostsIntervalSeconds"
    val INDEX_ELASTICSEARCH_COMPRESS_REQUEST_BODIES = "georocket.index.elasticsearch.compressRequestBodies"
    val INDEX_ELASTICSEARCH_JAVA_OPTS = "georocket.index.elasticsearch.javaOpts"
    val INDEX_ELASTICSEARCH_DOWNLOAD_URL = "georocket.index.elasticsearch.downloadUrl" // undocumented
    val INDEX_ELASTICSEARCH_INSTALL_PATH = "georocket.index.elasticsearch.installPath" // undocumented
    val INDEX_SPATIAL_PRECISION = "georocket.index.spatial.precision"

    val QUERY_COMPILER_CLASS = "georocket.query.defaultQueryCompiler" // undocumented
    val QUERY_DEFAULT_CRS = "georocket.query.defaultCRS"

    val TASKS_RETAIN_SECONDS = "georocket.tasks.retainSeconds"

    val DEFAULT_HOST = "127.0.0.1"
    val DEFAULT_PORT = 63020

    val DEFAULT_INDEX_MAX_BULK_SIZE = 200
    val DEFAULT_INDEX_MAX_PARALLEL_INSERTS = 5
    val DEFAULT_INDEX_MAX_QUEUED_CHUNKS = 10000
    val DEFAULT_INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE = 1024L * 1024 * 64 // 64 MB
    val DEFAULT_INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS: Long = 60

    val DEFAULT_TASKS_RETAIN_SECONDS = (60 * 2).toLong()

    /**
     * Get all configuration keys from this class and all extensions registered
     * through the Service Provider Interface API.
     * @see ConfigKeysProvider
     *
     * @return the list of configuration keys
     */
    val configKeys: List<String>
        get() {
            val r = getConfigKeys(ConfigConstants::class.java)

            val loader = FilteredServiceLoader.load(ConfigKeysProvider::class.java)
            for (ccp in loader) {
                r.addAll(ccp.configKeys)
            }

            return r
        }

    /**
     * Get all configuration keys by enumerating over all string constants beginning
     * with the prefix `georocket` from a given class
     * @param cls the class to inspect
     * @return the list of configuration keys
     */
    fun getConfigKeys(cls: Class<*>): MutableList<String> {
        return Arrays.stream<Field>(cls.fields)
                .map<Any> { f ->
                    try {
                        return@Arrays.stream(cls.fields)
                                .map f . get null
                    } catch (e: IllegalAccessException) {
                        throw RuntimeException("Could not access config constant", e)
                    }
                }
                .filter { s -> s is String }
                .map<String>(Function<Any, String> { String::class.java.cast(it) })
                .filter { s -> s.startsWith("georocket") }
                .collect<ArrayList<String>, Any>(Collectors.toCollection(Supplier<ArrayList<String>> { ArrayList() }))
    }
}// hidden constructor
