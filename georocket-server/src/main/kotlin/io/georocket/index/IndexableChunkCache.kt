package io.georocket.index

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import io.georocket.constants.ConfigConstants
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * A cache for chunks that are about to be indexed. The cache keeps chunks only
 * until they have been requested or until a configurable time has passed (see
 * [ConfigConstants.INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS]). The
 * cache has a configurable maximum size (see
 * [ConfigConstants.INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE]).
 * @author Michel Kraemer
 */
class IndexableChunkCache
/**
 * Create a new cache
 * @param maximumSize the cache's maximum size in bytes
 * @param maximumTime the maximum number of seconds a chunk stays in the cache
 */
@JvmOverloads internal constructor(private val maximumSize: Long = currentContext.config().getLong(ConfigConstants.INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE,
        ConfigConstants.DEFAULT_INDEX_INDEXABLE_CHUNK_CACHE_MAX_SIZE)!!, maximumTime: Long = currentContext.config().getLong(ConfigConstants.INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS,
        ConfigConstants.DEFAULT_INDEX_INDEXABLE_CHUNK_CACHE_MAX_TIME_SECONDS)!!) {
    private val cache: Cache<String, Buffer>
    private val size = AtomicLong()

    /**
     * Get the number of chunks currently in the cache
     * @return the number of chunks
     */
    val numberOfChunks: Long
        get() = cache.size()

    /**
     * A private class holding the singleton instance of this class
     */
    private object LazyHolder {
        internal val INSTANCE = IndexableChunkCache()
    }

    init {
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(maximumTime, TimeUnit.SECONDS)
                .removalListener<String, Buffer> { n -> size.addAndGet((-n.value.length()).toLong()) }
                .build()
    }

    /**
     * Adds a chunk to the cache. Do nothing if adding the chunk would exceed
     * the cache's maximum size
     * @param path the chunk's path
     * @param chunk the chunk
     */
    fun put(path: String, chunk: Buffer) {
        val chunkSize = chunk.length().toLong()
        var oldSize: Long
        var newSize: Long
        var cleanUpCalled = false
        while (true) {
            oldSize = size.get()
            newSize = oldSize + chunkSize
            if (newSize > maximumSize) {
                // make sure the chunk can be added if there were still some
                // removal events pending
                if (!cleanUpCalled) {
                    cleanUpCalled = true
                    cache.cleanUp()
                    continue
                }
                return
            }
            if (size.compareAndSet(oldSize, newSize)) {
                break
            }
        }
        cache.put(path, chunk)
    }

    /**
     * Gets and removes a chunk from the cache
     * @param path the chunk's path
     * @return the chunk or `null` if the chunk was not found in the cache
     */
    operator fun get(path: String): Buffer? {
        val r = cache.getIfPresent(path)
        if (r != null) {
            cache.invalidate(path)
        }
        return r
    }

    /**
     * Get the cache's size in bytes
     * @return the size
     */
    fun getSize(): Long {
        return size.get()
    }

    companion object {

        /**
         * Get the current Vert.x context
         * @return the context
         * @throws RuntimeException if the method was not called from within a
         * Vert.x context
         */
        private val currentContext: Context
            get() = Vertx.currentContext()
                    ?: throw RuntimeException("This class must be initiated within " + "a Vert.x context")

        /**
         * Gets the singleton instance of this class. Must be called from within a
         * Vert.x context.
         * @return the singleton instance
         * @throws RuntimeException if the method was not called from within a
         * Vert.x context
         */
        val instance: IndexableChunkCache
            get() = LazyHolder.INSTANCE
    }
}
/***
 * Create a new cache
 */
