package io.georocket.input

import io.georocket.storage.ChunkMeta
import io.georocket.util.StreamEvent
import rx.Observable

/**
 * Splits input tokens and returns chunks
 * @author Michel Kraemer
 * @param <E> the type of the stream events this splitter can process
 * @param <M> the type of the chunk metadata created by this splitter
</M></E> */
interface Splitter<E : StreamEvent, M : ChunkMeta> {
    /**
     * Result of the [Splitter.onEvent] method. Holds
     * a chunk and its metadata.
     * @param <M> the type of the metadata
    </M> */
    class Result<M : ChunkMeta>
    /**
     * Create a new result object
     * @param chunk the chunk
     * @param meta the chunk's metadata
     */
    (
            /**
             * @return the chunk
             */
            val chunk: String,
            /**
             * @return the chunk's metadata
             */
            val meta: M)

    /**
     * Will be called on every stream event
     * @param event the stream event
     * @return a new [Result] object (containing chunk and metadata) or
     * `null` if no result was produced
     */
    fun onEvent(event: E): Result<M>?

    /**
     * Observable version of [.onEvent]
     * @param event the stream event
     * @return an observable that will emit a [Result] object (containing
     * a chunk and metadata) or emit nothing if no chunk was produced
     */
    fun onEventObservable(event: E): Observable<Result<M>> {
        val result = onEvent(event) ?: return Observable.empty()
        return Observable.just(result)
    }
}
