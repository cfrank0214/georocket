package io.georocket.storage

import java.util.concurrent.atomic.AtomicLong

import org.apache.commons.lang3.tuple.Pair

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import rx.Observable
import rx.Producer
import rx.internal.operators.BackpressureUtils

/**
 * Wraps around [StoreCursor] so it can be used with RxJava
 * @author Michel Kraemer
 */
class RxStoreCursor
/**
 * Create a new rx-ified cursor
 * @param delegate the actual cursor to delegate to
 */
(
        /**
         * @return the actual non-rx-ified cursor
         */
        val delegate: StoreCursor) : StoreCursor {

    override fun hasNext(): Boolean {
        return delegate.hasNext()
    }

    override fun next(handler: Handler<AsyncResult<ChunkMeta>>) {
        delegate.next(handler)
    }

    override fun getChunkPath(): String {
        return delegate.chunkPath
    }

    override fun getInfo(): CursorInfo {
        return delegate.info
    }

    /**
     * Convert this cursor to an observable
     * @return an observable emitting the chunks from the cursor and their
     * respective path in the store (retrieved via [.getChunkPath])
     */
    fun toObservable(): Observable<Pair<ChunkMeta, String>> {
        return Observable.unsafeCreate { s ->
            s.setProducer(object : Producer {
                private val requested = AtomicLong()

                override fun request(n: Long) {
                    if (n > 0 && !s.isUnsubscribed &&
                            BackpressureUtils.getAndAddRequest(requested, n) == 0L) {
                        drain()
                    }
                }

                private fun drain() {
                    if (requested.get() > 0) {
                        if (!hasNext()) {
                            if (!s.isUnsubscribed) {
                                s.onCompleted()
                            }
                            return
                        }

                        next { ar ->
                            if (s.isUnsubscribed) {
                                return@next
                            }
                            if (ar.failed()) {
                                s.onError(ar.cause())
                            } else {
                                s.onNext(Pair.of(ar.result(), chunkPath))
                                requested.decrementAndGet()
                                drain()
                            }
                        }
                    }
                }
            })
        }
    }
}
