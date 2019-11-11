package io.georocket.storage

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import rx.Observable
import rx.Producer
import rx.internal.operators.BackpressureUtils

import java.util.concurrent.atomic.AtomicLong

/**
 * Wraps around [AsyncCursor] so it can be used with RxJava
 * @author Tim Hellhake
 * @param <T> type of the cursor item
</T> */
class RxAsyncCursor<T>
/**
 * Create a new rx-ified cursor
 * @param delegate the actual cursor to delegate to
 */
(
        /**
         * @return the actual non-rx-ified cursor
         */
        val delegate: AsyncCursor<T>) : AsyncCursor<T> {

    override fun hasNext(): Boolean {
        return delegate.hasNext()
    }

    override fun next(handler: Handler<AsyncResult<T>>) {
        delegate.next(handler)
    }

    /**
     * Convert this cursor to an observable
     * @return an observable emitting the items from the cursor
     */
    fun toObservable(): Observable<T> {
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
                                s.onNext(ar.result())
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