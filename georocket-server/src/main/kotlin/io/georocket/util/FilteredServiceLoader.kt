package io.georocket.util

import org.jooq.lambda.Seq
import java.util.ServiceLoader
import java.util.function.Predicate

/**
 * Wraps around [ServiceLoader] and lets [ServiceFilter] instances
 * decide whether a service instance should be used or skipped (filtered out).
 * [ServiceFilter]s will never be filtered by other [ServiceFilter]s.
 * [ServiceFilter]s may be called in any (random) order.
 * @param <S> the type of the services to load
 * @author Michel Kraemer
</S> */
class FilteredServiceLoader<S>
/**
 * Wrap around a [ServiceLoader]
 * @param loader the loader to wrap around
 */
private constructor(private val loader: ServiceLoader<S>) : Iterable<S> {
    private val filter: Predicate<Any>

    init {

        // load all service filters and combine to a single predicate
        var r: Predicate<Any>? = null
        for (f in ServiceLoader.load(ServiceFilter::class.java)) {
            if (r == null) {
                r = f
            } else {
                r = r.and(f)
            }
        }
        if (r == null) {
            r = { service -> true }
        }
        this.filter = r
    }

    /**
     * Returns an iterator to lazily load and instantiate the available
     * providers of this loader's service. Services will be filtered through
     * all available [ServiceFilter]s before they are returned.
     * @see ServiceLoader.iterator
     * @return the iterator
     */
    override fun iterator(): Iterator<S> {
        return Seq.seq(loader)
                .filter(filter)
                .iterator()
    }

    companion object {

        /**
         * Creates a new service loader for the given service type.
         * @see ServiceLoader.load
         * @param cls an interface or an abstract class representing the service
         * @param <S> the class of the service type
         * @return a new service loader
        </S> */
        fun <S> load(cls: Class<S>): FilteredServiceLoader<S> {
            return FilteredServiceLoader(ServiceLoader.load(cls))
        }
    }
}
