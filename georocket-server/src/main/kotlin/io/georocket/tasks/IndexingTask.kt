package io.georocket.tasks

/**
 * A task started by the [io.georocket.index.IndexerVerticle]
 * @author Michel Kraemer
 */
class IndexingTask : AbstractTask {
    /**
     * Get the number of chunks already indexed by this task
     * @return the number of indexed chunks
     */
    /**
     * Set the number of chunks already indexed by this task
     * @param indexedChunks the number of indexed chunks
     */
    var indexedChunks: Long = 0

    /**
     * Package-visible default constructor
     */
    internal constructor() {
        // nothing to do here
    }

    /**
     * Default constructor
     * @param correlationId the correlation ID this task belongs to
     */
    constructor(correlationId: String) : super(correlationId) {}

    override fun inc(other: Task) {
        require(other is IndexingTask) { "Illegal task type" }
        super.inc(other)
        indexedChunks = indexedChunks + other.indexedChunks
    }
}
