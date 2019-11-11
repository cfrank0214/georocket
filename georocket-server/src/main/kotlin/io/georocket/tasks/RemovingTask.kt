package io.georocket.tasks

/**
 * A task started by the [io.georocket.index.IndexerVerticle] to track the
 * removal of chunks from the index
 * @author Michel Kraemer
 */
class RemovingTask : AbstractTask {
    /**
     * Get the total number of chunks to be removed by this task
     * @return the number of chunks to be removed
     */
    /**
     * Set the total number of chunks to be removed by this task
     * @param totalChunks the total number of chunks to be removed
     */
    var totalChunks: Long = 0
    /**
     * Get the number of chunks already removed by this task
     * @return the number of removed chunks
     */
    /**
     * Set the number of chunks already removed by this task
     * @param removedChunks the number of removed chunks
     */
    var removedChunks: Long = 0

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
        require(other is RemovingTask) { "Illegal task type" }
        super.inc(other)
        removedChunks = removedChunks + other.removedChunks
        totalChunks = Math.max(totalChunks, other.totalChunks)
    }
}
