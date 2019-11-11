package io.georocket.tasks

/**
 * A task started by the [io.georocket.storage.indexed.IndexedStore] to
 * track the deletion of chunks from the store
 * @author Michel Kraemer
 */
class PurgingTask : AbstractTask {
    /**
     * Get the total number of chunks to be purged by this task
     * @return the number of chunks to be purged
     */
    /**
     * Set the total number of chunks to be purged by this task
     * @param totalChunks the total number of chunks to be purged
     */
    var totalChunks: Long = 0
    /**
     * Get the number of chunks already purged by this task
     * @return the number of purged chunks
     */
    /**
     * Set the number of chunks already purged by this task
     * @param purgedChunks the number of purged chunks
     */
    var purgedChunks: Long = 0

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
        require(other is PurgingTask) { "Illegal task type" }
        super.inc(other)
        purgedChunks = purgedChunks + other.purgedChunks
        totalChunks = Math.max(totalChunks, other.totalChunks)
    }
}
