package io.georocket.tasks

/**
 * A task started by the [io.georocket.ImporterVerticle]
 * @author Michel Kraemer
 */
class ImportingTask : AbstractTask {
    /**
     * Get the number of chunks already imported by this task
     * @return the number of imported chunks
     */
    /**
     * Set the number of chunks already imported by this task
     * @param importedChunks the number of imported chunks
     */
    var importedChunks: Long = 0

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
        require(other is ImportingTask) { "Illegal task type" }
        super.inc(other)
        importedChunks = importedChunks + other.importedChunks
    }
}
