package io.georocket.tasks

/**
 * A task started by the [io.georocket.http.StoreEndpoint] when it
 * receives a file from the client
 * @author Michel Kraemer
 */
class ReceivingTask : AbstractTask {
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
}
