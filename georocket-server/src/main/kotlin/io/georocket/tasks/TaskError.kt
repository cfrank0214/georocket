package io.georocket.tasks

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.georocket.util.ThrowableHelper

/**
 * An error that occurred during the execution of a task
 * @author Michel Kraemer
 */
class TaskError {
    /**
     * Get the error type
     * @return the type
     */
    val type: String
    /**
     * Get a human-readable string giving details about the cause of the error
     * @return the reason for the error
     */
    val reason: String

    /**
     * Construct a new error from a throwable
     * @param t the throwable
     */
    constructor(t: Throwable) {
        this.type = t.javaClass.simpleName
        this.reason = ThrowableHelper.throwableToMessage(t, "Unknown reason")
    }

    /**
     * Construct a new error
     * @param type the error type
     * @param reason a human-readable string about the cause of the error
     */
    @JsonCreator
    constructor(@JsonProperty("type") type: String, @JsonProperty("reason") reason: String) {
        this.type = type
        this.reason = reason
    }
}
