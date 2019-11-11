package io.georocket.tasks

import java.time.Instant
import java.util.ArrayList
import java.util.function.Consumer

/**
 * Abstract base class for tasks
 * @author Michel Kraemer
 */
abstract class AbstractTask : Task {

    /**
     * Package-visible setter for the task's correlation ID
     * @param correlationId the correlation ID
     */
    override var correlationId: String = ""
        internal set
    /**
     * Set the task's start time
     * @param startTime the start time
     */
    override var startTime: Instant? = null
    /**
     * Set the task's end time
     * @param endTime the end time
     */
    override var endTime: Instant? = null
    private var errors: MutableList<TaskError>? = null

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
    constructor(correlationId: String) {
        this.correlationId = correlationId
    }

    override fun getErrors(): List<TaskError>? {
        return errors
    }

    /**
     * Set the list of errors that occurred during the task execution
     * @param errors the list errors (may be `null` or empty if no errors
     * have occurred)
     */
    fun setErrors(errors: MutableList<TaskError>) {
        this.errors = errors

        if (this.errors!!.size > MAX_ERRORS) {
            this.errors = this.errors!!.subList(0, MAX_ERRORS)
            addMoreErrors()
        }
    }

    /**
     * Add an error that occurred during the task execution
     * @param error the error to add
     */
    fun addError(error: TaskError) {
        if (errors == null) {
            errors = ArrayList()
        }
        if (errors!!.size == MAX_ERRORS) {
            addMoreErrors()
        } else if (errors!!.size < MAX_ERRORS) {
            errors!!.add(error)
        }
    }

    private fun addMoreErrors() {
        errors!!.add(TaskError("more", "There are more errors. Only " + MAX_ERRORS +
                " errors will be displayed. The server logs provide more information."))
    }

    override fun inc(other: Task) {
        if (startTime != null || other.startTime != null) {
            if (startTime != null && other.startTime == null) {
                startTime = startTime
            } else if (startTime == null && other.startTime != null) {
                startTime = other.startTime
            } else if (startTime!!.isBefore(other.startTime)) {
                startTime = startTime
            } else {
                startTime = other.startTime
            }
        }

        if (endTime != null || other.endTime != null) {
            if (endTime != null && other.endTime == null) {
                endTime = endTime
            } else if (endTime == null && other.endTime != null) {
                endTime = other.endTime
            } else if (endTime!!.isBefore(other.endTime)) {
                endTime = endTime
            } else {
                endTime = other.endTime
            }
        }

        if (other.errors != null) {
            other.errors.forEach(Consumer<TaskError> { this.addError(it) })
        }
    }

    companion object {
        private val MAX_ERRORS = 10
    }
}
