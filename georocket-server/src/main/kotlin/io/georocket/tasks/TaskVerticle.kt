package io.georocket.tasks

import io.georocket.constants.AddressConstants
import io.georocket.constants.ConfigConstants
import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Comparator
import java.util.LinkedHashMap
import java.util.TreeSet

/**
 * A verticle that tracks information about currently running tasks
 * @author Michel Kraemer
 */
class TaskVerticle : AbstractVerticle() {
    private var retainSeconds: Long = 0
    private val tasks = LinkedHashMap<String, Map<Class<out Task>, Task>>()
    private val finishedTasks = TreeSet(Comparator.comparing<Task, Instant>(Function<Task, Instant> { it.getEndTime() })
            .thenComparingInt(ToIntFunction<Task> { System.identityHashCode(it) }))

    override fun start() {
        retainSeconds = config().getLong(ConfigConstants.TASKS_RETAIN_SECONDS,
                ConfigConstants.DEFAULT_TASKS_RETAIN_SECONDS)!!

        vertx.eventBus().consumer<Void>(AddressConstants.TASK_GET_ALL, Handler<Message<Void>> { this.onGetAll(it) })
        vertx.eventBus().consumer<String>(AddressConstants.TASK_GET_BY_CORRELATION_ID,
                Handler<Message<String>> { this.onGetByCorrelationId(it) })
        vertx.eventBus().consumer<JsonObject>(AddressConstants.TASK_INC, Handler<Message<JsonObject>> { this.onInc(it) })
    }

    /**
     * Handle a request to get all tasks
     * @param msg the request
     */
    private fun onGetAll(msg: Message<Void>) {
        cleanUp()
        val result = JsonObject()
        tasks.forEach { (c, m) -> result.put(c, makeResponse(m)) }
        msg.reply(result)
    }

    /**
     * Handle a request to get all tasks of a given correlation ID
     * @param msg the request
     */
    private fun onGetByCorrelationId(msg: Message<String>) {
        cleanUp()
        val correlationId = msg.body()
        if (correlationId == null) {
            msg.fail(400, "Correlation ID expected")
            return
        }

        val m = tasks[correlationId]
        if (m == null) {
            msg.fail(404, "Unknown correlation ID")
            return
        }

        val result = JsonObject()
        result.put(correlationId, makeResponse(m))
        msg.reply(result)
    }

    /**
     * Make a response containing information about single correlation ID
     * @param m the information about the correlation ID
     * @return the response
     */
    private fun makeResponse(m: Map<Class<out Task>, Task>): JsonArray {
        val arr = JsonArray()
        m.forEach { (cls, t) ->
            val o = JsonObject.mapFrom(t)
            o.remove("correlationId")
            arr.add(o)
        }
        return arr
    }

    /**
     * Handle a request to increment the values of a task
     * @param msg the request
     */
    private fun onInc(msg: Message<JsonObject>) {
        val body = msg.body()
                ?: // ignore
                return

        var t = body.mapTo(Task::class.java)

        val m = (tasks as java.util.Map<String, Map<Class<out Task>, Task>>).computeIfAbsent(
                t.correlationId) { k -> LinkedHashMap() }

        val existingTask = m[t.javaClass]
        if (existingTask != null) {
            if (existingTask.endTime != null && t.endTime != null) {
                // End time will be updated. Temporarily remove existing task from finished tasks.
                finishedTasks.remove(existingTask)
            }
            existingTask.inc(t)
            t = existingTask
        } else {
            m.put(t.javaClass, t)
        }

        if (t.endTime != null) {
            finishedTasks.add(t)
        }

        // help the indexer verticle and finish the indexing task if the importer
        // task has also finished and the number of indexed chunks equals the
        // number of imported chunks
        if (t is IndexingTask || t is ImportingTask) {
            val importingTask = m[ImportingTask::class.java] as ImportingTask
            if (importingTask != null && importingTask.endTime != null) {
                val indexingTask = m[IndexingTask::class.java] as IndexingTask
                if (indexingTask != null && indexingTask.endTime == null &&
                        indexingTask.indexedChunks == importingTask.importedChunks) {
                    indexingTask.endTime = Instant.now()
                    finishedTasks.add(indexingTask)
                }
            }
        }

        // help the indexer verticle and finish the removing task if all chunks
        // have been removed
        if (t is RemovingTask && t.endTime == null) {
            val rt = t
            if (rt.totalChunks == rt.removedChunks) {
                rt.endTime = Instant.now()
                finishedTasks.add(rt)
            }
        }

        cleanUp()
    }

    /**
     * Remove outdated tasks
     */
    private fun cleanUp() {
        val threshold = Instant.now().minus(retainSeconds, ChronoUnit.SECONDS)
        while (!finishedTasks.isEmpty() && finishedTasks.first().endTime.isBefore(threshold)) {
            val t = finishedTasks.pollFirst()
            val m = tasks[t!!.correlationId]
            if (m != null) {
                m.remove(t.javaClass)
                if (m.isEmpty()) {
                    tasks.remove(t.correlationId)
                }
            }
        }
    }
}
