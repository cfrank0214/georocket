package io.georocket.http

import io.georocket.constants.AddressConstants
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext

import io.georocket.http.Endpoint.fail

/**
 * An HTTP endpoint for accessing the information about tasks maintained by
 * the [io.georocket.tasks.TaskVerticle]
 * @author Michel Kraemer
 */
class TaskEndpoint : Endpoint {
    private var vertx: Vertx? = null

    override fun getMountPoint(): String {
        return "/tasks"
    }

    override fun createRouter(vertx: Vertx): Router {
        this.vertx = vertx

        val router = Router.router(vertx)
        router.get("/").handler(Handler<RoutingContext> { this.onGetAll(it) })
        router.get("/:id").handler(Handler<RoutingContext> { this.onGetByCorrelationId(it) })
        router.options("/").handler(Handler<RoutingContext> { this.onOptions(it) })

        return router
    }

    /**
     * Return all tasks
     * @param context the context for handling HTTP requests
     */
    private fun onGetAll(context: RoutingContext) {
        onGet(context, AddressConstants.TASK_GET_ALL, null)
    }

    /**
     * Return all tasks belonging to a given correlation ID
     * @param context the context for handling HTTP requests
     */
    private fun onGetByCorrelationId(context: RoutingContext) {
        val correlationId = context.pathParam("id")
        onGet(context, AddressConstants.TASK_GET_BY_CORRELATION_ID, correlationId)
    }

    /**
     * Helper method to generate a response containing task information
     * @param context the context for handling HTTP requests
     * @param address the address to send a request to
     * @param correlationId the correlation ID to send to the address
     */
    private fun onGet(context: RoutingContext, address: String, correlationId: String?) {
        vertx!!.eventBus().send<JsonObject>(address, correlationId) { ar ->
            if (ar.failed()) {
                fail(context.response(), ar.cause())
            } else {
                context.response()
                        .setStatusCode(200)
                        .putHeader("Content-Type", "application/json")
                        .end(ar.result().body().encode())
            }
        }
    }

    /**
     * Handle the HTTP options request
     * @param context the context for handling HTTP requests
     */
    protected fun onOptions(context: RoutingContext) {
        context.response()
                .putHeader("Allow", "GET")
                .setStatusCode(200)
                .end()
    }
}
