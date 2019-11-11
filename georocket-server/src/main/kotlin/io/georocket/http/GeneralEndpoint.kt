package io.georocket.http

import java.io.IOException
import java.net.URL
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils

import io.georocket.GeoRocket
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext

/**
 * An HTTP endpoint for general requests
 * @author Michel Kraemer
 */
class GeneralEndpoint : Endpoint {
    private val version: String

    /**
     * Create the endpoint
     */
    init {
        version = getVersion()
    }

    /**
     * @return the tool's version string
     */
    private fun getVersion(): String {
        val u = GeoRocket::class.java.getResource("version.dat")
        val version: String
        try {
            version = IOUtils.toString(u, StandardCharsets.UTF_8)
        } catch (e: IOException) {
            throw RuntimeException("Could not read version information", e)
        }

        return version
    }

    override fun getMountPoint(): String {
        return "/"
    }

    override fun createRouter(vertx: Vertx): Router {
        val router = Router.router(vertx)
        router.get("/").handler(Handler<RoutingContext> { this.onInfo(it) })
        router.head("/").handler(Handler<RoutingContext> { this.onPing(it) })
        router.options("/").handler(Handler<RoutingContext> { this.onOptions(it) })
        return router
    }

    /**
     * Create a JSON object that can be returned by [.onInfo]
     * @param context the current routing context
     * @return the JSON object
     */
    protected fun createInfo(context: RoutingContext): JsonObject {
        return JsonObject()
                .put("name", "GeoRocket")
                .put("version", version)
                .put("tagline", "It's not rocket science!")
    }

    /**
     * Send information about GeoRocket to the client
     * @param context the context for handling HTTP requests
     */
    protected fun onInfo(context: RoutingContext) {
        val o = createInfo(context)
        context.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(o.encodePrettily())
    }

    /**
     * Send an empty response with a status code of 200 to the client
     * @param context the context for handling HTTP requests
     */
    protected fun onPing(context: RoutingContext) {
        context.response()
                .setStatusCode(200)
                .end()
    }

    /**
     * Handle the HTTP options request
     * @param context the context for handling HTTP requests
     */
    protected fun onOptions(context: RoutingContext) {
        context.response()
                .putHeader("Allow", "GET,HEAD")
                .setStatusCode(200)
                .end()
    }
}
