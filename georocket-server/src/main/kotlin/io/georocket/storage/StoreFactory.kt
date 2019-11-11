package io.georocket.storage

import io.georocket.constants.ConfigConstants
import io.georocket.storage.file.FileStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

/**
 * A factory for chunk stores
 * @author Michel Kraemer
 */
object StoreFactory {
    /**
     * Create the configured chunk store
     * @param vertx the Vert.x instance
     * @return the store
     */
    fun createStore(vertx: Vertx): Store {
        val config = vertx.orCreateContext.config()
        val cls = config.getString(ConfigConstants.STORAGE_CLASS,
                FileStore::class.java.name)
        try {
            return Class.forName(cls).getConstructor(Vertx::class.java).newInstance(vertx) as Store
        } catch (e: ReflectiveOperationException) {
            throw RuntimeException("Could not create chunk store", e)
        }

    }
}
