package io.georocket.index.xml

import java.io.IOException
import java.io.InputStream
import java.util.Properties

import io.georocket.index.generic.BoundingBoxIndexerFactory

/**
 * Create instances of [XMLBoundingBoxIndexer]
 * @author Michel Kraemer
 */
class XMLBoundingBoxIndexerFactory : BoundingBoxIndexerFactory(), XMLIndexerFactory {

    override fun createIndexer(): XMLIndexer {
        if (configuredClass == null) {
            return XMLBoundingBoxIndexer()
        }
        try {
            return configuredClass!!.newInstance()
        } catch (e: ReflectiveOperationException) {
            throw RuntimeException("Could not create custom " + "BoundingBoxIndexer", e)
        }

    }

    companion object {
        private val PROPERTIES_FILENAME = "io.georocket.index.xml.XMLBoundingBoxIndexerFactory.properties"
        private val BOUNDINGBOXINDEXER_CLASSNAME = "io.georocket.index.xml.XMLBoundingBoxIndexer"
        private var configuredClass: Class<out XMLBoundingBoxIndexer>? = null

        init {
            loadConfig()
        }

        private fun loadConfig() {
            try {
                XMLBoundingBoxIndexerFactory::class.java.classLoader
                        .getResourceAsStream(PROPERTIES_FILENAME)!!.use { `is` ->
                    if (`is` != null) {
                        val p = Properties()
                        p.load(`is`)
                        val cls = p.getProperty(BOUNDINGBOXINDEXER_CLASSNAME)
                        if (cls != null) {
                            try {
                                configuredClass = Class.forName(cls) as Class<out XMLBoundingBoxIndexer>
                            } catch (e: ClassNotFoundException) {
                                throw RuntimeException("Could not find custom "
                                        + "BoundingBoxIndexer class: " + cls, e)
                            }

                        }
                    }
                }
            } catch (e: IOException) {
                // ignore
            }

        }
    }
}
