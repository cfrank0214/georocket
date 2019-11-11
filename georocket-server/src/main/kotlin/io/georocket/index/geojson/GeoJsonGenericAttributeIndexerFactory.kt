package io.georocket.index.geojson

import io.georocket.index.generic.GenericAttributeIndexerFactory
import io.georocket.index.xml.JsonIndexer
import io.georocket.index.xml.JsonIndexerFactory

/**
 * Create instances of [GeoJsonGenericAttributeIndexer]
 * @author Michel Kraemer
 */
class GeoJsonGenericAttributeIndexerFactory : GenericAttributeIndexerFactory(), JsonIndexerFactory {
    override fun createIndexer(): JsonIndexer {
        return GeoJsonGenericAttributeIndexer()
    }
}
