package io.georocket.index.geojson

import io.georocket.index.generic.BoundingBoxIndexerFactory
import io.georocket.index.xml.JsonIndexer
import io.georocket.index.xml.JsonIndexerFactory

/**
 * Create instances of [GeoJsonBoundingBoxIndexer]
 * @author Michel Kraemer
 */
class GeoJsonBoundingBoxIndexerFactory : BoundingBoxIndexerFactory(), JsonIndexerFactory {
    override fun createIndexer(): JsonIndexer {
        return GeoJsonBoundingBoxIndexer()
    }
}
