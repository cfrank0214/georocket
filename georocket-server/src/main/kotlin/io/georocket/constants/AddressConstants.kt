package io.georocket.constants

/**
 * Constants for addresses on the event bus
 * @author Michel Kraemer
 */
object AddressConstants {
    val GEOROCKET = "georocket"
    val IMPORTER_IMPORT = "georocket.importer.import"
    val IMPORTER_PAUSE = "georocket.importer.pause"
    val INDEXER_ADD = "georocket.indexer.add"
    val INDEXER_QUERY = "georocket.indexer.query"
    val INDEXER_DELETE = "georocket.indexer.delete"
    val METADATA_GET_ATTRIBUTE_VALUES = "georocket.metadata.attribute.values.get"
    val METADATA_GET_PROPERTY_VALUES = "georocket.metadata.property.values.get"
    val METADATA_SET_PROPERTIES = "georocket.metadata.properties.set"
    val METADATA_REMOVE_PROPERTIES = "georocket.metadata.properties.remove"
    val METADATA_APPEND_TAGS = "georocket.metadata.tags.append"
    val METADATA_REMOVE_TAGS = "georocket.metadata.tags.remove"
    val TASK_GET_ALL = "georocket.task.getAll"
    val TASK_GET_BY_CORRELATION_ID = "georocket.task.getByCorrelationId"
    val TASK_INC = "georocket.task.inc"
}// hidden constructor
