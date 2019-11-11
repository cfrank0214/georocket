package io.georocket.index.generic

import io.georocket.query.ElasticsearchQueryHelper.geoShapeQuery
import io.georocket.query.ElasticsearchQueryHelper.shape

import java.util.Arrays
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.google.common.collect.ImmutableMap

import io.georocket.constants.ConfigConstants
import io.georocket.index.IndexerFactory
import io.georocket.query.QueryCompiler
import io.georocket.util.CoordinateTransformer
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.geotools.referencing.CRS
import org.opengis.referencing.FactoryException
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.TransformException

/**
 * Base class for factories creating indexers that manage bounding boxes
 * @author Michel Kraemer
 */
abstract class BoundingBoxIndexerFactory : IndexerFactory {
    private var defaultCrs: String? = null

    /**
     * Default constructor
     */
    init {
        val ctx = Vertx.currentContext()
        if (ctx != null) {
            val config = ctx.config()
            if (config != null) {
                setDefaultCrs(config.getString(ConfigConstants.QUERY_DEFAULT_CRS))
            }
        }
    }

    /**
     * Set the default CRS which is used to transform query bounding box coordinates
     * to WGS84 coordinates
     * @param defaultCrs the CRS string (see [CoordinateTransformer.decode]).
     */
    fun setDefaultCrs(defaultCrs: String) {
        this.defaultCrs = defaultCrs
    }

    override fun getMapping(): Map<String, Any> {
        val precisionKey: String
        var precisionValue: Any? = null

        // check if we have a current Vert.x context from which we can get the config
        val ctx = Vertx.currentContext()
        if (ctx != null) {
            val config = ctx.config()
            if (config != null) {
                precisionValue = config.getString(ConfigConstants.INDEX_SPATIAL_PRECISION)
            }
        }

        if (precisionValue == null) {
            // use the maximum number of tree levels to achieve highest precision
            // see org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree.MAX_LEVELS_POSSIBLE
            precisionKey = "tree_levels"
            precisionValue = 29
        } else {
            precisionKey = "precision"
        }

        return ImmutableMap.of<String, Any>("properties", ImmutableMap.of("bbox", ImmutableMap.of(
                "type", "geo_shape",

                // for a discussion on the tree type to use see
                // https://github.com/elastic/elasticsearch/issues/14181

                // quadtree uses less memory and seems to be a lot faster than geohash
                // see http://tech.taskrabbit.com/blog/2015/06/09/elasticsearch-geohash-vs-geotree/
                "tree", "quadtree",

                precisionKey, precisionValue
        )))
    }

    override fun getQueryPriority(search: String): QueryCompiler.MatchPriority {
        val bboxMatcher = BBOX_PATTERN.matcher(search)
        return if (bboxMatcher.matches()) {
            QueryCompiler.MatchPriority.ONLY
        } else QueryCompiler.MatchPriority.NONE
    }

    override fun compileQuery(search: String): JsonObject? {
        var crs: CoordinateReferenceSystem? = null
        var crsCode: String? = null
        val co: String
        val index = search.lastIndexOf(':')

        if (index > 0) {
            crsCode = search.substring(0, index)
            co = search.substring(index + 1)
        } else {
            co = search
        }

        var points = Arrays.stream(co.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
                .map<String>(Function<String, String> { it.trim({ it <= ' ' }) })
                .mapToDouble(ToDoubleFunction<String> { java.lang.Double.parseDouble(it) })
                .toArray()

        if (crsCode != null) {
            try {
                crs = CRS.decode(crsCode)
            } catch (e: FactoryException) {
                throw RuntimeException(
                        String.format("CRS %s could not be parsed: %s",
                                crsCode, e.message), e)
            }

        } else if (defaultCrs != null) {
            try {
                crs = CoordinateTransformer.decode(defaultCrs)
            } catch (e: FactoryException) {
                throw RuntimeException(
                        String.format("Default CRS %s could not be parsed: %s",
                                defaultCrs, e.message), e)
            }

        }

        if (crs != null) {
            try {
                val transformer = CoordinateTransformer(crs)
                points = transformer.transform(points, -1)
            } catch (e: FactoryException) {
                throw RuntimeException(String.format("CRS %s could not be parsed: %s",
                        crsCode, e.message), e)
            } catch (e: TransformException) {
                throw RuntimeException(String.format("Coordinates %s could not be " + "transformed to %s: %s", co, crsCode, e.message), e)
            }

        }

        val minX = points[0]
        val minY = points[1]
        val maxX = points[2]
        val maxY = points[3]
        val coordinates = JsonArray()
        coordinates.add(JsonArray().add(minX).add(maxY))
        coordinates.add(JsonArray().add(maxX).add(minY))
        return geoShapeQuery("bbox", shape("envelope", coordinates), "intersects")
    }

    companion object {
        private val FLOAT_REGEX = "[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?"
        private val COMMA_REGEX = "\\s*,\\s*"
        private val CODE_PREFIX = "([a-zA-Z]+:\\d+:)?"
        private val BBOX_REGEX = CODE_PREFIX + FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX +
                COMMA_REGEX + FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX
        private val BBOX_PATTERN = Pattern.compile(BBOX_REGEX)
    }
}
