package io.georocket.index.generic

import java.io.IOException
import java.io.InputStream

import org.yaml.snakeyaml.Yaml

import com.google.common.collect.ImmutableMap

import io.georocket.index.xml.MetaIndexer
import io.georocket.index.xml.MetaIndexerFactory
import io.georocket.query.ElasticsearchQueryHelper
import io.georocket.query.KeyValueQueryPart
import io.georocket.query.QueryPart
import io.georocket.query.StringQueryPart
import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.vertx.core.json.JsonObject

/**
 * Factory for [DefaultMetaIndexer] instances. Contains default mappings
 * required for essential GeoRocket indexer operations.
 * @author Michel Kraemer
 */
class DefaultMetaIndexerFactory : MetaIndexerFactory {
    private val mappings: Map<String, Any>

    /**
     * Default constructor
     */
    init {
        // load default mapping
        val yaml = Yaml()
        var mappings: MutableMap<String, Any>
        try {
            this.javaClass.getResourceAsStream("index_defaults.yaml").use { `is` -> mappings = yaml.load<Any>(`is`) as Map<String, Any> }
        } catch (e: IOException) {
            throw RuntimeException("Could not load default mappings", e)
        }

        // remove unnecessary node
        mappings.remove("variables")

        this.mappings = ImmutableMap.copyOf(mappings)
    }

    override fun getMapping(): Map<String, Any> {
        return mappings
    }

    override fun getQueryPriority(queryPart: QueryPart): QueryCompiler.MatchPriority {
        return if (queryPart is StringQueryPart || queryPart is KeyValueQueryPart) {
            QueryCompiler.MatchPriority.SHOULD
        } else QueryCompiler.MatchPriority.NONE
    }

    override fun compileQuery(queryPart: QueryPart): JsonObject? {
        var result: JsonObject? = null
        if (queryPart is StringQueryPart) {
            // match values of all fields regardless of their name
            val search = queryPart.searchString
            result = ElasticsearchQueryHelper.termQuery("tags", search)
        } else if (queryPart is KeyValueQueryPart) {
            val key = queryPart.key
            val value = queryPart.value
            val comp = queryPart.comparisonOperator

            when (comp) {
                KeyValueQueryPart.ComparisonOperator.EQ -> result = ElasticsearchQueryHelper.termQuery("props.$key", value)
                KeyValueQueryPart.ComparisonOperator.GT -> result = ElasticsearchQueryHelper.gtQuery("props.$key", value)
                KeyValueQueryPart.ComparisonOperator.GTE -> result = ElasticsearchQueryHelper.gteQuery("props.$key", value)
                KeyValueQueryPart.ComparisonOperator.LT -> result = ElasticsearchQueryHelper.ltQuery("props.$key", value)
                KeyValueQueryPart.ComparisonOperator.LTE -> result = ElasticsearchQueryHelper.lteQuery("props.$key", value)
            }

            if (queryPart.comparisonOperator == ComparisonOperator.EQ && "correlationId" == queryPart.key) {
                val cq = ElasticsearchQueryHelper.termQuery("correlationId", value)
                if (result != null) {
                    val bool = ElasticsearchQueryHelper.boolQuery(1)
                    ElasticsearchQueryHelper.boolAddShould(bool, result)
                    ElasticsearchQueryHelper.boolAddShould(bool, cq)
                    result = bool
                } else {
                    result = cq
                }
            }
        }
        return result
    }

    override fun createIndexer(): MetaIndexer {
        return DefaultMetaIndexer()
    }
}
