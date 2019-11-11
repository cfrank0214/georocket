package io.georocket.index.generic

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap

import io.georocket.index.IndexerFactory
import io.georocket.query.*
import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.vertx.core.json.JsonObject

/**
 * Base class for factories creating indexers that manage arbitrary generic
 * string attributes (i.e. key-value pairs)
 * @author Michel Kraemer
 */
abstract class GenericAttributeIndexerFactory : IndexerFactory {
    override fun getMapping(): Map<String, Any> {
        // dynamic mapping: do not analyze generic attributes
        return ImmutableMap.of<String, Any>("dynamic_templates", ImmutableList.of(ImmutableMap.of(
                "genAttrsFields", ImmutableMap.of(
                "path_match", "genAttrs.*",
                "mapping", ImmutableMap.of(
                "type", "keyword"
        )
        )
        )))
    }

    override fun getQueryPriority(queryPart: QueryPart): QueryCompiler.MatchPriority {
        return if (queryPart is StringQueryPart || queryPart is KeyValueQueryPart) {
            QueryCompiler.MatchPriority.SHOULD
        } else QueryCompiler.MatchPriority.NONE
    }

    override fun compileQuery(queryPart: QueryPart): JsonObject? {
        if (queryPart is StringQueryPart) {
            // match values of all fields regardless of their name
            val search = queryPart.searchString
            return ElasticsearchQueryHelper.multiMatchQuery(search, "genAttrs.*")
        } else if (queryPart is KeyValueQueryPart) {
            val key = queryPart.key
            val value = queryPart.value
            val comp = queryPart.comparisonOperator

            when (comp) {
                KeyValueQueryPart.ComparisonOperator.EQ -> return ElasticsearchQueryHelper.termQuery("genAttrs.$key", value)
                KeyValueQueryPart.ComparisonOperator.GT -> return ElasticsearchQueryHelper.gtQuery("genAttrs.$key", value)
                KeyValueQueryPart.ComparisonOperator.GTE -> return ElasticsearchQueryHelper.gteQuery("genAttrs.$key", value)
                KeyValueQueryPart.ComparisonOperator.LT -> return ElasticsearchQueryHelper.ltQuery("genAttrs.$key", value)
                KeyValueQueryPart.ComparisonOperator.LTE -> return ElasticsearchQueryHelper.lteQuery("genAttrs.$key", value)
            }
        }
        return null
    }
}
