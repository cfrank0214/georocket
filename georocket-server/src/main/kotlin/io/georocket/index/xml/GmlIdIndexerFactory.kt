package io.georocket.index.xml

import com.google.common.collect.ImmutableMap

import io.georocket.query.ElasticsearchQueryHelper
import io.georocket.query.KeyValueQueryPart
import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.georocket.query.QueryPart
import io.georocket.query.StringQueryPart
import io.vertx.core.json.JsonObject

/**
 * Create instances of [GmlIdIndexer]
 * @author Michel Kraemer
 */
class GmlIdIndexerFactory : XMLIndexerFactory {
    override fun createIndexer(): XMLIndexer {
        return GmlIdIndexer()
    }

    override fun getMapping(): Map<String, Any> {
        return ImmutableMap.of<String, Any>("properties", ImmutableMap.of("gmlIds", ImmutableMap.of(
                "type", "keyword" // array of keywords actually, auto-supported by Elasticsearch
        )))
    }

    /**
     * Test if the given key-value query part refers to a gmlId and if it uses
     * the EQ operator (e.g. EQ(gmlId myId) or EQ(gml:id myId))
     * @param kvqp the key-value query part to check
     * @return true if it refers to a gmlId, false otherwise
     */
    private fun isGmlIdEQ(kvqp: KeyValueQueryPart): Boolean {
        val key = kvqp.key
        val comp = kvqp.comparisonOperator
        return comp == ComparisonOperator.EQ && ("gmlId" == key || "gml:id" == key)
    }

    override fun getQueryPriority(queryPart: QueryPart): QueryCompiler.MatchPriority {
        if (queryPart is StringQueryPart) {
            return QueryCompiler.MatchPriority.SHOULD
        }
        return if (queryPart is KeyValueQueryPart && isGmlIdEQ(queryPart)) {
            QueryCompiler.MatchPriority.SHOULD
        } else QueryCompiler.MatchPriority.NONE
    }

    override fun compileQuery(queryPart: QueryPart): JsonObject? {
        if (queryPart is StringQueryPart) {
            val search = queryPart.searchString
            return ElasticsearchQueryHelper.termQuery("gmlIds", search)
        } else if (queryPart is KeyValueQueryPart) {
            if (isGmlIdEQ(queryPart)) {
                return ElasticsearchQueryHelper.termQuery("gmlIds", queryPart.value)
            }
        }
        return null
    }
}
