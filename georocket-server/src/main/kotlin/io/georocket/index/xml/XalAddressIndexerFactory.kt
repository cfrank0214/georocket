package io.georocket.index.xml

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import io.georocket.query.KeyValueQueryPart
import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.georocket.query.QueryPart
import io.georocket.query.StringQueryPart
import io.vertx.core.json.JsonObject

import io.georocket.query.ElasticsearchQueryHelper.gtQuery
import io.georocket.query.ElasticsearchQueryHelper.gteQuery
import io.georocket.query.ElasticsearchQueryHelper.ltQuery
import io.georocket.query.ElasticsearchQueryHelper.lteQuery
import io.georocket.query.ElasticsearchQueryHelper.multiMatchQuery
import io.georocket.query.ElasticsearchQueryHelper.termQuery

/**
 * Creates instances of [XalAddressIndexer]
 * @author Michel Kraemer
 */
class XalAddressIndexerFactory : XMLIndexerFactory {
    override fun createIndexer(): XMLIndexer {
        return XalAddressIndexer()
    }

    override fun getMapping(): Map<String, Any> {
        return ImmutableMap.of<String, Any>("dynamic_templates", ImmutableList.of(ImmutableMap.of(
                "addressFields", ImmutableMap.of(
                "path_match", "address.*",
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
            return multiMatchQuery(search, "address.*")
        } else if (queryPart is KeyValueQueryPart) {
            val key = queryPart.key
            val value = queryPart.value
            val comp = queryPart.comparisonOperator
            val name = "address.$key"
            when (comp) {
                KeyValueQueryPart.ComparisonOperator.EQ -> return termQuery(name, value)
                KeyValueQueryPart.ComparisonOperator.GT -> return gtQuery(name, value)
                KeyValueQueryPart.ComparisonOperator.GTE -> return gteQuery(name, value)
                KeyValueQueryPart.ComparisonOperator.LT -> return ltQuery(name, value)
                KeyValueQueryPart.ComparisonOperator.LTE -> return lteQuery(name, value)
            }
        }
        return null
    }
}
