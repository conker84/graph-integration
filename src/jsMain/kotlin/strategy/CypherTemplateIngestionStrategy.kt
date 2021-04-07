package strategy

import convert
import org.neo4j.graph_integration.strategy.cypher.CypherTemplateIngestionStrategy

@JsExport
class CypherTemplateIngestionStrategy(query: String): IngestionStrategy {
    private val strategy = CypherTemplateIngestionStrategy<Any, Any>(query)
    override fun events(elements: Array<Any>): Any = convert(elements) { strategy.events(it) }
}