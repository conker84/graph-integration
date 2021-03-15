package strategy
import convert
import org.neo4j.graph_integration.strategy.cud.CUDIngestionStrategy

@JsExport
class CUDIngestionStrategy: IngestionStrategy {

    private val strategy = CUDIngestionStrategy<Map<String, Any>, Map<String, Any>>()

    override fun mergeNodeEvents(elements: Array<Any>): Any = convert(elements) { strategy.mergeNodeEvents(it) }

    override fun deleteNodeEvents(elements: Array<Any>): Any = convert(elements) { strategy.deleteNodeEvents(it) }

    override fun mergeRelationshipEvents(elements: Array<Any>): Any =
        convert(elements) { strategy.mergeRelationshipEvents(it) }

    override fun deleteRelationshipEvents(elements: Array<Any>): Any =
        convert(elements) { strategy.deleteRelationshipEvents(it) }
}