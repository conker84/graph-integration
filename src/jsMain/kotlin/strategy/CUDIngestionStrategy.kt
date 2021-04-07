package strategy
import convert
import org.neo4j.graph_integration.strategy.cud.CUDIngestionStrategy

@JsExport
class CUDIngestionStrategy: IngestionStrategy {

    private val strategy = CUDIngestionStrategy<Any, Any>()

    override fun events(elements: Array<Any>): Any = convert(elements) { strategy.events(it) }
}