package strategy

@JsExport
interface IngestionStrategy {
    @JsName("mergeNodeEvents")
    fun mergeNodeEvents(elements: Array<Any>): Any
    @JsName("deleteNodeEvents")
    fun deleteNodeEvents(elements: Array<Any>): Any
    @JsName("mergeRelationshipEvents")
    fun mergeRelationshipEvents(elements: Array<Any>): Any
    @JsName("deleteRelationshipEvents")
    fun deleteRelationshipEvents(elements: Array<Any>): Any
}