package strategy

@JsExport
interface IngestionStrategy {
    @JsName("events")
    fun events(elements: Array<Any>): Any
}
