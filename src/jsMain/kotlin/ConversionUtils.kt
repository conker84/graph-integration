
import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.Event
import org.neo4j.graph_integration.IngestionEvent
import org.neo4j.graph_integration.InvalidEvent

private fun entriesOf(jsObject: dynamic): List<Pair<String, Any?>> =
    (js("Object.entries") as (dynamic) -> Array<Array<Any?>>)
        .invoke(jsObject)
        .map { entry -> entry[0] as String to toKotlin(entry[1]) }

fun mapOf(jsObject: dynamic): Map<String, Any?> = entriesOf(jsObject).toMap()

private fun toKotlin(input: Any?): Any? {
    return when (input) {
        is Array<*> -> input.map { toKotlin(it) }
        null -> null
        else -> {
            when (jsTypeOf(input)) {
                "object" -> {
                    mapOf(input.asDynamic())
                }
                else -> input
            }
        }
    }
}

private fun toJavascript(input: Any?): dynamic {
    return when (input) {
        is Collection<*> -> input.map { toJavascript(it) }.toTypedArray()
        is Array<*> -> input.map { toJavascript(it) }.toTypedArray()
        is Event -> {
            val data = js("({})")
            data["query"] = input.query
            data["events"] = toJavascript(input.events)
            data
        }
        is IngestionEvent -> {
            val data = js("({})")
            data["events"] = toJavascript(input.events)
            data["invalidEvents"] = toJavascript(input.invalidEvents)
            data
        }
        is Map<*, *> -> {
            val data = js("({})")
            input.forEach { e -> data[e.key] = toJavascript(e.value) }
            data
        }
        is InvalidEvent -> {
            val data = js("({})")
            data["error"] = toJavascript(input.error)
            data["event"] = toJavascript(input.event)
            data["meta"] = toJavascript(input.meta)
            data
        }
        else -> input?.asDynamic()
    }
}

fun dynamicOf(json: Any): dynamic = toJavascript(json)

fun entityOf(jsObject: dynamic): Entity<Map<String, Any?>, Map<String, Any?>> {
    val key = jsObject["key"]
    val keyMap = if (key != null) {
        mapOf(key)
    } else {
        null
    }
    return Entity(keyMap, mapOf(jsObject["value"]))
}

fun convert(elements: Array<Any>, body: (List<dynamic>) -> IngestionEvent): Any {
    val listEvents = elements.map { entityOf(it.asDynamic()) }
    val result = body(listEvents)
    return dynamicOf(result) as Any
}