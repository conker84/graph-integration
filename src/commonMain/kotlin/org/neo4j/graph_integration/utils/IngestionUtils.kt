package org.neo4j.graph_integration.utils

object IngestionUtils {
    const val labelSeparator = ":"
    const val keySeparator = ", "

    const val UNWIND = "UNWIND \$events AS event"

    const val WITH_EVENT_FROM = "WITH event, from"

    fun getLabelsAsString(labels: Collection<String>): String = labels
        .joinToString(labelSeparator) { it.quote() }
        .let { if (it.isNotBlank()) "$labelSeparator$it" else it }

    fun getNodeKeysAsString(prefix: String = "properties", keys: Set<String>): String =
        keys.joinToString(keySeparator) { toQuotedProperty(prefix, it) }

    private fun toQuotedProperty(prefix: String = "properties", property: String): String {
        val quoted = property.quote()
        return "$quoted: event.$prefix.$quoted"
    }

    fun getNodeMergeKeys(prefix: String, keys: Set<String>): String = keys
            .joinToString(keySeparator) {
                val quoted = it.quote()
                "$quoted: event.$prefix.$quoted"
            }

    fun containsProp(key: String, properties: List<String>): Boolean = if (key.contains(".")) {
        properties.contains(key) || properties.any { key.startsWith("$it.") }
    } else {
        properties.contains(key)
    }
}