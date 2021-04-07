package org.neo4j.graph_integration.utils

private val JAVA_VARIABLE_REGEX = """^([a-zA-Z_${'$'}][a-zA-Z\d_${'$'}]*)${'$'}""".toRegex()
fun String.quote(): String = if (this.matches(JAVA_VARIABLE_REGEX)) this else "`$this`"
fun String.isJavaIdentifierStart() = JAVA_VARIABLE_REGEX.matches(if (this.isNotBlank()) this[0].toString() else "")

fun Map<String, Any?>.flatten(map: Map<String, Any?> = this, prefix: String = ""): Map<String, Any?> {
    return map.flatMap {
        val key = it.key
        val value = it.value
        val newKey = if (prefix != "") "$prefix.$key" else key
        if (value is Map<*, *>) {
            flatten(value as Map<String, Any>, newKey).toList()
        } else {
            listOf(newKey to value)
        }
    }.toMap()
}