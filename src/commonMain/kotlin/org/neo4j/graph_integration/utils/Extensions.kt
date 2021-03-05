package org.neo4j.graph_integration.utils

private val JAVA_VARIABLE_REGEX = """^([a-zA-Z_${'$'}][a-zA-Z\d_${'$'}]*)${'$'}""".toRegex()
fun String.quote(): String = if (this.matches(JAVA_VARIABLE_REGEX)) this else "`$this`"