package org.neo4j.graph_integration

import kotlin.js.JsExport

@JsExport
data class Entity<KEY, VALUE>(val key: KEY? = null, val value: VALUE?)