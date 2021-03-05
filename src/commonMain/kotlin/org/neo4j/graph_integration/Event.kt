package org.neo4j.graph_integration

import kotlin.js.JsExport

@JsExport
data class InvalidEvent<EVT>(val error: Throwable? = null, val event: EVT, val meta: Map<String, Any> = emptyMap())

@JsExport
data class Event<EVT>(val query: String, val events: List<EVT>)

@JsExport
data class IngestionEvent<EVT>(val events: List<Event<EVT>>, val invalidEvents: List<InvalidEvent<EVT>> = emptyList())