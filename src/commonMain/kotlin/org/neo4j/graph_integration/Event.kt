package org.neo4j.graph_integration

data class InvalidEvent(val error: Throwable? = null, val event: Any?, val meta: Map<String, Any> = emptyMap())

data class Event(val query: String, val events: List<Map<String, Any?>>)

data class IngestionEvent(val events: List<Event>, val invalidEvents: List<InvalidEvent> = emptyList())