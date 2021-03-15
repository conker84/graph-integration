package org.neo4j.graph_integration

data class InvalidEvent<EVT>(val error: Throwable? = null, val event: EVT, val meta: Map<String, Any> = emptyMap())

data class Event<EVT>(val query: String, val events: List<EVT>) {}

data class IngestionEvent<EVT>(val events: List<Event<EVT>>, val invalidEvents: List<InvalidEvent<EVT>> = emptyList())