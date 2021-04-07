package org.neo4j.graph_integration.strategy.cypher

import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.Event
import org.neo4j.graph_integration.IngestionEvent
import org.neo4j.graph_integration.IngestionStrategy
import org.neo4j.graph_integration.utils.IngestionUtils

class CypherTemplateIngestionStrategy<KEY, VALUE>(query: String,
                                                       private val transformationFunction: (Entity<KEY, VALUE>) -> Map<String, Any>? = { it.value as? Map<String, Any> }): IngestionStrategy<KEY, VALUE>() {
    private val fullQuery = "${IngestionUtils.UNWIND} $query"
    override fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent {
        return IngestionEvent(listOf(Event(fullQuery, events.mapNotNull(transformationFunction).toList())))
    }

    override fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = IngestionEvent(emptyList())

    override fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = IngestionEvent(emptyList())

    override fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = IngestionEvent(emptyList())

}