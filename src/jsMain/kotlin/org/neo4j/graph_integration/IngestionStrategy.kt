package org.neo4j.graph_integration

actual abstract class IngestionStrategy<KEY, VALUE> {
    actual abstract fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    actual abstract fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    actual abstract fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    actual abstract fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    actual fun events(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = listOf(
        mergeNodeEvents(events),
        deleteNodeEvents(events),
        mergeRelationshipEvents(events),
        deleteRelationshipEvents(events)
    ).reduce { acc, ingestionEvent ->
        IngestionEvent(
            events = acc.events + ingestionEvent.events,
            invalidEvents = acc.invalidEvents + ingestionEvent.invalidEvents
        )
    }

}