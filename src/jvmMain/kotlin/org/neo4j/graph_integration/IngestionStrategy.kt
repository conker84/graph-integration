package org.neo4j.graph_integration

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking

actual abstract class IngestionStrategy<KEY, VALUE> {
    actual abstract fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    actual abstract fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    actual abstract fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    actual abstract fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    actual fun events(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = runBlocking {
        listOf(GlobalScope.async { mergeNodeEvents(events) },
                GlobalScope.async { deleteNodeEvents(events) },
                GlobalScope.async { mergeRelationshipEvents(events) },
                GlobalScope.async { deleteRelationshipEvents(events) })
            .awaitAll()
            .reduce { acc, ingestionEvent ->
                IngestionEvent(
                    events = acc.events + ingestionEvent.events,
                    invalidEvents = acc.invalidEvents + ingestionEvent.invalidEvents
                )
            }
    }

}