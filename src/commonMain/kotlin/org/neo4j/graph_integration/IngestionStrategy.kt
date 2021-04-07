package org.neo4j.graph_integration

expect abstract class IngestionStrategy<KEY, VALUE>() {
    abstract fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    abstract fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    abstract fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent
    abstract fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent

    fun events(events: Collection<Entity<KEY, VALUE>>): IngestionEvent

}
