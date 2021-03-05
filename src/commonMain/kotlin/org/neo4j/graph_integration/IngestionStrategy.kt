package org.neo4j.graph_integration

interface IngestionStrategy<KEY, VALUE, EVT> {
    fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent<EVT>
    fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent<EVT>
    fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent<EVT>
    fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent<EVT>
}