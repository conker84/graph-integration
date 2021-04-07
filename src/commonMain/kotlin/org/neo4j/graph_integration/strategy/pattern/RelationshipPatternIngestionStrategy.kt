package org.neo4j.graph_integration.strategy.pattern

import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.Event
import org.neo4j.graph_integration.IngestionEvent
import org.neo4j.graph_integration.IngestionStrategy
import org.neo4j.graph_integration.utils.IngestionUtils
import org.neo4j.graph_integration.utils.IngestionUtils.containsProp
import org.neo4j.graph_integration.utils.IngestionUtils.getLabelsAsString
import org.neo4j.graph_integration.utils.IngestionUtils.getNodeMergeKeys
import org.neo4j.graph_integration.utils.flatten
import streams.service.sink.strategy.PatternConfigurationType
import streams.service.sink.strategy.RelationshipPatternConfiguration

class RelationshipPatternIngestionStrategy<KEY, VALUE>(private val relationshipPatternConfiguration: RelationshipPatternConfiguration): IngestionStrategy<KEY, VALUE>() {

    private val mergeRelationshipTemplate: String = """
                |${IngestionUtils.UNWIND}
                |MERGE (start${getLabelsAsString(relationshipPatternConfiguration.start.labels)}{${
                    getNodeMergeKeys("start.keys", relationshipPatternConfiguration.start.keys)
                }})
                |SET start = event.start.properties
                |SET start += event.start.keys
                |MERGE (end${getLabelsAsString(relationshipPatternConfiguration.end.labels)}{${
                    getNodeMergeKeys("end.keys", relationshipPatternConfiguration.end.keys)
                }})
                |SET end = event.end.properties
                |SET end += event.end.keys
                |MERGE (start)-[r:${relationshipPatternConfiguration.relType}]->(end)
                |SET r = event.properties
            """.trimMargin()

    private val deleteRelationshipTemplate: String = """
                |${IngestionUtils.UNWIND}
                |MATCH (start${getLabelsAsString(relationshipPatternConfiguration.start.labels)}{${
                    getNodeMergeKeys("start.keys", relationshipPatternConfiguration.start.keys)
                }})
                |MATCH (end${getLabelsAsString(relationshipPatternConfiguration.end.labels)}{${
                    getNodeMergeKeys("end.keys", relationshipPatternConfiguration.end.keys)
                }})
                |MATCH (start)-[r:${relationshipPatternConfiguration.relType}]->(end)
                |DELETE r
            """.trimMargin()

    override fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = IngestionEvent(emptyList())

    override fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = IngestionEvent(emptyList())

    override fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent {
        return events
                .mapNotNull {
                    it.value?.let {
                        toData(relationshipPatternConfiguration, it as Map<String, Any?>)
                    }
                }
                .let {
                    val list = if (it.isEmpty()) {
                        emptyList()
                    } else {
                        listOf(Event(mergeRelationshipTemplate, it))
                    }
                    IngestionEvent(list)
                }
    }

    override fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent {
        return events
                .filter { it.value == null && it.key != null }
                // .mapNotNull { if (it.key != null) JSONUtils.asMap(it.key) else null }
                .mapNotNull {
                    toData(relationshipPatternConfiguration, it.key as Map<String, Any?>, false)
                }
                .let {
                    val list = if (it.isEmpty()) {
                        emptyList()
                    } else {
                        listOf(Event(deleteRelationshipTemplate, it))
                    }
                    IngestionEvent(list)
                }
    }

    companion object {

        fun toData(relationshipPatternConfiguration: RelationshipPatternConfiguration, props: Map<String, Any?>, withProperties: Boolean = true): Map<String, Any?>? {
            val properties = props.flatten()
            val containsKeys = relationshipPatternConfiguration.start.keys.all { properties.containsKey(it) }
                    && relationshipPatternConfiguration.end.keys.all { properties.containsKey(it) }
            return if (containsKeys) {
                val startConf = relationshipPatternConfiguration.start
                val endConf = relationshipPatternConfiguration.end

                val start = NodePatternIngestionStrategy.toData(startConf, props)
                val end = NodePatternIngestionStrategy.toData(endConf, props)
                if (withProperties) {
                    val filteredProperties = when (relationshipPatternConfiguration.type) {
                        PatternConfigurationType.ALL -> properties.filterKeys { relationshipPatternConfiguration.isRelationshipProperty(it) }
                        PatternConfigurationType.EXCLUDE -> properties.filterKeys {
                            val containsProp = containsProp(it, relationshipPatternConfiguration.properties)
                            relationshipPatternConfiguration.isRelationshipProperty(it) && !containsProp
                        }
                        PatternConfigurationType.INCLUDE -> properties.filterKeys {
                            val containsProp = containsProp(it, relationshipPatternConfiguration.properties)
                            relationshipPatternConfiguration.isRelationshipProperty(it) && containsProp
                        }
                    }
                    mapOf("start" to start, "end" to end, "properties" to filteredProperties)
                } else {
                    mapOf("start" to start, "end" to end)
                }
            } else {
                null
            }
        }
    }

}