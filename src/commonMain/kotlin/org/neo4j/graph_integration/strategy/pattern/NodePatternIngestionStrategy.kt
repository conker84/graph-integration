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
import streams.service.sink.strategy.NodePatternConfiguration
import streams.service.sink.strategy.PatternConfigurationType

class NodePatternIngestionStrategy<KEY, VALUE>(private val nodePatternConfiguration: NodePatternConfiguration): IngestionStrategy<KEY, VALUE>() {

    private val mergeNodeTemplate: String = """
                |${IngestionUtils.UNWIND}
                |MERGE (n${getLabelsAsString(nodePatternConfiguration.labels)}{${
                    getNodeMergeKeys("keys", nodePatternConfiguration.keys)
                }})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin()

    private val deleteNodeTemplate: String = """
                |${IngestionUtils.UNWIND}
                |MATCH (n${getLabelsAsString(nodePatternConfiguration.labels)}{${
                    getNodeMergeKeys("keys", nodePatternConfiguration.keys)
                }})
                |DETACH DELETE n
            """.trimMargin()

    override fun mergeNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent {
        return events
            // .mapNotNull { if (it.value != null) JSONUtils.asMap(it.value) else null }
            .mapNotNull { it.value as? Map<String, Any> }
            .mapNotNull { toData(nodePatternConfiguration, it) }
            .let {
                val list = if (it.isEmpty()) {
                    emptyList()
                } else {
                    listOf(Event(mergeNodeTemplate, it))
                }
                IngestionEvent(list)
            }
    }

    override fun deleteNodeEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent {
        return events
            .filter { it.value == null && it.key != null }
            .mapNotNull { it.key as? Map<String, Any> }
            //.mapNotNull { if (it.key != null) JSONUtils.asMap(it.key) else null }
            .mapNotNull { toData(nodePatternConfiguration, it, false) }
            .let {
                val list = if (it.isEmpty()) {
                    emptyList()
                } else {
                    listOf(Event(deleteNodeTemplate, it))
                }
                IngestionEvent(list)
            }
    }

    override fun mergeRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = IngestionEvent(emptyList())

    override fun deleteRelationshipEvents(events: Collection<Entity<KEY, VALUE>>): IngestionEvent = IngestionEvent(emptyList())

    companion object {
        fun toData(nodePatternConfiguration: NodePatternConfiguration, props: Map<String, Any?>, withProperties: Boolean = true): Map<String, Map<String, Any?>>? {
            val properties = props.flatten()
            val containsKeys = nodePatternConfiguration.keys.all { properties.containsKey(it) }
            return if (containsKeys) {
                if (withProperties) {
                    val filteredProperties = when (nodePatternConfiguration.type) {
                        PatternConfigurationType.ALL -> properties.filterKeys { !nodePatternConfiguration.keys.contains(it) }
                        PatternConfigurationType.EXCLUDE -> properties.filterKeys { key ->
                            val containsProp = containsProp(key, nodePatternConfiguration.properties)
                            !nodePatternConfiguration.keys.contains(key) && !containsProp
                        }
                        PatternConfigurationType.INCLUDE -> properties.filterKeys { key ->
                            val containsProp = containsProp(key, nodePatternConfiguration.properties)
                            !nodePatternConfiguration.keys.contains(key) && containsProp
                        }
                    }
                    mapOf("keys" to properties.filterKeys { nodePatternConfiguration.keys.contains(it) },
                            "properties" to filteredProperties)
                } else {
                    mapOf("keys" to properties.filterKeys { nodePatternConfiguration.keys.contains(it) })
                }
            } else {
                null
            }
        }
    }

}