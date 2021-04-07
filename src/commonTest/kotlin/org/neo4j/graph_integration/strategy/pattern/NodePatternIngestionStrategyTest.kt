package org.neo4j.graph_integration.strategy.pattern

import org.neo4j.graph_integration.Entity
import org.neo4j.graph_integration.utils.IngestionUtils
import streams.service.sink.strategy.NodePatternConfiguration
import kotlin.test.Test
import kotlin.test.assertEquals

class NodePatternIngestionStrategyTest {

    @Test
    fun `shouldGetAllProperties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})")
        val strategy = NodePatternIngestionStrategy<Any, Any>(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(Entity<Any, Any>(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                    "properties" to mapOf("foo" to "foo", "bar" to "bar", "foobar" to "foobar"))
                ),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun shouldGetNestedProperties() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo.bar})")
        val strategy = NodePatternIngestionStrategy<Any, Any>(config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"))

        // when
        val events = listOf(Entity<Any, Any>(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
            queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar"))),
            queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun shouldExcludeNestedProperties() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, -foo})")
        val strategy = NodePatternIngestionStrategy<Any, Any>(config)
        val map = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val events = listOf(Entity<Any, Any>(map, map))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("prop" to 100))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun shouldIncludeNestedProperties() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo})")
        val strategy = NodePatternIngestionStrategy<Any, Any>(config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val events = listOf(Entity<Any, Any>(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar", "foo.foobar" to "foobar"))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun shouldExcludeTheProperties() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,-foo,-bar})")
        val strategy = NodePatternIngestionStrategy<Any, Any>(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(Entity<Any, Any>(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foobar" to "foobar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun shouldIncludeTheProperties() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,foo,bar})")
        val strategy = NodePatternIngestionStrategy<Any, Any>(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(Entity<Any, Any>(data, data))
        val queryEvents = strategy.mergeNodeEvents(events).events

        // then
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

    @Test
    fun shouldDeleteTheNode() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})")
        val strategy = NodePatternIngestionStrategy<Any, Any>(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(Entity<Any, Any>(data, null))
        val queryEvents = strategy.deleteNodeEvents(events).events

        // then
        assertEquals("""
                |${IngestionUtils.UNWIND}
                |MATCH (n:LabelA:LabelB{id: event.keys.id})
                |DETACH DELETE n
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events).events)
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events).events)
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events).events)
    }

}