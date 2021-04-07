package org.neo4j.graph_integration.strategy.pattern

import streams.service.sink.strategy.NodePatternConfiguration
import streams.service.sink.strategy.PatternConfigurationType
import streams.service.sink.strategy.RelationshipPatternConfiguration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

class NodePatternConfigurationTest {

    @Test
    fun shouldExtractAllParams() {
        // given
        val pattern = "(:LabelA:LabelB{!id,*})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.ALL,
                labels = listOf("LabelA", "LabelB"), properties = emptyList())
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllFixedParams() {
        // given
        val pattern = "(:LabelA{!id,foo,bar})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractComplexParams() {
        // given
        val pattern = "(:LabelA{!id,foo.bar})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo.bar"))
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractCompositeKeysWithFixedParams() {
        // given
        val pattern = "(:LabelA{!idA,!idB,foo,bar})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("idA", "idB"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllExcludedParams() {
        // given
        val pattern = "(:LabelA{!id,-foo,-bar})"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.EXCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseOfMixedConfiguration() {
        // given
        val pattern = "(:LabelA{!id,-foo,bar})"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern is not homogeneous", e.message)
        }
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseOfInvalidPattern() {
        // given
        val pattern = "(LabelA{!id,-foo,bar})"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern is invalid", e.message)
            
        }
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseThePatternShouldContainsAKey() {
        // given
        val pattern = "(:LabelA{id,-foo,bar})"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern must contains at lest one key", e.message)
            
        }
    }

    @Test
    fun shouldExtractAllParamsSimple() {
        // given
        val pattern = "LabelA:LabelB{!id,*}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.ALL,
                labels = listOf("LabelA", "LabelB"), properties = emptyList())
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllFixedParamsSimple() {
        // given
        val pattern = "LabelA{!id,foo,bar}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractComplexParamsSimple() {
        // given
        val pattern = "LabelA{!id,foo.bar}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo.bar"))
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractCompositeKeysWithFixedParamsSimple() {
        // given
        val pattern = "LabelA{!idA,!idB,foo,bar}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("idA", "idB"), type = PatternConfigurationType.INCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllExcludedParamsSimple() {
        // given
        val pattern = "LabelA{!id,-foo,-bar}"

        // when
        val result = NodePatternConfiguration.parse(pattern)

        // then
        val expected = NodePatternConfiguration(keys = setOf("id"), type = PatternConfigurationType.EXCLUDE,
                labels = listOf("LabelA"), properties = listOf("foo", "bar"))
        assertEquals(expected, result)
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseOfMixedConfigurationSimple() {
        // given
        val pattern = "LabelA{!id,-foo,bar}"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern is not homogeneous", e.message)
            
        }
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseThePatternShouldContainsAKeySimple() {
        // given
        val pattern = "LabelA{id,-foo,bar}"

        try {
            // when
            NodePatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Node pattern $pattern must contains at lest one key", e.message)
            
        }
    }
}

class RelationshipPatternConfigurationTest {

    @Test
    fun shouldExtractAllParams() {
        // given
        val startPattern = "LabelA{!id,aa}"
        val endPattern = "LabelB{!idB,bb}"
        val pattern = "(:$startPattern)-[:REL_TYPE]->(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = NodePatternConfiguration.parse(startPattern)
        val end = NodePatternConfiguration.parse(endPattern)
        val properties = emptyList<String>()
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.ALL
        )
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllParamsWithReverseSourceAndTarget() {
        // given
        val startPattern = "LabelA{!id,aa}"
        val endPattern = "LabelB{!idB,bb}"
        val pattern = "(:$startPattern)<-[:REL_TYPE]-(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = NodePatternConfiguration.parse(startPattern)
        val end = NodePatternConfiguration.parse(endPattern)
        val properties = emptyList<String>()
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = end, end = start, relType = relType,
                properties = properties, type = PatternConfigurationType.ALL
        )
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllFixedParams() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "(:$startPattern)-[:REL_TYPE{foo, BAR}]->(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo", "BAR")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.INCLUDE
        )
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractComplexParams() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "(:$startPattern)-[:REL_TYPE{foo.BAR, BAR.foo}]->(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo.BAR", "BAR.foo")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.INCLUDE
        )
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllExcludedParams() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "(:$startPattern)-[:REL_TYPE{-foo, -BAR}]->(:$endPattern)"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo", "BAR")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.EXCLUDE
        )
        assertEquals(expected, result)
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseOfMixedConfiguration() {
        // given
        val pattern = "(:LabelA{!id})-[:REL_TYPE{foo, -BAR}]->(:LabelB{!idB})"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is not homogeneous", e.message)
            
        }
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseThePatternShouldContainsNodesWithOnlyIds() {
        // given
        val pattern = "(:LabelA{id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is invalid", e.message)
            
        }
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseThePatternIsInvalid() {
        // given
        val pattern = "(LabelA{!id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is invalid", e.message)
            
        }
    }

    @Test
    fun shouldExtractAllParamsSimple() {
        // given
        val startPattern = "LabelA{!id,aa}"
        val endPattern = "LabelB{!idB,bb}"
        val pattern = "$startPattern REL_TYPE $endPattern"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = NodePatternConfiguration.parse(startPattern)
        val end = NodePatternConfiguration.parse(endPattern)
        val properties = emptyList<String>()
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.ALL
        )
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllFixedParamsSimple() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "$startPattern REL_TYPE{foo, BAR} $endPattern"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo", "BAR")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.INCLUDE
        )
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractComplexParamsSimple() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "$startPattern REL_TYPE{foo.BAR, BAR.foo} $endPattern"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo.BAR", "BAR.foo")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.INCLUDE
        )
        assertEquals(expected, result)
    }

    @Test
    fun shouldExtractAllExcludedParamsSimple() {
        // given
        val startPattern = "LabelA{!id}"
        val endPattern = "LabelB{!idB}"
        val pattern = "$startPattern REL_TYPE{-foo, -BAR} $endPattern"

        // when
        val result = RelationshipPatternConfiguration.parse(pattern)

        // then
        val start = RelationshipPatternConfiguration.getNodeConf(startPattern)
        val end = RelationshipPatternConfiguration.getNodeConf(endPattern)
        val properties = listOf("foo", "BAR")
        val relType = "REL_TYPE"
        val expected = RelationshipPatternConfiguration(start = start, end = end, relType = relType,
                properties = properties, type = PatternConfigurationType.EXCLUDE
        )
        assertEquals(expected, result)
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseOfMixedConfigurationSimple() {
        // given
        val pattern = "LabelA{!id} REL_TYPE{foo, -BAR} LabelB{!idB}"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is not homogeneous", e.message)
            
        }
    }

    @Test//(expected = IllegalArgumentException::class)
    fun shouldThrowAnExceptionBecauseThePatternShouldContainsNodesWithOnlyIdsSimple() {
        // given
        val pattern = "LabelA{id} REL_TYPE{foo,BAR} LabelB{!idB}"

        try {
            // when
            RelationshipPatternConfiguration.parse(pattern)
            fail("should throw IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // then
            assertEquals("The Relationship pattern $pattern is invalid", e.message)
            
        }
    }
}