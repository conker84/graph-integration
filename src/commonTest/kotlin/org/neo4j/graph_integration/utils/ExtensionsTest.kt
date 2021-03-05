package org.neo4j.graph_integration.utils

import kotlin.test.Test
import kotlin.test.assertEquals

class ExtensionsTest {
    @Test
    fun testQuote() {
        assertEquals("foo", "foo".quote())
        assertEquals("\$foo", "\$foo".quote())
        assertEquals("`@foo`", "@foo".quote())
    }
}