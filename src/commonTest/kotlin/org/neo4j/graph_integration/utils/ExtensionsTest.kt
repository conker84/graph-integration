package org.neo4j.graph_integration.utils

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ExtensionsTest {
    @Test
    fun testQuote() {
        assertEquals("foo", "foo".quote())
        assertEquals("\$foo", "\$foo".quote())
        assertEquals("`@foo`", "@foo".quote())
    }

    @Test
    fun testIsJavaIdentifierStart() {
        assertTrue { "foo".isJavaIdentifierStart() }
        assertTrue { "\$foo".isJavaIdentifierStart() }
        assertFalse { "@foo".isJavaIdentifierStart() }
    }
}