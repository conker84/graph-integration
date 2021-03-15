package org.neo4j.graph_integration

data class Entity<KEY, VALUE>(val key: KEY? = null, val value: VALUE?)