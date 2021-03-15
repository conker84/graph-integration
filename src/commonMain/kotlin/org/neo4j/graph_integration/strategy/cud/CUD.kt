package org.neo4j.graph_integration.strategy.cud

enum class CUDOperations { create, merge, update, delete, match }

enum class EntityType { node, relationship }

abstract class CUD {
    abstract val op: CUDOperations
    abstract val type: EntityType
    abstract val properties: Map<String, Any?>
}

data class CUDNode(override val op: CUDOperations,
                   override val properties: Map<String, Any?> = emptyMap(),
                   val ids: Map<String, Any?> = emptyMap(),
                   val detach: Boolean = true,
                   val labels: List<String> = emptyList()): CUD() {
    override val type = EntityType.node

    fun toMap(): Map<String, Any> {
        return when (op) {
            CUDOperations.delete -> mapOf("ids" to ids)
            else -> mapOf("ids" to ids, "properties" to properties)
        }
    }

    companion object {
        fun from(map: Map<String, Any?>): CUDNode = CUDNode(
            op = CUDOperations.valueOf(map["op"]!!.toString()),
            properties = map["properties"] as? Map<String, Any?> ?: emptyMap(),
            ids = map["ids"] as? Map<String, Any?> ?: emptyMap(),
            detach = map["detach"]?.toString().toBoolean(),
            labels = map["labels"] as? List<String> ?: emptyList()
        )
    }
}

data class CUDNodeRel(val ids: Map<String, Any?> = emptyMap(),
                      val labels: List<String>,
                      val op: CUDOperations = CUDOperations.match
) {
    companion object {
        fun from(map: Map<String, Any?>) = CUDNodeRel(
            ids = map["ids"] as? Map<String, Any?> ?: emptyMap(),
            labels = map["labels"] as? List<String> ?: emptyList(),
            op = map["op"]?.let { CUDOperations.valueOf(it as String) } ?: CUDOperations.match
        )
    }
}
data class CUDRelationship(override val op: CUDOperations,
                           override val properties: Map<String, Any?> = emptyMap(),
                           val rel_type: String,
                           val from: CUDNodeRel,
                           val to: CUDNodeRel
): CUD() {
    override val type = EntityType.relationship

    fun toMap(): Map<String, Any> {
        val from = mapOf("ids" to from.ids)
        val to = mapOf("ids" to to.ids)
        return when (op) {
            CUDOperations.delete -> mapOf(CUDIngestionStrategy.FROM_KEY to from, CUDIngestionStrategy.TO_KEY to to)
            else -> mapOf(CUDIngestionStrategy.FROM_KEY to from, CUDIngestionStrategy.TO_KEY to to, "properties" to properties)
        }
    }

    companion object {
        fun from(map: Map<String, Any?>) = CUDRelationship(
            op = CUDOperations.valueOf(map["op"]!!.toString()),
            properties = map["properties"] as? Map<String, Any?> ?: emptyMap(),
            rel_type = map["rel_type"]!!.toString(),
            from = CUDNodeRel.from(map[CUDIngestionStrategy.FROM_KEY] as Map<String, Any?>),
            to = CUDNodeRel.from(map[CUDIngestionStrategy.TO_KEY] as Map<String, Any?>)
        )
    }
}
