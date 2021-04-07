package strategy
import kotlin.test.Test
import kotlin.test.assertEquals

class CypherTemplateIngestionStrategyTest {

    @Test
    fun shouldReturnEvents() {
        val strategy = CypherTemplateIngestionStrategy("MERGE (n:Node{id: event.keys.id}) SET n += event.properties")
        val list = js("[{value: {keys: {id: 1}, properties: {foo: \"foo1\"}}}]")
        val expected = """{"events":[{"query":"UNWIND ${'$'}events AS event MERGE (n:Node{id: event.keys.id}) SET n += event.properties","events":[{"keys":{"id":1},"properties":{"foo":"foo1"}}]}],"invalidEvents":[]}""";
        val result = strategy.events(list)
        val actual = js("JSON.stringify(result)").toString()
        assertEquals(expected, actual)
    }

}