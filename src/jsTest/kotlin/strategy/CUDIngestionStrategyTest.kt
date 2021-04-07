package strategy
import kotlin.test.Test
import kotlin.test.assertEquals

class CUDIngestionStrategyTest {

    @Test
    fun shouldCreateMergeAndUpdateNodes() {
        val strategy = CUDIngestionStrategy()
        val list = js("[{key: null, value: {\n" +
                "        op: \"merge\",\n" +
                "        properties: {\n" +
                "          foo: \"value\",\n" +
                "          key: 1\n" +
                "        },\n" +
                "        ids: {key: 1, otherKey:  \"foo\"},\n" +
                "        labels: [\"Foo\",\"Bar\"],\n" +
                "        type: \"node\",\n" +
                "        detach: false\n" +
                "      }}]")
        val expected = """{"events":[{"query":"UNWIND ${'$'}events AS event\nMERGE (n:Foo:Bar {key: event.ids.key, otherKey: event.ids.otherKey})\nSET n += event.properties","events":[{"ids":{"key":1,"otherKey":"foo"},"properties":{"foo":"value","key":1}}]}],"invalidEvents":[]}""";
        val result = strategy.events(list)
        val actual = js("JSON.stringify(result)").toString()
        assertEquals(expected, actual)
    }

}