package hackee12.bqhelper.avro

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.api.services.bigquery.model.TableFieldSchema
import org.apache.avro.Schema
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

internal class NaiveSchemaAdapterTest {

    @Test
    fun test() {
        val avroSchema = Schema.Parser().parse(File("src/test/resources/RegistryRecord.avsc"))
        val expected: List<TableFieldSchema> = ObjectMapper().readValue(
            File("src/test/resources/registry.table.json").readText(),
            mutableListOf<TableFieldSchema>().javaClass
        )
        val actual: List<TableFieldSchema> = NaiveSchemaAdapter().asTableFields("_ignore_", avroSchema)[0].fields
        assertEquals(expected, actual)
    }
}