package hackee12.bqhelper.avro

import com.google.api.services.bigquery.model.TableFieldSchema
import org.apache.avro.Schema

class NaiveSchemaAdapter : SchemaAdapter {

    override fun asTableFields(genericName: String, genericSchema: Schema): List<TableFieldSchema> {
        return when (val genericType = genericSchema.type) {
            Schema.Type.UNION -> asTableFields(
                genericName,
                genericSchema.types.filterNot { t -> t.type == Schema.Type.NULL }.first()
            )
            Schema.Type.RECORD -> listOf(nullable(record(genericName, genericSchema)))
            Schema.Type.ARRAY -> listOf(array(genericName, genericSchema))
            Schema.Type.MAP -> listOf(repeated(keyValue(genericName, genericSchema)))
            Schema.Type.STRING -> listOf(nullable(field(genericName).setType("STRING")))
            Schema.Type.INT,
            Schema.Type.LONG -> listOf(nullable(field(genericName).setType("INTEGER")))
            Schema.Type.FLOAT,
            Schema.Type.DOUBLE -> listOf(nullable(field(genericName).setType("FLOAT")))
            Schema.Type.BOOLEAN -> listOf(nullable(field(genericName).setType("BOOLEAN")))
            Schema.Type.BYTES -> listOf(nullable(field(genericName).setType("BYTES")))
            Schema.Type.FIXED,
            Schema.Type.ENUM -> listOf(failNotImplemented(genericType))
            Schema.Type.NULL -> listOf(failDecline(genericType))
        }
    }

    private fun record(recordName: String, recordSchema: Schema): TableFieldSchema {
        val recordFields: List<TableFieldSchema> = recordSchema.fields
            .map { f -> asTableFields(f.name(), f.schema()) }
            .flatten()
        return field(recordName).setType("RECORD").setFields(recordFields)
    }

    private fun array(arrayName: String, arraySchema: Schema): TableFieldSchema {
        return when (val arrayType = arraySchema.elementType.type) {
            Schema.Type.RECORD -> repeated(record(arrayName, arraySchema))
            Schema.Type.STRING -> repeated(field(arrayName).setType("STRING"))
            Schema.Type.INT,
            Schema.Type.LONG -> repeated(field(arrayName).setType("INTEGER"))
            Schema.Type.FLOAT,
            Schema.Type.DOUBLE -> repeated(field(arrayName).setType("FLOAT"))
            Schema.Type.BOOLEAN,
            Schema.Type.ARRAY,
            Schema.Type.MAP,
            Schema.Type.ENUM,
            Schema.Type.FIXED,
            Schema.Type.BYTES -> failNotImplemented(arrayType)
            Schema.Type.UNION,
            Schema.Type.NULL -> failDecline(arrayType)
        }
    }

    private fun keyValue(keyValueName: String, keyValueSchema: Schema): TableFieldSchema {
        val keyValue = field(keyValueName)
        val key = field("key").setType("STRING")
        val value = field("value")
        keyValue.fields = listOf(key, value)
        when (val valueType = keyValueSchema.valueType.type) {
            Schema.Type.BOOLEAN -> value.type = "BOOLEAN"
            Schema.Type.STRING -> value.type = "STRING"
            Schema.Type.INT,
            Schema.Type.LONG -> value.type = "INTEGER"
            Schema.Type.FLOAT,
            Schema.Type.DOUBLE -> value.type = "FLOAT"
            Schema.Type.RECORD,
            Schema.Type.ARRAY,
            Schema.Type.MAP,
            Schema.Type.FIXED,
            Schema.Type.BYTES,
            Schema.Type.ENUM -> failNotImplemented(valueType)
            Schema.Type.UNION,
            Schema.Type.NULL -> failDecline(valueType)
        }
        return keyValue
    }

    private fun repeated(tableFieldSchema: TableFieldSchema) = tableFieldSchema.setMode("REPEATED")

    private fun nullable(tableFieldSchema: TableFieldSchema) = tableFieldSchema.setMode("NULLABLE")

    private fun field(tableFieldName: String) = TableFieldSchema().setName(tableFieldName)

    private fun failNotImplemented(type: Schema.Type): TableFieldSchema {
        throw NotImplementedError("#FIXME: implement handler for avro type '%s'.".format(type))
    }

    private fun failDecline(type: Schema.Type): TableFieldSchema {
        throw IllegalArgumentException("#FIXME: I won't process '%s' type.".format(type))
    }

    private fun failUnknown(type: Schema.Type): TableFieldSchema {
        throw RuntimeException("#FIXME: I don't know avro type '%s'.".format(type))
    }
}