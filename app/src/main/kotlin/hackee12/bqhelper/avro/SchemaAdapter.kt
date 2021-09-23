package hackee12.bqhelper.avro

import com.google.api.services.bigquery.model.TableFieldSchema
import org.apache.avro.Schema

interface SchemaAdapter {
    fun asTableFields(genericName: String, genericSchema: Schema): List<TableFieldSchema>
}