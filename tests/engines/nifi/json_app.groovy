// tests/reference/json_app.rs's mapping to ECS. Every record returned shares one schema, built once, so the writer
// writes each record's fields; a field a record lacks is null and the writer leaves it out.
import groovy.transform.Field
import java.time.Instant
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType

@Field static final def STR = RecordFieldType.STRING.dataType
@Field static final def LONG = RecordFieldType.LONG.dataType
@Field static final def schemaOf = { Map<String, Object> fields ->
    new SimpleRecordSchema(fields.collect { name, type -> new RecordField(name, type) })
}
@Field static final def EVENT = schemaOf(duration: LONG)
@Field static final def LOG = schemaOf(level: STR, logger: STR)
@Field static final def LABELS = schemaOf(attempt: LONG, tier: STR, order_id: STR)
@Field static final def USER = schemaOf(id: STR)
@Field static final def ERROR = schemaOf(type: STR, message: STR, stack_trace: STR)
@Field static final def ECS = schemaOf(
    '@timestamp': STR,
    event: RecordFieldType.RECORD.getRecordDataType(EVENT),
    log: RecordFieldType.RECORD.getRecordDataType(LOG),
    message: STR,
    labels: RecordFieldType.RECORD.getRecordDataType(LABELS),
    user: RecordFieldType.RECORD.getRecordDataType(USER),
    error: RecordFieldType.RECORD.getRecordDataType(ERROR),
)

if (record.getValue('level') == 'debug') return null
def user = record.getValue('user')
def error = record.getValue('error')
return new MapRecord(ECS, [
    '@timestamp': Instant.parse(record.getValue('ts')).toString(),
    event: new MapRecord(EVENT, [duration: Math.round(((Number) record.getValue('latency_ms')).doubleValue() * 1000000)]),
    log: new MapRecord(LOG, [level: record.getValue('level'), logger: record.getValue('logger')]),
    message: record.getValue('msg'),
    labels: new MapRecord(LABELS, [attempt: record.getValue('attempt'), tier: user.getValue('tier'), order_id: record.getValue('order_id')]),
    user: new MapRecord(USER, [id: user.getValue('id')]),
    error: error == null ? null : new MapRecord(ERROR, [type: error.getValue('type'), message: error.getValue('message'), stack_trace: error.getValue('stack')]),
])
