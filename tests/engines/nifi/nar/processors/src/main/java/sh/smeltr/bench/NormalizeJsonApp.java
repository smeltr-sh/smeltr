package sh.smeltr.bench;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

/** tests/reference/json_app.rs's mapping to ECS, a record at a time. */
@Tags({"json_app", "ecs"})
@CapabilityDescription("Maps the json_app application's records to ECS and drops its debug records.")
public class NormalizeJsonApp extends AbstractProcessor {
    static final PropertyDescriptor READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();
    static final Relationship SUCCESS = new Relationship.Builder().name("success").build();
    static final Relationship FAILURE = new Relationship.Builder().name("failure").build();

    private static final DataType STR = RecordFieldType.STRING.getDataType();
    private static final DataType LONG = RecordFieldType.LONG.getDataType();
    private static final RecordSchema EVENT = schema("duration", LONG);
    private static final RecordSchema LOG = schema("level", STR, "logger", STR);
    private static final RecordSchema LABELS = schema("attempt", LONG, "tier", STR, "order_id", STR);
    private static final RecordSchema USER = schema("id", STR);
    private static final RecordSchema ERROR = schema("type", STR, "message", STR, "stack_trace", STR);
    // Every record written shares this schema, so the writer writes each record's own fields; a field a record
    // lacks is null, and the writer leaves it out.
    private static final RecordSchema ECS = schema(
            "@timestamp", STR,
            "event", record(EVENT),
            "log", record(LOG),
            "message", STR,
            "labels", record(LABELS),
            "user", record(USER),
            "error", record(ERROR));

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(SUCCESS, FAILURE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(READER, WRITER);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile in = session.get();
        if (in == null) {
            return;
        }
        RecordReaderFactory readers = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
        RecordSetWriterFactory writers = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
        FlowFile out = session.create(in);
        try (InputStream is = session.read(in);
                RecordReader reader = readers.createRecordReader(in, is, getLogger());
                OutputStream os = session.write(out);
                RecordSetWriter writer = writers.createWriter(getLogger(), ECS, os, out)) {
            writer.beginRecordSet();
            Record record;
            while ((record = reader.nextRecord()) != null) {
                if (!"debug".equals(record.getValue("level"))) {
                    writer.write(ecs(record));
                }
            }
            writer.finishRecordSet();
        } catch (Exception e) {
            getLogger().error("Could not map {}", in, e);
            session.remove(out);
            session.transfer(in, FAILURE);
            return;
        }
        session.transfer(out, SUCCESS);
        session.remove(in);
    }

    private static Record ecs(Record r) {
        Record user = (Record) r.getValue("user");
        Record error = (Record) r.getValue("error");
        Map<String, Object> ecs = new HashMap<>();
        ecs.put("@timestamp", Instant.parse(r.getAsString("ts")).toString());
        ecs.put("event", new MapRecord(EVENT, Map.of("duration",
                Math.round(((Number) r.getValue("latency_ms")).doubleValue() * 1_000_000))));
        ecs.put("log", new MapRecord(LOG, values("level", r.getValue("level"), "logger", r.getValue("logger"))));
        ecs.put("message", r.getValue("msg"));
        ecs.put("labels", new MapRecord(LABELS, values("attempt", r.getValue("attempt"),
                "tier", user.getValue("tier"), "order_id", r.getValue("order_id"))));
        ecs.put("user", new MapRecord(USER, values("id", user.getValue("id"))));
        if (error != null) {
            ecs.put("error", new MapRecord(ERROR, values("type", error.getValue("type"),
                    "message", error.getValue("message"), "stack_trace", error.getValue("stack"))));
        }
        return new MapRecord(ECS, ecs);
    }

    /** A map of name, value pairs that may hold nulls, as a record's values may. */
    private static Map<String, Object> values(Object... pairs) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], pairs[i + 1]);
        }
        return map;
    }

    private static RecordSchema schema(Object... pairs) {
        RecordField[] fields = new RecordField[pairs.length / 2];
        for (int i = 0; i < pairs.length; i += 2) {
            fields[i / 2] = new RecordField((String) pairs[i], (DataType) pairs[i + 1]);
        }
        return new SimpleRecordSchema(List.of(fields));
    }

    private static DataType record(RecordSchema schema) {
        return RecordFieldType.RECORD.getRecordDataType(schema);
    }
}
