package sh.smeltr.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.db.Reader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * tests/reference/suricata.rs's mapping to ECS, and its enrichment, an event a line. A flowfile is lines of EVE JSON
 * and leaves as lines of ECS JSON. What Suricata sent is kept whole under {@code suricata.eve}, objects of whatever
 * shape each event type gives them, so the events are read as JSON trees rather than as records of one schema.
 */
@Tags({"suricata", "ecs", "geoip"})
@CapabilityDescription("Maps Suricata's EVE events to ECS, enriched from a GeoIP database, the company's networks and its"
        + " domain lists.")
public class NormalizeSuricata extends AbstractProcessor {
    static final PropertyDescriptor GEOIP = file("GeoIP Database");
    static final PropertyDescriptor NETWORKS = file("Networks Database");
    static final PropertyDescriptor DOMAINS = file("Domains");
    static final PropertyDescriptor PATTERNS = file("Domain Patterns");
    static final Relationship SUCCESS = new Relationship.Builder().name("success").build();
    static final Relationship FAILURE = new Relationship.Builder().name("failure").build();

    /** What Suricata sends, each event type's object, moved whole under {@code suricata.eve}. */
    private static final Set<String> EVE_OBJECTS = Set.of("alert", "flow", "tcp", "dns", "http", "tls", "fileinfo",
            "smtp", "email", "vlan", "tx_id", "direction", "payload", "payload_printable", "stream", "host");
    /** Suricata's instant: microseconds, and an offset written {@code +0000}. */
    private static final DateTimeFormatter EVE_TIME = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .appendOffset("+HHMM", "Z")
            .toFormatter();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    private volatile Reader geoip;
    private volatile Reader networks;
    /** Each listed domain's category and owner. */
    private volatile Map<String, String[]> domains;
    /** The patterns in their file's order, each with its verdict and reason: the first that matches wins. */
    private volatile List<Verdict> patterns;

    private record Verdict(Pattern pattern, String verdict, String reason) {}

    private static PropertyDescriptor file(String name) {
        return new PropertyDescriptor.Builder().name(name).required(true).addValidator(Validator.VALID).build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(SUCCESS, FAILURE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(GEOIP, NETWORKS, DOMAINS, PATTERNS);
    }

    @OnScheduled
    public void open(ProcessContext context) throws IOException {
        geoip = new Reader(new File(context.getProperty(GEOIP).getValue()));
        networks = new Reader(new File(context.getProperty(NETWORKS).getValue()));
        Map<String, String[]> listed = new HashMap<>();
        for (CSVRecord row : csv(context.getProperty(DOMAINS).getValue())) {
            listed.put(row.get("domain"), new String[] {row.get("category"), row.get("owner")});
        }
        domains = listed;
        List<Verdict> verdicts = new ArrayList<>();
        for (CSVRecord row : csv(context.getProperty(PATTERNS).getValue())) {
            verdicts.add(new Verdict(Pattern.compile(row.get("pattern")), row.get("verdict"), row.get("reason")));
        }
        patterns = verdicts;
    }

    @OnStopped
    public void close() throws IOException {
        geoip.close();
        networks.close();
    }

    private static List<CSVRecord> csv(String path) throws IOException {
        try (var in = Files.newBufferedReader(Path.of(path), StandardCharsets.UTF_8)) {
            return CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(in).getRecords();
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile in = session.get();
        if (in == null) {
            return;
        }
        FlowFile out = session.create(in);
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(session.read(in), StandardCharsets.UTF_8));
                Writer writer = new BufferedWriter(new OutputStreamWriter(session.write(out), StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (!line.isEmpty()) {
                    writer.write(JSON.writeValueAsString(ecs((ObjectNode) JSON.readTree(line))));
                    writer.write('\n');
                }
            }
        } catch (Exception e) {
            getLogger().error("Could not map {}", in, e);
            session.remove(out);
            session.transfer(in, FAILURE);
            return;
        }
        session.transfer(session.putAttribute(out, CoreAttributes.MIME_TYPE.key(), "application/json"), SUCCESS);
        session.remove(in);
    }

    private ObjectNode ecs(ObjectNode e) throws IOException {
        ObjectNode doc = NODES.objectNode();
        String kind = e.path("event_type").asText();
        boolean alert = kind.equals("alert");
        set(doc, "@timestamp", instant(get(e, "timestamp")));
        set(doc, "event.module", NODES.textNode("suricata"));
        set(doc, "event.dataset", NODES.textNode("suricata.eve"));
        set(doc, "event.kind", NODES.textNode(alert ? "alert" : "event"));
        set(doc, "event.category", NODES.textNode(alert ? "intrusion_detection" : "network"));
        set(doc, "source.ip", get(e, "src_ip"));
        set(doc, "source.port", get(e, "src_port"));
        set(doc, "destination.ip", get(e, "dest_ip"));
        set(doc, "destination.port", get(e, "dest_port"));
        String proto = text(e, "proto");
        set(doc, "network.transport", proto == null ? null : NODES.textNode(proto.toLowerCase()));
        if (!"failed".equals(text(e, "app_proto"))) {
            set(doc, "network.protocol", get(e, "app_proto"));
        }
        set(doc, "network.community_id", get(e, "community_id"));
        set(doc, "observer.ingress.interface.name", get(e, "in_iface"));
        set(doc, "observer.hostname", get(e, "host"));
        // The name the far end goes by, whichever protocol said it.
        JsonNode domain = get(e, "http.hostname");
        if (domain == null) {
            domain = get(e, "tls.sni");
        }
        if (domain == null) {
            domain = get(e, "dns.rrname");
        }
        set(doc, "destination.domain", domain);

        if (alert || kind.equals("flow")) {
            set(doc, "source.packets", get(e, "flow.pkts_toserver"));
            set(doc, "source.bytes", get(e, "flow.bytes_toserver"));
            set(doc, "destination.packets", get(e, "flow.pkts_toclient"));
            set(doc, "destination.bytes", get(e, "flow.bytes_toclient"));
            set(doc, "network.packets", sum(get(e, "flow.pkts_toserver"), get(e, "flow.pkts_toclient")));
            set(doc, "network.bytes", sum(get(e, "flow.bytes_toserver"), get(e, "flow.bytes_toclient")));
        }
        if (kind.equals("http") || kind.equals("fileinfo")) {
            set(doc, "url.domain", get(e, "http.hostname"));
            set(doc, "url.original", get(e, "http.url"));
            String url = text(e, "http.url");
            if (url != null) {
                int query = url.indexOf('?');
                set(doc, "url.path", NODES.textNode(query < 0 ? url : url.substring(0, query)));
            }
            set(doc, "http.request.method", get(e, "http.http_method"));
            set(doc, "http.response.status_code", get(e, "http.status"));
            set(doc, "http.response.body.bytes", get(e, "http.length"));
            set(doc, "user_agent.original", get(e, "http.http_user_agent"));
        }
        switch (kind) {
            case "alert" -> {
                set(doc, "rule.id", get(e, "alert.signature_id"));
                set(doc, "rule.name", get(e, "alert.signature"));
                set(doc, "rule.category", get(e, "alert.category"));
                set(doc, "event.severity", get(e, "alert.severity"));
                set(doc, "event.action", get(e, "alert.action"));
                String action = text(e, "alert.action");
                if (action != null) {
                    set(doc, "event.outcome", NODES.textNode(action.equals("blocked") ? "failure" : "success"));
                }
            }
            case "flow" -> {
                set(doc, "event.start", instant(get(e, "flow.start")));
                set(doc, "event.end", instant(get(e, "flow.end")));
            }
            case "dns" -> {
                set(doc, "dns.type", get(e, "dns.type"));
                set(doc, "dns.id", get(e, "dns.id"));
                set(doc, "dns.question.name", get(e, "dns.rrname"));
                set(doc, "dns.question.type", get(e, "dns.rrtype"));
                set(doc, "dns.response_code", get(e, "dns.rcode"));
            }
            case "tls" -> {
                set(doc, "tls.client.server_name", get(e, "tls.sni"));
                set(doc, "tls.version_protocol", NODES.textNode("tls"));
                String version = text(e, "tls.version");
                if (version != null) {
                    set(doc, "tls.version", NODES.textNode(version.split(" ")[1]));
                }
                set(doc, "tls.server.subject", get(e, "tls.subject"));
                set(doc, "tls.server.issuer", get(e, "tls.issuerdn"));
                set(doc, "tls.client.ja3", get(e, "tls.ja3.hash"));
            }
            case "fileinfo" -> {
                String name = text(e, "fileinfo.filename");
                if (name != null) {
                    set(doc, "file.name", NODES.textNode(name.split("/")[2]));
                }
                set(doc, "file.size", get(e, "fileinfo.size"));
                set(doc, "file.hash.md5", get(e, "fileinfo.md5"));
                set(doc, "file.hash.sha1", get(e, "fileinfo.sha1"));
                set(doc, "file.hash.sha256", get(e, "fileinfo.sha256"));
            }
            case "smtp" -> {
                // SMTP's `<address>`, without the brackets.
                String from = text(e, "smtp.mail_from");
                if (from != null) {
                    set(doc, "email.from.address", NODES.textNode(
                            from.substring(from.offsetByCodePoints(0, 1), from.offsetByCodePoints(from.length(), -1))));
                }
                set(doc, "email.subject", get(e, "email.subject"));
                set(doc, "source.domain", get(e, "smtp.helo"));
            }
            default -> {}
        }

        // Both ends: GeoIP, and the company's network.
        for (String side : new String[] {"source", "destination"}) {
            JsonNode ip = doc.path(side).path("ip");
            if (!ip.isTextual()) {
                continue;
            }
            // An address literal, which names no host to look up.
            InetAddress address = InetAddress.getByName(ip.asText());
            Map<?, ?> city = geoip.get(address, Map.class);
            if (city != null) {
                set(doc, side + ".geo.country_iso_code", value(city, "country", "iso_code"));
                set(doc, side + ".geo.city_name", value(city, "city", "names", "en"));
                set(doc, side + ".geo.location.lat", value(city, "location", "latitude"));
                set(doc, side + ".geo.location.lon", value(city, "location", "longitude"));
            }
            Map<?, ?> network = networks.get(address, Map.class);
            if (network != null) {
                set(doc, side + ".site", value(network, "site"));
                set(doc, side + ".network_tier", value(network, "tier"));
                set(doc, side + ".business_unit", value(network, "business_unit"));
            }
        }
        if (domain != null && domain.isTextual()) {
            String name = domain.asText();
            String[] listed = domains.get(name);
            if (listed != null) {
                set(doc, "destination.domain_category", NODES.textNode(listed[0]));
                set(doc, "destination.domain_owner", NODES.textNode(listed[1]));
            }
            for (Verdict v : patterns) {
                if (v.pattern().matcher(name).find()) {
                    set(doc, "destination.domain_verdict", NODES.textNode(v.verdict()));
                    set(doc, "destination.domain_reason", NODES.textNode(v.reason()));
                    break;
                }
            }
        }

        // What Suricata sent, under `suricata.eve`; what the mapping reads from the top is left out.
        ObjectNode eve = NODES.objectNode();
        e.properties().forEach(field -> {
            if (EVE_OBJECTS.contains(field.getKey())) {
                eve.set(field.getKey(), field.getValue());
            }
        });
        eve.set("event_type", e.get("event_type"));
        if (e.hasNonNull("flow_id")) {
            eve.set("flow_id", e.get("flow_id"));
        }
        set(doc, "suricata.eve", eve);
        return doc;
    }

    /** The field at dotted {@code path} of {@code e}, or null where it is missing or null. */
    private static JsonNode get(JsonNode e, String path) {
        JsonNode at = e.at("/" + path.replace('.', '/'));
        return at.isMissingNode() || at.isNull() ? null : at;
    }

    private static String text(JsonNode e, String path) {
        JsonNode at = get(e, path);
        return at != null && at.isTextual() ? at.asText() : null;
    }

    /** Sets the field at dotted {@code path} of {@code doc}, making the objects on the way; null sets nothing. */
    private static void set(ObjectNode doc, String path, JsonNode value) {
        if (value == null) {
            return;
        }
        String[] parts = path.split("\\.");
        ObjectNode at = doc;
        for (int i = 0; i < parts.length - 1; i++) {
            at = at.has(parts[i]) ? (ObjectNode) at.get(parts[i]) : at.putObject(parts[i]);
        }
        at.set(parts[parts.length - 1], value);
    }

    private static JsonNode sum(JsonNode a, JsonNode b) {
        return a != null && b != null && a.isIntegralNumber() && b.isIntegralNumber()
                ? NODES.numberNode(a.asLong() + b.asLong())
                : null;
    }

    private static JsonNode instant(JsonNode at) {
        return at == null ? null : NODES.textNode(OffsetDateTime.parse(at.asText(), EVE_TIME).toInstant().toString());
    }

    /** The value at {@code path} in a MaxMind record, as JSON. */
    private static JsonNode value(Map<?, ?> record, String... path) {
        Object at = record;
        for (String key : path) {
            if (!(at instanceof Map<?, ?> map)) {
                return null;
            }
            at = map.get(key);
        }
        return at == null ? null : JSON.valueToTree(at);
    }
}
