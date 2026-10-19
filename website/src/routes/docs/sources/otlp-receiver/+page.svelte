<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'paths', title: 'Paths it serves' },
    { id: 'records', title: 'Records it gives' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'senders', title: 'Tokens and certificates' },
    { id: 'answers', title: 'What it answers' },
    { id: 'health', title: 'Health check' },
    { id: 'clients', title: 'Senders it takes' },
    { id: 'port', title: 'Open its port' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
  const curl = `curl -H "Authorization: Bearer $OTEL_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"demo"}}]},
       "scopeLogs":[{"logRecords":[{"severityText":"INFO","body":{"stringValue":"hello"}}]}]}]}' \\
  https://workers.example.com:4318/v1/logs`;
  const otel = `receivers:
  otlp:
    protocols:
      grpc:
      http:

exporters:
  otlphttp:
    endpoint: https://workers.example.com:4318
    headers:
      Authorization: Bearer \${env:OTEL_TOKEN}

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [otlphttp]
    traces:
      receivers: [otlp]
      exporters: [otlphttp]`;
  const sdk = `export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=https://workers.example.com:4318
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer%20$OTEL_TOKEN"`;
</script>

<GuideLayout
  title="OTLP receiver source"
  description="Take the logs, metrics and traces OpenTelemetry exporters send over OTLP, over HTTP or gRPC, to a port every worker listens on: an example, every setting, the records and fields it gives, what it answers, and which senders work."
  {sections}
>
  <h1>OTLP receiver</h1>
  <p class="lede">
    Takes one signal OpenTelemetry exporters send over OTLP to its port, logs, metrics or traces:
    over HTTP, as protobuf or JSON, and over gRPC. It answers each export once every destination has
    its records.
  </p>
  <p>
    Receivers of different signals may share one port. Each signal is records of its own fields,
    which its own pipelines read.
  </p>
  <p>
    OpenTelemetry SDKs, Collectors and other OTLP clients send to it. Every worker in the group
    listens on its port, and the sender, or a balancer in front of the workers, picks a worker for
    each export. Add workers to take more.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Takes OTLP logs on port 4318, over TLS with the
    <a href="/docs/secrets#identities">identity</a> <code>receiver-tls</code>. Senders present the
    token held in the worker-host variable <code>OTEL_TOKEN</code>.
  </p>
  <pre><code>{@html highlight(json(examples.otlpReceiverSource))}</code></pre>
  <ProductScreenshot
    source="otlp-receiver-source-form"
    height={2000}
    crop={{ x: 608, y: 552, width: 1664, height: 896 }}
    alt="The OTLP receiver form on the Sources page: id otel-logs, port 4318, signal logs, no tokens yet."
    caption="The same source in the Sources page's add dialog, before you add its token and identity."
  />
  <p>Takes traces on the same port, from the same senders:</p>
  <pre><code>{@html highlight(json(examples.otlpTracesSource))}</code></pre>
  <p>A sender posts an export of logs like this:</p>
  <pre><code>{curl}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Pick a port no other receiver in the group uses. The one port takes both HTTP and gRPC. Point
      each sender at the port you pick.
    </li>
    <li>
      Decide how senders prove who they are: a token, a client certificate, or both. See
      <a href="#senders">Tokens and certificates</a>.
    </li>
    <li>Put each token in a <a href="/docs/secrets">credential</a>.</li>
    <li>
      Open the port on every worker, and give senders one address that reaches them all. See
      <a href="#port">Open its port</a>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="otlpReceiver" />

  <h2 id="paths">Paths it serves</h2>
  <dl>
    <dt>
      <strong>POST</strong> <code>/v1/logs</code>, <code>/v1/metrics</code>, <code>/v1/traces</code>
    </dt>
    <dd>
      OTLP over HTTP: the body is an <code>Export*ServiceRequest</code> of its signal, protobuf with
      <code>Content-Type: application/x-protobuf</code>, or OTLP's JSON with
      <code>Content-Type: application/json</code>. The receiver answers in the encoding the export
      was sent in, and inflates a body sent with <code>Content-Encoding: gzip</code>.
    </dd>
    <dt>
      <strong>POST</strong> <code>/opentelemetry.proto.collector.logs.v1.LogsService/Export</code>,
      and the <code>MetricsService</code> and <code>TraceService</code> methods
    </dt>
    <dd>
      OTLP over gRPC, over HTTP/2 in the clear or through TLS: one message per call, plain or
      gzipped as <code>grpc-encoding</code> names it.
    </dd>
    <dt><strong>GET</strong> <code>/health</code></dt>
    <dd>Says whether the port's receivers take exports. See <a href="#health">Health check</a>.</dd>
  </dl>
  <p>
    An export of a signal no receiver on the port takes is refused 404, or
    <code>UNIMPLEMENTED</code> over gRPC.
  </p>

  <h2 id="records">Records it gives</h2>
  <p>
    Each log record, metric data point or span of an export becomes one record, its fields typed as
    OTLP types them. A field it leaves unset is null. Times are datetimes, to the microsecond; IDs
    lowercase hex, and an ID that is all zero, or not 16 and 8 bytes long, is null; an enum is its
    number. <code>attributes</code> and the other variants keep each value's own type: a string, an
    int, a double, a bool, bytes, a list or a map. Read an attribute as text with
    <code>attributes["http.method"] as str</code>.
  </p>
  <h3>Logs</h3>
  <dl>
    <dt><code>time</code>, <code>observedTime</code></dt>
    <dd>When it happened, and when it was seen.</dd>
    <dt><code>severityNumber</code>, <code>severityText</code></dt>
    <dd>The severity as OTLP's number, 1 to 24, and as the sender's text.</dd>
    <dt><code>body</code>, <code>attributes</code></dt>
    <dd>Variants.</dd>
    <dt><code>traceId</code>, <code>spanId</code>, <code>flags</code></dt>
    <dd>The span it was written in.</dd>
    <dt><code>eventName</code>, <code>droppedAttributesCount</code></dt>
    <dd>As the log record gives them.</dd>
  </dl>
  <h3>Metrics</h3>
  <p>One record per data point, each with its metric's fields.</p>
  <dl>
    <dt><code>name</code>, <code>description</code>, <code>unit</code>, <code>metadata</code></dt>
    <dd>Its metric's.</dd>
    <dt><code>type</code></dt>
    <dd>
      <code>gauge</code>, <code>sum</code>, <code>histogram</code>,
      <code>exponentialHistogram</code> or <code>summary</code>.
    </dd>
    <dt><code>aggregationTemporality</code>, <code>isMonotonic</code></dt>
    <dd>1 for delta and 2 for cumulative, where its type has one; whether a sum only grows.</dd>
    <dt><code>startTime</code>, <code>time</code>, <code>attributes</code>, <code>flags</code></dt>
    <dd>The point's.</dd>
    <dt><code>value</code></dt>
    <dd>A gauge's or sum's number, an int or a double as it was sent.</dd>
    <dt><code>count</code>, <code>sum</code>, <code>min</code>, <code>max</code></dt>
    <dd>A histogram's, exponential histogram's or summary's. Counts are doubles.</dd>
    <dt><code>bucketCounts</code>, <code>explicitBounds</code></dt>
    <dd>
      A histogram's buckets: each one's count and each one's upper bound, both arrays of doubles.
    </dd>
    <dt>
      <code>scale</code>, <code>zeroCount</code>, <code>zeroThreshold</code>, <code>positive</code>,
      <code>negative</code>
    </dt>
    <dd>
      An exponential histogram's; <code>positive</code> and <code>negative</code> each hold
      <code>offset</code> and an array of doubles, <code>bucketCounts</code>.
    </dd>
    <dt><code>quantileValues</code></dt>
    <dd>A summary's, an array of <code>quantile</code> and <code>value</code>, both doubles.</dd>
    <dt><code>exemplars</code></dt>
    <dd>A variant list of the point's exemplars.</dd>
  </dl>
  <h3>Traces</h3>
  <dl>
    <dt>
      <code>traceId</code>, <code>spanId</code>, <code>parentSpanId</code>, <code>traceState</code>,
      <code>flags</code>
    </dt>
    <dd>Where the span sits in its trace.</dd>
    <dt><code>name</code>, <code>kind</code></dt>
    <dd>Its name, and OTLP's kind: 1 internal, 2 server, 3 client, 4 producer, 5 consumer.</dd>
    <dt><code>startTime</code>, <code>endTime</code></dt>
    <dd>When it started and ended.</dd>
    <dt><code>attributes</code>, <code>events</code>, <code>links</code></dt>
    <dd>
      Its attributes, and lists of its events, each with its <code>time</code>, <code>name</code>
      and
      <code>attributes</code>, and of the spans it links to.
    </dd>
    <dt><code>statusCode</code>, <code>statusMessage</code></dt>
    <dd>1 ok, 2 error, and why.</dd>
    <dt>
      <code>droppedAttributesCount</code>, <code>droppedEventsCount</code>,
      <code>droppedLinksCount</code>
    </dt>
    <dd>As the span gives them.</dd>
  </dl>

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    The resource and instrumentation scope a record came under are <code>__source</code> fields,
    which every record of that scope shares. <code>__source.remote</code> holds the IP address the export
    came from: the balancer's, when one is in front of the workers.
  </p>
  <FieldsTable names={kind(SOURCES, 'otlpReceiver').fields ?? []} />

  <h2 id="senders">Tokens and certificates</h2>
  <p>
    Anyone a receiver admits can put records into the group's pipelines, so a receiver needs
    <code>tokens</code>, an <code>identity</code> with authorities, or both. The leader refuses a receiver
    with neither.
  </p>
  <ul>
    <li>
      <strong>Tokens:</strong> a sender presents one as
      <code>Authorization: Bearer &lt;token&gt;</code>, set as a header on its exporter. The
      receiver refuses any other request before reading its body. Without an identity, the token
      crosses the network in the clear, so use a token alone only behind a balancer that terminates
      TLS.
    </li>
    <li>
      <strong>Client certificates:</strong> with an
      <a href="/docs/secrets#identities">identity</a>, the receiver serves TLS with the identity's
      certificate and key. If the identity has authorities, every connection, health checks
      included, must present a certificate that chains to one of them.
    </li>
  </ul>
  <p>With both, a sender presents a certificate and a token.</p>

  <h2 id="answers">What it answers</h2>
  <p>
    The receiver answers success only once every destination has the export's records, so an
    exporter that retries an unanswered export loses nothing. Each refusal below carries an OTLP <code
      >Status</code
    >
    whose message says why. The receiver takes an export whole or refuses it whole. Over gRPC, the same
    answers are
    <a href="#grpc">gRPC status codes</a>.
  </p>
  <dl>
    <dt><strong>200</strong></dt>
    <dd>
      Every destination has the export's records. The body is an empty
      <code>Export*ServiceResponse</code>, <code>{'{}'}</code> in JSON.
    </dd>
    <dt><strong>503</strong>, with <code>Retry-After</code></dt>
    <dd>
      The receiver is full, <a href="/docs/delivery#hold">blocked</a> or stopping, or the sender took
      more than 30 seconds to send the whole body. It refuses a new export at once, and a stopping receiver
      answers every export it holds. An exporter may send it again.
    </dd>
    <dt><strong>401</strong></dt>
    <dd>
      The request has no token, gives one other than as <code>Bearer &lt;token&gt;</code>, or gives
      one the receiver does not hold.
    </dd>
    <dt><strong>400</strong></dt>
    <dd>The body is not an OTLP export of the receiver's signal. An exporter may drop it.</dd>
    <dt><strong>413</strong></dt>
    <dd>
      The body, or what it inflates to, is larger than <code>maxRequestBytes</code>, or its records
      hold more decoded than a receiver holds at once. Send smaller exports.
    </dd>
    <dt><strong>404</strong></dt>
    <dd>No receiver on the port takes the export's signal. An exporter may drop it.</dd>
    <dt><strong>415</strong></dt>
    <dd>
      The body's <code>Content-Type</code> is neither <code>application/x-protobuf</code> nor
      <code>application/json</code>, or its <code>Content-Encoding</code> is other than
      <code>gzip</code> or <code>identity</code>.
    </dd>
  </dl>

  <p id="grpc">
    Over gRPC, an export is answered <code>OK</code> once every destination has it, with an empty
    <code>Export*ServiceResponse</code>. A refusal is <code>UNAVAILABLE</code> where HTTP says 503,
    which an exporter may send again; <code>UNAUTHENTICATED</code> where it says 401;
    <code>INVALID_ARGUMENT</code> where it says 400, 413 or 415, which an exporter may drop; and
    <code>UNIMPLEMENTED</code> where it says 404, and for a <code>grpc-encoding</code> other than
    <code>gzip</code> or <code>identity</code>. Its <code>grpc-message</code> says why. A request
    whose
    <code>content-type</code> is not <code>application/grpc</code> is a plain 415.
  </p>

  <h2 id="health">Health check</h2>
  <p>
    <code>GET /health</code> on the receiver's port answers 200 while every receiver on it takes exports
    and 503 while one is full, blocked or stopping. It needs no token. Point a balancer's health check
    or a readiness probe at it, so a busy worker gets no exports until it can take them. Under an identity
    with authorities it needs a client certificate like any other request.
  </p>

  <h2 id="clients">Senders it takes</h2>
  <p>
    Any sender of OTLP works: over HTTP, as protobuf or JSON, or over gRPC, plain or gzipped. Over
    gRPC, a sender presents its token as the <code>authorization</code> metadata,
    <code>Bearer &lt;token&gt;</code>.
  </p>
  <ul>
    <li>The OpenTelemetry Collector's <code>otlphttp</code> and <code>otlp</code> exporters.</li>
    <li>OpenTelemetry SDKs, with their exporters on any OTLP protocol.</li>
    <li>
      Fluent Bit's <code>opentelemetry</code> output, and Vector's <code>opentelemetry</code> sink.
    </li>
  </ul>
  <p>
    This OpenTelemetry Collector configuration sends logs and traces to the port of the
    <a href="#example">example</a>'s two receivers, with
    <code>OTEL_TOKEN</code> set in the collector's environment:
  </p>
  <pre><code>{otel}</code></pre>
  <p>An SDK reads the same from its environment:</p>
  <pre><code>{sdk}</code></pre>

  <h2 id="port">Open its port</h2>
  <p>
    Every worker in the group listens on the receiver's <code>port</code>. Open that port to senders
    on each worker, and give senders one address that reaches them all, such as a balancer's. On a
    Unix host, an export sent while a worker deploys a new configuration waits, and the new
    configuration answers it.
  </p>
  <ul>
    <li>
      On Kubernetes, add the port to the worker pods and put a Service in front of them:
      <a href="/docs/enroll-worker/kubernetes#receiver">Expose a receiver</a>.
    </li>
    <li>
      On a host or in a container, open the host's firewall or publish the container's port:
      <a href="/docs/enroll-worker/host">Enroll a worker on a host</a>.
    </li>
  </ul>

  <h2 id="verify">Verify</h2>
  <p>
    POST an export you can recognize, as in the <a href="#example">example</a>. The receiver answers
    200 with <code>{'{}'}</code> once every destination has it. Then find it at the destination.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The sender cannot connect</strong></dt>
    <dd>
      Check that the port is open on every worker, that the sender's address reaches them, and that
      its endpoint names the receiver's port. An identity with authorities refuses a connection
      without a certificate that chains to one of them.
    </dd>
    <dt><strong>Every export gets 401</strong></dt>
    <dd>
      Check that the sender presents <code>Authorization: Bearer &lt;token&gt;</code>, with a token
      one of the receiver's credentials holds.
    </dd>
    <dt><strong>Every export gets 404</strong></dt>
    <dd>
      No receiver on the port takes the signal it sends: add one for it, or stop sending it. Or the
      sender posts to a path other than its signal's, such as <code>/v1/logs</code>.
    </dd>
    <dt><strong>Every export gets 503</strong></dt>
    <dd>
      Check the source's status. A blocked receiver takes nothing until its destinations take
      records again. A full one takes more with more workers.
    </dd>
    <dt><strong>The same records arrive twice</strong></dt>
    <dd>
      An exporter that hears no answer may send the export again. Give the exporter a timeout longer
      than its destinations take to deliver.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/http-receiver">HTTP receiver source</a></li>
    <li><a href="/docs/sources/hec-receiver">HEC receiver source</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
