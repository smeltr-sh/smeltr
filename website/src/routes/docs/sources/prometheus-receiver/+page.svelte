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
  const prometheus = `remote_write:
  - url: https://workers.example.com:9090/api/v1/write
    authorization:
      credentials_file: /etc/prometheus/smeltr-token
    tls_config:
      ca_file: /etc/prometheus/smeltr-ca.pem`;
  const alloy = `prometheus.remote_write "smeltr" {
  endpoint {
    url = "https://workers.example.com:9090/api/v1/write"
    bearer_token = sys.env("PROM_TOKEN")
    tls_config {
      ca_file = "/etc/alloy/smeltr-ca.pem"
    }
  }
}`;
  const pipeline = `if (name != "http_requests_total") drop();
job = attributes["job"] as str;`;
</script>

<GuideLayout
  title="Prometheus receiver source"
  description="Take the samples Prometheus and its agents send over remote_write to a port every worker listens on: an example, every setting, the records it gives, what it answers, and which senders work."
  {sections}
>
  <h1>Prometheus receiver</h1>
  <p class="lede">
    Takes the samples Prometheus sends over remote_write to its port. It answers each request once
    every destination has its samples.
  </p>
  <p>
    Prometheus and any agent that speaks remote_write 1.0 or 2.0 send to it. Every worker in the
    group listens on its port, and the sender, or a balancer in front of the workers, picks a worker
    for each request. Add workers to take more.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Takes remote_write on port 9090, over TLS with the
    <a href="/docs/certificates#connectors">certificate</a> <code>receiver-tls</code>. Senders
    present the token held in the worker-host variable <code>PROM_TOKEN</code>.
  </p>
  <pre><code>{@html highlight(json(examples.prometheusReceiverSource))}</code></pre>
  <ProductScreenshot
    source="prometheus-receiver-source-form"
    height={2000}
    crop={{ x: 608, y: 638, width: 1664, height: 724 }}
    alt="The Prometheus receiver form on the Sources page: id prometheus, port 9090, no tokens yet."
    caption="The same source in the Sources page's add dialog, before you add its token and certificate."
  />
  <p>Prometheus sends to it with a <code>remote_write</code> block like this:</p>
  <pre><code>{prometheus}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Pick a port no other receiver in the group uses.</li>
    <li>
      For native histograms, set <code>send_native_histograms: true</code> in the sender's
      <a
        href="https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write"
        >remote_write</a
      > settings.
    </li>
    <li>
      Decide how senders prove who they are: a token, a client certificate, or both. See
      <a href="#senders">Tokens and certificates</a>.
    </li>
    <li>Put each token in a <a href="/docs/secrets">credential</a>.</li>
    <li>
      For exemplars, set <code>send_exemplars: true</code> in the sender's
      <a
        href="https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write"
        >remote_write</a
      > settings.
    </li>
    <li>
      Open the port on every worker, and give senders one address that reaches them all. See
      <a href="#port">Open its port</a>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="prometheusReceiver" />

  <h2 id="paths">Paths it serves</h2>
  <dl>
    <dt><strong>POST</strong> <code>/api/v1/write</code></dt>
    <dd>A remote_write 1.0 or 2.0 request, as the sender sends it.</dd>
    <dt><strong>GET</strong> <code>/health</code></dt>
    <dd>Says whether the receiver takes requests. See <a href="#health">Health check</a>.</dd>
  </dl>

  <h2 id="records">Records it gives</h2>
  <p>
    Each sample of a request becomes one record: a float sample, or a native histogram. A series of
    three samples is three records, each with the series' labels. Each field has the name and type
    of an <a href="/docs/sources/otlp-receiver#records">OTLP metric point's</a> field, so a pipeline reads
    a metric from either receiver alike. A field a sample does not have is null.
  </p>
  <dl>
    <dt><code>time</code></dt>
    <dd>A datetime, from the sample's milliseconds.</dd>
    <dt><code>name</code></dt>
    <dd>The series' metric name, its <code>__name__</code> label.</dd>
    <dt><code>value</code></dt>
    <dd>
      A variant holding the sample's double, bit for bit, so a staleness marker stays one. Read it
      as a number with <code>value as f64</code>.
    </dd>
    <dt><code>attributes</code></dt>
    <dd>
      The series' other labels, an object of strings, or null when the series has no label but
      <code>__name__</code>. Read one with <code>attributes["job"] as str</code>.
    </dd>
    <dt><code>type</code></dt>
    <dd>
      A native histogram's: <code>exponentialHistogram</code>, or <code>histogram</code> for one with
      custom buckets. Null for a float sample.
    </dd>
    <dt><code>aggregationTemporality</code></dt>
    <dd>2, cumulative, for a counter's histogram; null for a gauge histogram.</dd>
    <dt><code>count</code>, <code>sum</code></dt>
    <dd>A histogram's count and sum, doubles.</dd>
    <dt>
      <code>scale</code>, <code>zeroCount</code>, <code>zeroThreshold</code>, <code>positive</code>,
      <code>negative</code>
    </dt>
    <dd>
      An exponential histogram's, as <a
        href="https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram"
        >OpenTelemetry</a
      >
      lays it out. <code>positive</code> and <code>negative</code> each hold <code>offset</code> and
      <code>bucketCounts</code>, an array of doubles.
    </dd>
    <dt><code>resetHint</code></dt>
    <dd>
      <code>yes</code> or <code>no</code> where the sender said whether the histogram's counter reset,
      and null where it did not say.
    </dd>
    <dt><code>bucketCounts</code>, <code>explicitBounds</code></dt>
    <dd>
      A histogram of custom buckets: its bucket counts and their bounds, as
      <a href="https://opentelemetry.io/docs/specs/otel/metrics/data-model/#histogram"
        >OpenTelemetry</a
      > lays them out.
    </dd>
    <dt><code>exemplars</code></dt>
    <dd>
      The series' exemplars, on its last record of the request, each shaped as an OTLP point's
      exemplar: <code>time</code>, <code>value</code>, <code>traceId</code>, <code>spanId</code>,
      and its other labels in <code>filteredAttributes</code>. A series sent with exemplars alone is
      one record, its <code>value</code> null.
    </dd>
    <dt><code>startTime</code></dt>
    <dd>
      When the series started counting, which remote_write 2.0 may send with a sample or histogram.
      Null from 1.0, and where the sender did not say.
    </dd>
  </dl>
  <p>
    Each metric family's metadata is a record of its own. Its <code>time</code> and
    <code>value</code> are null, <code>name</code> is the family's, <code>description</code> its
    help and <code>unit</code> its unit. Its <code>type</code> is as an OTLP metric's: a counter is
    <code>sum</code> with <code>isMonotonic</code> true. A pipeline that wants samples alone drops
    records with no <code>time</code>.
  </p>
  <p>
    Over remote_write 2.0, a family's metadata record comes once a request, ahead of the family's
    first series.
  </p>
  <p>
    Classic histograms arrive as samples of their <code>_bucket</code>, <code>_sum</code> and
    <code>_count</code> series, as a
    <a href="https://prometheus.io/docs/concepts/metric_types/#histogram">classic histogram</a> is laid
    out.
  </p>
  <p>This pipeline keeps one metric and reads its job as text:</p>
  <pre><code>{pipeline}</code></pre>

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    <code>__source.remote</code> holds the IP address the request came from: the balancer's, when one
    is in front of the workers.
  </p>
  <FieldsTable names={kind(SOURCES, 'prometheusReceiver').fields ?? []} />

  <h2 id="senders">Tokens and certificates</h2>
  <p>
    Anyone a receiver admits can put records into the group's pipelines, so a receiver needs
    <code>tokens</code>, an <code>certificate</code> and <code>trustedAuthorities</code>, or both.
    The leader refuses a receiver with neither.
  </p>
  <ul>
    <li>
      <strong>Tokens:</strong> a sender presents one as
      <code>Authorization: Bearer &lt;token&gt;</code>, Prometheus's <code>authorization</code>
      block. The receiver refuses any other request before reading its body. Without a certificate, the
      token crosses the network in the clear, so use a token alone only behind a balancer that terminates
      TLS.
    </li>
    <li>
      <strong>Client certificates:</strong> with a
      <a href="/docs/certificates#connectors">certificate</a>, the receiver serves TLS with it. With
      <code>trustedAuthorities</code>, every connection, health checks included, must present a
      certificate that chains to one of them: Prometheus's
      <code>tls_config</code> with <code>cert_file</code> and <code>key_file</code>.
    </li>
  </ul>
  <p>With both, a sender presents a certificate and a token.</p>

  <h2 id="answers">What it answers</h2>
  <p>
    The receiver answers success only once every destination has the request's samples, so a sender
    that retries an unanswered request loses nothing. The receiver takes a request whole or refuses
    it whole, and each refusal's body says why.
  </p>
  <dl>
    <dt><strong>204</strong></dt>
    <dd>Every destination has the request's samples.</dd>
    <dt><strong>503</strong>, with <code>Retry-After</code></dt>
    <dd>
      The receiver is full, <a href="/docs/delivery#hold">blocked</a> or stopping, or the sender took
      more than 30 seconds to send the whole body. A sender may send it again.
    </dd>
    <dt><strong>401</strong></dt>
    <dd>
      The request has no token, gives one other than as <code>Bearer &lt;token&gt;</code>, or gives
      one the receiver does not hold.
    </dd>
    <dt><strong>400</strong></dt>
    <dd>
      The body does not read as remote_write, or it holds a series or histogram remote_write does
      not allow, such as a label with an empty name or a histogram whose count is not what its
      buckets hold. The answer's body names what is wrong. A sender may drop it.
    </dd>
    <dt><strong>413</strong></dt>
    <dd>
      The body is larger than <code>maxRequestBytes</code>, or what it holds decompressed and
      decoded is more than a receiver holds at once. Send smaller requests, or raise
      <code>maxRequestBytes</code>.
    </dd>
    <dt><strong>415</strong></dt>
    <dd>The request is neither remote_write 1.0 nor 2.0. The answer's body names what is wrong.</dd>
  </dl>

  <h2 id="health">Health check</h2>
  <p>
    <code>GET /health</code> on the receiver's port answers 200 while the receiver takes requests
    and 503 while it is full, blocked or stopping. It needs no token. Point a balancer's health
    check or a readiness probe at it, so a busy worker gets no requests until it can take them. With
    <code>trustedAuthorities</code>
    with authorities it needs a client certificate like any other request.
  </p>

  <h2 id="clients">Senders it takes</h2>
  <p>Any sender of remote_write 1.0 or 2.0 works.</p>
  <ul>
    <li>
      Prometheus's <a
        href="https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write"
        >remote_write</a
      >, in server or agent mode. Set
      <code>protobuf_message: io.prometheus.write.v2.Request</code> to send 2.0, which carries each series'
      metadata and start time.
    </li>
    <li>Grafana Alloy's <code>prometheus.remote_write</code>.</li>
    <li>VictoriaMetrics' <code>vmagent</code>.</li>
    <li>
      The OpenTelemetry Collector's <code>prometheusremotewrite</code> exporter, exponential histograms
      included.
    </li>
  </ul>
  <p>
    Grafana Alloy sends to it what its scrape components forward to
    <code>prometheus.remote_write.smeltr.receiver</code>, with <code>PROM_TOKEN</code> set in its environment:
  </p>
  <pre><code>{alloy}</code></pre>

  <h2 id="port">Open its port</h2>
  <p>
    Every worker in the group listens on the receiver's <code>port</code>. Open that port to senders
    on each worker, and give senders one address that reaches them all, such as a balancer's. On a
    Unix host, a request sent while a worker deploys a new configuration waits, and the new
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
    Point a sender at the receiver and check in its own logs or metrics that its requests succeed.
    Then find its samples at the destination.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The sender cannot connect</strong></dt>
    <dd>
      Check that the port is open on every worker and that the sender's address reaches them. A
      receiver with <code>trustedAuthorities</code> refuses a connection without a certificate that chains
      to one of them.
    </dd>
    <dt><strong>Every request gets 401</strong></dt>
    <dd>
      Check that the sender presents <code>Authorization: Bearer &lt;token&gt;</code>, with a token
      one of the receiver's credentials holds.
    </dd>
    <dt><strong>Every request gets 415</strong></dt>
    <dd>
      The sender does not speak remote_write 1.0 or 2.0. The answer's body names what is wrong.
    </dd>
    <dt><strong>Every request gets 503</strong></dt>
    <dd>
      Check the source's status. A blocked receiver takes nothing until its destinations take
      records again. A full one takes more with more workers.
    </dd>
    <dt><strong>The same samples arrive twice</strong></dt>
    <dd>
      A sender that hears no answer may send the request again. Give it a
      <code>remote_timeout</code> longer than its destinations take to deliver.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/http-receiver">HTTP receiver source</a></li>
    <li><a href="/docs/sources/hec-receiver">HEC receiver source</a></li>
    <li><a href="/docs/sources/otlp-receiver">OTLP receiver source</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
