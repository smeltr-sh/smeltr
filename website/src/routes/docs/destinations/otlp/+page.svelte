<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import { DESTINATIONS } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'records', title: 'Records it sends' },
    { id: 'requests', title: 'Requests' },
    { id: 'answers', title: 'Answers' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
  const carry = `resource = __source.resource;
scope = {name: __source.scope.name, version: __source.scope.version, attributes: __source.scope.attributes};`;
</script>

<GuideLayout
  title="OTLP destination"
  description="Send logs, metrics or traces over OTLP/HTTP to an OpenTelemetry Collector or any OTLP endpoint: an example, every setting, the records it takes, and how it reads each answer."
  {sections}
>
  <h1>OTLP</h1>
  <p class="lede">
    Sends each record over OTLP/HTTP, as protobuf, to any OTLP endpoint, such as an OpenTelemetry
    Collector. One destination sends one signal: logs, metrics or traces.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Sends spans to a Collector, and reads the <code>Authorization</code> header's value from the
    credential <code>OTEL_TOKEN</code>.
  </p>
  <pre><code>{@html highlight(json(examples.otlpDestination))}</code></pre>
  <p>To pass a received record's resource and scope on, set them from <code>__source</code>:</p>
  <pre><code>{carry}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Find the endpoint's OTLP/HTTP address, without a signal's path. A Collector's
      <a
        href="https://github.com/open-telemetry/opentelemetry-collector/blob/main/receiver/otlpreceiver/README.md"
        >OTLP receiver</a
      > listens on port 4318.
    </li>
    <li>Make sure the endpoint takes protobuf over HTTP. gRPC and OTLP/JSON are not sent.</li>
    <li>Allow workers to reach the endpoint.</li>
    <li>
      Put each secret header's whole value in a <a href="/docs/secrets">credential</a>. For
      <code>Authorization</code>, the credential holds <code>Bearer &lt;token&gt;</code>, prefix
      included.
    </li>
    <li>To send more than one signal, add a destination for each.</li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="otlp" />

  <h2 id="records">Records it sends</h2>
  <p>
    Each record is one log record, metric data point or span, in the form the <a
      href="/docs/sources/otlp-receiver#records">OTLP receiver</a
    > of the same signal gives, so a record the receiver gave can be sent on as it is. Two fields of the
    record say what it came under:
  </p>
  <dl>
    <dt><code>resource</code></dt>
    <dd>
      An object of the resource's attributes, as the receiver's <code>__source.resource</code>.
    </dd>
    <dt><code>scope</code></dt>
    <dd>
      An object of the instrumentation scope's <code>name</code>, <code>version</code> and
      <code>attributes</code>, as the receiver's <code>__source.scope</code> fields.
    </dd>
  </dl>
  <p>
    A record without them goes under an empty resource or scope. A field OTLP has no place for is
    left out. A metric data point's <code>type</code> is its metric's: <code>gauge</code>,
    <code>sum</code>, <code>histogram</code>, <code>exponentialHistogram</code> or
    <code>summary</code>.
  </p>
  <p>
    A value keeps its record's type, so an attribute that was bytes is sent as its base64 text. A
    NaN or an infinity is null in a record, because JSON holds neither. In a list, a null is sent as
    NaN; a lone null is left unset.
  </p>
  <p>
    The destination refuses a record it cannot send as its signal, such as a span with no
    <code>traceId</code>, and names why.
    <a href="/docs/delivery#refused">Choose what happens to it →</a>
  </p>

  <h2 id="requests">Requests</h2>
  <p>
    Each request goes to the signal's path added to <code>url</code>: <code>/v1/logs</code>,
    <code>/v1/metrics</code> or <code>/v1/traces</code>. It groups records by resource, then by
    scope, and for metrics by metric, each group once, with its records in the order they came.
  </p>
  <p>
    The destination sends <code>inFlight</code> requests at once to start with, and more as its buffer
    allows, so records of one series or trace may arrive out of order.
  </p>

  <h2 id="answers">Answers</h2>
  <p>
    It reads each answer as the <a href="/docs/destinations/http#answers">HTTP destination</a> does.
    A 2xx whose <code>partialSuccess</code> says the endpoint refused some of the request counts the
    request as delivered, and what was refused in
    <code>smeltr_destination_unwritten_total</code>, with the endpoint's message in the worker's
    log.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record through the source, then find it where the endpoint sends it on, such as a
    Collector's <code>debug</code> exporter. If the destination stalls, the Status page flags it,
    and the worker's <a href="/docs/logs-metrics#gui">log</a> names the status the endpoint answered.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Every record is refused</strong></dt>
    <dd>
      The refusal names the field at fault. A span needs a <code>traceId</code> and a
      <code>spanId</code>; a metric data point needs a <code>type</code>. A pipeline renames a
      record's own fields to them.
    </dd>
    <dt><strong>The endpoint answers 404</strong></dt>
    <dd>
      Give the endpoint's address without a signal's path; the destination adds the path itself.
    </dd>
    <dt><strong>The endpoint answers 415</strong></dt>
    <dd>The endpoint does not take OTLP/HTTP protobuf. Point it at its OTLP/HTTP port.</dd>
    <dt><strong>The endpoint answers 401 or 403</strong></dt>
    <dd>
      Check that the credential holds the header's whole value, prefix included, such as
      <code>Bearer &lt;token&gt;</code>.
    </dd>
    <dt><strong>Records arrive without their resource or scope</strong></dt>
    <dd>
      Set <code>resource</code> and <code>scope</code> from <code>__source</code> in the pipeline,
      as in the <a href="#example">example</a>.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/sources/otlp-receiver">OTLP receiver source</a></li>
    <li><a href="/docs/destinations/http">HTTP destination</a></li>
    <li><a href="/docs/delivery#refused">Handle refused records</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
