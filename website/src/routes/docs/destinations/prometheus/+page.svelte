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
    { id: 'samples', title: 'Records it sends' },
    { id: 'requests', title: 'Requests' },
    { id: 'answers', title: 'Answers' },
    { id: 'stores', title: 'Stores it sends to' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
  const sample = json({
    time: '2026-10-11T18:00:00.123Z',
    name: 'http_requests_total',
    value: 1027,
    attributes: { job: 'api', instance: 'api-1:9100', code: '200' },
  });
</script>

<GuideLayout
  title="Prometheus remote_write destination"
  description="Send records as samples over remote_write to Prometheus or any store that takes it: an example, every setting, the records it takes, and how it reads each answer."
  {sections}
>
  <h1>Prometheus remote_write</h1>
  <p class="lede">
    Sends each record as a sample over remote_write 1.0 or 2.0 to any store that takes it.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Sends to a Mimir tenant, and reads the <code>Authorization</code> header's value from the
    credential
    <code>PROMETHEUS_TOKEN</code>.
  </p>
  <pre><code>{@html highlight(json(examples.prometheusDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Find the store's remote_write URL, path included:
      <ul>
        <li>
          <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#remote-write-receiver"
            >Prometheus</a
          >
        </li>
        <li>
          <a href="https://grafana.com/docs/mimir/latest/references/http-api/#remote-write">Mimir</a
          >
        </li>
        <li><a href="https://cortexmetrics.io/docs/api/#remote-write">Cortex</a></li>
        <li><a href="https://thanos.io/tip/components/receive.md/">Thanos Receive</a></li>
        <li>
          <a href="https://docs.victoriametrics.com/victoriametrics/integrations/prometheus/"
            >VictoriaMetrics</a
          >, or for a cluster its
          <a
            href="https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/#url-format"
            >URL format</a
          >
        </li>
      </ul>
    </li>
    <li>To send native histograms, make sure the store takes them. See its docs.</li>
    <li>
      To send remote_write 2.0, make sure the store takes it. See its docs. 1.0 is the default.
    </li>
    <li>To keep exemplars, make sure the store keeps them. See its docs.</li>
    <li>Allow workers to reach the store.</li>
    <li>
      Put each secret header's whole value in a <a href="/docs/secrets">credential</a>. For
      <code>Authorization</code>, the credential holds <code>Bearer &lt;token&gt;</code>, prefix
      included.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="prometheus" />

  <h2 id="samples">Records it sends</h2>
  <p>
    Each record is one sample, a float or a native histogram, in the shape the <a
      href="/docs/sources/prometheus-receiver#records">Prometheus receiver</a
    > gives, so its records pass through unchanged. An OTLP receiver's metric points have the same shape.
    A float sample:
  </p>
  <pre><code>{@html highlight(sample)}</code></pre>
  <dl>
    <dt><code>name</code></dt>
    <dd>A non-empty string: the series' metric name, sent as its <code>__name__</code> label.</dd>
    <dt><code>value</code></dt>
    <dd>A number.</dd>
    <dt><code>time</code></dt>
    <dd>A datetime, sent as its milliseconds.</dd>
    <dt><code>attributes</code></dt>
    <dd>
      An object of the series' other labels. A number or a boolean is sent as its text. A label that
      is null or empty is none, and a record with no labels but its name may leave
      <code>attributes</code> out or null.
    </dd>
    <dt><code>type</code></dt>
    <dd>
      Absent, null, <code>gauge</code> or <code>sum</code> for a float sample.
      <code>exponentialHistogram</code> for a native histogram, or <code>histogram</code> for a
      histogram of explicit bounds. Neither needs <code>value</code>. A <code>summary</code> is refused.
    </dd>
  </dl>
  <p>A histogram also needs these, as the Prometheus receiver gives them:</p>
  <dl>
    <dt><code>count</code>, <code>sum</code></dt>
    <dd>Numbers.</dd>
    <dt><code>aggregationTemporality</code></dt>
    <dd>
      2 for a counter's histogram; absent or null for a gauge histogram. A delta, 1, is refused.
    </dd>
    <dt>
      <code>scale</code>, <code>zeroCount</code>, <code>zeroThreshold</code>, <code>positive</code>,
      <code>negative</code>
    </dt>
    <dd>An exponential histogram's.</dd>
    <dt><code>bucketCounts</code>, <code>explicitBounds</code></dt>
    <dd>
      A <code>histogram</code>'s, sent as a
      <a href="https://prometheus.io/docs/concepts/metric_types/#histogram">classic histogram</a>:
      its
      <code>_bucket</code>, <code>_sum</code> and <code>_count</code> series.
    </dd>
    <dt><code>resetHint</code></dt>
    <dd>
      <code>yes</code> or <code>no</code>, sent as an exponential histogram's reset hint. Left out,
      the hint sent is unknown.
    </dd>
  </dl>
  <p>Any record may also carry exemplars, as either receiver gives them:</p>
  <dl>
    <dt><code>exemplars</code></dt>
    <dd>
      An array of objects, each a <code>value</code>, a <code>time</code> or none for the record's,
      <code>traceId</code>, <code>spanId</code> and <code>filteredAttributes</code>. An exemplar the
      store would not keep is left out, and its record still sent.
    </dd>
  </dl>
  <p>
    A record with no <code>time</code> and no <code>value</code> is a family's metadata, as the
    Prometheus receiver gives it: its <code>description</code>, <code>unit</code> and
    <code>type</code> go as the family's. A point's <code>description</code> or <code>unit</code>,
    as an OTLP point has them, goes as its family's too.
  </p>
  <p>
    Over remote_write 2.0, each series carries its family's latest metadata. A record's <code
      >startTime</code
    > goes as its series' start time.
  </p>
  <p>
    The destination refuses a record that is no sample, and names why. <a
      href="/docs/delivery#refused">Choose what happens to it →</a
    > A value that was NaN or an infinity arrives with none, because JSON holds neither, so such a record
    is refused. Staleness markers are not sent.
  </p>

  <h2 id="requests">Requests</h2>
  <p>
    A request is remote_write 1.0, or 2.0 with <code>version</code> set to it. A series' records in a
    request go oldest first.
  </p>
  <p>
    The destination sends one request at a time, and each lands before the next is sent, so a
    series' samples arrive in order.
  </p>

  <h2 id="answers">Answers</h2>
  <div class="table-scroll">
    <table>
      <thead
        ><tr><th scope="col">The store answers</th><th scope="col">The destination</th></tr></thead
      >
      <tbody>
        <tr
          ><th scope="row">2xx</th><td>counts every record the request carried as delivered.</td
          ></tr
        >
        <tr><th scope="row">429, 5xx</th><td>backs off and sends the request again.</td></tr>
        <tr
          ><th scope="row">400, 409, 413, 422 to 2.0, saying it wrote some of the request</th><td
            >counts the request as delivered, and what the store did not write in
            <code>smeltr_destination_unwritten_total</code>.</td
          ></tr
        >
        <tr
          ><th scope="row">400, 409, 413, 422</th><td
            >splits the request in halves and sends each. Halves the store takes land; a sample it
            still refuses on its own, the destination refuses alone.</td
          ></tr
        >
        <tr
          ><th scope="row">415, or a success that stored nothing, to 2.0</th><td
            >stalls, and reports that the store takes only remote_write 1.0.</td
          ></tr
        >
        <tr
          ><th scope="row">401, 403</th><td
            >stalls, and reports that the store refused its credentials.</td
          ></tr
        >
        <tr
          ><th scope="row">anything else, a redirect included</th><td
            >stalls, and reports the status. It never follows a redirect.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    A batch counts as delivered once every request carrying it gets a 2xx. A sample sent again after
    a failure may already be in the store.
  </p>

  <h2 id="stores">Stores it sends to</h2>
  <p>
    A store may refuse a sample older than the last it holds for that series, such as one from a
    second worker or another writer. Each store's out-of-order docs:
  </p>
  <ul>
    <li>
      <a href="https://prometheus.io/docs/prometheus/latest/configuration/configuration/#tsdb"
        >Prometheus</a
      >
    </li>
    <li>
      <a
        href="https://grafana.com/docs/mimir/latest/configure/configure-out-of-order-samples-ingestion/"
        >Mimir</a
      >
    </li>
    <li>
      <a href="https://cortexmetrics.io/docs/configuration/configuration-file/#limits_config"
        >Cortex</a
      >
    </li>
    <li>
      <a href="https://thanos.io/tip/components/receive.md/">Thanos Receive</a>
    </li>
  </ul>

  <h2 id="verify">Verify</h2>
  <p>
    Send a sample through the source, then query its series in the store. If the destination stalls,
    the Status page flags it, and the worker's
    <a href="/docs/logs-metrics#gui">log</a> names the status the store answered.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Every record is refused</strong></dt>
    <dd>
      The refusal names the field at fault. A float sample needs <code>name</code>,
      <code>value</code> and <code>time</code>; a histogram needs <code>count</code>,
      <code>sum</code> and its buckets in place of <code>value</code>. A pipeline renames a record's
      own fields to them.
    </dd>
    <dt><strong>The store answers 404 or 405</strong></dt>
    <dd>
      Give the remote_write URL, path included, and check that the store's remote_write receiver is
      on.
    </dd>
    <dt><strong>The store answers 415, or a success that stored nothing</strong></dt>
    <dd>
      The store does not take remote_write 2.0. Set <code>version</code> to <code>1.0</code>.
    </dd>
    <dt><strong>The store answers 401 or 403</strong></dt>
    <dd>
      Check that the credential holds the header's whole value, prefix included, such as
      <code>Bearer &lt;token&gt;</code>, and that the store needs no other header, such as a
      tenant's.
    </dd>
    <dt><strong>Samples are refused as out of order</strong></dt>
    <dd>
      A record arrived older than one of its series already sent, from the source itself, from
      another writer, or from a second worker sending the same series. Send each series from one
      worker, in time order, or turn on the store's out-of-order window.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/sources/prometheus-receiver">Prometheus receiver source</a></li>
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
