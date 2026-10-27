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
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'senders', title: 'Tokens and certificates' },
    { id: 'answers', title: 'What it answers' },
    { id: 'acks', title: 'Indexer acknowledgement' },
    { id: 'health', title: 'Health check' },
    { id: 'clients', title: 'Senders it takes' },
    { id: 'port', title: 'Open its port' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
  const curl = `curl -H "Authorization: Splunk $HEC_TOKEN" \\
  -d '{"event":"hello","host":"web-1","sourcetype":"demo"}' \\
  https://workers.example.com:8088/services/collector/event`;
  const httpout = `# outputs.conf on the forwarder
[httpout]
httpEventCollectorToken = <a token of the receiver>
uri = https://workers.example.com:8088`;
  const otel = `exporters:
  splunk_hec:
    token: \${env:HEC_TOKEN}
    endpoint: https://workers.example.com:8088/services/collector
    source: otel
    sourcetype: otel

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [splunk_hec]`;
</script>

<GuideLayout
  title="HEC receiver source"
  description="Take what Splunk HTTP Event Collector clients send to a port every worker listens on: an example, every setting, what it answers, which senders work, and how to open its port."
  {sections}
>
  <h1>HEC receiver</h1>
  <p class="lede">
    Takes what Splunk HTTP Event Collector (HEC) clients send to its port, at Splunk's paths. It
    answers each request once every destination has its events.
  </p>
  <p>
    OpenTelemetry Collectors, Fluent Bit, Vector, logging libraries and Splunk forwarders' <code
      >httpout</code
    > send to it. Every worker in the group listens on its port, and the sender, or a balancer in front
    of the workers, picks a worker for each request. Add workers to take more.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Takes HEC events on port 8088, over TLS with the
    <a href="/docs/certificates#connectors">certificate</a> <code>receiver-tls</code>. Senders
    present the token held in the worker-host variable <code>HEC_TOKEN</code>.
  </p>
  <pre><code>{@html highlight(json(examples.hecReceiverSource))}</code></pre>
  <ProductScreenshot
    source="hec-receiver-source-form"
    height={2000}
    crop={{ x: 608, y: 638, width: 1664, height: 724 }}
    alt="The HEC receiver form on the Sources page: id otel-logs, port 8088, no tokens yet."
    caption="The same source in the Sources page's add dialog, before you add its token and certificate."
  />
  <p>A sender posts an event like this:</p>
  <pre><code>{curl}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Pick a port no other receiver in the group uses.</li>
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
  <KindSettings group={SOURCES} type="hecReceiver" />

  <h2 id="paths">Paths it serves</h2>
  <dl>
    <dt>
      <strong>POST</strong> <code>/services/collector</code>,
      <code>/services/collector/event</code>,
      <code>/services/collector/event/1.0</code>
    </dt>
    <dd>
      The body holds HEC envelopes back to back, such as
      <code>{'{"event":"hello","host":"web-1"}'}</code>, with or without whitespace between them.
      Each envelope becomes one record: a string <code>event</code> becomes the record's text, and
      any other <code>event</code> its JSON. The envelope's <code>host</code>, <code>source</code>,
      <code>sourcetype</code>, <code>index</code>, <code>time</code> and <code>fields</code> become
      <a href="#fields-it-gives"><code>__source</code> fields</a>.
    </dd>
    <dt>
      <strong>POST</strong> <code>/services/collector/raw</code>,
      <code>/services/collector/raw/1.0</code>
    </dt>
    <dd>
      The body is text, one record per line. The query's <code>host</code>, <code>source</code>,
      <code>sourcetype</code>, <code>index</code> and <code>time</code> apply to every line, as in
      <code>/services/collector/raw?sourcetype=syslog</code>. Matching sourcetypes apply their
      linebreaker to replace the default of
      <code>([\r\n]+)</code>.
    </dd>
    <dt><strong>POST</strong> <code>/services/collector/s2s</code></dt>
    <dd>
      What a Splunk forwarder's <code>httpout</code> posts: S2S, as its <code>tcpout</code> sends to
      an
      <a href="/docs/sources/s2s-receiver">S2S receiver</a>, compressed. Each request is whole: its
      channels' events are broken by sourcetype as on the raw path, and the request ends each
      channel's last event. A record carries what the forwarder parsed, as an S2S receiver's does.
      See
      <a href="#clients">Senders it takes</a>.
    </dd>
    <dt>
      <strong>GET</strong> <code>/services/collector/health</code>,
      <code>/services/collector/health/1.0</code>
    </dt>
    <dd>Says whether the receiver takes requests. See <a href="#health">Health check</a>.</dd>
    <dt>
      <strong>POST</strong> <code>/services/collector/ack</code>,
      <code>/services/collector/ack/1.0</code>
    </dt>
    <dd>
      For a sender whose token has <code>acknowledgement</code>: takes
      <code>{'{"acks":[0,1]}'}</code> on a channel and answers
      <code>{'{"acks":{"0":true,"1":false}}'}</code>. See
      <a href="#acks">Indexer acknowledgement</a>.
    </dd>
  </dl>
  <p>
    <code>time</code> is Unix seconds, as a number or as text, and <code>fields</code> is a JSON
    object. The receiver inflates a body sent with <code>Content-Encoding: gzip</code>.
  </p>

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    A field the sender did not set is null. <code>__source.remote</code> holds the IP address the request
    came from: the balancer's, when one is in front of the workers.
  </p>
  <FieldsTable names={kind(SOURCES, 'hecReceiver').fields ?? []} />

  <h2 id="senders">Tokens and certificates</h2>
  <p>
    Anyone a receiver admits can put records into the group's pipelines, so a receiver needs
    <code>tokens</code>, an <code>certificate</code> and <code>trustedAuthorities</code>, or both.
    The leader refuses a receiver with neither.
  </p>
  <ul>
    <li>
      <strong>Tokens:</strong> a sender presents one as
      <code>Authorization: Splunk &lt;token&gt;</code>, as HEC clients do. The receiver refuses any
      other request before reading its body. Without a certificate, the token crosses the network in
      the clear, so use a token alone only behind a balancer that terminates TLS.
    </li>
    <li>
      <strong>Client certificates:</strong> with a
      <a href="/docs/certificates#connectors">certificate</a>, the receiver serves TLS with it. With
      <code>trustedAuthorities</code>, every connection, health checks included, must present a
      certificate that chains to one of them.
    </li>
  </ul>
  <p>With both, a sender presents a certificate and a token.</p>

  <h2 id="answers">What it answers</h2>
  <p>
    The receiver answers in HEC's JSON, <code>{'{"text": ..., "code": ...}'}</code>. The receiver
    answers success only once every destination has the request's events, so a sender that retries
    an unanswered request loses nothing.
  </p>
  <dl>
    <dt><strong>200</strong>, code <code>0</code></dt>
    <dd>
      <code>{'{"text":"Success","code":0}'}</code>: every destination has the request's events.
    </dd>
    <dt><strong>503</strong>, code <code>9</code>, with <code>Retry-After</code></dt>
    <dd>
      The receiver is full, <a href="/docs/delivery#hold">blocked</a> or stopping. It refuses a new request
      at once, and a stopping receiver answers every request it holds. A sender may send it again.
    </dd>
    <dt><strong>401</strong>, code <code>2</code> or <code>3</code></dt>
    <dd>
      The request has no token (<code>2</code>), or gives one other than as
      <code>Splunk &lt;token&gt;</code> (<code>3</code>).
    </dd>
    <dt><strong>403</strong>, code <code>4</code></dt>
    <dd>The receiver does not hold the token.</dd>
    <dt>
      <strong>400</strong>, code <code>5</code>, <code>6</code>, <code>12</code>, <code>13</code> or
      <code>15</code>
    </dt>
    <dd>
      The body is empty (<code>5</code>), is not HEC envelopes, or at
      <code>/services/collector/s2s</code> not whole S2S (<code>6</code>), has an envelope without
      an <code>event</code> (<code>12</code>) or with an empty one (<code>13</code>), or has
      <code>fields</code> that are not an object (<code>15</code>).
      <code>invalid-event-number</code> names the faulty envelope, counting from 0. The receiver refuses
      the whole body and takes none of its events.
    </dd>
    <dt><strong>400</strong>, code <code>10</code>, <code>11</code> or <code>14</code></dt>
    <dd>
      A sender whose token has <code>acknowledgement</code> named no channel (<code>10</code>), a
      channel is not 1 to 128 letters, digits, <code>-</code> and <code>_</code> (<code>11</code>),
      or a sender whose token lacks <code>acknowledgement</code> asked after an
      <code>ackId</code> (<code>14</code>, ACK is disabled).
    </dd>
    <dt><strong>413</strong></dt>
    <dd>
      The body, or what it inflates to, is larger than <code>maxRequestBytes</code>, or a
      forwarder's
      <code>httpout</code> request holds more than the receiver holds at once.
    </dd>
    <dt><strong>408</strong></dt>
    <dd>The sender took more than 30 seconds to send the whole body.</dd>
    <dt><strong>415</strong></dt>
    <dd>
      The body uses a <code>Content-Encoding</code> other than <code>gzip</code> or
      <code>identity</code>.
    </dd>
  </dl>

  <h2 id="acks">Indexer acknowledgement</h2>
  <p>
    Each token sets its own acknowledgement. Set
    <code>acknowledgement: true</code> on the token of a sender that has indexer acknowledgement turned
    on:
  </p>
  <pre><code
      >{@html highlight(
        json({
          vector: { token: 'VECTOR_HEC_TOKEN', acknowledgement: true },
          fluentbit: { token: 'FLUENTBIT_HEC_TOKEN' },
        }),
      )}</code
    ></pre>
  <p>
    That sender names a channel, in the <code>X-Splunk-Request-Channel</code> header or the
    <code>channel</code> query. The receiver answers at once with an ID,
    <code>{'{"text":"Success","code":0,"ackId":7}'}</code>, and the sender asks after it at
    <code>/services/collector/ack</code>. The ID reads <code>true</code> once every destination has the
    request's events, and the receiver forgets it once the sender has heard that.
  </p>
  <p>
    An ID reads <code>false</code> while its events are on their way, and also when the worker asked does
    not hold it, because another worker gave it or the worker restarted. The sender then sends the request
    again: a duplicate, never a loss. A balancer in front of the workers must therefore keep each sender's
    connections on one worker, so a channel's requests and its ack queries reach the same worker.
  </p>
  <p>
    The receiver holds the request of a sender whose token lacks <code>acknowledgement</code> until
    its events are delivered, whether or not it names a channel. A sender that names a channel but
    never asks after an <code>ackId</code> needs such a token.
  </p>

  <h2 id="health">Health check</h2>
  <p>
    <code>GET /services/collector/health</code> answers 200 with code <code>17</code>,
    <code>HEC is healthy</code>, while the receiver takes requests, and 503 with code
    <code>18</code> while it is full, blocked or stopping. It needs no token. Point a balancer's
    health check or a readiness probe at it, so a busy worker gets no requests until it can take
    them. With
    <code>trustedAuthorities</code>, it needs a client certificate like any other request.
  </p>

  <h2 id="clients">Senders it takes</h2>
  <p>Any sender that speaks HEC over HTTP works:</p>
  <ul>
    <li>The OpenTelemetry Collector's <code>splunk_hec</code> exporter.</li>
    <li>Fluent Bit's <code>splunk</code> output.</li>
    <li>
      Vector's <code>splunk_hec_logs</code> sink. With indexer acknowledgements on, set
      <a href="#acks"><code>acknowledgement</code></a> on its token.
    </li>
    <li>Logging libraries that send to HEC.</li>
  </ul>
  <p>
    A Splunk universal or heavy forwarder's <code>httpout</code> posts S2S to
    <code>/services/collector/s2s</code>, presenting one of the receiver's tokens. The receiver
    holds each request until delivered, whatever its token's <code>acknowledgement</code>. A
    forwarder sending with <code>tcpout</code> needs an
    <a href="/docs/sources/s2s-receiver">S2S receiver</a>
    instead.
  </p>
  <pre><code>{httpout}</code></pre>
  <p>
    This OpenTelemetry Collector configuration sends logs to the receiver, with
    <code>HEC_TOKEN</code> set in the collector's environment:
  </p>
  <pre><code>{otel}</code></pre>

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
    POST an event you can recognize, as in the <a href="#example">example</a>. The receiver answers
    <code>{'{"text":"Success","code":0}'}</code> once every destination has it. Then find it at the destination.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The sender cannot connect</strong></dt>
    <dd>
      Check that the port is open on every worker and that the sender's address reaches them. A
      receiver with <code>trustedAuthorities</code> refuses a connection without a certificate that chains
      to one of them.
    </dd>
    <dt><strong>Every request gets code 2, 3 or 4</strong></dt>
    <dd>
      Check that the sender presents <code>Authorization: Splunk &lt;token&gt;</code>, with a token
      one of the receiver's credentials holds.
    </dd>
    <dt><strong>Every request gets code 9</strong></dt>
    <dd>
      Check the source's status. A blocked receiver takes nothing until its destinations take
      records again. A full one takes more with more workers.
    </dd>
    <dt><strong>Acks never read true, and the sender sends everything again</strong></dt>
    <dd>
      The ack queries reach a different worker from the one that took the request. Keep each
      sender's connections on one worker at the balancer, or turn indexer acknowledgement off in
      both the sender and its token.
    </dd>
    <dt><strong>The sender reports code 10 or 14</strong></dt>
    <dd>
      The sender and its token disagree on indexer acknowledgement. Set
      <code>acknowledgement: true</code> on the token when the sender has it on, and leave it out when
      not.
    </dd>
    <dt><strong>A Splunk forwarder cannot send to it</strong></dt>
    <dd>
      A forwarder sending with <code>tcpout</code> needs an
      <a href="/docs/sources/s2s-receiver">S2S receiver</a>. Point it at one, or switch it to
      <code>httpout</code>, which this receiver takes at <code>/services/collector/s2s</code>.
    </dd>
    <dt><strong>The same events arrive twice</strong></dt>
    <dd>
      A sender that hears no answer may send the request again. Give the sender a timeout longer
      than its destinations take to deliver.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/http-receiver">HTTP receiver source</a></li>
    <li><a href="/docs/destinations/splunk">Splunk destination</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
