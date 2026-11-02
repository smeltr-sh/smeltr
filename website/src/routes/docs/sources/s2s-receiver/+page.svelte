<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'forwarder', title: 'Point a forwarder at it' },
    { id: 'events', title: 'How lines become events' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'senders', title: 'Tokens and certificates' },
    { id: 'acks', title: 'Acknowledgement' },
    { id: 'full', title: 'When it is full' },
    { id: 'clients', title: 'Forwarders it takes' },
    { id: 'port', title: 'Open its port' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const outputs = `[tcpout]
defaultGroup = smeltr

[tcpout:smeltr]
server = worker1:9997, worker2:9997
useACK = true
token = 0d2a5f6c-4b1e-4c3a-9f7d-2e8b6a1c5d40`;
</script>

<GuideLayout
  title="S2S receiver source"
  description="Take what Splunk forwarders send over Splunk-to-Splunk (S2S): an example, every setting, the forwarder's outputs.conf, how lines become events, and how to open its port."
  {sections}
>
  <h1>S2S receiver</h1>
  <p class="lede">Takes what Splunk forwarders send over Splunk-to-Splunk (S2S).</p>
  <p>Every worker in the group listens on its port. Add workers to take more.</p>

  <h2 id="example">Example</h2>
  <p>
    Takes forwarders on port 9997, over TLS with the
    <a href="/docs/certificates#connectors">certificate</a> <code>s2s-tls</code>. Forwarders present
    the token held in the worker-host variable <code>S2S_TOKEN</code>. An event of the sourcetype
    <code>java:app</code> starts at a line that starts with a date and holds the lines after it. The
    sourcetype <code>access_combined</code> breaks at every line end, as every sourcetype not named does,
    so its row is there only to show the default.
  </p>
  <ProductScreenshot
    source="s2s-receiver-source-form"
    height={2000}
    crop={{ x: 608, y: 508, width: 1664, height: 988 }}
    alt="The S2S receiver form on the Sources page: id forwarders, port 9997, no tokens yet, the sourcetype java:app with its date line breaker, and access_combined with ([\r\n]+)."
    caption="The same source in the Sources page's add dialog, with its sourcetypes, before you add its token and certificate."
  />
  <p>A forwarder sends to it with this outputs.conf:</p>
  <pre><code>{outputs}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Pick a port no other receiver in the worker group uses.</li>
    <li>
      Decide how forwarders prove who they are: a token, a client certificate, or both. See
      <a href="#senders">Tokens and certificates</a>.
    </li>
    <li>Put each token in a <a href="/docs/secrets">credential</a>.</li>
    <li>
      List only the sourcetypes whose events span several lines, such as stack traces, or break
      somewhere other than a line end. Every other sourcetype is an event per line. See
      <a href="#events">How lines become events</a>.
    </li>
    <li>
      Open the port on every worker, and give forwarders the address of each. See
      <a href="#port">Open its port</a>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="s2sReceiver" />

  <h2 id="forwarder">Point a forwarder at it</h2>
  <p>
    In the forwarder's outputs.conf, a <code>[tcpout]</code> group's <code>server</code> names every
    worker on the receiver's port, as in the <a href="#example">example</a>. Splunk's
    <a
      href="https://help.splunk.com/en/splunk-enterprise/forward-and-process-data/universal-forwarder-manual/10.6/forward-data/configure-forwarding-with-outputs.conf"
      >Configure forwarding with outputs.conf</a
    > covers the file.
  </p>
  <ul>
    <li>
      <code>useACK = true</code> makes delivery at-least-once. See
      <a href="#acks">Acknowledgement</a>.
    </li>
    <li>
      <code>token</code> is the value one of the receiver's <code>tokens</code> holds. Leave it out when
      the receiver has no tokens.
    </li>
    <li>
      <code>compressed = true</code>, when set, needs no change on the receiver: it takes compressed
      and uncompressed connections alike.
    </li>
  </ul>

  <h2 id="events">How lines become events</h2>
  <p>
    The receiver breaks what a forwarder sends into events by the rule <code>sourcetypes</code> names
    for its sourcetype:
  </p>
  <ul>
    <li>
      A sourcetype not named in <code>sourcetypes</code> is an event per line, so most need no row.
    </li>
    <li>
      A line ends at <code>\n</code>, <code>\r\n</code> or a lone <code>\r</code>.
    </li>
    <li>
      A sourcetype named in <code>sourcetypes</code> ends an event where its
      <code>lineBreaker</code>'s first capture group matches. What the group matched is dropped;
      what follows it starts the next event, so the pattern can say how an event starts. The pattern
      needs a capture group and must not match the empty string.
    </li>
    <li>An event that arrives already broken is one record as it came.</li>
    <li>
      A line still unfinished when its connection ends, or after 30 seconds with no more of it from
      a forwarder with <code>useACK</code>, is an event as it stands. A line longer than
      <code>maxRequestBytes</code> is cut at that length.
    </li>
  </ul>
  <p>
    A sourcetype not named, or one whose line breaker is left blank, breaks as
    <code>{'([\\r\\n]+)'}</code> does: at every line end, with no event for a blank line.
  </p>
  <p>
    The line breaker <code>{'([\\r\\n]+)\\d{4}-\\d{2}-\\d{2}'}</code> keeps each Java stack trace
    with the log line above it: it breaks only at line ends followed by a date, and only the log
    line starts with one. The line breaker <code>{'</event>(\\s*)'}</code> makes an event of each XML
    event in a stream that has no line ends between them.
  </p>

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    Each event is a record, its text in <code>_raw</code>. The host, source, sourcetype, index and
    time the forwarder files it under, and its indexed fields, become <code>__source</code> fields.
    <code>__source.remote</code> holds the IP address the forwarder's connection came from.
  </p>
  <FieldsTable names={kind(SOURCES, 's2sReceiver').fields ?? []} />

  <h2 id="senders">Tokens and certificates</h2>
  <p>
    Anyone a receiver admits can put records into the worker group's pipelines, so a receiver needs
    <code>tokens</code>, an <code>certificate</code> and <code>trustedAuthorities</code>, or both.
    The leader refuses a receiver with neither.
  </p>
  <ul>
    <li>
      <strong>Tokens:</strong> a forwarder presents one as its outputs.conf <code>token</code>. The
      receiver closes the connection of a forwarder with no token or a wrong one before admitting
      any of its data. Without a certificate, the token crosses the network in the clear.
    </li>
    <li>
      <strong>Client certificates:</strong> with a
      <a href="/docs/certificates#connectors">certificate</a>, the receiver serves TLS with it. With
      <code>trustedAuthorities</code>, every forwarder must present a certificate that chains to one
      of them.
    </li>
  </ul>
  <p>With both, a forwarder presents a certificate and a token.</p>

  <h2 id="acks">Acknowledgement</h2>
  <p>
    The receiver acknowledges an event once every record holding its bytes has reached every
    destination. Set <code>useACK = true</code> on every forwarder whose events must all arrive; an
    event can then arrive twice, never be lost. Splunk's
    <a
      href="https://help.splunk.com/en/splunk-enterprise/forward-and-process-data/forwarding-and-receiving-data/10.2/perform-advanced-configuration/protect-against-loss-of-in-flight-data"
      >Protect against loss of in-flight data</a
    > covers the forwarder's side.
  </p>

  <h2 id="full">When it is full</h2>
  <p>
    A receiver that is full, or <a href="/docs/delivery#hold">blocked</a>, stops reading from its
    connections until its destinations take records again. Name every worker in the forwarder's
    <code>server</code>, so it has others to send to.
  </p>

  <h2 id="clients">Forwarders it takes</h2>
  <ul>
    <li>
      A forwarder sending with <code>tcpout</code> and its default protocol, compressed or not.
    </li>
    <li>A forwarder set to <code>enableOldS2SProtocol = true</code> is refused.</li>
    <li>
      A forwarder's <code>httpout</code> posts S2S to <code>/services/collector/s2s</code>, which
      the
      <a href="/docs/sources/hec-receiver">HEC receiver</a> serves.
    </li>
  </ul>

  <h2 id="port">Open its port</h2>
  <p>
    Every worker in the group listens on the receiver's <code>port</code>. Open that port to
    forwarders on each worker, and name every worker in the forwarder's <code>server</code>.
  </p>
  <ul>
    <li>
      On Kubernetes, add the port to the worker pods:
      <a href="/docs/enroll-worker/kubernetes#receiver">Expose a receiver</a>.
    </li>
    <li>
      On a host or in a container, open the host's firewall or publish the container's port:
      <a href="/docs/enroll-worker/host">Enroll a worker on a host</a>.
    </li>
  </ul>

  <h2 id="verify">Verify</h2>
  <ol>
    <li>
      Point a forwarder at the receiver, as in the <a href="#forwarder">outputs.conf above</a>.
    </li>
    <li>Append a line you can recognize to a file the forwarder monitors.</li>
    <li>Find it at the destination, with its host, source and sourcetype.</li>
  </ol>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The forwarder cannot connect</strong></dt>
    <dd>
      Check that the port is open on every worker and that the forwarder's addresses reach them. A
      receiver with <code>trustedAuthorities</code> refuses a connection without a certificate that chains
      to one of them.
    </dd>
    <dt><strong>The receiver closes the forwarder's connections at once</strong></dt>
    <dd>
      The forwarder has no token, or one no credential of the receiver's <code>tokens</code> holds.
      Set its outputs.conf <code>token</code> to one the receiver holds.
    </dd>
    <dt><strong>The forwarder is refused on every connection</strong></dt>
    <dd>
      Check its outputs.conf for <code>enableOldS2SProtocol = true</code>, and remove it. The
      receiver takes only the forwarder's default protocol.
    </dd>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      Check the source's status. A blocked receiver reads nothing until its destinations take
      records again. A full one takes more with more workers.
    </dd>
    <dt><strong>A multi-line event arrives as one record per line</strong></dt>
    <dd>
      Name its sourcetype in <code>sourcetypes</code> with a <code>lineBreaker</code> that matches
      only line ends before an event's first line, such as <code>{'([\\r\\n]+)\\d{4}-'}</code>.
    </dd>
    <dt><strong>Many events arrive as one record</strong></dt>
    <dd>
      No line end separates them. Name their sourcetype in
      <code>sourcetypes</code> with a <code>lineBreaker</code> that matches between two events, such
      as <code>{'</event>(\\s*)'}</code>.
    </dd>
    <dt><strong>A line arrives cut in two, and the worker warns that a line was cut</strong></dt>
    <dd>
      No more of the line arrived for 30 seconds, as when its file stopped growing mid-line, or when
      the source's destinations are slow. A line longer than <code>maxRequestBytes</code> is cut at that
      length.
    </dd>
    <dt><strong>Events are missing after a worker stopped</strong></dt>
    <dd>
      Set <code>useACK = true</code> on the forwarder. See <a href="#acks">Acknowledgement</a>.
    </dd>
    <dt><strong>The same events arrive twice</strong></dt>
    <dd>
      With <code>useACK</code>, an event not acknowledged when its connection was cut can arrive
      again. See <a href="#acks">Acknowledgement</a>.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/hec-receiver">HEC receiver source</a></li>
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
