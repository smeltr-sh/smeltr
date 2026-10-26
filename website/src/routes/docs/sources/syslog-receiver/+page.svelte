<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  import parserExamples from '#lib/steps/parser.json?raw';
  import { stepExamples } from '#lib/step-pages.ts';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'transport', title: 'TCP or UDP' },
    { id: 'settings', title: 'Settings' },
    { id: 'senders', title: 'Who it admits' },
    { id: 'rsyslog', title: 'Send from rsyslog' },
    { id: 'syslog-ng', title: 'Send from syslog-ng' },
    { id: 'messages', title: 'Messages it takes' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'parse', title: 'Parse the header' },
    { id: 'delivery', title: 'Delivery' },
    { id: 'port', title: 'Open its port' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
  const rsyslogTls = `global(
  DefaultNetstreamDriver="gtls"
  DefaultNetstreamDriverCAFile="/etc/rsyslog.d/smeltr-ca.pem"
)

action(
  type="omfwd"
  target="worker1.example.com"
  port="6514"
  protocol="tcp"
  TCP_Framing="octet-counted"
  StreamDriver="gtls"
  StreamDriverMode="1"
  StreamDriverAuthMode="x509/name"
  StreamDriverPermittedPeers="workers.example.com"
  template="RSYSLOG_SyslogProtocol23Format"
  queue.type="LinkedList"
  queue.filename="smeltr"
  queue.saveOnShutdown="on"
  action.resumeRetryCount="-1"
)`;
  const rsyslogPlain = `action(
  type="omfwd"
  target="worker1.example.com"
  port="1514"
  protocol="tcp"
  TCP_Framing="octet-counted"
  template="RSYSLOG_SyslogProtocol23Format"
)`;
  const rsyslogUdp = `action(
  type="omfwd"
  target="worker1.example.com"
  port="514"
  protocol="udp"
  template="RSYSLOG_SyslogProtocol23Format"
)`;
  const syslogNg = `destination d_smeltr {
  syslog("worker1.example.com"
    transport("tls")
    port(6514)
    tls(ca-file("/etc/syslog-ng/smeltr-ca.pem"))
  );
};

log { source(s_src); destination(d_smeltr); };`;
  const syslogNgUdp = `destination d_smeltr {
  network("worker1.example.com"
    transport("udp")
    port(514)
  );
};`;
  const rfc5424 = `<165>1 2026-10-11T22:14:15.003Z fw01.example.com sshd 4123 AUTH [origin ip="10.20.0.7"] Accepted publickey for deploy`;
  const rfc3164 = `<34>Oct 11 22:14:15 fw01 su: 'su root' failed for deploy on /dev/pts/8`;
  // Run against the engine by `tests/website-reference-tests.rs`.
  const parser = stepExamples('parser', parserExamples).grokSyslog;
</script>

<GuideLayout
  title="Syslog receiver source"
  description="Take what syslog senders send over TCP, TLS or UDP: an example, every setting, who it admits, rsyslog and syslog-ng configurations, parsing the header in a pipeline, and delivery."
  {sections}
>
  <h1>Syslog receiver</h1>
  <p class="lede">
    Takes what syslog senders send over TCP, plain or under TLS, or over UDP: rsyslog, syslog-ng,
    network devices and appliances. Each message is a record, exactly as the sender wrote it.
  </p>
  <p>
    Every worker in the group listens on its port, and a sender, or a balancer in front of the
    workers, picks one. Add workers to take more.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Takes syslog on port 6514 over TLS with the
    <a href="/docs/certificates#connectors">certificate</a> <code>syslog-tls</code>, from senders in
    <code>10.20.0.0/16</code> and from the one host <code>192.168.1.20</code>.
  </p>
  <pre><code>{@html highlight(json(examples.syslogReceiverSource))}</code></pre>
  <ProductScreenshot
    source="syslog-receiver-source-form"
    height={2000}
    crop={{ x: 608, y: 548, width: 1664, height: 904 }}
    alt="The syslog receiver form on the Sources page: id network-syslog, port 6514, transport tcp, allow from 10.20.0.0/16 and 192.168.1.20."
    caption="The same source in the Sources page's add dialog, before you pick its certificate."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Pick TCP or UDP. See <a href="#transport">TCP or UDP</a>.
    </li>
    <li>Pick a port no other receiver of that transport in the group uses.</li>
    <li>
      Decide how senders are admitted: the networks they connect from, a client certificate, or
      both. See <a href="#senders">Who it admits</a>.
    </li>
    <li>
      For TLS, add the receiver's <a href="/docs/certificates#connectors">certificate</a> and key under
      Admin → Certificates, and give each sender the authority that signed that certificate.
    </li>
    <li>
      Open the port on every worker, and give senders the address of each, or of a balancer in front
      of them. See <a href="#port">Open its port</a>.
    </li>
  </ul>

  <h2 id="transport">TCP or UDP</h2>
  <p>
    <code>transport</code> is <code>tcp</code> unless the receiver gives <code>udp</code>. Take TCP
    wherever the sender can send it. Take UDP for senders that send only UDP.
  </p>
  <p>Over UDP each datagram is one message (RFC 5426). UDP costs this:</p>
  <ul>
    <li>
      <strong>No TLS.</strong> Messages cross the network in the clear, and the leader refuses a UDP
      receiver that names a <code>certificate</code>.
    </li>
    <li>
      <strong>No backpressure.</strong> Nothing slows a UDP sender. A datagram that arrives while
      the receiver is full or <a href="/docs/delivery#hold">blocked</a> is dropped, and counted in
      <a href="/docs/logs-metrics#metrics"><code>smeltr_source_dropped_total</code></a>.
    </li>
    <li>
      <strong>Spoofable senders.</strong> <code>allowFrom</code> is the only thing that admits a UDP
      sender, and a datagram's source address is whatever its sender writes. See
      <a href="#senders">Who it admits</a>.
    </li>
  </ul>
  <p>
    A TCP and a UDP receiver may share a port number, so one group can take syslog on TCP 514 and
    UDP 514.
  </p>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="syslogReceiver" />

  <h2 id="senders">Who it admits</h2>
  <p>
    Anyone a receiver admits can put records into the group's pipelines. Syslog carries no token, so
    a receiver needs <code>allowFrom</code>, an <code>certificate</code> and
    <code>trustedAuthorities</code>, or both. The leader refuses a receiver with neither.
  </p>
  <ul>
    <li>
      <strong>Networks:</strong> <code>allowFrom</code> lists the networks a sender may connect
      from, as CIDR blocks such as <code>10.0.0.0/8</code> or as single addresses. The receiver closes
      a connection from any other address before reading any of it.
    </li>
    <li>
      <strong>Client certificates:</strong> with a
      <a href="/docs/certificates#connectors">certificate</a>, the receiver serves TLS (RFC 5425)
      with it. With
      <code>trustedAuthorities</code>, every sender must present a certificate that chains to one of
      them. With <code>allowFrom</code> empty, every sender TLS admits is admitted.
    </li>
  </ul>
  <p>With both, a sender must connect from a listed network and present a certificate.</p>
  <p>
    Over UDP there is no TLS, so <code>allowFrom</code> is required and is all that admits a sender.
    The receiver drops a datagram from any other address. It is weaker than over TCP: a TCP sender
    must answer from its address to connect, but a datagram's source address is whatever its sender
    writes, so anyone who can reach the port can claim an address in <code>allowFrom</code>. Take
    UDP only where the network between senders and workers drops datagrams that forge their source,
    and take TCP with a certificate wherever a sender offers it.
  </p>
  <p>
    <code>allowFrom</code> checks the address the connection or datagram comes from. A balancer or address
    translation in front of the workers makes that the balancer's address, unless it keeps the sender's.
  </p>
  <p>
    Without a certificate, messages cross the network in the clear. Serve TLS wherever the network
    between senders and workers is not trusted.
  </p>

  <h2 id="rsyslog">Send from rsyslog</h2>
  <p>
    Use rsyslog's
    <a href="https://www.rsyslog.com/doc/configuration/modules/omfwd.html">omfwd</a> action with
    <code>TCP_Framing="octet-counted"</code>, so a message holding a newline arrives as one record.
    This example sends over TLS, checks the receiver's certificate against
    <code>smeltr-ca.pem</code>, and queues on disk while the receiver is out of reach:
  </p>
  <pre><code>{rsyslogTls}</code></pre>
  <p>
    For a client certificate, add <code>DefaultNetstreamDriverCertFile</code> and
    <code>DefaultNetstreamDriverKeyFile</code> to <code>global</code>. Without TLS, leave out the
    <code>StreamDriver</code> settings and send to the plain port:
  </p>
  <pre><code>{rsyslogPlain}</code></pre>
  <p>
    To a UDP receiver, use <code>protocol="udp"</code>:
  </p>
  <pre><code>{rsyslogUdp}</code></pre>

  <h2 id="syslog-ng">Send from syslog-ng</h2>
  <p>
    This example uses syslog-ng's
    <a href="https://syslog-ng.github.io/admin-guide/070_Destinations/170_syslog/README">syslog()</a
    >
    driver over TLS and checks the receiver's certificate against <code>smeltr-ca.pem</code>:
  </p>
  <pre><code>{syslogNg}</code></pre>
  <p>
    For a client certificate, add <code>cert-file()</code> and <code>key-file()</code> to
    <code>tls()</code>. Without TLS, use <code>transport("tcp")</code> and the plain port.
  </p>
  <p>To a UDP receiver, use <code>transport("udp")</code>:</p>
  <pre><code>{syslogNgUdp}</code></pre>

  <h2 id="messages">Messages it takes</h2>
  <ul>
    <li>RFC 5424 messages, and RFC 3164 messages, the older BSD format.</li>
    <li>
      Messages are framed by their octet count, as <code>123 &lt;34&gt;1 ...</code>, or end at a
      newline (RFC 6587). Set <code>lineBreaker</code> to end them at its pattern instead.
    </li>
    <li>Plain TCP, or TLS (RFC 5425) when the receiver names a certificate.</li>
    <li>
      Over UDP, one message to a datagram (RFC 5426). A newline, or carriage return and newline,
      that ends the datagram is dropped. An empty datagram gives no record.
    </li>
    <li>
      A message larger than <code>maxRequestBytes</code> closes the connection. The sender connects again
      and goes on from where its own rules take it. A datagram larger than it is dropped.
    </li>
  </ul>
  <p>An RFC 5424 message, as it arrives in <code>_raw</code>:</p>
  <pre><code>{rfc5424}</code></pre>
  <p>An RFC 3164 message:</p>
  <pre><code>{rfc3164}</code></pre>

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    Each message is a record. Its <code>_raw</code> is the message as the sender sent it, its
    <code>&lt;PRI&gt;</code> header included. <code>__source.remote</code> holds the address the sender's
    connection or datagram came from.
  </p>
  <FieldsTable names={kind(SOURCES, 'syslogReceiver').fields ?? []} />

  <h2 id="parse">Parse the header</h2>
  <p>
    The receiver does not parse the header, since every vendor writes syslog its own way. A
    <a href="/docs/steps/parser">Parser step</a> with the
    <a href="/docs/steps/parser#grok-matching">Regex/Grok</a> format reads it in the pipeline, where each
    source's quirks can have a pattern of their own. This step reads an RFC 5424 header with its first
    pattern and an RFC 3164 header with its second:
  </p>
  <pre><code>{@html highlight(json(parser.steps[0]))}</code></pre>
  <p>Given these two messages as <code>_raw</code>:</p>
  {#each parser.input as record, i (i)}
    <pre><code>{record}</code></pre>
  {/each}
  <p>it gives these fields:</p>
  {#each parser.output as record, i (i)}
    <pre><code>{@html highlight(json(record))}</code></pre>
  {/each}
  <p>
    The facility is <code>pri / 8</code> and the severity <code>pri % 8</code>: 165 is facility 20,
    <code>local4</code>, at severity 5, notice. To make <code>time</code> a datetime, declare it
    with a format; see <a href="/docs/steps/parser">Parser step</a>.
  </p>

  <h2 id="delivery">Delivery</h2>
  <p>
    Syslog has no acknowledgement. The receiver never tells a sender a message arrived, so a sender
    counts a message delivered once it has written it to the connection.
  </p>
  <ul>
    <li>
      Delivery is best-effort. What a connection carries when it is cut is lost, and so is what a
      worker holds and has not yet delivered when it stops.
    </li>
    <li>
      A receiver that is full, or <a href="/docs/delivery#hold">blocked</a>, stops reading from its
      connections. The sender's TCP window fills, and the sender buffers or drops by its own rules.
      Give rsyslog a disk-assisted queue, as in the <a href="#rsyslog">example above</a>, or
      syslog-ng a <code>disk-buffer()</code>, so that a pause holds messages rather than losing
      them.
    </li>
    <li>
      Over UDP nothing slows the sender. A receiver that is full or blocked drops each datagram that
      arrives, and counts it in
      <a href="/docs/logs-metrics#metrics"><code>smeltr_source_dropped_total</code></a>. On a Linux
      or other Unix host, datagrams wait in the host's receive buffer while a worker deploys a new
      configuration, until that buffer fills.
    </li>
  </ul>

  <h2 id="port">Open its port</h2>
  <p>
    Every worker in the group listens on the receiver's <code>port</code>. Open that port to senders
    on each worker, for TCP or UDP as its <code>transport</code> says. A balancer in front of a UDP
    receiver must balance UDP and keep each sender's source address, or <code>allowFrom</code> sees the
    balancer's. A sender names one address: a balancer in front of the workers, or a DNS name that resolves
    to each of them.
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
      Point a sender at the receiver, as in <a href="#rsyslog">Send from rsyslog</a>.
    </li>
    <li>
      On the sender, run <code>logger -t smeltr-test hello</code> to log a line you can recognize.
      To send it straight to a UDP receiver, run
      <code>logger --udp --server worker1.example.com --port 514 -t smeltr-test hello</code>.
    </li>
    <li>Find it at the destination, its <code>_raw</code> starting with its PRI header.</li>
  </ol>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The sender cannot connect</strong></dt>
    <dd>
      Check that the port is open on every worker and that the sender's address reaches them. A
      receiver with <code>trustedAuthorities</code> refuses a connection without a certificate that chains
      to one of them.
    </dd>
    <dt><strong>The sender connects and is closed at once</strong></dt>
    <dd>
      Its address is in none of the networks <code>allowFrom</code> lists. Behind a balancer or address
      translation, the address is the balancer's.
    </dd>
    <dt><strong>A message with several lines arrives as one record per line</strong></dt>
    <dd>
      The sender ends messages with a newline. Set it to frame them by octet count:
      <code>TCP_Framing="octet-counted"</code> in rsyslog, or syslog-ng's
      <code>syslog()</code> driver.
    </dd>
    <dt><strong>The connection closes on a long message</strong></dt>
    <dd>
      The message is larger than <code>maxRequestBytes</code>. Raise it, or have the sender cut its
      messages shorter.
    </dd>
    <dt><strong>Nothing arrives, and the sender's queue grows</strong></dt>
    <dd>
      Check the source's status. A blocked receiver reads nothing until its destinations take
      records again. A full one takes more with more workers.
    </dd>
    <dt>
      <strong>Datagrams are missing, and <code>smeltr_source_dropped_total</code> grows</strong>
    </dt>
    <dd>
      The UDP receiver was full or blocked when they arrived. Check the source's status. A blocked
      receiver takes nothing until its destinations take records again; a full one takes more with
      more workers. A sender that can send TCP loses nothing to a full receiver over it.
    </dd>
    <dt><strong>Datagrams are missing, and nothing is counted</strong></dt>
    <dd>
      The receiver drops a datagram from an address outside <code>allowFrom</code>, and one larger
      than
      <code>maxRequestBytes</code>, before counting anything. A datagram can also be lost on the
      network, or in the host's receive buffer when it fills.
    </dd>
    <dt><strong>Messages are missing after a worker stopped</strong></dt>
    <dd>
      Syslog has no acknowledgement, so what was in flight when the connection was cut is lost. See
      <a href="#delivery">Delivery</a>.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/steps/parser">Parser step</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
