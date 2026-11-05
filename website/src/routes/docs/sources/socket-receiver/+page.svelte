<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'framing', title: 'Where a message ends' },
    { id: 'transport', title: 'TCP or UDP' },
    { id: 'settings', title: 'Settings' },
    { id: 'senders', title: 'Who it admits' },
    { id: 'messages', title: 'Messages it takes' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'delivery', title: 'Delivery' },
    { id: 'port', title: 'Open its port' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
  const frame = `00 05  68 65 6c 6c 6f     length 5, then "hello"
00 03  61 62 63           length 3, then "abc"`;
  const header = `01 02  00 05  68 65 6c 6c 6f   a 2-byte header, length 5, then "hello"`;
  const send = `python3 - <<'EOF'
import socket, struct
s = socket.create_connection(("worker1.example.com", 9000))
for m in (b"hello", b"smeltr"):
    s.sendall(struct.pack(">H", len(m)) + m)
s.close()
EOF`;
</script>

<GuideLayout
  title="Socket receiver source"
  description="Take what senders send in a protocol of their own over TCP, TLS or UDP: lines, length-prefixed binary messages or messages of a fixed size. An example, every setting, framings, who it admits, and delivery."
  {sections}
>
  <h1>Socket receiver</h1>
  <p class="lede">
    Takes what senders send to its port in a protocol of their own, over TCP, plain or under TLS, or
    over UDP: lines, length-prefixed binary messages, or messages of a fixed size. Each message is a
    record, its bytes as the sender sent them.
  </p>
  <p>
    Every worker in the group listens on its port, and a sender, or a balancer in front of the
    workers, picks one. Add workers to take more. For syslog, take the
    <a href="/docs/sources/syslog-receiver">syslog receiver</a> instead.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Takes binary messages on port 9000 from senders in <code>10.30.0.0/16</code>, each starting with
    its length in 2 bytes, most significant first. <code>encoding</code> <code>none</code> keeps each
    message as bytes, with no text decoded from it.
  </p>
  <pre><code>{@html highlight(json(examples.socketReceiverSource))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Find out how your sender ends each message. See <a href="#framing">Where a message ends</a>.
    </li>
    <li>
      Pick TCP or UDP, and a port no other receiver of that transport in the worker group uses.
    </li>
    <li>
      Decide how senders are admitted: the networks they connect from, a client certificate, or
      both. See <a href="#senders">Who it admits</a>.
    </li>
    <li>
      For TLS, add the receiver's <a href="/docs/certificates#connectors">certificate</a> and key under
      Admin → Certificates, and give each sender the authority that signed that certificate.
    </li>
    <li>
      Open the port on every worker. See <a href="#port">Open its port</a>.
    </li>
  </ul>

  <h2 id="framing">Where a message ends</h2>
  <p>
    Over TCP, <code>framing</code> says where one message ends and the next starts. Over UDP each datagram
    is one message, and the leader refuses a framing.
  </p>
  <ul>
    <li>
      <strong><code>lines</code></strong>, the default: one message per line. A line ends at a
      newline, and a carriage return before it is dropped. With <code>lineBreaker</code>, a message
      ends where its pattern matches instead.
    </li>
    <li>
      <strong><code>lengthPrefix</code></strong>: each message carries its own length, in
      <code>bytes</code> 1 to 8 bytes long, after <code>offset</code> bytes of header. The length is
      big-endian unless <code>order</code> is <code>little</code>. It counts the bytes after it,
      unless
      <code>counts</code> is <code>message</code>, when it counts the whole message, its header and
      length included.
    </li>
    <li><strong><code>fixed</code></strong>: every message is <code>fixedBytes</code> bytes.</li>
    <li>
      <strong><code>object</code></strong>: all a sender sends before it hangs up is one message.
    </li>
    <li>
      <strong><code>escapedLines</code></strong>: one message per line, unescaped as a destination
      with format <code>bytes</code> and newlines <code>escape</code> wrote it.
    </li>
  </ul>
  <p>Two messages as <code>lengthPrefix</code> with <code>bytes</code> 2 reads them, in hex:</p>
  <pre><code>{frame}</code></pre>
  <p>
    With <code>offset</code> 2, the length comes after 2 bytes of header, which stay part of the message:
  </p>
  <pre><code>{header}</code></pre>

  <h2 id="transport">TCP or UDP</h2>
  <p>
    <code>transport</code> is <code>tcp</code> unless the receiver gives <code>udp</code>. Take TCP
    wherever the sender can send it. Over UDP:
  </p>
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
      <strong>Spoofable senders.</strong> <code>allowFrom</code> is the only thing that admits a UDP sender,
      and a datagram's source address is whatever its sender writes.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="socketReceiver" />

  <h2 id="senders">Who it admits</h2>
  <p>
    Anyone a receiver admits can put records into the worker group's pipelines. What it receives
    carries no token, so a receiver needs <code>allowFrom</code>, a <code>certificate</code> and
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
      <a href="/docs/certificates#connectors">certificate</a>, the receiver serves TLS with it. With
      <code>trustedAuthorities</code>, every sender must present a certificate that chains to one of
      them.
    </li>
  </ul>
  <p>With both, a sender must connect from a listed network and present a certificate.</p>
  <p>
    <code>allowFrom</code> checks the address the connection or datagram comes from. A balancer or address
    translation in front of the workers makes that the balancer's address, unless it keeps the sender's.
  </p>

  <h2 id="messages">Messages it takes</h2>
  <ul>
    <li>
      A message larger than <code>maxRequestBytes</code> is cut to that size. The messages after it arrive
      as usual.
    </li>
    <li>
      A length over 64 MiB, or one shorter than its own header, means the sender and the receiver
      disagree on the framing. The receiver closes the connection, keeping the messages before it.
    </li>
    <li>
      A message the sender hung up partway through is dropped. A TLS sender that hangs up without
      saying so first is taken to have hung up partway.
    </li>
    <li>An empty datagram gives no record.</li>
  </ul>

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    Each message is a record. Its <code>__source.bytes</code> is the message as the sender sent it,
    and its <code>_raw</code> is that decoded as <code>encoding</code> says, or absent with
    <code>encoding</code> <code>none</code>. <code>__source.remote</code> holds the address the
    sender's connection or datagram came from. A destination with format <code>bytes</code> writes each
    message's bytes as they arrived.
  </p>
  <FieldsTable names={kind(SOURCES, 'socketReceiver').fields ?? []} />

  <h2 id="delivery">Delivery</h2>
  <p>
    The receiver never tells a sender a message arrived, so a sender counts a message delivered once
    it has written it to the connection.
  </p>
  <ul>
    <li>
      Delivery is best-effort. What a connection carries when it is cut is lost, and so is what a
      worker holds and has not yet delivered when it stops.
    </li>
    <li>
      A receiver that is full, or <a href="/docs/delivery#hold">blocked</a>, stops reading from its
      connections. The sender's TCP window fills, and the sender waits or drops by its own rules.
    </li>
    <li>
      Over UDP nothing slows the sender. A receiver that is full or blocked drops each datagram that
      arrives, and counts it in
      <a href="/docs/logs-metrics#metrics"><code>smeltr_source_dropped_total</code></a>.
    </li>
  </ul>

  <h2 id="port">Open its port</h2>
  <p>
    Every worker in the group listens on the receiver's <code>port</code>. Open that port to senders
    on each worker, for TCP or UDP as its <code>transport</code> says.
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
      From a host in <code>allowFrom</code>, send two messages framed as in the
      <a href="#example">example</a>:
      <pre><code>{send}</code></pre>
    </li>
    <li>
      Find two records at the destination, their <code>__source.bytes</code> <code>hello</code> and
      <code>smeltr</code>.
    </li>
  </ol>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The sender connects and is closed at once</strong></dt>
    <dd>
      Its address is in none of the networks <code>allowFrom</code> lists. Behind a balancer or address
      translation, the address is the balancer's.
    </dd>
    <dt><strong>The connection is closed partway, and later messages are missing</strong></dt>
    <dd>
      The receiver read a length it could not take: over 64 MiB, or shorter than its header. Check
      <code>bytes</code>, <code>offset</code>, <code>order</code> and <code>counts</code> against what
      the sender writes.
    </dd>
    <dt><strong>Records hold the wrong bytes, or two messages in one</strong></dt>
    <dd>
      The framing does not match the sender's. Capture a few records and compare their bytes with
      the protocol's description.
    </dd>
    <dt><strong>A long message arrives cut short</strong></dt>
    <dd>
      The message is larger than <code>maxRequestBytes</code>. Raise it.
    </dd>
    <dt><strong>The last message arrives only when the sender hangs up</strong></dt>
    <dd>
      With <code>lines</code>, a line arrives at its newline, and the last one without a newline at
      the hang-up. With <code>object</code>, everything arrives at the hang-up.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/syslog-receiver">Syslog receiver</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
