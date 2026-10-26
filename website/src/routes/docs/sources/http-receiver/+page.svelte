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
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'senders', title: 'Tokens and certificates' },
    { id: 'answers', title: 'What it answers' },
    { id: 'health', title: 'Health check' },
    { id: 'port', title: 'Open its port' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="HTTP receiver source"
  description="Take what senders POST to a path on a port every worker listens on: an example, every setting, what it answers, and how to open its port."
  {sections}
>
  <h1>HTTP receiver</h1>
  <p class="lede">
    Takes what senders POST to a path on its port, and answers each request once every destination
    has its records.
  </p>
  <p>
    A webhook, a log shipper or a script sends to it. Every worker of the group listens on the
    receiver's port, and the sender, or a balancer in front of the workers, picks one worker for
    each request. More workers take more.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Takes what a CI system POSTs to <code>/hooks/ci</code> on port 8088, one record per line, over
    TLS with the <a href="/docs/certificates#connectors">certificate</a> <code>receiver-tls</code>.
    Senders present the token held in the worker-host variable <code>HOOK_TOKEN</code>.
  </p>
  <pre><code>{@html highlight(json(examples.httpReceiverSource))}</code></pre>
  <ProductScreenshot
    source="http-receiver-source-form"
    height={2000}
    crop={{ x: 608, y: 486, width: 1664, height: 1028 }}
    alt="The HTTP receiver form on the Sources page: id ci-hooks, port 8088, path /hooks/ci, records one per line, no tokens yet."
    caption="The same source in the Sources page's add dialog, before you add its token and certificate."
  />
  <p>A sender posts a file of lines like this:</p>
  <pre><code
      >curl --data-binary @build.log \
  -H "Authorization: Bearer $HOOK_TOKEN" \
  https://workers.example.com:8088/hooks/ci</code
    ></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Pick a port no other receiver of the group listens on.</li>
    <li>
      Decide how senders prove who they are: a token, a client certificate, or both. See
      <a href="#senders">Tokens and certificates</a>.
    </li>
    <li>Put each token in a <a href="/docs/secrets">credential</a>.</li>
    <li>
      Open the port on every worker of the group, and give senders one address that reaches them.
      See
      <a href="#port">Open its port</a>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="httpReceiver" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    Each record holds its text exactly as the sender wrote it. <code>__source.remote</code> holds the
    IP address the request came from: the balancer's, when one is in front of the workers.
  </p>
  <FieldsTable names={kind(SOURCES, 'httpReceiver').fields ?? []} />

  <h2 id="senders">Tokens and certificates</h2>
  <p>
    Whoever a receiver admits puts records into the group's pipelines, so a receiver needs
    <code>tokens</code>, an <code>certificate</code> and <code>authorities</code>, or both. A
    receiver with neither is refused at load.
  </p>
  <ul>
    <li>
      <strong>Tokens:</strong> a sender presents one as
      <code>Authorization: Bearer &lt;token&gt;</code>. Any other request is refused 401 before any
      of its body is read. Without a certificate, the token crosses the network in the clear, which
      suits a receiver behind a balancer that ends TLS for it.
    </li>
    <li>
      <strong>Client certificates:</strong> under a
      <a href="/docs/certificates#connectors">certificate</a>, the receiver serves TLS with it. With
      <code>authorities</code>, every connection must present a certificate that chains to one of
      them, health checks included.
    </li>
  </ul>
  <p>With both, a sender presents a certificate and a token.</p>

  <h2 id="answers">What it answers</h2>
  <p>
    The receiver answers 200 only once every destination has the request's records, so a sender that
    retries an unanswered request loses nothing.
  </p>
  <dl>
    <dt><strong>200</strong></dt>
    <dd>Every destination has the request's records.</dd>
    <dt><strong>503</strong> with <code>Retry-After</code></dt>
    <dd>
      The receiver is full, <a href="/docs/delivery#hold">blocked</a>, or stopping. It answers a new
      request at once, and a stopping receiver answers every request it holds. A sender may send it
      again.
    </dd>
    <dt><strong>401</strong></dt>
    <dd>The request presents none of the receiver's tokens.</dd>
    <dt><strong>400</strong></dt>
    <dd>
      The body does not read as <code>records</code> says, such as one with no array at
      <code>recordsAt</code>. It is refused whole: none of its records is taken.
    </dd>
    <dt><strong>413</strong></dt>
    <dd>The body, or what it inflates to, is larger than <code>maxRequestBytes</code>.</dd>
    <dt><strong>415</strong></dt>
    <dd>
      The body uses a <code>Content-Encoding</code> other than <code>gzip</code> or
      <code>identity</code>. The receiver inflates a gzip body itself.
    </dd>
    <dt><strong>408</strong></dt>
    <dd>The sender did not send the whole body within 30 seconds.</dd>
  </dl>

  <h2 id="health">Health check</h2>
  <p>
    <code>GET /health</code> on the receiver's port answers 200 while the receiver takes requests
    and 503 while it is full, blocked or stopping. It needs no token, and says only whether the
    receiver takes requests. Point a balancer's health check or a readiness probe at it, so a busy
    worker gets no requests until it can take them. With <code>authorities</code> it needs a client certificate
    like any other request.
  </p>

  <h2 id="port">Open its port</h2>
  <p>
    Every worker of the group listens on the receiver's <code>port</code>. Open that port to senders
    on each worker, and give senders one address that reaches all of them, such as a balancer's. On
    a Unix host, a request sent while a worker deploys a new configuration waits for it and is
    answered by the new one.
  </p>
  <ul>
    <li>
      On Kubernetes, add the port to the worker pods and a Service in front of them:
      <a href="/docs/enroll-worker/kubernetes#receiver">Expose a receiver</a>.
    </li>
    <li>
      On a host or in a container, open the host's firewall or publish the container's port:
      <a href="/docs/enroll-worker/host">Enroll a worker on a host</a>.
    </li>
  </ul>

  <h2 id="verify">Verify</h2>
  <p>
    POST a record you can recognize, as in the <a href="#example">example</a>. The receiver answers
    200 once every destination has it. Find it at the destination.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The sender cannot connect</strong></dt>
    <dd>
      Check that the port is open on every worker and that the sender's address reaches them. A
      receiver with <code>authorities</code> refuses a connection with no certificate that chains to them.
    </dd>
    <dt><strong>Every request is answered 401</strong></dt>
    <dd>
      Check that the sender presents <code>Authorization: Bearer &lt;token&gt;</code> with a token one
      of the receiver's credentials holds.
    </dd>
    <dt><strong>Every request is answered 503</strong></dt>
    <dd>
      Check the source's status. A blocked receiver takes nothing until its destinations take
      records again. A full one takes more with more workers.
    </dd>
    <dt><strong>The same records arrive twice</strong></dt>
    <dd>
      A sender that hears no answer may send the request again. Give the sender a timeout longer
      than its destinations take to deliver.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/http-api">HTTP API source</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
