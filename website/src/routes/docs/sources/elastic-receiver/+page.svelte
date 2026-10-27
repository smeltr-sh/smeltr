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
  const filebeat = `output.elasticsearch:
  hosts: ["https://workers.example.com:9200"]
  api_key: "\${ES_TOKEN}"
  ssl.certificate_authorities: ["/etc/filebeat/smeltr-ca.pem"]
setup.template.enabled: false
setup.ilm.enabled: false`;
  const bulk = `{"index":{"_index":"web","_id":"a1"}}
{"message":"GET /cart 200","host":"web-1"}
{"create":{"_index":"web"}}
{"message":"GET /cart 500","host":"web-2"}`;
  const answer = `{
  "took": 3,
  "errors": false,
  "items": [
    {
      "index": {
        "_index": "web",
        "_id": "a1",
        "_version": 1,
        "result": "created",
        "_shards": { "total": 1, "successful": 1, "failed": 0 },
        "_seq_no": 0,
        "_primary_term": 1,
        "status": 201
      }
    },
    {
      "create": {
        "_index": "web",
        "_id": "GcEG4pVuTOAAAAAAAAAA",
        "_version": 1,
        "result": "created",
        "_shards": { "total": 1, "successful": 1, "failed": 0 },
        "_seq_no": 0,
        "_primary_term": 1,
        "status": 201
      }
    }
  ]
}`;
  const fluentBit = `[OUTPUT]
    Name               es
    Match              *
    Host               workers.example.com
    Port               9200
    Index              apps
    HTTP_User          fluent
    HTTP_Passwd        \${ES_TOKEN}
    Suppress_Type_Name On
    tls                On
    tls.ca_file        /etc/fluent-bit/smeltr-ca.pem`;
  const curl = `curl -H "Authorization: Bearer $ES_TOKEN" \\
  -H "Content-Type: application/x-ndjson" \\
  --data-binary $'{"index":{"_index":"test"}}\\n{"message":"hello"}\\n' \\
  https://workers.example.com:9200/_bulk`;
</script>

<GuideLayout
  title="Elasticsearch receiver source"
  description="Take what Beats, Fluent Bit and other Elasticsearch clients send to the Bulk API at a port every worker listens on: an example, every setting, the records it gives, what it answers, and which senders work."
  {sections}
>
  <h1>Elasticsearch receiver</h1>
  <p class="lede">
    Takes what Elasticsearch clients send to Elasticsearch's Bulk API at its port. It answers each
    request once every destination has its documents.
  </p>
  <p>
    Filebeat and the other Beats, Fluent Bit and other clients of the Bulk API send to it. Every
    worker in the group listens on its port, and the sender, or a balancer in front of the workers,
    picks a worker for each request. Add workers to take more.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Takes bulk requests on port 9200, over TLS with the
    <a href="/docs/certificates#connectors">certificate</a> <code>receiver-tls</code>. Senders
    present the token held in the worker-host variable <code>ES_TOKEN</code>.
  </p>
  <pre><code>{@html highlight(json(examples.elasticReceiverSource))}</code></pre>
  <ProductScreenshot
    source="elastic-receiver-source-form"
    height={2000}
    crop={{ x: 608, y: 638, width: 1664, height: 724 }}
    alt="The Elasticsearch receiver form on the Sources page: id beats, port 9200, no tokens yet."
    caption="The same source in the Sources page's add dialog, before you add its token and certificate."
  />
  <p>
    Filebeat sends to it with an <code>output.elasticsearch</code> block like this, where
    <code>ES_TOKEN</code> holds an <code>id:key</code> such as <code>beats:k3y</code>, as the
    receiver's credential does:
  </p>
  <pre><code>{filebeat}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Pick a port no other receiver in the group uses, such as 9200.</li>
    <li>
      Turn off the sender's setup of index templates and lifecycle policies. The receiver holds
      neither, and answers <code>404</code> to a request for one. On a Beat, set
      <code>setup.template.enabled: false</code> and <code>setup.ilm.enabled: false</code>.
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
  <KindSettings group={SOURCES} type="elasticReceiver" />

  <h2 id="paths">Paths it serves</h2>
  <dl>
    <dt>
      <strong>POST</strong> or <strong>PUT</strong> <code>/_bulk</code>,
      <code>/&#123;index&#125;/_bulk</code>
    </dt>
    <dd>
      The body is a bulk request: newline-delimited JSON, plain or with
      <code>Content-Encoding: gzip</code>. An action line names what to do, and the
      <code>index</code>, <code>create</code> and <code>update</code> actions are followed by their
      document. An index in the path, and a <code>pipeline</code> in the query, apply to each action that
      names none itself.
    </dd>
    <dt><strong>GET</strong> <code>/health</code></dt>
    <dd>Says whether the receiver takes requests. See <a href="#health">Health check</a>.</dd>
  </dl>

  <h2 id="records">Records it gives</h2>
  <p>
    Each <code>index</code> or <code>create</code> action's document becomes one record: the document
    as it was sent. This request gives two records:
  </p>
  <pre><code>{bulk}</code></pre>
  <p>
    The receiver takes no <code>update</code> or <code>delete</code>. Each is answered
    <code>400</code> in its own item, and the request's other actions are taken all the same. So is a
    document that is not a JSON object.
  </p>

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    The index, <code>_id</code> and <code>pipeline</code> an action names for its document, or the
    path and query name for it, ride with the record. A field the sender did not set is null.
    <code>__source.remote</code> holds the IP address the request came from: the balancer's, when one
    is in front of the workers.
  </p>
  <FieldsTable names={kind(SOURCES, 'elasticReceiver').fields ?? []} />

  <h2 id="senders">Tokens and certificates</h2>
  <p>
    Anyone a receiver admits can put records into the group's pipelines, so a receiver needs
    <code>tokens</code>, an <code>certificate</code> and <code>trustedAuthorities</code>, or both.
    The leader refuses a receiver with neither.
  </p>
  <ul>
    <li>
      <strong>Tokens:</strong> a sender presents one in any of these three ways. The receiver
      refuses any other request before reading its body. Without a certificate, the token crosses
      the network in the clear, so use a token alone only behind a balancer that terminates TLS.
      <ul>
        <li>
          <code>Authorization: Bearer &lt;token&gt;</code>.
        </li>
        <li>
          <code>Authorization: ApiKey &lt;key&gt;</code>, a Beat's <code>api_key</code>. The
          credential holds either the key's base64 or its <code>id:key</code>.
        </li>
        <li>
          Basic, with the token as the password, Fluent Bit's <code>HTTP_Passwd</code>. Any user
          name will do.
        </li>
      </ul>
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
    The receiver answers success only once every destination has the request's documents, so a
    sender that retries an unanswered request loses nothing. Each refusal's body holds an error that
    says why in the reason field.
  </p>
  <dl>
    <dt><strong>200</strong></dt>
    <dd>
      Every destination has the request's documents. The body holds an item per action, in the order
      sent: <code>201</code> for each document taken, with the <code>_id</code> its action named or
      one the receiver made up, and <code>400</code> for each action not taken.
      <pre><code>{answer}</code></pre>
    </dd>
    <dt><strong>429</strong>, with <code>Retry-After</code></dt>
    <dd>
      The receiver is full, <a href="/docs/delivery#hold">blocked</a> or stopping. A sender may send it
      again.
    </dd>
    <dt><strong>408</strong></dt>
    <dd>The sender took more than 30 seconds to send the whole body.</dd>
    <dt><strong>401</strong></dt>
    <dd>
      The request has no token, or gives one the receiver does not hold. See
      <a href="#senders">Tokens and certificates</a>.
    </dd>
    <dt><strong>400</strong></dt>
    <dd>
      The body holds no action, an action line does not read, an action has no document after it, or
      an action names no index and neither does the path. Nothing of the request is taken.
    </dd>
    <dt><strong>413</strong></dt>
    <dd>
      The body is larger than <code>maxRequestBytes</code>: send smaller requests, or raise
      <code>maxRequestBytes</code>. Or the body holds more actions than a receiver answers at once:
      send fewer actions to a request.
    </dd>
    <dt><strong>415</strong></dt>
    <dd>
      The body uses a <code>Content-Encoding</code> other than <code>gzip</code> or
      <code>identity</code>.
    </dd>
  </dl>

  <h2 id="health">Health check</h2>
  <p>
    <code>GET /health</code> on the receiver's port answers 200 while the receiver takes requests
    and 429 while it is full, blocked or stopping. It needs no token. Point a balancer's health
    check or a readiness probe at it, so a busy worker gets no requests until it can take them. With
    <code>trustedAuthorities</code>
    with authorities it needs a client certificate like any other request.
  </p>

  <h2 id="clients">Senders it takes</h2>
  <p>These senders are tested against it:</p>
  <ul>
    <li>
      Filebeat 8.17, through <code>output.elasticsearch</code>, as in the
      <a href="#example">example</a>.
    </li>
    <li>Fluent Bit 3.2's <code>es</code> output, with <code>Suppress_Type_Name On</code>.</li>
  </ul>
  <p>
    Fluent Bit sends to it with an <code>es</code> output like this, with <code>ES_TOKEN</code> set in
    its environment:
  </p>
  <pre><code>{fluentBit}</code></pre>

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
  <p>Send one document, and look for <code>"errors":false</code> in the answer:</p>
  <pre><code>{curl}</code></pre>
  <p>Then find the document at the destination.</p>

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
      Check that the sender presents a token one of the receiver's credentials holds, as a bearer
      token, an API key or a password.
    </dd>
    <dt><strong>A Beat stops at startup with a 404</strong></dt>
    <dd>
      Set
      <code>setup.template.enabled: false</code> and <code>setup.ilm.enabled: false</code>.
    </dd>
    <dt><strong>Items come back 400 for <code>update</code> or <code>delete</code></strong></dt>
    <dd>The receiver takes <code>index</code> and <code>create</code> actions only.</dd>
    <dt><strong>Every request gets 429</strong></dt>
    <dd>
      Check the source's status. A blocked receiver takes nothing until its destinations take
      records again. A full one takes more with more workers.
    </dd>
    <dt><strong>The same documents arrive twice</strong></dt>
    <dd>
      A sender that hears no answer may send the request again. Give it a timeout longer than its
      destinations take to deliver.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/hec-receiver">HEC receiver source</a></li>
    <li><a href="/docs/sources/http-receiver">HTTP receiver source</a></li>
    <li><a href="/docs/destinations/elasticsearch">Elasticsearch destination</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
