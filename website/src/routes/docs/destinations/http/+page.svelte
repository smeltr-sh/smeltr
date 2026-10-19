<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import { DESTINATIONS } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="HTTP destination"
  description="POST records to any HTTP service: an example, every setting, the bodies it sends, and how it reads each answer."
  {sections}
>
  <h1>HTTP</h1>
  <p class="lede">
    POSTs records to any HTTP service, batched as NDJSON, a JSON array or text lines, or one record
    per request.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Posts each batch as a JSON array, and reads the <code>DD-API-KEY</code> header's value from the
    worker-host variable <code>INTAKE_API_KEY</code>.
  </p>
  <pre><code>{@html highlight(json(examples.httpDestination))}</code></pre>
  <ProductScreenshot
    source="http-destination-form"
    height={2500}
    crop={{ x: 608, y: 656, width: 1664, height: 1188 }}
    alt="The HTTP form on the Destinations page: id intake, its URL, batched requests as a JSON array, and the header X-Source."
    caption="The Destinations page's HTTP form. Choose batched or one record per request, then the format. Credentials name a secret; Advanced holds the rest."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Find the URL the service takes records at, and the body it expects.</li>
    <li>Allow workers to reach the service.</li>
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
  <KindSettings group={DESTINATIONS} type="http" />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The destination sends every record to the one URL. It reads no <code>__destination</code> field.
  </p>
  <h3 id="bodies">Bodies</h3>
  <p>
    Choose a <code>body</code> and a <code>format</code> from these nine pairs. A batch takes
    <code>lines</code> or <code>jsonArray</code>; one record per request takes <code>single</code>.
  </p>
  <div class="table-scroll">
    <table>
      <thead
        ><tr
          ><th scope="col">body</th><th scope="col">format</th><th scope="col"
            >What the service receives</th
          ></tr
        ></thead
      >
      <tbody>
        <tr
          ><th scope="row">lines (default)</th><td>json (default)</td><td
            >NDJSON: one record per line, as <code>application/x-ndjson</code>.</td
          ></tr
        >
        <tr
          ><th scope="row">jsonArray</th><td>json</td><td
            >One JSON array of records, as <code>application/json</code>.</td
          ></tr
        >
        <tr
          ><th scope="row">lines</th><td>text</td><td
            >One field's text per line, as <code>text/plain</code>. The destination writes a line
            break inside the text as <code>\n</code> and a carriage return as <code>\r</code>, so
            each record stays one line.</td
          ></tr
        >
        <tr
          ><th scope="row">jsonArray</th><td>text</td><td
            >One JSON array of one field's text, a string per record, as
            <code>application/json</code>.</td
          ></tr
        >
        <tr
          ><th scope="row">lines</th><td>bytes</td><td
            >One field's bytes per line in base64, as <code>text/plain</code>. The field defaults to
            <code>__source.bytes</code>, each record's original bytes.</td
          ></tr
        >
        <tr
          ><th scope="row">jsonArray</th><td>bytes</td><td
            >One JSON array of one field's bytes, a base64 string per record, as
            <code>application/json</code>.</td
          ></tr
        >
        <tr
          ><th scope="row">single</th><td>json</td><td
            >One record per request, as <code>application/json</code>.</td
          ></tr
        >
        <tr
          ><th scope="row">single</th><td>text</td><td
            >One field's text per request, as <code>text/plain</code>.</td
          ></tr
        >
        <tr
          ><th scope="row">single</th><td>bytes</td><td
            >One field's bytes per request, as <code>application/octet-stream</code>. The field
            defaults to <code>__source.bytes</code>, each record's original bytes.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    The destination writes bytes in a batch as base64, because bytes may hold line breaks and are
    not JSON text.
  </p>

  <h3 id="answers">Answers</h3>
  <div class="table-scroll">
    <table>
      <thead
        ><tr><th scope="col">The service answers</th><th scope="col">The destination</th></tr
        ></thead
      >
      <tbody>
        <tr
          ><th scope="row">2xx</th><td>counts every record the request carried as delivered.</td
          ></tr
        >
        <tr
          ><th scope="row">408, 429, 500, 502, 503, 504</th><td
            >backs off and sends the request again.</td
          ></tr
        >
        <tr
          ><th scope="row">400, 413, 422</th><td
            >splits the request in halves and sends each. Halves the service takes land; a record it
            still refuses on its own, the destination refuses alone. <a
              href="/docs/delivery#refused">Choose what happens to it →</a
            ></td
          ></tr
        >
        <tr
          ><th scope="row">401, 403</th><td
            >stalls, and reports that the service refused its credentials.</td
          ></tr
        >
        <tr
          ><th scope="row">anything else, a redirect included</th><td
            >stalls, and reports the status and the start of the answer. It never follows a
            redirect.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    A batch counts as delivered once every request carrying it gets a 2xx. A record resent after a
    failure can arrive twice. Under <code>single</code>, the destination sends a batch's records one
    after another. If a request stalls the destination, it sends the whole batch again.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record with a unique marker through the source, then find it in the service. If the
    destination stalls, the Status page flags it, and the worker's
    <a href="/docs/logs-metrics#gui">log</a> names the status the service answered.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The service answers 401 or 403</strong></dt>
    <dd>
      Check that the credential holds the header's whole value, prefix included, such as
      <code>Bearer &lt;token&gt;</code>.
    </dd>
    <dt><strong>The service answers 404 or 405</strong></dt>
    <dd>Give the service's intake URL, path included, not its base address.</dd>
    <dt><strong>The service answers a redirect</strong></dt>
    <dd>Give the URL the service redirects to.</dd>
    <dt><strong>The destination refuses to start, naming a credential</strong></dt>
    <dd>
      The credential's value holds a line break or another control character, often a trailing
      newline pasted from a file. Store it again without one.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/delivery#refused">Handle refused records</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
