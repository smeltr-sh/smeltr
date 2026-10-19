<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { DESTINATIONS, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-reads', title: 'Fields it reads' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Elasticsearch destination"
  description="Index documents through the Elasticsearch Bulk API: an example, every setting, the fields it reads, and what to have ready."
  {sections}
>
  <h1>Elasticsearch</h1>
  <p class="lede">
    Writes each record as a JSON document through Elasticsearch's Bulk API, many documents to a
    request.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Authorizes with the worker-host variable <code>ELASTIC_AUTH</code>, which holds the complete
    Authorization header. Replace the URL and index.
  </p>
  <pre><code>{@html highlight(json(examples.elasticDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Prepare the target index or data stream and its mappings, or let the account create them.
    </li>
    <li>
      Give an account permission to write there, and put its complete Authorization header, such as <code
        >ApiKey &lt;encoded-key&gt;</code
      >, in a <a href="/docs/secrets">credential</a>.
    </li>
    <li>
      Select a <a href="/docs/secrets#identities">connection identity</a> for a private CA or a client
      certificate.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="elasticsearch" />

  <h2 id="fields-it-reads">Fields it reads</h2>
  <FieldsTable names={kind(DESTINATIONS, 'elasticsearch').reads ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <h3 id="duplicate-documents">Duplicate documents</h3>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">Policy</th><th scope="col">Behavior</th></tr></thead>
      <tbody>
        <tr
          ><th scope="row">auto (default)</th><td
            >Sends no document ID. A record resent after a crash can create a second document.</td
          ></tr
        >
        <tr
          ><th scope="row">dedupe</th><td
            >Use a stable ID and keep the first document written with that ID.</td
          ></tr
        >
        <tr
          ><th scope="row">overwrite</th><td
            >Replace the document with that ID. Concurrent requests can arrive out of order. This
            policy cannot write to a data stream.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    For dedupe or overwrite, set <code>idField</code> to a stable field such as
    <code>event.id</code>, or have the pipeline set <code>__destination.id</code>. Keep auto when
    records have no stable ID.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record with a unique marker through the source, then search the index for it after a
    refresh. Check the document's fields and the worker's delivery errors.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>401 or 403</strong></dt>
    <dd>
      Check the full Authorization header, that each worker has the secret, and permissions on the
      target index.
    </dd>
    <dt><strong>Documents are refused</strong></dt>
    <dd>
      Read the per-record error. Check mappings, field types, document ids and the refused-record
      answer before retrying.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/sources/elastic-receiver">Elasticsearch receiver source</a></li>
    <li><a href="/docs/delivery#refused">Handle refused records</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
