<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import DataPath from '#lib/components/DataPath.svelte';
  import ConnectorIndex from '#lib/components/ConnectorIndex.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { DESTINATIONS } from '#lib/connector-reference.ts';
  import { destinations, upcomingDestinations } from '#lib/connections.ts';
  // Every __destination field some kind reads, once each, in the order the kinds list them.
  const reads = [...new Set(DESTINATIONS.kinds.flatMap((k) => k.reads ?? []))];
  const sections = [
    { id: 'kinds', title: 'Kinds' },
    { id: 'flow', title: 'How it works' },
    { id: 'settings', title: 'Settings' },
    { id: 'formats', title: 'Formats' },
    { id: 'fields', title: 'Fields' },
    { id: 'create', title: 'Add a destination' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Destinations"
  description="What a destination is, every kind of destination, the settings and fields all destinations share, and how to add and verify one."
  {sections}
>
  <h1>Destinations</h1>
  <p class="lede">
    A destination writes the records a route sends it to a system. Each kind of destination has a
    page of its own.
  </p>

  <h2 id="kinds">Kinds</h2>
  <ConnectorIndex entries={destinations} />
  {#if upcomingDestinations.length}
    <p>
      Planned: {#each upcomingDestinations as connection, i (connection.id)}{#if i > 0},
        {/if}{connection.name}{/each}.
    </p>
  {/if}

  <h2 id="flow">How it works</h2>
  <DataPath />

  <h2 id="settings">Settings</h2>
  <p>
    A destination is one entry of the bundle's Destinations array. Every kind takes these settings:
  </p>
  <SettingsTable settings={DESTINATIONS.common} />

  <h2 id="formats">Formats</h2>
  <p>
    A destination's <code>format</code> decides what it writes of each record. Routes and exception answers
    name only destinations, so one record can reach two destinations in two forms.
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">format</th><th scope="col">What it writes</th></tr></thead>
      <tbody>
        <tr
          ><th scope="row">json, ndjson, parquet, avro, csv</th><td
            >The record as the pipeline wrote it.</td
          ></tr
        >
        <tr
          ><th scope="row">text</th><td
            >The text of the field <code>field</code> names, <code>_raw</code> by default. The destination
            refuses a record without that field.</td
          ></tr
        >
        <tr
          ><th scope="row">bytes</th><td
            >One field's bytes, once per source record. The destination's <code>field</code> names
            the field, and defaults to <code>__source.bytes</code>: each record's bytes as the
            source received them, before any pipeline ran. Those bytes let you
            <a href="/docs/delivery#recovery">replay</a> records: a source that reads them back gets what
            the original source got.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>Each kind writes only some formats:</p>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">Destination</th><th scope="col">format</th></tr></thead>
      <tbody>
        <tr
          ><th scope="row">HTTP</th><td
            >json (default), text, bytes (base64 in a batch: <a
              href="/docs/destinations/http#bodies">bodies</a
            >)</td
          ></tr
        >
        <tr><th scope="row">Kafka</th><td>json (default), text, bytes, avro</td></tr>
        <tr><th scope="row">Elasticsearch</th><td>json</td></tr>
        <tr><th scope="row">Splunk HEC, event endpoint</th><td>json (default), text</td></tr>
        <tr><th scope="row">Splunk HEC, raw endpoint</th><td>json (default), text, bytes</td></tr>
        <tr
          ><th scope="row">S3, Google Cloud Storage, Azure Blob, SFTP</th><td
            >ndjson (default), json, parquet, avro, csv, text, bytes</td
          ></tr
        >
        <tr
          ><th scope="row">Apache Iceberg, Delta Lake</th><td>parquet, as the table's data files</td
          ></tr
        >
      </tbody>
    </table>
  </div>

  <h2 id="fields">Fields</h2>
  <p>
    Smeltr never delivers a record field whose name starts with <code>__</code>. A pipeline sets
    <code>__destination</code> fields to steer a record, and each kind reads only its own:
  </p>
  <FieldsTable names={reads} />

  <h2 id="create">Add a destination</h2>
  <ol>
    <li>Select the worker group and work on a branch.</li>
    <li>Open Build → Destinations and choose <strong>+ Add destination</strong>.</li>
    <li>
      Choose its type, give it a unique id and fill in its settings, then choose <strong>Add</strong
      >. Save the page.
    </li>
    <li>
      Connect it to a route's delivery path, whose condition selects the records it receives from a
      <a href="/docs/sources">source</a>.
      <a href="/docs/first-pipeline#route">Follow the routing example</a>.
    </li>
    <li><a href="/docs/review">Review and deploy</a>, then <a href="#verify">verify it</a>.</li>
  </ol>

  <h2 id="verify">Verify</h2>
  <ol>
    <li>Check that the intended workers are online and run the deployed commit.</li>
    <li>Send a record you can recognize through a source and a route to the destination.</li>
    <li>Check Status and Logs for source, routing or destination errors.</li>
    <li>Find the record at the destination and check its fields.</li>
  </ol>
  <p>A preview of a saved sample checks the pipeline, not the live connection.</p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>No records arrive</strong></dt>
    <dd>
      Follow <a href="/docs/troubleshooting#no-output">No records arriving</a> to check input, worker
      configuration, routing, filtering and delivery in order.
    </dd>
    <dt><strong>TLS verification fails</strong></dt>
    <dd>
      Check the server name and the connector's <code>authorities</code>. If the server uses a
      private CA, the connector must trust it.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/first-pipeline">First pipeline tutorial</a></li>
    <li><a href="/docs/routing/records">Routing records</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
