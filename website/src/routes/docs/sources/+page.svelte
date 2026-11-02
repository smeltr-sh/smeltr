<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import DataPath from '#lib/components/DataPath.svelte';
  import ConnectorIndex from '#lib/components/ConnectorIndex.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { sourceGroups, sources } from '#lib/connections.ts';
  import { SOURCES } from '#lib/connector-reference.ts';
  const sections = [
    { id: 'kinds', title: 'Kinds' },
    { id: 'flow', title: 'How it works' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields', title: 'Fields' },
    { id: 'create', title: 'Add a source' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Sources"
  description="What a source is, every kind of source, the settings and fields all sources share, and how to add and verify one."
  {sections}
>
  <h1>Sources</h1>
  <p class="lede">
    A source reads records from a system and hands them to the routes that name it. Each kind of
    source has a page of its own.
  </p>

  <h2 id="kinds">Kinds</h2>
  <ConnectorIndex entries={sources} groups={sourceGroups} />

  <h2 id="flow">How it works</h2>
  <DataPath />

  <h2 id="settings">Settings</h2>
  <p>A source is one entry of the bundle's Sources array. Every kind takes these settings:</p>
  <SettingsTable settings={SOURCES.common} />

  <h2 id="fields">Fields</h2>
  <p>
    Every record carries its text in <code>_raw</code>, unless its source's
    <code>encoding</code> is <code>none</code>. An Avro or SQL source gives its declared fields
    instead. Each kind of source adds fields of its own.
  </p>
  <FieldsTable names={['_raw']} />

  <h2 id="create">Add a source</h2>
  <ol>
    <li>Select the worker group and work on a branch.</li>
    <li>Open Build → Sources and choose <strong>+ Add source</strong>.</li>
    <li>
      Choose how the worker group reads it (<strong>Pull, all workers</strong>,
      <strong>Pull, one worker</strong>, <strong>Push, all workers</strong> or
      <strong>Local, each worker</strong>), then its type. Give it a unique id and fill in its
      settings, then choose <strong>Add</strong>. Save the page.
    </li>
    <li>
      Connect it to a route, which sends its records through a pipeline to a
      <a href="/docs/destinations">destination</a>.
      <a href="/docs/first-pipeline#route">Follow the routing example</a>.
    </li>
    <li><a href="/docs/review">Review and deploy</a>, then <a href="#verify">verify it</a>.</li>
  </ol>

  <h2 id="verify">Verify</h2>
  <ol>
    <li>Check that the intended workers are online and run the deployed commit.</li>
    <li>Send a record you can recognize into the source's system.</li>
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
      Check the server name and the connector's <code>trustedAuthorities</code>. If the server uses
      a private CA, the connector must trust it.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/first-pipeline">First pipeline tutorial</a></li>
    <li><a href="/docs/routing/records">Routing records</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
