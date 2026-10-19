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
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Smeltr logs source"
  description="Collect each worker's own log records: an example, every setting and the fields it gives."
  {sections}
>
  <h1>Smeltr logs</h1>
  <p class="lede">
    Gives the worker's own log records at or above a chosen level. It reads locally and needs no
    credentials.
  </p>

  <h2 id="example">Example</h2>
  <p>Gives every log record at info or above.</p>
  <pre><code>{@html highlight(json(examples.logsSource))}</code></pre>
  <ProductScreenshot
    source="logs-source-form"
    height={2000}
    crop={{ x: 608, y: 748, width: 1664, height: 504 }}
    alt="The smeltr logs form on the Sources page: id worker-logs, min level info."
    caption="The same source in the Sources page's add dialog, as it is added."
  />

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="logs" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <FieldsTable names={kind(SOURCES, 'logs').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The source reads only what the process collects. To collect debug records, also set the worker's
    startup log level to debug and restart it. See
    <a href="/docs/operations#logs">Change log verbosity</a>.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Compare a new worker log at or above the chosen level with the record at the destination. A
    quiet worker may have nothing new to send.
  </p>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/metrics">Smeltr metrics source</a></li>
    <li><a href="/docs/logs-metrics">Logs &amp; metrics</a></li>
    <li><a href="/docs/operations#logs">Change log verbosity</a></li>
  </ul>
</GuideLayout>
