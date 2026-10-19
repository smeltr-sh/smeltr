<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  import metrics from '#lib/metrics.json';
  const metricRecord = {
    metric: 'smeltr_rows_polled_total',
    source: 'orders-west',
    thread: 'reader-0',
    value: 1250,
    ts: 1791100800000,
  };
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
  title="Smeltr metrics source"
  description="Collect each worker's own metrics: an example, every setting, the fields it gives, and the records it makes."
  {sections}
>
  <h1>Smeltr metrics</h1>
  <p class="lede">
    Gives the worker's own metrics as records, one per series every bucket. It reads locally and
    needs no credentials.
  </p>

  <h2 id="example">Example</h2>
  <p>Gives a record per series every 10 seconds. A shorter bucket gives records more often.</p>
  <pre><code>{@html highlight(json(examples.metricsSource))}</code></pre>
  <ProductScreenshot
    source="metrics-source-form"
    height={2000}
    crop={{ x: 608, y: 768, width: 1664, height: 464 }}
    alt="The smeltr metrics form on the Sources page: id worker-metrics, bucket seconds 10."
    caption="The same source in the Sources page's add dialog, as it is added."
  />

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="metrics" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <FieldsTable names={kind(SOURCES, 'metrics').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Each bucket, the source gives one record per series. <code>metric</code> holds the series' name,
    each label is a string field of its own, <code>value</code> holds the number, and
    <code>ts</code> holds the time of the bucket in Unix epoch milliseconds. Every record's
    <code>__source.topic</code> is <code>__smeltr_metrics</code>.
  </p>
  <pre><code>{@html highlight(json(metricRecord))}</code></pre>
  <p>
    A counter's value is how much it grew since its last record, and a counter that did not grow
    gives no record. A gauge gives its current value every bucket. The first bucket comes at once
    and holds everything counted since the worker started. A bucket larger than the source's batch
    limits is cut, and the counts cut from it are lost.
  </p>
  <p>
    Every record carries <code>thread</code>, the worker thread that counted it:
    <code>reader-N</code> for a source, <code>transform-N</code> for a pipeline thread,
    <code>egress-N</code> for a destination, <code>deploy</code> for the deployment as a whole, and
    <code>capture</code> for a sample capture. The <code>source</code>, <code>destination</code>,
    <code>route</code>, <code>pipeline</code> and <code>function</code> labels hold the names they have
    in the deployment.
  </p>
  <div class="table-scroll">
    <table class="metrics">
      <colgroup
        ><col style="width: 41%" /><col style="width: 11%" /><col style="width: 16%" /><col
        /></colgroup
      >
      <thead
        ><tr
          ><th scope="col">Metric</th><th scope="col">Kind</th><th scope="col">Labels</th><th
            scope="col">Measures</th
          ></tr
        ></thead
      >
      <tbody>
        {#each metrics as row, i (i)}
          {#if i === 0 || metrics[i - 1].group !== row.group}
            <tr><th scope="colgroup" colspan="4">{row.group}</th></tr>
          {/if}
          <tr
            ><td><code>{row.name}</code></td><td>{row.kind}</td><td
              >{#each row.labels as label, j (label)}{#if j > 0},
                {/if}<code>{label}</code>{/each}</td
            ><td
              >{#each row.what.split('`') as piece, j (j)}{#if j % 2 === 1}<code>{piece}</code
                  >{:else}{piece}{/if}{/each}</td
            ></tr
          >
        {/each}
      </tbody>
    </table>
  </div>

  <h2 id="verify">Verify</h2>
  <p>
    Wait at least one bucket, then find worker metrics at the destination. If none arrive, check the
    running commit and the route.
  </p>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/logs">Smeltr logs source</a></li>
    <li><a href="/docs/logs-metrics">Logs &amp; metrics</a></li>
    <li><a href="/docs/operations">Operate the fleet</a></li>
  </ul>
</GuideLayout>

<style>
  table {
    table-layout: fixed;
  }
  .metrics code,
  .metrics td:nth-child(2) {
    white-space: nowrap;
  }
</style>
