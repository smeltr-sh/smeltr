<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import metrics from '#lib/metrics.json';
  const logMetrics = metrics.filter((row) => row.group === 'Logs');
  const sections = [
    { id: 'where', title: 'Where logs and metrics go' },
    { id: 'gui', title: 'Read them in the GUI' },
    { id: 'route', title: 'Send them to a destination' },
    { id: 'metrics', title: 'What metrics exist' },
    { id: 'lost', title: 'Check for lost logs' },
    { id: 'verbosity', title: 'Change log verbosity' },
  ];
</script>

<GuideLayout
  title="Logs & metrics"
  description="Watch the fleet's own logs and metrics in the GUI, and route them to a destination."
  {sections}
>
  <h1>Logs &amp; metrics</h1>
  <p class="lede">
    Every Smeltr process keeps two accounts of itself: logs, which are timed, structured events, and
    metrics, which are counters and gauges. A worker's heartbeat carries both to the leader, and a
    bundle can route either through a pipeline to any destination.
  </p>

  <h2 id="where">Where logs and metrics go</h2>
  <div class="table-scroll">
    <table>
      <thead
        ><tr><th scope="col">Goes to</th><th scope="col">Logs</th><th scope="col">Metrics</th></tr
        ></thead
      >
      <tbody>
        <tr>
          <th scope="row">The console</th>
          <td>Every process writes one line per event to stderr.</td>
          <td>—</td>
        </tr>
        <tr>
          <th scope="row">The leader</th>
          <td>
            Each worker sends its logs with its heartbeat. The leader keeps the newest 100,000 rows
            for the fleet.
          </td>
          <td>
            Each worker sends its metrics with its heartbeat. The leader keeps them for 30 days, in
            coarser buckets as they age.
          </td>
        </tr>
        <tr>
          <th scope="row">A destination</th>
          <td>Through a <a href="/docs/sources/logs">Smeltr logs source</a>.</td>
          <td>Through a <a href="/docs/sources/metrics">Smeltr metrics source</a>.</td>
        </tr>
      </tbody>
    </table>
  </div>
  <p>
    The leader's copy is its operational view. Send logs and metrics you need to keep or search
    elsewhere to a destination.
  </p>

  <h2 id="gui">Read them in the GUI</h2>
  <ul>
    <li>
      <strong>Status → Logs</strong> shows the fleet's logs, newest first. Choose severities, a
      worker, a worker group, a site, or text in the message, then choose <strong>Search</strong>. A
      severity is not a minimum: choose <code>error</code> and <code>warn</code> to see both. Select a
      row's fields to open them. The filters are in the page's address, so you can share a view as a link.
    </li>
    <li>
      <strong>Status</strong> shows Rows in and out, Errors and Furthest from normal, from the metrics
      the heartbeat carries. Choose the window of Rows in and out: 15m, 60m, 24h, 7d or 30d. A gap in
      a line is missing history, not zero.
    </li>
  </ul>
  <p>
    The leader cuts a log's site, message and each text field to 1,000 characters, and keeps its
    first 16 fields. A search cannot find what the leader cut. A worker out of contact with the
    leader holds the oldest 4,096 of its own events and the newest 2,048 from the process running
    its pipelines.
  </p>

  <h2 id="route">Send them to a destination</h2>
  <ol>
    <li>Select the worker group and work on a branch.</li>
    <li>
      Open Build → Sources, choose <strong>+ Add source</strong>, choose
      <strong>Local, each worker</strong>, and then <strong>smeltr logs</strong> or
      <strong>smeltr metrics</strong>. Its settings are on the
      <a href="/docs/sources/logs#settings">Smeltr logs</a> and
      <a href="/docs/sources/metrics#settings">Smeltr metrics</a> pages.
    </li>
    <li>
      Connect it to a route, which sends its records through a pipeline to a
      <a href="/docs/destinations">destination</a>.
      <a href="/docs/first-pipeline#route">Follow the routing example</a>.
    </li>
    <li><a href="/docs/review">Review and deploy</a>.</li>
  </ol>
  <p>
    Each source gives the logs or metrics of the worker it runs on, covering both of that worker's
    processes: the <code>supervisor</code>, which talks to the leader, and the
    <code>runtime</code>, which runs its pipelines. Add one to every worker group you want to watch.
    Its records carry
    <code>__source.topic</code> <code>__smeltr_logs</code> or <code>__smeltr_metrics</code>.
  </p>

  <h2 id="metrics">What metrics exist</h2>
  <p>
    Smeltr has {metrics.length} metrics, grouped by what they measure: sources, processing, pipeline steps,
    destinations, memory, the worker and its logs. Each is a counter or a gauge with labels naming its
    subject. The <a href="/docs/sources/metrics#how-it-works">Smeltr metrics source</a>
    lists every one, with its labels and what it measures, and shows the record each one makes.
  </p>

  <h2 id="lost">Check for lost logs</h2>
  <p>
    Writing a log never waits. A process with no room for an event drops it and counts it. These
    metrics carry a <code>process</code> label naming the process that wrote them.
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">Metric</th><th scope="col">Measures</th></tr></thead>
      <tbody>
        {#each logMetrics as row (row.name)}
          <tr
            ><td><code>{row.name}</code></td><td
              >{#each row.what.split('`') as piece, j (j)}{#if j % 2 === 1}<code>{piece}</code
                  >{:else}{piece}{/if}{/each}</td
            ></tr
          >
        {/each}
      </tbody>
    </table>
  </div>

  <h2 id="verbosity">Change log verbosity</h2>
  <p>
    A process collects logs at or above its log level, <code>info</code> unless you set it. The
    console, the leader and every Smeltr logs source get only what the process collects. To change
    it, see
    <a href="/docs/operations#logs">Change log verbosity</a>.
  </p>
</GuideLayout>

<style>
  td code {
    white-space: nowrap;
  }
</style>
