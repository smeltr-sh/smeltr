<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import Prose from '#lib/components/Prose.svelte';
  import data from '#lib/worker-sizing.json';

  const sections = [
    { id: 'memory', title: 'Memory' },
    { id: 'cpu', title: 'CPU' },
    { id: 'disk', title: 'Disk' },
    { id: 'see-also', title: 'See also' },
  ];

  const MiB = 1024 * 1024;
  const UNITS: Record<string, number> = { MB: 1e6, MiB: MiB, GiB: 1024 * MiB };
  /** `"64 MiB"` to bytes. */
  function bytes(text: string): number {
    const [n, unit] = text.split(' ');
    return Number(n) * UNITS[unit];
  }
  const part = (id: string) => bytes(data.parts.find((p) => p.id === id)!.bytes);

  // The worked example: four cores, one Kafka source on a cluster of three brokers, one Elasticsearch and one S3 destination.
  const threads = 4;
  const brokers = 3;
  const example = [
    { what: 'Columnar budget', bytes: part('transform') },
    {
      what: `Lanes, 1 source and ${threads} threads`,
      bytes: (1 + threads) * part('lanes'),
    },
    {
      what: `Fetch queue, a cluster of ${brokers} brokers`,
      bytes: part('kafka') + (part('kafka') / 2) * brokers,
    },
    { what: 'Elasticsearch buffer', bytes: part('buffer') },
    { what: 'S3 buffer and open files', bytes: part('objectBuffer') },
  ];
  const total = example.reduce((sum, row) => sum + row.bytes, 0);
  const mib = (b: number) => `${Math.round(b / MiB)} MiB`;
</script>

<GuideLayout
  title="Size a worker"
  description="How much memory, CPU and disk a Smeltr worker needs for what it runs."
  {sections}
>
  <h1>Size a worker</h1>
  <p class="lede">
    A worker's memory is a fixed columnar budget plus a ceiling for each source and destination it
    runs. Its CPU is one transform thread per core. Its disk is the state directory.
  </p>

  <h2 id="memory">Memory</h2>
  <p>
    Give a worker at least the sum of the parts below for the sources and destinations its group
    runs, plus about 150 MiB for the process itself and the size of every lookup table the group
    uses and its index, which a worker holds in memory whole. Each amount below is the default; a
    source or destination with a larger <code>fetchBytes</code> or <code>bufferBytes</code> holds that
    instead. Every worker in a group runs the whole group's configuration, so size each one for all of
    it. The warning below counts the parts and not the lookup tables.
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Part</th><th>Default</th><th>For each</th><th>Why</th></tr></thead>
      <tbody>
        {#each data.parts as p (p.id)}
          <tr>
            <td>{p.part}</td>
            <td>{p.bytes}</td>
            <td><Prose text={p.each} /></td>
            <td><Prose text={p.what} /></td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
  <p>
    A group on four cores reading one Kafka topic from three brokers into Elasticsearch and S3 adds
    up to {mib(total)}:
  </p>
  <div class="table-scroll">
    <table>
      <tbody>
        {#each example as row (row.what)}
          <tr><td>{row.what}</td><td>{mib(row.bytes)}</td></tr>
        {/each}
        <tr><th>Total</th><th>{mib(total)}</th></tr>
      </tbody>
    </table>
  </div>
  <p>
    Each ceiling is a worst case, and they rarely all fill at once. A worker whose deployment adds
    up to more than its memory limit still runs, but can be killed for memory under load. It logs a
    warning naming each part and its share, and the warning stays on Issues while it holds. Select
    the worker on Status and open <strong>Memory by part</strong> to see which part it is. Raise the
    limit, or lower <code>fetchBytes</code>, <code>bufferBytes</code> or
    <code>SMELTR_MEMORY_BUDGET_BYTES</code>.
  </p>
  <p>
    The limit a worker checks against is its container's memory limit, or the host's memory when it
    has none. Set a memory limit on every worker container, so the warning compares against what the
    worker may really use.
  </p>
  <p>
    <code>smeltr_memory_peak_bytes</code> shows how much of the columnar budget batches have needed. A
    peak well under the budget means the budget can come down.
  </p>

  <h2 id="cpu">CPU</h2>
  <p>
    Transform does most of the CPU work, and it runs one thread per core the worker may use. Readers
    and destinations run threads of their own that mostly wait on the network, except a receiver
    decompressing gzip and an object destination compressing its files. Two cores is a sensible
    start; add cores or workers while CPU stays high under normal load.
  </p>
  <p>
    The cores a worker counts are its CPU limit, or the host's cores when it has none. A container
    with a CPU request and no limit counts every core on its node. Set
    <code>worker.runtime.thread_limit</code> to its request, or give it a CPU limit.
  </p>

  <h2 id="disk">Disk</h2>
  <p>
    The state directory holds the worker's credential, its clone of the configuration repository,
    the lookup tables that configuration names and the progress of the single-worker sources it
    holds. A new version of a lookup table, and the index built beside it, arrive before the old
    ones are removed, so leave room for each table and its index twice. Once this volume is full,
    the worker takes no new configuration, lookup table or source handoff.
  </p>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/enroll-worker/kubernetes">Enroll workers on Kubernetes</a></li>
    <li><a href="/docs/reference">Startup configuration reference</a></li>
    <li><a href="/docs/logs-metrics">Logs and metrics</a></li>
  </ul>
</GuideLayout>
