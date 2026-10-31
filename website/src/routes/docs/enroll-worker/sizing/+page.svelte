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

  const KiB = 1024;
  const MiB = 1024 * KiB;
  const UNITS: Record<string, number> = { KiB, MiB, GiB: 1024 * MiB };
  /** `"64 MiB"` to bytes. */
  function bytes(text: string): number {
    const [n, unit] = text.split(' ');
    return Number(n) * UNITS[unit];
  }
  const least = (id: string) => bytes(data.parts.find((p) => p.id === id)!.least);

  // The worked example: four cores, one Kafka source on a cluster of three brokers, one Elasticsearch and one S3
  // destination, on a worker with room for whole batches.
  const threads = 4;
  const brokers = 3;
  const batch = 4 * MiB;
  const fetch = least('kafka');
  const example = [
    { what: 'Processes', bytes: least('processes') },
    { what: `Lanes, 1 source and ${threads} threads`, bytes: (1 + threads) * batch },
    {
      what: `Fetch queue and its client, ${brokers} brokers`,
      bytes: 2 * fetch + (fetch / 2) * brokers,
    },
    { what: 'Elasticsearch queue and 4 senders', bytes: batch + 4 * least('senders') },
    { what: 'S3 queue and open files', bytes: 2 * batch },
    { what: 'Columnar budget', bytes: least('transform') },
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
    A worker sizes itself to its memory. Its CPU is one transform thread per core. Its disk is the
    state directory.
  </p>

  <h2 id="memory">Memory</h2>
  <p>
    A worker uses 90% of its container's memory limit or host memory;
    <code>worker.runtime.memory</code> sets the amount instead. It sets aside the least each part below
    needs, and the parts with room to grow share the rest, so a worker with more memory holds more in
    flight with no change to its configuration. A destination's queue grows as its traffic asks, so a
    busy destination takes what idle ones leave.
  </p>
  <p>
    Give a worker at least the sum of the parts below for the sources and destinations its group
    runs. Every worker in a group runs the whole group's configuration, so size each one for all of
    it.
  </p>
  <div class="table-scroll">
    <table>
      <thead
        ><tr><th>Part</th><th>Least</th><th>Grows to</th><th>For each</th><th>Why</th></tr></thead
      >
      <tbody>
        {#each data.parts as p (p.id)}
          <tr>
            <td>{p.part}</td>
            <td>{p.least}</td>
            <td>{p.most ?? ''}</td>
            <td><Prose text={p.each} /></td>
            <td><Prose text={p.what} /></td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
  <p>
    A group on four cores reading one Kafka topic from three brokers into Elasticsearch and S3 has
    full batches at {mib(total)}:
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
    A stated <code>fetchBytes</code>, <code>bufferBytes</code> or
    <code>SMELTR_MEMORY_BUDGET_BYTES</code>
    fixes that part at what it states, and the worker sets it aside whole. A worker whose fixed parts
    add up to more than its memory limit still runs, but can be killed for memory under load. It logs
    a warning naming each part and its share, and the warning stays on Issues while it holds. Select the
    worker on Status and open <strong>Memory by part</strong> to see which part it is. Raise the limit,
    or lower what the configuration fixes.
  </p>
  <p>
    Set a memory limit on every worker container, so the worker sizes itself to what it may really
    use. Lookup tables are read from disk as they are needed and are not counted here.
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
