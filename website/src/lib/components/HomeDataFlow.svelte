<script lang="ts">
  import Logo from './Logo.svelte';
  import { diagramGlyphs } from '#lib/diagram-glyphs.ts';
  import example from '#lib/filtering-example.json';
  import { destinations, sources, type Connection } from '#lib/connections.ts';
  const kept = example.rows.filter((row) => row.output !== null);
  // Smeltr's own logs and metrics are not systems a reader brings.
  const outside = sources.filter((source) => source.id !== 'logs' && source.id !== 'metrics');
  /** The named kinds of `list`, in the order of `ids`, and how many more it has. */
  const named = (list: Connection[], ids: string[]) => ({
    shown: ids.map((id) => list.find((kind) => kind.id === id)!),
    more: list.length - ids.length,
  });
  const reads = named(outside, [
    'kafka',
    's3List',
    'syslogReceiver',
    'otlpReceiver',
    'hecReceiver',
  ]);
  const writes = named(destinations, ['splunk', 'elasticsearch', 's3', 'kafka', 'iceberg']);
  const steps = [
    { title: 'Filter', detail: 'Keep useful records and fields' },
    { title: 'Parse', detail: 'Turn raw text into named fields' },
    { title: 'Enrich', detail: 'Add context from your data' },
    { title: 'Route', detail: 'Choose where each record goes' },
  ];
</script>

<figure aria-labelledby="flow-caption">
  <div class="flow">
    <div class="endpoint">
      <div class="column-title">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d={diagramGlyphs.source} /></svg>
        <h3>Sources</h3>
      </div>
      <p class="description">
        {#each reads.shown as kind}<a href={kind.href}>{kind.name}</a>{', '}{/each}<a
          href="/docs/sources">and {reads.more} more</a
        >
      </p>
      <div class="records">
        {#each example.rows as row}
          <div class="record">
            <span class="record-type"
              >{row.input.event_type === 'health_check'
                ? 'Health check'
                : row.input.event_type === 'order'
                  ? 'Order'
                  : 'Error'}</span
            >
            <span class="message">{row.input.message}</span>
            <code class="debug">debug: {row.input.debug}</code>
          </div>
        {/each}
      </div>
      <span class="connection" aria-hidden="true">→</span>
    </div>

    <div class="processor">
      <div class="processor-title">
        <Logo size={30} />
        <h3>Smeltr</h3>
      </div>
      <ol class="steps">
        {#each steps as step, i}
          <li>
            <span class="number">0{i + 1}</span>
            <div><strong>{step.title}</strong><span>{step.detail}</span></div>
          </li>
        {/each}
      </ol>
      <span class="connection" aria-hidden="true">→</span>
    </div>

    <div class="endpoint destination">
      <div class="column-title">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d={diagramGlyphs.destination} /></svg>
        <h3>Destinations</h3>
      </div>
      <p class="description">
        {#each writes.shown as kind}<a href={kind.href}>{kind.name}</a>{', '}{/each}<a
          href="/docs/destinations">and {writes.more} more</a
        >
      </p>
      <div class="records">
        {#each kept as row}
          <div class="record kept">
            <span class="record-type">{row.input.event_type === 'order' ? 'Order' : 'Error'}</span>
            <span class="message">{row.output?.message}</span>
          </div>
        {/each}
      </div>
      <p class="ready">Useful records.<br />Only the fields you need.</p>
      <p class="uses">Search · Detection · Reporting</p>
    </div>
  </div>
  <figcaption id="flow-caption">
    Example policy: drop health checks and remove debug fields. Three records in, two smaller
    records out.
  </figcaption>
</figure>

<style>
  figure {
    margin: 2.5rem 0 0;
  }
  .flow {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.2fr) minmax(0, 1fr);
    gap: 3.5rem;
    align-items: center;
  }
  .endpoint,
  .processor {
    position: relative;
    min-width: 0;
  }
  .column-title,
  .processor-title {
    display: flex;
    align-items: center;
    gap: 0.65rem;
  }
  .column-title svg {
    width: 25px;
    height: 25px;
    fill: none;
    stroke: var(--dim);
    stroke-width: 1.6;
    stroke-linecap: round;
    stroke-linejoin: round;
  }
  h3 {
    margin: 0;
    font-family: var(--font-body);
    font-size: 1.15rem;
    font-weight: 600;
  }
  .description {
    margin: 0.4rem 0 1.2rem;
    font-size: 0.88rem;
    line-height: 1.6;
    color: var(--dim);
  }
  .description a {
    color: inherit;
    text-decoration: none;
  }
  .description a:hover {
    text-decoration: underline;
  }
  .records {
    display: grid;
    gap: 0.6rem;
  }
  .record {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    padding: 0.8rem 0.9rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
  }
  .record-type {
    font-size: 0.78rem;
    font-weight: 600;
    color: var(--ink);
  }
  .message {
    font-size: 0.85rem;
    overflow-wrap: anywhere;
    color: var(--dim);
  }
  .debug {
    font-size: 0.7rem;
    color: var(--dim);
    overflow-wrap: anywhere;
  }
  .processor {
    padding: 1.25rem;
    border: 1px solid var(--ink);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  .processor-title {
    padding-bottom: 1rem;
    border-bottom: 1px solid var(--border);
  }
  .processor-title h3 {
    font-family: var(--font-display);
    font-size: 1.8rem;
  }
  .steps {
    list-style: none;
    padding: 0;
    margin: 0;
  }
  .steps li {
    display: flex;
    align-items: baseline;
    gap: 0.9rem;
    padding: 0.85rem 0;
    border-bottom: 1px solid var(--border);
  }
  .number {
    color: var(--dim);
    font-size: 0.75rem;
  }
  .steps strong {
    display: block;
    font-size: 1.1rem;
  }
  .steps div > span {
    display: block;
    margin-top: 0.2rem;
    font-size: 0.78rem;
    color: var(--dim);
  }
  .connection {
    position: absolute;
    left: calc(100% + 0.75rem);
    top: 48%;
    width: 2rem;
    text-align: center;
    color: var(--dim);
    font-size: 1.8rem;
  }
  .kept {
    border-left: 3px solid var(--accent);
    padding-block: 1rem;
  }
  .ready {
    font-size: 1.1rem;
    color: var(--ink);
    line-height: 1.4;
    margin: 1.2rem 0 0.6rem;
  }
  .uses {
    font-size: 0.75rem;
    color: var(--dim);
  }
  figcaption {
    text-align: center;
    margin-top: 1.5rem;
    color: var(--ink);
    font-size: 0.8rem;
  }
  @media (max-width: 48rem) {
    .flow {
      grid-template-columns: minmax(0, 1fr);
      gap: 3rem;
      max-width: 30rem;
      margin-inline: auto;
    }
    .connection {
      top: auto;
      bottom: -2.6rem;
      left: calc(50% - 1rem);
      transform: rotate(90deg);
    }
    .column-title {
      justify-content: center;
    }
    .description,
    .ready,
    .uses {
      text-align: center;
    }
    .processor {
      padding: 1.1rem;
    }
  }
</style>
