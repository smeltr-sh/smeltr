<script lang="ts">
  import example from '#lib/siem-cut-example.json';
  const bytes = (n: number) => n.toLocaleString('en-US');
  const saved = Math.round((1 - example.bytesOut / example.bytesIn) * 100);
</script>

<div class="example" aria-label="Firewall traffic before and after a pipeline that trims it">
  <dl class="totals">
    <div>
      <dt>In</dt>
      <dd>{example.recordsIn} records · {bytes(example.bytesIn)} bytes</dd>
    </div>
    <div>
      <dt>Out</dt>
      <dd>{example.recordsOut} records · {bytes(example.bytesOut)} bytes</dd>
    </div>
    <div>
      <dt>Cut</dt>
      <dd>{saved}% of the bytes</dd>
    </div>
  </dl>
  {#each example.rows as row, index (index)}
    <article class="record">
      <div>
        <p class="label">PAN-OS line · {bytes(row.rawBytes)} bytes</p>
        <pre><code>{row.raw}</code></pre>
      </div>
      <div>
        {#if row.output}
          <p class="label">Sent to the SIEM · {bytes(row.outputBytes)} bytes</p>
          <pre><code>{row.output}</code></pre>
        {:else}
          <p class="label">Not sent to the SIEM</p>
        {/if}
        <p class="reason">{row.reason}</p>
      </div>
    </article>
  {/each}
</div>

<style>
  .example {
    margin-block: 1.5rem;
  }
  .totals {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem 2.5rem;
    margin: 0 0 0.5rem;
  }
  .totals dt {
    color: var(--dim);
    font-size: 0.9rem;
  }
  .totals dd {
    margin: 0;
    font-weight: 600;
    color: var(--ink);
  }
  .record {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 1.25rem;
    padding: 1.25rem 0;
    border-top: 1px solid var(--border);
  }
  .record > div {
    min-width: 0;
  }
  .label {
    margin: 0 0 0.5rem;
    font-weight: 600;
    color: var(--ink);
  }
  pre {
    margin: 0;
    padding: 0.8rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
  code {
    padding: 0;
    background: none;
    color: var(--ink);
    font-size: 0.85rem;
  }
  .reason {
    margin: 0.75rem 0 0;
    font-size: 0.9rem;
    line-height: 1.6;
    color: var(--ink);
  }
  @media (max-width: 60rem) {
    .record {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
