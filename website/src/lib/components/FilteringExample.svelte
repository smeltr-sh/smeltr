<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import example from '#lib/filtering-example.json';
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<div class="example" aria-label="Filtering example: three input records, two output records">
  <p class="summary">3 records in · 2 records out · debug fields removed</p>
  {#each example.rows as row, index (index)}
    <article class="record">
      <div>
        <p class="label">Input {index + 1}</p>
        <pre><code>{@html highlight(json(row.input))}</code></pre>
      </div>
      <div>
        <p class="label">{row.output ? 'Kept' : 'Dropped'}</p>
        {#if row.output}
          <pre><code>{@html highlight(json(row.output))}</code></pre>
        {:else}
          <p class="empty">No record sent to the destination.</p>
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
  .summary {
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
  .reason,
  .empty {
    font-size: 0.9rem;
    line-height: 1.6;
  }
  .reason {
    margin: 0.75rem 0 0;
    color: var(--ink);
  }
  .empty {
    color: var(--dim);
  }
  @media (max-width: 60rem) {
    .record {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
