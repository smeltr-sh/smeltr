<!--
  A catalog list as Secrets and Certificates show one: a header row and a row per entry, scrolling sideways in a
  narrow frame. Cells take `one-line` for a short answer that should not wrap, `row-actions` for the buttons, and
  `note` for a sentence under a name.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let { head, children }: { head: Snippet; children: Snippet } = $props();
</script>

<div class="table-wrap">
  <table>
    <thead><tr>{@render head()}</tr></thead>
    <tbody>{@render children()}</tbody>
  </table>
</div>

<style>
  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-body);
  }

  table :global(th) {
    text-align: left;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
    padding: 0.4rem 0.6rem;
  }

  table :global(td) {
    padding: 0.5rem 0.6rem;
    border-bottom: 1px solid var(--border);
  }

  table :global(tbody tr:last-child td) {
    border-bottom: none;
  }

  table :global(th),
  table :global(.one-line),
  table :global(.row-actions) {
    white-space: nowrap;
  }

  table :global(.num) {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  table :global(.note) {
    display: block;
    color: var(--dim);
    font-size: var(--text-note);
    font-weight: 400;
    line-height: 1.4;
  }
</style>
