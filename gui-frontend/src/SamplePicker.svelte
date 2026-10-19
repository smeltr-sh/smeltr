<!--
  The samples a pipeline can run over, as a table to search, sort and pick from. A row is picked by a click
  anywhere on it. It opens in its own order: the samples a test of this pipeline locks first, then the newest
  captured.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { SampleSummary } from './api';
  import LockIcon from './LockIcon.svelte';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';
  import { dateTime } from './time';

  let {
    samples,
    lockedBy,
    onpick,
    tools,
  }: {
    samples: SampleSummary[];
    /** The names of this pipeline's tests that lock each sample, by sample id. */
    lockedBy: Map<string, string[]>;
    onpick: (id: string) => void;
    /** Beside the search: how a sample is added. */
    tools?: Snippet;
  } = $props();

  let query = $state('');

  const locked = (s: SampleSummary) => (lockedBy.get(s.id)?.length ?? 0) > 0;
  let own = $derived(
    [...samples].sort(
      (a, b) => Number(locked(b)) - Number(locked(a)) || (b.captured ?? 0) - (a.captured ?? 0),
    ),
  );
  let found = $derived.by(() => {
    const q = query.trim().toLowerCase();
    if (!q) return own;
    return own.filter((s) => `${s.name} ${s.source ?? ''}`.toLowerCase().includes(q));
  });

  const sort = new TableSort<'locked' | 'name' | 'source' | 'records' | 'captured'>();
  let sorted = $derived(
    sort.apply(found, {
      locked: (s) => (locked(s) ? 0 : 1),
      name: (s) => s.name,
      source: (s) => s.source,
      records: (s) => s.records,
      captured: (s) => s.captured,
    }),
  );
</script>

<div class="picker" data-guide="samples">
  <p class="dim lead">No sample yet. Pick one to run the steps over its records.</p>
  <div class="tools">
    <label class="search"
      ><span aria-hidden="true">⌕</span><input
        type="search"
        bind:value={query}
        placeholder="Search samples by name or source…"
        aria-label="Search samples"
      /></label
    >
    {@render tools?.()}
  </div>
  <table>
    <thead>
      <tr>
        <th
          class="lock-cell"
          aria-sort={sort.aria('locked')}
          title="Locked by a test of this pipeline"
          ><SortButton {sort} key="locked"
            ><LockIcon /><span class="visually-hidden">Locked</span></SortButton
          ></th
        >
        <th aria-sort={sort.aria('name')}><SortButton {sort} key="name">Name</SortButton></th>
        <th aria-sort={sort.aria('source')}><SortButton {sort} key="source">From</SortButton></th>
        <th class="num" aria-sort={sort.aria('records')}
          ><SortButton {sort} key="records">Records</SortButton></th
        >
        <th aria-sort={sort.aria('captured')}
          ><SortButton {sort} key="captured">Captured</SortButton></th
        >
      </tr>
    </thead>
    <tbody>
      {#each sorted as s (s.id)}
        {@const tests = lockedBy.get(s.id) ?? []}
        <!-- The name is the row's button, for a keyboard; a pointer may click anywhere on the row. -->
        <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_noninteractive_element_interactions -->
        <tr onclick={() => onpick(s.id)}>
          <td class="lock-cell">
            {#if tests.length > 0}
              <span
                class="lock"
                title={`Locked by ${tests.length === 1 ? 'the test' : 'the tests'} ${tests.join(', ')}`}
                ><LockIcon /></span
              >
            {/if}
          </td>
          <td><button type="button" class="name">{s.name}</button></td>
          <td class="dim">{s.source ?? '—'}</td>
          <td class="num">{s.records}</td>
          <td class="dim">{s.captured === undefined ? '—' : dateTime(s.captured)}</td>
        </tr>
      {:else}
        <tr class="none"><td colspan="5" class="dim">No sample matches.</td></tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .picker {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }
  .lead {
    margin: 0;
    font-size: var(--text-note);
  }
  .dim {
    color: var(--dim);
  }
  .tools {
    display: flex;
    gap: 0.6rem;
    align-items: center;
  }
  /* ClusterPage's worker search. */
  .search {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex: 1;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0 0.7rem;
    color: var(--dim);
    font-weight: 400;
  }
  .search input {
    width: 100%;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--ink);
    font: inherit;
    padding: 1px 2px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-body);
  }
  th {
    text-align: left;
    font-weight: 500;
    color: var(--dim);
    font-size: var(--text-note);
    padding: 0.45rem 0.6rem;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  td {
    white-space: nowrap;
    padding: 0.5rem 0.6rem;
    border-bottom: 1px solid var(--border);
  }
  .num {
    text-align: right;
  }
  .lock-cell {
    width: 1%;
    padding-inline: 0.25rem;
  }
  .lock-cell :global(svg) {
    width: 14px;
    height: 14px;
    vertical-align: -2px;
  }
  .lock {
    display: inline-flex;
    color: var(--accent);
  }
  tbody tr:not(.none) {
    cursor: pointer;
  }
  tbody tr:not(.none):hover td {
    background: var(--panel);
  }
  .name {
    padding: 0;
    border: 0;
    background: none;
    font: inherit;
    font-weight: 600;
    color: var(--ink);
    cursor: pointer;
  }
  tbody tr:hover .name,
  .name:focus-visible {
    text-decoration: underline;
  }
  .visually-hidden {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
    white-space: nowrap;
  }
</style>
