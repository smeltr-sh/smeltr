<!--
  A lookup table's rows, editable in place: an inline table's own, or a CSV file's read into the
  page. Paged and searchable, so a file of many thousand rows draws one page of inputs. A row whose
  field count is not the header's is a problem the compiler refuses, listed under the grid.
  Buttons, inputs and the bar take the page's styles.
-->
<script lang="ts">
  import InlineError from './InlineError.svelte';
  import { count } from './time';
  import { tick } from 'svelte';
  import CrossButton from './CrossButton.svelte';
  import MoreButton from './MoreButton.svelte';
  import Locked from './Locked.svelte';
  import { editable } from './access';

  let {
    headers = $bindable(),
    rows = $bindable(),
    onedit,
  }: { headers: string[]; rows: string[][]; onedit: () => void } = $props();

  const readOnly = !editable();

  const PAGE = 100;
  let q = $state('');
  let page = $state(0);
  let focus = $state(-1);
  let grid: HTMLElement | undefined = $state();

  let matches = $derived.by(() => {
    const needle = q.trim().toLowerCase();
    const all = rows.map((_, r) => r);
    return needle ? all.filter((r) => rows[r].some((v) => v?.toLowerCase().includes(needle))) : all;
  });
  let pages = $derived(Math.max(1, Math.ceil(matches.length / PAGE)));
  let shown = $derived(matches.slice(page * PAGE, (page + 1) * PAGE));
  let ragged = $derived(rows.flatMap((r, i) => (r.length === headers.length ? [] : [i])));
  /** The ragged rows listed before a button lists the rest. */
  const RAGGED_NAMED = 5;
  let raggedAll = $state(false);

  function edit(f: () => void) {
    f();
    onedit();
  }

  /** Adds a column at the end, or removes column `at`, in the header and every row. */
  function column(at?: number) {
    if (at === undefined) headers.push(`col${headers.length + 1}`);
    else headers.splice(at, 1);
    for (const r of rows) {
      if (at === undefined) r.push('');
      else r.splice(at, 1);
    }
    onedit();
  }

  function addRow() {
    edit(() => rows.push(headers.map(() => '')));
    void show(rows.length - 1);
  }

  async function show(r: number) {
    q = '';
    page = Math.floor(r / PAGE);
    focus = r;
    await tick();
    grid?.querySelector('tr.focus')?.scrollIntoView({ block: 'nearest' });
  }
</script>

<div class="bar">
  <input
    type="search"
    placeholder="Search {count(rows.length, 'row')}"
    bind:value={q}
    oninput={() => (page = 0)}
  />
  {#if q}<span class="dim">{matches.length} match</span>{/if}
  <span class="grow"></span>
  {#if !readOnly}
    <button type="button" onclick={() => column()}>+ column</button>
    <button type="button" onclick={addRow}>+ row</button>
  {/if}
</div>

<div class="scroll" bind:this={grid}>
  <Locked>
    <table>
      <thead>
        <tr>
          <th class="rn">#</th>
          {#each headers as _, c (c)}
            <th>
              <span class="cell">
                <input aria-label="column name" bind:value={headers[c]} oninput={onedit} />
                {#if !readOnly}
                  <span class="reveal"
                    ><CrossButton
                      removes
                      label="Remove column {headers[c]}"
                      onclick={() => column(c)}
                    /></span
                  >
                {/if}
              </span>
            </th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#each shown as r (r)}
          <tr class:focus={r === focus}>
            <td class="rn">
              {r + 1}
              {#if !readOnly}
                <span class="reveal"
                  ><CrossButton
                    removes
                    label="Remove row {r + 1}"
                    onclick={() => edit(() => rows.splice(r, 1))}
                  /></span
                >
              {/if}
            </td>
            {#each headers as _, c (c)}
              <td class:missing={c >= rows[r].length}>
                <input
                  aria-label="row {r + 1} {headers[c]}"
                  value={rows[r][c] ?? ''}
                  oninput={(e) => edit(() => (rows[r][c] = e.currentTarget.value))}
                />
              </td>
            {/each}
          </tr>
        {/each}
      </tbody>
    </table>
  </Locked>
</div>

<div class="bar dim">
  <button type="button" disabled={page === 0} onclick={() => page--}>‹</button>
  page {page + 1} of {pages}
  <button type="button" disabled={page >= pages - 1} onclick={() => page++}>›</button>
  <span class="grow"></span>
  {count(rows.length, 'row')} · {count(headers.length, 'column')}
</div>

{#each raggedAll ? ragged : ragged.slice(0, RAGGED_NAMED) as r (r)}
  <p class="bar">
    <InlineError inline
      >row {r + 1} has {rows[r].length} fields; the header has {headers.length}.</InlineError
    >
    <button type="button" class="quiet" onclick={() => show(r)}>show</button>
  </p>
{/each}
{#if !raggedAll && ragged.length > RAGGED_NAMED}
  <p class="bar">
    <MoreButton count={ragged.length - RAGGED_NAMED} onclick={() => (raggedAll = true)} />
  </p>
{/if}

<style>
  .scroll {
    max-height: 32rem;
    overflow: auto;
    border: 1px solid var(--border);
    background: var(--field);
  }

  table {
    min-width: 100%;
  }

  th,
  td {
    padding: 0;
    border: solid var(--border);
    border-width: 0 1px 1px 0;
  }

  thead th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--panel);
  }

  .scroll input {
    width: 100%;
    min-width: 9ch;
    border: 0;
    border-radius: 0;
    background: transparent;
    padding: 0.3rem 0.55rem;
    font-size: var(--text-note);
  }

  th input {
    font-family: var(--font-body);
    font-weight: 600;
  }

  .scroll input:focus {
    outline: none;
    box-shadow: inset 0 0 0 2px var(--accent);
  }

  .rn {
    vertical-align: middle;
    white-space: nowrap;
    padding: 0 0.3rem;
    background: var(--panel);
    color: var(--dim);
  }

  .cell {
    display: flex;
    align-items: center;
    justify-content: flex-end;
  }

  /* A field this row does not have: the texture an invalid input takes. */
  td.missing {
    background-image: repeating-linear-gradient(
      135deg,
      color-mix(in oklch, var(--error) 14%, transparent) 0 4px,
      transparent 4px 8px
    );
  }

  tr.focus td {
    background-color: color-mix(in oklab, var(--ink) 8%, var(--field));
  }

  /* Remove controls appear on hover; a touch screen has none, so it shows them. */
  .reveal {
    display: inline-grid;
    vertical-align: middle;
    opacity: 0;
  }

  /* A text action beside a row's error, quiet so the error leads. */
  .quiet {
    padding: 0.3rem 0.55rem;
    border-color: transparent;
    background: none;
    color: var(--dim);
    font-size: var(--text-note);
  }

  tr:hover .reveal,
  th:hover .reveal,
  .reveal:focus-within {
    opacity: 1;
  }

  @media (hover: none) {
    .reveal {
      opacity: 1;
    }
  }
</style>
