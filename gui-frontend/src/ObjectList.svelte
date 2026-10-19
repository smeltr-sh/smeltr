<!--
  A page's list of configured objects -- its sources, its destinations -- as one table: a search,
  a chip per type where the list mixes types, column heads that sort (`sort.svelte.ts`), and rows
  that fold open in place to edit. What `shared/` declares sits among this scope's own, marked,
  and opens nowhere: it is edited in `shared/`. Where the scope is not the person's to change
  (`access.ts`), nothing is offered to add, edit as JSON or remove, and the rows open to read.

  A row keeps its place while it is edited. The order is taken when the sort, the search, a chip or
  the list's length changes, not as a field is typed, so renaming an open row does not move it out
  from under the cursor.
-->
<script lang="ts" module>
  import type { SortValue } from './sort.svelte';

  export type ObjectColumn<T> = {
    key: string;
    label: string;
    /** What the column sorts by. */
    value: (item: T) => SortValue;
    /** What the cell shows, where that is not the value itself. */
    text?: (item: T) => string;
    /** The column's grid track. */
    width?: string;
  };
</script>

<script lang="ts" generics="T">
  import type { Snippet } from 'svelte';
  import { tick, untrack } from 'svelte';
  import { SvelteSet } from 'svelte/reactivity';
  import ChipPicker from './ChipPicker.svelte';
  import Chips from './Chips.svelte';
  import Collapsible from './Collapsible.svelte';
  import MoreIcon from './MoreIcon.svelte';
  import OnlyNotice from './OnlyNotice.svelte';
  import { Narrowing } from './narrowing.svelte';
  import SortButton from './SortButton.svelte';
  import ValidationItem from './ValidationItem.svelte';
  import { TableSort } from './sort.svelte';
  import { editable } from './access';
  import type { Validation } from './validation.svelte';

  let {
    items,
    shared = [],
    noun,
    idOf,
    typeOf,
    typeLabel,
    columns,
    anchor,
    highlight,
    empty,
    toolbar,
    extra,
    body,
    rowActions,
    sharedActions,
    validation,
    only,
  }: {
    /** This scope's own, in the file's order. */
    items: T[];
    /** What `shared/` declares, which this scope runs too. */
    shared?: T[];
    /** One of them, for the search's placeholder: `source`. */
    noun: string;
    idOf: (item: T) => string;
    typeOf?: (item: T) => string;
    typeLabel?: (type: string) => string;
    columns: ObjectColumn<T>[];
    /** The row's element id, so a link from elsewhere lands on it. */
    anchor: (item: T) => string;
    /** The id of a row to show and mark: one a link landed on, or one just added. */
    highlight?: string;
    empty: string;
    /** Before the search: the add button. */
    toolbar: Snippet;
    /** At the far end of the toolbar. */
    extra?: Snippet;
    /** A row of this scope's, open. `i` is its index in `items`. */
    body: Snippet<[T, number]>;
    /** A row's menu: `.pick` buttons (`ChipPicker`). */
    rowActions: Snippet<[T, number]>;
    sharedActions: Snippet<[T]>;
    /** The page's save errors, which open the row they name. */
    validation?: Validation;
    /** The ids the list is narrowed to (`?only=`), where it is. */
    only?: string[];
  } = $props();

  const readOnly = !editable();
  const ID = '#id';
  const TYPE = '#type';
  const sort = new TableSort<string>();
  let query = $state('');
  let chip = $state<string | null>(null);
  const opened = new SvelteSet<T>();
  /** The row whose menu is open. */
  let menuFor = $state<T | null>(null);

  const all = $derived([...shared, ...items]);
  const narrowing = new Narrowing(() => only);
  const kept = (item: T) => narrowing.keeps(idOf(item));
  const types = $derived.by(() => {
    if (!typeOf) return [];
    const counts = new Map<string, number>();
    for (const item of all) counts.set(typeOf(item), (counts.get(typeOf(item)) ?? 0) + 1);
    return [...counts].sort(([a], [b]) => label(a).localeCompare(label(b)));
  });

  const label = (type: string) => typeLabel?.(type) ?? type;

  function matches(item: T, q: string): boolean {
    if (q === '') return true;
    const texts = [
      idOf(item),
      ...columns.map((c) => c.text?.(item) ?? String(c.value(item) ?? '')),
    ];
    if (typeOf) texts.push(label(typeOf(item)));
    return texts.some((t) => t.toLowerCase().includes(q));
  }

  type Row = { item: T; i: number };
  const order: Row[] = $derived.by(() => {
    const q = query.trim().toLowerCase();
    // A chip whose type has gone, or whose chips are hidden, would hide every row with no way back.
    const wanted = showType && types.some(([t]) => t === chip) ? chip : null;
    void [sort.key, sort.dir, items.length, shared.length, narrowing.ids];
    return untrack(() => {
      const rows: Row[] = [
        ...shared.map((item) => ({ item, i: -1 })),
        ...items.map((item, i) => ({ item, i })),
      ].filter(
        ({ item }) =>
          kept(item) && (wanted === null || typeOf?.(item) === wanted) && matches(item, q),
      );
      const by: Record<string, (r: Row) => SortValue> = {
        [ID]: (r) => idOf(r.item),
        [TYPE]: (r) => (typeOf ? label(typeOf(r.item)) : undefined),
      };
      for (const c of columns) by[c.key] = (r) => c.value(r.item);
      return sort.apply(rows, by);
    });
  });

  const showType = $derived(types.length > 1);
  const template = $derived(
    [
      'minmax(10rem, 1.2fr)',
      ...(showType ? ['8rem'] : []),
      ...columns.map((c) => c.width ?? 'minmax(6rem, 1fr)'),
    ].join(' '),
  );

  /** A save error on a row the search, a chip or `?only=` hides: show the whole list again, then the row. */
  async function reveal(item: T) {
    if (!order.some((r) => r.item === item)) {
      query = '';
      chip = null;
      if (!kept(item)) narrowing.widen();
    }
    opened.add(item);
    await tick();
  }

  // Every row of this scope's opens to its errors, shown or not: one the search or a chip hides
  // has no `ValidationItem` on screen to do it.
  $effect(() => {
    if (!validation) return;
    const offs = items.map((item) => validation.opener(idOf(item), () => void reveal(item)));
    return () => offs.forEach((off) => off());
  });

  $effect(() => {
    const id = highlight;
    if (!id) return;
    const item = untrack(() => all.find((x) => idOf(x) === id));
    if (!item) return;
    untrack(() => reveal(item)).then(() =>
      document.getElementById(anchor(item))?.scrollIntoView({ block: 'center' }),
    );
  });
</script>

<OnlyNotice {narrowing} ids={all.map(idOf)} />
<div class="toolbar">
  {#if !readOnly}{@render toolbar()}{/if}
  <label class="search">
    <span aria-hidden="true">⌕</span>
    <input
      type="search"
      bind:value={query}
      placeholder="Search {all.length} {noun}{all.length === 1 ? '' : 's'}"
      aria-label="Search {noun}s"
    />
  </label>
  {#if showType}
    <Chips
      items={types.map(([type, n]) => ({ key: type, label: label(type), count: n }))}
      bind:value={chip}
      label="Type"
      allCount={all.length}
      toggles
    />
  {/if}
  <span class="gap"></span>
  {#if !readOnly}{@render extra?.()}{/if}
</div>

{#snippet menu(item: T, i: number)}
  <ChipPicker
    kind="row"
    compact
    ariaLabel="More actions for {idOf(item)}"
    bind:open={() => menuFor === item, (v) => (menuFor = v ? item : null)}
  >
    {#snippet face()}<MoreIcon />{/snippet}
    <!-- Choosing closes the menu (`ChipPicker` leaves that to its caller). -->
    <div class="menu-picks" onclick={() => (menuFor = null)} role="presentation">
      {#if i < 0}{@render sharedActions(item)}{:else}{@render rowActions(item, i)}{/if}
    </div>
  </ChipPicker>
{/snippet}

{#if all.length === 0}
  <p class="dim">{empty}</p>
{:else}
  <div class="list" style:--columns={template}>
    <div class="head">
      <div>
        <SortButton {sort} key={ID}>id</SortButton>
      </div>
      {#if showType}
        <div>
          <SortButton {sort} key={TYPE}>type</SortButton>
        </div>
      {/if}
      {#each columns as c (c.key)}
        <div>
          <SortButton {sort} key={c.key}>{c.label}</SortButton>
        </div>
      {/each}
    </div>
    {#each order as { item, i } (item)}
      {@const id = idOf(item)}
      {#snippet cells()}
        <span class="row-cells">
          <span class="cell id">
            {#if i < 0}<span class="shared-dot" title="from shared/"></span>{/if}{id}
          </span>
          {#if showType && typeOf}<span class="cell type">{label(typeOf(item))}</span>{/if}
          {#each columns as c (c.key)}
            <span class="cell">{c.text?.(item) ?? c.value(item) ?? ''}</span>
          {/each}
        </span>
      {/snippet}
      <div class="row" class:highlight={highlight === id} id={anchor(item)}>
        {#if i < 0}
          <Collapsible variant="row" fixed>
            {#snippet summary()}{@render cells()}{/snippet}
            {#snippet actions()}{@render menu(item, i)}{/snippet}
          </Collapsible>
        {:else}
          <ValidationItem {id} open={() => void reveal(item)}>
            <Collapsible
              variant="row"
              bind:open={
                () => opened.has(item), (v) => (v ? opened.add(item) : opened.delete(item))
              }
            >
              {#snippet summary()}{@render cells()}{/snippet}
              {#snippet actions()}{#if !readOnly}{@render menu(item, i)}{/if}{/snippet}
              {@render body(item, i)}
            </Collapsible>
          </ValidationItem>
        {/if}
      </div>
    {:else}
      <p class="none">No {noun} matches.</p>
    {/each}
  </div>
{/if}

<style>
  .toolbar {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem 0.6rem;
    align-items: center;
    margin: 1.2rem 0 0.8rem;
  }

  .gap {
    flex: 1;
  }

  .search {
    position: relative;
  }

  .search span {
    position: absolute;
    left: 0.5rem;
    top: 50%;
    transform: translateY(-50%);
    color: var(--dim);
    pointer-events: none;
  }

  .search input {
    width: 16rem;
    padding-left: 1.5rem;
  }

  .list {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
  }

  /* The heads and every row's cells share one set of tracks, so the columns line up down the list;
     the heads are indented past the caret each row starts with. */
  .head,
  .row-cells {
    display: grid;
    grid-template-columns: var(--columns);
    gap: 0.7rem;
    align-items: center;
  }

  .row-cells {
    flex: 1;
    min-width: 0;
  }

  /* Past the caret on the left and the row's menu on the right, so the heads sit over their cells. */
  .head {
    padding: 0.4rem calc(0.7rem + 0.5rem + 1.6rem) 0.4rem 1.85rem;
    border-bottom: 1px solid var(--border);
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
  }

  .row {
    position: relative;
  }

  .row + .row {
    border-top: 1px solid var(--border);
  }

  /* Landed on from the map, or just added. Drawn above the row, whose open head paints a panel. */
  .row.highlight::before {
    content: '';
    position: absolute;
    inset: 0 auto 0 0;
    z-index: 1;
    width: 3px;
    background: var(--accent);
  }

  .cell {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-family: var(--mono);
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .cell.id {
    font-size: var(--text-label);
    font-weight: 600;
    color: var(--ink);
  }

  .cell.type {
    font-family: inherit;
  }

  .shared-dot {
    display: inline-block;
    width: 0.45rem;
    height: 0.45rem;
    margin-right: 0.35rem;
    border-radius: 50%;
    background: oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  /* Its picks stay the panel's own children for layout. */
  .menu-picks {
    display: contents;
  }

  .none,
  .dim {
    margin: 0;
    color: var(--dim);
  }

  .none {
    padding: 0.6rem 0.7rem;
  }
</style>
