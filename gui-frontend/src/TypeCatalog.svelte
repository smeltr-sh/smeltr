<!--
  The first step of adding something that has a type: every type it may be, found by name or by
  category, and picked with one click. Categories, and the types under each, are in order of name.

  Given kinds, it asks for the kind first, then shows the catalog with that kind's chip on. The
  chips stay, so another kind is one click away; each tile carries its kind's sign.
-->
<script lang="ts" module>
  import type { Glyph } from './stepGlyphs';
  import Help from './Help.svelte';
  import Chips from './Chips.svelte';

  export type CatalogType<T extends string, K extends string = string> = {
    type: T;
    label: string;
    category: string;
    glyph: Glyph;
    /** Which of the catalog's kinds it is, when it has kinds. */
    kind?: K;
    /** What the type does: behind a `?` on its tile, or under its name on its card. */
    help?: string;
  };

  /** A way of sorting the types above their categories, asked for before them. */
  export type CatalogKind<K extends string> = {
    kind: K;
    label: string;
    /** What the kind means, behind a `?` on its card and its chip. */
    help: string;
    glyph: Glyph;
  };
</script>

<script lang="ts" generics="T extends string, K extends string">
  let {
    types,
    onpick,
    kinds = [],
    kind = null,
    onkind,
    cards = false,
  }: {
    types: CatalogType<T, K>[];
    onpick: (type: T) => void;
    kinds?: CatalogKind<K>[];
    /** The kind last picked, on the first step or by a chip; null while it is being asked. The owner
     * keeps it, so its dialog's back returns to the question, or from a type's form to that kind. */
    kind?: K | null;
    onkind?: (kind: K) => void;
    /** Each type on a kind's card, its help written under its name, for a catalog small enough to read whole. */
    cards?: boolean;
  } = $props();

  const uid = $props.id();
  const ALL = 'All';
  let query = $state('');
  let category = $state(ALL);
  /** The chip on: the kind picked first, until another chip is chosen. Null is All. Taken from
   * `kind` once, when the catalog mounts, as it does again on coming back from a type's form. */
  // svelte-ignore state_referenced_locally
  let chip: K | null = $state(kind);

  /** Only kinds that have a type: a kind with nothing to pick is no choice. */
  const offered = $derived(kinds.filter((k) => types.some((t) => t.kind === k.kind)));
  const asking = $derived(offered.length > 1 && kind === null);
  const glyphOf = (k: K | undefined) => offered.find((o) => o.kind === k)?.glyph;
  /** The types the chip leaves, which the rail and the tiles are drawn from. */
  const inKind = $derived(chip === null ? types : types.filter((t) => t.kind === chip));

  const byName = (a: string, b: string) => a.localeCompare(b, undefined, { sensitivity: 'base' });
  const categories = $derived([...new Set(inKind.map((t) => t.category))].sort(byName));
  const sorted = $derived(
    [...(query.trim() === '' ? inKind : types)].sort((a, b) => byName(a.label, b.label)),
  );
  /** A search reaches every category and every kind, so a match is never hidden behind the one chosen. */
  const groups = $derived.by(() => {
    const q = query.trim().toLowerCase();
    const shown = q === '' ? categories : [...new Set(types.map((t) => t.category))].sort(byName);
    return shown
      .filter((c) => q !== '' || category === ALL || c === category)
      .map((c) => ({
        category: c,
        types: sorted.filter(
          (t) =>
            t.category === c &&
            (q === '' || t.label.toLowerCase().includes(q) || c.toLowerCase().includes(q)),
        ),
      }))
      .filter((g) => g.types.length > 0);
  });

  function focusSearch(node: HTMLInputElement) {
    node.focus();
  }
</script>

{#snippet sign(glyph: Glyph)}
  <svg viewBox="0 0 64 64" aria-hidden="true"
    ><path d={glyph.d} fill="currentColor" fill-rule={glyph.evenodd ? 'evenodd' : 'nonzero'} /></svg
  >
{/snippet}

<!-- A big card: a kind with its first types beside a `?`, or, given `helpId`, a type with its help under its name. -->
{#snippet card(glyph: Glyph, label: string, text: string, onclick: () => void, helpId?: string)}
  <!-- A type is named by its label alone, so its help reads as its description rather than its name. -->
  <button
    type="button"
    class="kind-card"
    class:bare={helpId !== undefined}
    aria-label={helpId === undefined ? undefined : label}
    aria-describedby={helpId !== undefined && text !== '' ? helpId : undefined}
    {onclick}
  >
    {@render sign(glyph)}
    <span>
      <b>{label}</b>
      <span class={helpId === undefined ? 'examples' : 'card-text'} id={helpId}>{text}</span>
    </span>
  </button>
{/snippet}

{#if asking}
  <div class="kinds">
    {#each offered as k (k.kind)}
      <div class="tile-box">
        {@render card(
          k.glyph,
          k.label,
          types
            .filter((t) => t.kind === k.kind)
            .slice(0, 3)
            .map((t) => t.label)
            .join(' · '),
          () => {
            // Coming back to the question starts the catalog over, so nothing from another kind hides this one's types.
            chip = k.kind;
            category = ALL;
            query = '';
            onkind?.(k.kind);
          },
        )}
        <span class="card-help"><Help label={k.label}>{k.help}</Help></span>
      </div>
    {/each}
  </div>
{:else}
  <div class="catalog">
    <label class="search">
      <span aria-hidden="true">⌕</span>
      <input
        type="search"
        bind:value={query}
        placeholder="Search {types.length} types"
        aria-label="Search types"
        use:focusSearch
      />
    </label>
    {#if offered.length > 1}
      <Chips
        items={offered.map((k) => ({ key: k.kind, label: k.label, glyph: k.glyph, help: k.help }))}
        bind:value={chip}
        label="Kinds"
        onpick={(k) => {
          // A search reaches every kind, so a chip picked during one would filter nothing.
          query = '';
          // The owner keeps the kind last chosen, so coming back from a type's form returns to it.
          if (k !== null) onkind?.(k);
          if (k !== null && !types.some((t) => t.kind === k && t.category === category))
            category = ALL;
        }}
      />
    {/if}
    <div class="split" class:single={categories.length < 2}>
      {#if categories.length > 1}
        <nav class="categories" aria-label="Categories">
          {#each [ALL, ...categories] as c (c)}
            <button
              type="button"
              class:on={category === c && query.trim() === ''}
              onclick={() => {
                category = c;
                query = '';
              }}
            >
              {c}<span class="count"
                >{c === ALL ? inKind.length : inKind.filter((t) => t.category === c).length}</span
              >
            </button>
          {/each}
        </nav>
      {/if}
      <div class="types">
        {#each groups as g (g.category)}
          {#if categories.length > 1 || groups.length > 1}<div class="category">
              {g.category}
            </div>{/if}
          {#if cards}
            <div class="kinds">
              {#each g.types as t (t.type)}
                {@render card(
                  t.glyph,
                  t.label,
                  t.help ?? '',
                  () => onpick(t.type),
                  `${uid}-${t.type}`,
                )}
              {/each}
            </div>
          {:else}
            <div class="tiles">
              {#each g.types as t (t.type)}
                <!-- The `?` sits beside the tile's button, not in it: a click on it must not pick the type. -->
                <div class="tile-box">
                  <button
                    type="button"
                    class="tile"
                    class:helped={t.help}
                    onclick={() => onpick(t.type)}
                  >
                    {@render sign(t.glyph)}
                    {t.label}
                    {#if glyphOf(t.kind)}<span class="kind-sign"
                        >{@render sign(glyphOf(t.kind)!)}</span
                      >{/if}
                  </button>
                  {#if t.help}<span class="tile-help"><Help label={t.label}>{t.help}</Help></span
                    >{/if}
                </div>
              {/each}
            </div>
          {/if}
        {:else}
          <p class="none">No type matches.</p>
        {/each}
      </div>
    </div>
  </div>
{/if}

<style>
  .kinds {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(18rem, 1fr));
    gap: 0.6rem;
  }

  .kind-card {
    display: flex;
    align-items: center;
    gap: 0.8rem;
    padding: 1rem 2.4rem 1rem 1rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--bg);
    font: inherit;
    color: var(--ink);
    text-align: left;
    cursor: pointer;
  }

  /* No `?` beside it to make room for. */
  .kind-card.bare {
    padding-right: 1rem;
  }

  .kind-card:hover,
  .kind-card:focus-visible {
    border-color: var(--accent);
  }

  .kind-card svg {
    flex: none;
    width: 1.8rem;
    height: 1.8rem;
  }

  .kind-card b {
    display: block;
    font-size: var(--text-section);
  }

  .card-text {
    display: block;
    font-size: var(--text-label);
    color: var(--dim);
  }

  .examples {
    font-family: var(--mono);
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .card-help {
    position: absolute;
    top: 0.7rem;
    right: 0.7rem;
  }

  .catalog {
    display: flex;
    flex-direction: column;
    gap: 0.8rem;
  }

  .search {
    position: relative;
    display: block;
  }

  .search span {
    position: absolute;
    left: 0.55rem;
    top: 50%;
    transform: translateY(-50%);
    color: var(--dim);
    pointer-events: none;
  }

  .search input {
    width: 100%;
    padding-left: 1.6rem;
  }

  .split {
    display: grid;
    grid-template-columns: 11rem 1fr;
    gap: 1rem;
    min-height: 16rem;
  }

  .split.single {
    grid-template-columns: 1fr;
    min-height: 0;
  }

  .categories {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
  }

  .categories button {
    display: flex;
    justify-content: space-between;
    gap: 0.5rem;
    padding: 0.3rem 0.5rem;
    border: none;
    border-radius: var(--radius);
    background: none;
    font: inherit;
    font-size: var(--text-label);
    color: var(--dim);
    text-align: left;
    cursor: pointer;
  }

  .categories button:hover {
    color: var(--ink);
  }

  .categories button.on {
    background: color-mix(in oklch, var(--ink) 8%, var(--panel));
    color: var(--ink);
    font-weight: 600;
  }

  .count {
    font-variant-numeric: tabular-nums;
  }

  /* The uppercase label (STYLE_GUIDE.md, "Typography"). */
  .category {
    margin: 0.9rem 0 0.4rem;
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
  }

  .category:first-child {
    margin-top: 0;
  }

  .tiles {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(12rem, 1fr));
    gap: 0.45rem;
  }

  .tile-box {
    position: relative;
    display: grid;
  }

  .tile-help {
    position: absolute;
    top: 50%;
    right: 0.7rem;
    transform: translateY(-50%);
  }

  .tile {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.6rem 0.7rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--bg);
    font: inherit;
    font-size: var(--text-label);
    color: var(--ink);
    text-align: left;
    cursor: pointer;
  }

  .tile.helped {
    padding-right: 2.2rem;
  }

  .tile:hover,
  .tile:focus-visible {
    border-color: var(--accent);
  }

  .tile svg {
    flex: none;
    width: 1.3rem;
    height: 1.3rem;
    color: var(--dim);
  }

  /* The kind's sign, at the tile's far end; the type's own leads it. */
  .kind-sign {
    display: flex;
    margin-left: auto;
  }

  .tile .kind-sign svg {
    width: 1rem;
    height: 1rem;
  }

  .none {
    margin: 0;
    color: var(--dim);
  }
</style>
