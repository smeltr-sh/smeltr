<!--
  Every top-level page opens with this. A tab with sibling pages (Status,
  Build, Changes, Admin) gets a rail naming them, current one marked, with
  the page itself beside it -- replacing the flat `<main class="column">` a
  single-page tab (Map) still uses directly. One place decides whether a
  route has siblings, so no page answers that question itself.

  The rail folds to its icons to give the page its width.

  The group-hue accent (STYLE_GUIDE.md, "Group hues") used to run down
  `.column`'s own left gutter; it now marks the content pane specifically,
  at the seam past the rail -- the rail's own links aren't a group's, only
  what sits beside it is.
-->
<script lang="ts">
  import type { Snippet, Component } from 'svelte';
  import ScopePicker from './ScopePicker.svelte';
  import PersonIcon from './PersonIcon.svelte';
  import LockIcon from './LockIcon.svelte';
  import CertificateIcon from './CertificateIcon.svelte';
  import WorkerIcon from './WorkerIcon.svelte';
  import WorkerGroupIcon from './WorkerGroupIcon.svelte';
  import SlidersIcon from './SlidersIcon.svelte';
  import MenuIcon from './MenuIcon.svelte';
  import { RAIL_GLYPHS } from './railGlyphs';
  import { parseRoute, section, showsScope, TABS } from './nav';

  /** Empty when the page could not load: the error bar says why. */
  let { children }: { children?: Snippet } = $props();

  let hash = $state(location.hash);
  $effect(() => {
    const sync = () => (hash = location.hash);
    addEventListener('hashchange', sync);
    return () => removeEventListener('hashchange', sync);
  });

  const route = $derived(parseRoute(hash));
  const here = $derived(section(route));
  const tabLabel = $derived(TABS.find((t) => t.tab === route.tab)?.label ?? '');

  // Folded or not is the operator's, across every section and every reload.
  const FOLDED_KEY = 'smeltr-rail-folded';
  let folded = $state(loadFolded());
  function loadFolded(): boolean {
    try {
      return localStorage.getItem(FOLDED_KEY) === 'true';
    } catch {
      return false;
    }
  }
  function fold() {
    folded = !folded;
    try {
      localStorage.setItem(FOLDED_KEY, String(folded));
    } catch {
      // Best-effort: a blocked or full localStorage just forgets across reloads.
    }
  }

  // Admin's items draw the icon components their pages already use; every
  // other item draws its sign from `RAIL_GLYPHS`.
  const RAIL_ICONS: Partial<Record<string, Component>> = {
    Workers: WorkerIcon,
    'Worker groups': WorkerGroupIcon,
    Secrets: LockIcon,
    Certificates: CertificateIcon,
    Accounts: PersonIcon,
    System: SlidersIcon,
  };
</script>

{#if here}
  <div class="with-rail">
    <nav class="rail" class:folded aria-label="{tabLabel} sections">
      <div class="rail-head">
        <div class="rail-top">
          <button
            type="button"
            class="fold"
            aria-expanded={!folded}
            aria-label={folded
              ? `Show ${tabLabel} section names`
              : `Hide ${tabLabel} section names`}
            onclick={fold}><MenuIcon /></button
          >
          {#if !folded}<span class="rail-title">{tabLabel}</span>{/if}
        </div>
        <!-- Folded, the picker stays mounted: it keeps the scope valid and the hues set. -->
        {#if showsScope(route)}<div hidden={folded}><ScopePicker /></div>{/if}
      </div>
      {#each here.items as item (item.hash)}
        {@const Icon = RAIL_ICONS[item.label]}
        {@const glyph = RAIL_GLYPHS[item.label]}
        <!-- Folded, the label is truncated away entirely, so `title` carries it. -->
        <a
          href={item.hash}
          class:on={item.label === here.here}
          aria-label={folded ? item.label : undefined}
          title={folded ? item.label : undefined}
        >
          {#if Icon}<Icon />{:else if glyph}<svg
              viewBox="0 0 64 64"
              width="18"
              height="18"
              aria-hidden="true"
              ><path
                d={glyph.d}
                fill="currentColor"
                fill-rule={glyph.evenodd ? 'evenodd' : 'nonzero'}
              /></svg
            >{/if}
          {#if !folded}{item.label}{/if}
        </a>
      {/each}
    </nav>
    <main class="rail-content">
      {@render children?.()}
    </main>
  </div>
{:else}
  <main class="column">
    {@render children?.()}
  </main>
{/if}

<style>
  .with-rail {
    display: flex;
    flex-wrap: wrap;
    gap: 2.25rem;
    max-width: var(--measure, var(--width-page));
    margin-inline: auto;
    padding: 2.25rem 1.5rem 4rem;
  }

  .rail {
    flex: 0 0 11rem;
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    border-right: 1px solid var(--border);
    padding-right: 1.25rem;
  }

  /* Folded, the rail keeps its icons and the control that unfolds it, and
     the page beside it takes the width. */
  .rail.folded {
    flex-basis: auto;
    padding-right: 0.75rem;
  }

  /* The section's own name, so the rail reads on its own even scrolled past
     its own top -- and the scope, where a page has one, naming what the
     pane beside it edits. The seam past the rail is already painted in the
     scope's color, so the picker sits on top of its own indicator. */
  .rail-head {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    padding: 0 0.6rem 0.6rem;
    margin-bottom: 0.5rem;
    border-bottom: 1px solid var(--border);
  }

  .rail-top {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .fold {
    display: inline-flex;
    padding: 0.25rem;
    border: none;
    border-radius: var(--radius);
    background: none;
    color: var(--dim);
    cursor: pointer;
  }

  .fold:hover {
    color: var(--ink);
    background: color-mix(in oklab, var(--ink) 8%, var(--bg));
  }

  .fold:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 1px;
  }

  .rail-title {
    font-family: var(--font-display);
    font-size: var(--text-section);
    font-weight: 600;
    color: var(--ink);
  }

  .rail a {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    padding: 0.5rem 0.6rem;
    border-radius: var(--radius);
    border-left: 3px solid transparent;
    color: var(--dim);
    font-size: var(--text-body);
    font-weight: 500;
    text-decoration: none;
  }

  /* Menus (STYLE_GUIDE.md): a row tints toward ink over the surface
     it sits on -- `--bg` here, the rail's own ground -- rather than taking
     the color of the page behind it. */
  .rail a:hover {
    color: var(--ink);
    background: color-mix(in oklab, var(--ink) 8%, var(--bg));
  }

  /* The row already chosen carries a left bar in the accent, not a filled
     background of it -- the same convention Menus documents for a chosen
     row, so the rail reads like every other choice list in the app rather
     than like a button. It does not respond to hover beyond that: its own
     colors already are the hover colors. */
  .rail a.on {
    background: color-mix(in oklab, var(--ink) 10%, var(--bg));
    border-left-color: var(--accent);
    color: var(--ink);
    font-weight: 600;
  }

  .rail a.on :global(svg) {
    color: var(--ink);
  }

  .rail-content {
    flex: 1 1 20rem;
    min-width: 0;
    position: relative;
  }

  /* `--scope-h` is set on the root only for the pages a scope applies to.
     With it absent this is invalid at computed-value time and paints
     nothing, which is how a Changes or Admin page stays uncolored. */
  .rail-content::before {
    content: '';
    position: absolute;
    left: -1.5rem;
    top: -0.2rem;
    bottom: -0.2rem;
    width: 4px;
    border-radius: 2px;
    background: oklch(var(--scope-l) var(--scope-c) var(--scope-h));
  }
</style>
