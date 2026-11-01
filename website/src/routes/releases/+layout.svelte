<script lang="ts">
  import type { Snippet } from 'svelte';
  import NavGroups from '#lib/components/NavGroups.svelte';
  import { railGroups } from '#lib/releases.ts';

  let { children }: { children: Snippet } = $props();
  const groups = railGroups();
  // On a narrow screen the rail folds behind a button, as the docs sidebar does.
  let toggle: HTMLButtonElement;
  let expanded = $state(false);
</script>

<svelte:window
  onkeydown={(event) => {
    if (event.key === 'Escape' && expanded) {
      expanded = false;
      toggle.focus();
    }
  }}
/>

<div class="releases-layout">
  <aside>
    <button
      bind:this={toggle}
      aria-expanded={expanded}
      aria-controls="release-navigation"
      onclick={() => (expanded = !expanded)}
    >
      Releases <span aria-hidden="true">{expanded ? '−' : '+'}</span>
    </button>
    <div id="release-navigation" class:expanded>
      <NavGroups label="Releases" links={[{ href: '/releases', title: 'All releases' }]} {groups} />
    </div>
  </aside>
  <div class="content">{@render children()}</div>
</div>

<style>
  .releases-layout {
    max-width: 100rem;
    margin: 0 auto;
    padding: 2.5rem 1.5rem;
    display: grid;
    grid-template-columns: 14rem minmax(0, 1fr);
    gap: 2rem;
  }
  aside {
    position: sticky;
    top: calc(var(--site-header-height, 5rem) + 1rem);
    align-self: start;
  }
  .content {
    min-width: 0;
  }
  button {
    display: none;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    gap: 1rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.75rem;
    background: var(--panel);
    color: var(--ink);
    font: inherit;
    cursor: pointer;
  }
  @media (max-width: 60rem) {
    .releases-layout {
      grid-template-columns: minmax(0, 1fr);
      gap: 1rem;
      padding-top: 0.75rem;
    }
    aside {
      position: static;
    }
    button {
      display: flex;
    }
    #release-navigation {
      display: none;
      padding: 1rem;
      border: 1px solid var(--border);
    }
    #release-navigation.expanded {
      display: block;
    }
  }
  @media (max-width: 32rem) {
    .releases-layout {
      padding-inline: 1rem;
    }
  }
</style>
