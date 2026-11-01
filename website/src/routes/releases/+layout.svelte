<script lang="ts">
  import type { Snippet } from 'svelte';
  import NavGroups from '#lib/components/NavGroups.svelte';
  import { railGroups } from '#lib/releases.ts';

  let { children }: { children: Snippet } = $props();
  const groups = railGroups();
</script>

<div class="releases-layout">
  <aside>
    <NavGroups
      label="Releases"
      id="release-navigation"
      links={[{ href: '/releases', title: 'All releases' }]}
      {groups}
    />
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
  @media (max-width: 60rem) {
    .releases-layout {
      grid-template-columns: minmax(0, 1fr);
      gap: 1rem;
      padding-top: 0.75rem;
    }
    aside {
      position: static;
    }
  }
  @media (max-width: 32rem) {
    .releases-layout {
      padding-inline: 1rem;
    }
  }
</style>
