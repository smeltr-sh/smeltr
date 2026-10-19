<!--
  Secondary tabs (STYLE_GUIDE.md, "Secondary tabs"): a page's own sections,
  each a bookmarkable link, distinct from the section rail -- the rail says
  which page a tab's whole sub-tree belongs to, this says which one you are
  on. Nothing shown for a single tab: one choice is not a choice.
-->
<script lang="ts">
  import type { SectionItem } from './nav';

  let { tabs, active, ariaLabel }: { tabs: SectionItem[]; active: string; ariaLabel: string } =
    $props();
</script>

{#if tabs.length > 1}
  <nav class="tabs" aria-label={ariaLabel}>
    {#each tabs as tab (tab.hash)}
      <a href={tab.hash} aria-current={tab.hash === active ? 'page' : undefined}>
        {tab.label}
      </a>
    {/each}
  </nav>
{/if}

<style>
  .tabs {
    display: flex;
    flex-wrap: wrap;
    gap: 1.4rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1.2rem;
  }

  .tabs a {
    color: var(--dim);
    text-decoration: none;
    padding-bottom: 0.6rem;
    border-bottom: 3px solid transparent;
    font-size: var(--text-body);
  }

  .tabs a[aria-current='page'] {
    color: var(--ink);
    font-weight: 600;
    border-color: var(--accent);
  }
</style>
