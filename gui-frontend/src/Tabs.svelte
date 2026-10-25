<!--
  A row of tabs that switch what a page or menu shows, held in the page rather than the hash
  (STYLE_GUIDE.md, "Secondary tabs", draws them). `SecondaryTabs` is the one whose tabs are links.
-->
<script lang="ts" generics="T extends string">
  import type { Snippet } from 'svelte';

  let {
    tabs,
    selected,
    onselect,
    ariaLabel,
    label,
    after,
  }: {
    tabs: readonly T[];
    selected: T;
    onselect: (tab: T) => void;
    ariaLabel: string;
    /** A tab's face; the tab's own name without one. */
    label?: Snippet<[T]>;
    /** What sits at the row's end, after the tabs. */
    after?: Snippet;
  } = $props();
</script>

<div class="tabs" role="tablist" aria-label={ariaLabel}>
  {#each tabs as t (t)}
    <button type="button" role="tab" aria-selected={t === selected} onclick={() => onselect(t)}
      >{#if label}{@render label(t)}{:else}{t}{/if}</button
    >
  {/each}
  {@render after?.()}
</div>

<style>
  .tabs {
    display: flex;
    flex-wrap: wrap;
    align-items: end;
    gap: 1.2rem;
    border-bottom: 1px solid var(--border);
  }

  .tabs > button[role='tab'] {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    background: none;
    padding: 0.35rem 0;
    border-width: 0 0 3px;
    border-radius: 0;
    border-color: transparent;
    color: var(--dim);
    font-size: var(--text-body);
  }

  .tabs > button[aria-selected='true'] {
    color: var(--ink);
    font-weight: 600;
    border-color: var(--accent);
  }
</style>
