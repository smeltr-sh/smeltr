<!--
  Secondary tabs (STYLE_GUIDE.md, "Secondary tabs"): a page's own sections, distinct from the section rail -- the
  rail says which page a tab's whole sub-tree belongs to, this says which one you are on. Given `onselect`, a tab is a
  button that switches what the page or menu around it shows, held there rather than in the hash; without it, each
  is a bookmarkable link. Nothing shown for a single tab: one choice is not a choice.
-->
<script lang="ts" generics="T extends string">
  import type { Snippet } from 'svelte';
  import type { SectionItem } from './nav';

  type Links = { tabs: SectionItem[]; onselect?: undefined };
  type Buttons = {
    tabs: readonly T[];
    onselect: (tab: T) => void;
    /** A tab's face; the tab's own name without one. */
    label?: Snippet<[T]>;
    /** What sits at the row's end, after the tabs. */
    after?: Snippet;
  };

  let props: {
    /** The open tab: a link's hash, or a button's name. */
    active: string;
    ariaLabel: string;
  } & (Links | Buttons) = $props();
</script>

{#if props.tabs.length > 1}
  {#if props.onselect}
    {@const { tabs, onselect, label, after } = props as Buttons}
    <div class="tabs buttons" role="tablist" aria-label={props.ariaLabel}>
      {#each tabs as t (t)}
        <button
          type="button"
          role="tab"
          aria-selected={t === props.active}
          onclick={() => onselect(t)}
          >{#if label}{@render label(t)}{:else}{t}{/if}</button
        >
      {/each}
      {@render after?.()}
    </div>
  {:else}
    <nav class="tabs" aria-label={props.ariaLabel}>
      {#each props.tabs as SectionItem[] as tab (tab.hash)}
        <a href={tab.hash} aria-current={tab.hash === props.active ? 'page' : undefined}>
          {tab.label}
        </a>
      {/each}
    </nav>
  {/if}
{/if}

<style>
  .tabs {
    display: flex;
    flex-wrap: wrap;
    align-items: end;
    gap: 1.4rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1.2rem;
  }

  /* Buttons sit inside what they switch, which sets its own spacing. */
  .tabs.buttons {
    margin-bottom: 0;
  }

  .tabs a,
  .tabs > button[role='tab'] {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    color: var(--dim);
    text-decoration: none;
    padding: 0 0 0.6rem;
    border: 0 solid transparent;
    border-bottom-width: 3px;
    border-radius: 0;
    background: none;
    font-size: var(--text-body);
  }

  .tabs a[aria-current='page'],
  .tabs > button[aria-selected='true'] {
    color: var(--ink);
    font-weight: 600;
    border-color: var(--accent);
  }
</style>
