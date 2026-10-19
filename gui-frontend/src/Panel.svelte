<!--
  A card (STYLE_GUIDE.md, "Cards" and "Controls"): the one grouped surface,
  with its title in a head ruled off from the body. `actions` sits at the
  head's far end. `onsubmit` makes the card the form its fields submit.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    title,
    actions,
    onsubmit,
    children,
  }: {
    title?: Snippet | string;
    actions?: Snippet;
    onsubmit?: (e: SubmitEvent) => void;
    children: Snippet;
  } = $props();

  // A titled card is a region named by its title, so it can be found by name.
  const headingId = $props.id();
  const named = $derived(title ? headingId : undefined);
</script>

{#snippet body()}
  {#if title || actions}
    <div class="head">
      {#if title}
        <h2 id={headingId}>
          {#if typeof title === 'string'}{title}{:else}{@render title()}{/if}
        </h2>
      {/if}
      {@render actions?.()}
    </div>
  {/if}
  {@render children()}
{/snippet}

{#if onsubmit}
  <form class="panel" {onsubmit} aria-labelledby={named}>{@render body()}</form>
{:else}
  <section class="panel" aria-labelledby={named}>{@render body()}</section>
{/if}

<style>
  .panel {
    display: grid;
    gap: 0.9rem;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    padding: 1.2rem;
  }

  .head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.75rem;
    padding-bottom: 0.9rem;
    border-bottom: 1px solid var(--border);
  }

  h2 {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    margin: 0;
    font-family: var(--font-display);
    font-size: var(--text-section);
    font-weight: 600;
  }
</style>
