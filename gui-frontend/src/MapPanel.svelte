<!--
  A panel docked beside the map: a top line with what kind of thing it shows, its group and its
  actions, ✕ last; then its title, which takes focus when the panel opens, so a keyboard or
  screen reader user lands on what the selection opened rather than back at the top of the map.
  Shared by an object's panel (`FocusView.svelte`) and a stack's (`StackView.svelte`), so the
  two read as one panel.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import CrossButton from './CrossButton.svelte';

  let {
    kind,
    group,
    title,
    mono = false,
    onclose,
    actions,
    children,
  }: {
    kind: string;
    group: string;
    title: string;
    /** Whether the title is a name, set in the code face. */
    mono?: boolean;
    onclose: () => void;
    /** Icons before ✕, each a `.icon`. */
    actions?: Snippet;
    children: Snippet;
  } = $props();

  const titleId = $props.id();
  let heading: HTMLHeadingElement | undefined = $state();
  $effect(() => {
    heading?.focus({ preventScroll: true });
  });
</script>

<aside class="focus" aria-labelledby={titleId}>
  <div class="head">
    <span class="eyebrow">{kind}</span>
    <span class="pill mono">{group}</span>
    <span class="actions">
      {@render actions?.()}
      <CrossButton label="Close" onclick={onclose} />
    </span>
  </div>
  <h2 id={titleId} class:mono tabindex="-1" bind:this={heading}>{title}</h2>
  {@render children()}
</aside>

<style>
  .focus {
    padding: 0.9rem 1rem 1rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
    color: var(--ink);
  }

  .head {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .eyebrow {
    color: var(--dim);
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .pill {
    padding: 0.05rem 0.4rem;
    border-radius: var(--radius);
    background: var(--panel);
    font-size: var(--text-micro);
  }

  .mono {
    font-family: var(--mono);
  }

  h2 {
    margin: 0.55rem 0 0.15rem;
    font-size: var(--text-section);
    font-weight: 600;
    overflow-wrap: anywhere;
  }

  .actions {
    display: flex;
    gap: 0.15rem;
    margin-left: auto;
  }

  .actions :global(.icon),
  .actions :global(.cross) {
    width: 24px;
    height: 24px;
    display: flex;
    align-items: center;
    justify-content: center;
    border: 0;
    border-radius: var(--radius);
    background: none;
    color: var(--dim);
    cursor: pointer;
  }

  .actions :global(.icon:hover),
  .actions :global(.cross:hover) {
    color: var(--ink);
    background: color-mix(in oklch, var(--ink) 12%, transparent);
  }
</style>
