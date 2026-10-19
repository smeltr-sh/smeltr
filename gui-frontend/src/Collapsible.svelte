<!--
  A disclosure block: an always-visible header and a body shown on demand.

  - Open state is the caller's, bindable, and keyed by the item's identity
    where the list reorders, so an open panel never slides onto another
    item.
  - `summary` renders inside the toggle button, so it holds text and inert
    markup only. Controls go in `actions`, outside the toggle.
  - Inside a `ValidationItem`, a save error on a field of that item opens it.
  - `row` is one row of a list that holds many (`ObjectList`): no card of its own, a hairline between
    rows. A `fixed` row has nothing to open and keeps the caret's room, so its cells line up.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import { validationContext } from './validation.svelte';

  // No fallback on `open`: Svelte refuses to bind `undefined` to a prop
  // with one, and callers bind `expanded[item.id]` against an empty
  // record. Undefined reads as closed; `aria-expanded` coerces so the
  // attribute is never dropped.
  let {
    open = $bindable(),
    variant = 'card',
    fixed = false,
    summary,
    actions,
    children,
  }: {
    open?: boolean | undefined;
    variant?: 'card' | 'row';
    fixed?: boolean;
    summary: Snippet;
    actions?: Snippet;
    children?: Snippet;
  } = $props();

  const context = validationContext();
  $effect(() =>
    context?.validation.opener(context.id(), () => {
      open = true;
    }),
  );
</script>

<div class="collapsible {variant}" class:open={open && !fixed}>
  <div class="head">
    {#if fixed}
      <div class="toggle fixed">
        <span class="caret" aria-hidden="true"></span>
        <span class="summary">{@render summary()}</span>
      </div>
    {:else}
      <button
        type="button"
        class="toggle"
        aria-expanded={open ?? false}
        onclick={() => (open = !open)}
      >
        <span class="caret" class:open aria-hidden="true">&#9656;</span>
        <span class="summary">{@render summary()}</span>
      </button>
    {/if}
    {#if actions}
      <div class="actions">{@render actions()}</div>
    {/if}
  </div>
  {#if open && !fixed && children}
    <div class="body">{@render children()}</div>
  {/if}
</div>

<style>
  .collapsible {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
    margin-bottom: 0.7rem;
  }

  .head {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0.7rem;
  }

  .collapsible.open .head {
    border-bottom: 1px solid var(--border);
  }

  /* The toggle fills the header so the whole row is the click target. */
  .toggle {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex: 1;
    min-width: 0;
    background: none;
    border: none;
    padding: 0;
    margin: 0;
    font: inherit;
    color: inherit;
    text-align: left;
    cursor: pointer;
  }

  .toggle.fixed {
    cursor: default;
  }

  .toggle:hover .summary {
    color: var(--ink);
  }

  .caret {
    flex: none;
    width: 0.75em;
    font-size: 0.75em;
    color: var(--dim);
    transition: transform 0.12s ease;
  }

  .caret.open {
    transform: rotate(90deg);
  }

  .summary {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .actions {
    flex: none;
    display: flex;
    gap: 0.35rem;
  }

  /* The body stacks whatever it is given -- fields, tables, groups -- so
     the separation between them belongs here rather than to each caller. */
  .body {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
    padding: 0.7rem;
  }

  .collapsible.row {
    border: none;
    border-radius: 0;
    margin: 0;
  }

  .collapsible.row .head {
    padding: 0.4rem 0.7rem;
  }

  .collapsible.row .toggle:not(.fixed):hover {
    color: var(--ink);
  }

  .collapsible.row.open .head {
    background: var(--panel);
  }

  /* A child of the row's own head: a fold nested in the row's body is a card, and stretching its summary would lift
     a smaller label above the text beside it. */
  .collapsible.row > .head .summary {
    display: flex;
    flex: 1;
  }

  .collapsible.row .body {
    padding: 0.8rem 1rem 1rem 1.85rem;
  }
</style>
