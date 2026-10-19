<!--
  One entry of a conversation: the avatar of who it is from beside a card, its head above its body. The body is
  `text`, kept in the lines it was written in, or what the caller renders.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import Avatar from './Avatar.svelte';

  let {
    author,
    head,
    text,
    children,
    accent = false,
  }: {
    /** Whose avatar the entry shows; none without it. */
    author?: string;
    head?: Snippet;
    text?: string;
    children?: Snippet;
    /** Draws the card in the accent colour, for an entry that holds something back. */
    accent?: boolean;
  } = $props();
</script>

<div class="entry">
  {#if author !== undefined}<Avatar name={author} />{/if}
  <div class="card" class:accent>
    {#if head}<div class="card-head">{@render head()}</div>{/if}
    {#if text}
      <div class="card-body text">{text}</div>
    {:else if children}
      <div class="card-body">{@render children()}</div>
    {/if}
  </div>
</div>

<style>
  .entry {
    display: flex;
    gap: 0.6rem;
    align-items: flex-start;
  }
  .card {
    flex: 1 1 auto;
    min-width: 0;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  .card-head {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.4rem;
    padding: 0.45rem 0.75rem;
    border-bottom: 1px solid var(--border);
    background: var(--bg);
    border-radius: var(--radius-card) var(--radius-card) 0 0;
    font-size: var(--text-note);
  }
  .card-head:last-child {
    border-bottom: 0;
    border-radius: var(--radius-card);
  }
  .card-body {
    padding: 0.7rem 0.75rem;
  }
  .text {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    color: var(--ink);
    font-size: var(--text-body);
    line-height: 1.5;
  }
  .card.accent {
    border-color: var(--accent);
  }
  .card.accent .card-head {
    background: color-mix(in srgb, var(--accent) 12%, var(--panel));
  }
</style>
