<!-- A line of a conversation that tells what happened, behind a dot in the avatars' column. -->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let { children }: { children: Snippet } = $props();
</script>

<div class="event">
  <span class="dot" aria-hidden="true"></span>
  <span>{@render children()}</span>
</div>

<style>
  .event {
    display: flex;
    gap: 0.6rem;
    align-items: baseline;
    padding-left: 0.55rem;
    font-size: var(--text-note);
    color: var(--dim);
    overflow-wrap: anywhere;
  }
  .dot {
    flex-shrink: 0;
    width: 0.55rem;
    height: 0.55rem;
    border-radius: 50%;
    background: var(--border);
  }
</style>
