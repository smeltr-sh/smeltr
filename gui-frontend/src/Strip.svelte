<!--
  A full-bleed strip under the nav, for what holds on every page of the shell: the branch you are on, or that this
  is the demo. Each of its rows is a `.strip-row`, which keeps the page measure.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let { children }: { children: Snippet } = $props();
</script>

<div class="strip">{@render children()}</div>

<style>
  .strip {
    background: color-mix(in srgb, var(--accent) 10%, var(--bg));
    border-bottom: 1px solid var(--border);
  }

  .strip > :global(.strip-row) {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.8rem;
    row-gap: 0.3rem;
    max-width: var(--measure, var(--width-page));
    margin-inline: auto;
    padding: 0.4rem 1.5rem;
    font-size: var(--text-note);
  }
</style>
