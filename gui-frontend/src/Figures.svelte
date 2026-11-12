<!--
  A row of `Figure`s, as many to a line as the width holds; the rest wrap to the next line.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let { children }: { children: Snippet } = $props();
</script>

<div class="figures">{@render children()}</div>

<style>
  .figures {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr));
    gap: 0.6rem;
  }
</style>
