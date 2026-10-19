<!--
  Above a list narrowed to the objects an editor's Connections linked to (`?only=`): how many of
  the list it shows, and the way back to all of it. Nothing where the list is not narrowed.
-->
<script lang="ts">
  import type { Narrowing } from './narrowing.svelte';

  /** `ids` is every object the list holds, shown or not. */
  let { narrowing, ids }: { narrowing: Narrowing; ids: string[] } = $props();
</script>

{#if narrowing.ids}
  <p class="only">
    Showing {ids.filter((id) => narrowing.keeps(id)).length} of {ids.length}
    <span class="dim">·</span> <a class="link" href={narrowing.all}>Show all</a>
  </p>
{/if}

<style>
  .only {
    margin: 0 0 0.75rem;
    color: var(--dim);
  }

  .link {
    color: var(--ink);
    text-decoration: underline;
    text-underline-offset: 2px;
  }
</style>
