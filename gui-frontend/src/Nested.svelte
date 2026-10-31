<!--
  Fields that apply only under the choice above them: indented under it on a
  rule, so they read as that choice's own rather than as the next field of
  the form. Spaced as the form around it spaces its own fields.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    numbered = false,
    children,
  }: {
    /** Under a numbered row: starts past the row's number. */
    numbered?: boolean;
    children: Snippet;
  } = $props();
</script>

<div class="nested" class:numbered>{@render children()}</div>

<style>
  .nested {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: inherit;
    margin-left: 0.6rem;
    padding-left: 0.75rem;
    border-left: 2px solid var(--border);
  }

  .nested.numbered {
    margin-left: 1.7rem;
  }
</style>
