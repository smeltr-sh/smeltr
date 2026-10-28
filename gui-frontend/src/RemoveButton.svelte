<!--
  The control that takes an item out of the list it is in: `CrossIcon`, dim until hovered.
-->
<script lang="ts">
  import CrossIcon from './CrossIcon.svelte';

  let {
    label,
    onclick,
  }: {
    /** What it removes, for a screen reader and the hover title: `Remove row 3`. */
    label: string;
    onclick: (e: MouseEvent) => void;
  } = $props();
</script>

<button type="button" class="remove" aria-label={label} title={label} {onclick}
  ><CrossIcon /></button
>

<style>
  .remove {
    display: inline-grid;
    flex: none;
    place-items: center;
    padding: 0.2rem;
    border-color: transparent;
    background: none;
    color: var(--dim);
    font-size: var(--text-label);
    line-height: 1;
  }

  .remove:hover {
    color: var(--error);
  }
</style>
