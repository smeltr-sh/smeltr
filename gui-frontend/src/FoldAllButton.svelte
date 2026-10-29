<!--
  One button that folds every item of a list or unfolds them all. It folds when `allOpen`, so it unfolds a
  list where any item is folded. As `icon`, it is the chevron its items fold by, at the head of their column.
-->
<script lang="ts">
  import ChevronIcon from './ChevronIcon.svelte';
  let {
    allOpen,
    what,
    disabled = false,
    icon = false,
    onclick,
  }: {
    allOpen: boolean;
    /** What it folds, for a screen reader, where the page does not already say it. */
    what?: string;
    disabled?: boolean;
    icon?: boolean;
    onclick: () => void;
  } = $props();

  const text = $derived(allOpen ? 'Fold all' : 'Unfold all');
</script>

{#if icon}
  <button
    type="button"
    class="icon"
    {disabled}
    {onclick}
    title={text}
    aria-label={what ? `${text} ${what}` : text}><ChevronIcon open={allOpen} /></button
  >
{:else}
  <!-- The spoken name starts with the shown text, so voice control finds the button by what it says. -->
  <button type="button" {disabled} {onclick} aria-label={what ? `${text} ${what}` : undefined}
    >{text}</button
  >
{/if}

<style>
  .icon {
    font-size: var(--text-micro);
  }
</style>
