<!--
  One button that folds every item of a list or unfolds them all. It folds when `allOpen`, so it unfolds a
  list where any item is folded. As `icon`, it is the chevron its items fold by, at the head of their column.
-->
<script lang="ts">
  import Button from './Button.svelte';
  import ChevronIcon from './ChevronIcon.svelte';
  let {
    allOpen,
    what,
    disabled = '',
    icon = false,
    onclick,
  }: {
    allOpen: boolean;
    /** What it folds, for a screen reader, where the page does not already say it. */
    what?: string;
    /** Why it cannot fold or unfold now, or `''` when it can. */
    disabled?: string;
    icon?: boolean;
    onclick: () => void;
  } = $props();

  const text = $derived(allOpen ? 'Fold all' : 'Unfold all');
</script>

{#if icon}
  <Button
    type="button"
    class="icon fold-all"
    {disabled}
    {onclick}
    title={text}
    aria-label={what ? `${text} ${what}` : text}><ChevronIcon open={allOpen} /></Button
  >
{:else}
  <!-- The spoken name starts with the shown text, so voice control finds the button by what it says. -->
  <Button type="button" {disabled} {onclick} aria-label={what ? `${text} ${what}` : undefined}
    >{text}</Button
  >
{/if}

<style>
  /* A Button's own element, which this component's scoped styles do not reach. */
  :global(button.fold-all) {
    font-size: var(--text-micro);
  }
</style>
