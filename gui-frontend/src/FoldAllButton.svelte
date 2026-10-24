<!--
  One button that folds every item of a list or unfolds them all. It folds when `allOpen`, so it unfolds a
  list where any item is folded.
-->
<script lang="ts">
  let {
    allOpen,
    what,
    disabled = false,
    onclick,
  }: {
    allOpen: boolean;
    /** What it folds, for a screen reader, where the page does not already say it. */
    what?: string;
    disabled?: boolean;
    onclick: () => void;
  } = $props();

  const text = $derived(allOpen ? 'Fold all' : 'Unfold all');
</script>

<!-- The spoken name starts with the shown text, so voice control finds the button by what it says. -->
<button type="button" {disabled} {onclick} aria-label={what ? `${text} ${what}` : undefined}
  >{text}</button
>
