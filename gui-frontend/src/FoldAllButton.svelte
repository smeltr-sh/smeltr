<!--
  One button that folds every item of a list or unfolds them all. It folds when `allOpen`, so it unfolds a
  list where any item is folded.
-->
<script lang="ts">
  let {
    allOpen,
    disabled = false,
    onclick,
  }: { allOpen: boolean; disabled?: boolean; onclick: () => void } = $props();
</script>

<button type="button" {disabled} {onclick}>{allOpen ? 'Fold all' : 'Unfold all'}</button>
