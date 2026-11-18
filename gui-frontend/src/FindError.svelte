<!--
  Why the find box's expression was refused (`FindBox`'s `error`). Where the text was searched for instead, the
  leader's own words stand apart from the line that says so.
-->
<script lang="ts">
  import InlineError from './InlineError.svelte';
  import type { FindRefusal } from './FindBox.svelte';

  let { refusal }: { refusal: FindRefusal | null } = $props();
</script>

{#if refusal?.searched}
  <InlineError
    >Searched as text. Read as an expression, it is refused:<br /><code>{refusal.why}</code
    ></InlineError
  >
{:else if refusal}
  <InlineError message={refusal.why} />
{/if}
