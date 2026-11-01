<!--
  What a schema's field means, as its schema says it: a paragraph for each line, its `backticked` names drawn
  as code.
-->
<script lang="ts">
  import Ticked from './Ticked.svelte';

  let { text }: { text: string } = $props();

  let paragraphs = $derived(text.split(/\n+/).filter((p) => p.trim()));
</script>

{#each paragraphs as p, i (i)}<p><Ticked text={p} /></p>{/each}

<style>
  p {
    margin: 0;
  }

  p + p {
    margin-top: 0.35rem;
  }
</style>
