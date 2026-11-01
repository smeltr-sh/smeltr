<!--
  A sentence the leader tells, its `backticked` names drawn as code.
-->
<script lang="ts">
  let { text }: { text: string } = $props();

  // Split on backticks, every odd part is a name.
  let parts = $derived(text.split('`'));
</script>

{#each parts as part, i (i)}{#if i % 2}<code>{part}</code>{:else}{part}{/if}{/each}

<style>
  code {
    font-family: var(--mono);
    font-size: 0.92em;
  }
</style>
