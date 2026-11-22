<!--
  How many tests stand each way, each count in the color `TestStanding` gives its word. A count of none is left out.
-->
<script lang="ts">
  let {
    failing = 0,
    errored = 0,
    passing = 0,
  }: { failing?: number; errored?: number; passing?: number } = $props();
</script>

{#if failing > 0}<span class="test-failing">{failing} failing</span>{/if}
{#if errored > 0}<span class="test-error">{errored} could not run</span>{/if}
{#if passing > 0}<span class="test-passing">{passing} passing</span>{/if}
