<!--
  How many save errors a closed thing holds, in its header, so a reader knows to open it. Nothing when there are none.
-->
<script lang="ts">
  import { count } from './time';

  let { errors }: { errors: number } = $props();
</script>

{#if errors > 0}
  <span class="error-count">{count(errors, 'error')}</span>
{/if}

<style>
  .error-count {
    align-self: center;
    margin-left: 0.5rem;
    font-size: var(--text-micro);
    font-weight: 700;
    color: var(--error);
  }
</style>
