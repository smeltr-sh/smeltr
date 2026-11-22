<!--
  Where one test stands after its run, in a word colored for it: Failing, Could not run, Passing, or Running… before
  its run comes back.
-->
<script lang="ts">
  import type { Standing } from './objectChanges';

  let { standing, error }: { standing: Standing | undefined; error?: string } = $props();
</script>

{#if standing === 'failing'}
  <span class="failing">Failing</span>
{:else if standing === 'error'}
  <span class="warn" title={error}>Could not run</span>
{:else if standing === 'passing'}
  <span class="passing">Passing</span>
{:else}
  <span class="dim">Running…</span>
{/if}

<style>
  span {
    white-space: nowrap;
  }

  .failing {
    color: var(--error);
    font-weight: 600;
  }

  .warn {
    color: var(--warn);
  }

  .passing {
    color: var(--added);
    font-weight: 600;
  }
</style>
