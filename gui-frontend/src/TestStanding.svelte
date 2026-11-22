<!--
  Where one test stands after its run, in a word colored for it: Failing, Could not run, Passing, or Running… before
  its run comes back. Given the `error` it could not run with, a `?` beside it says what that was.
-->
<script lang="ts">
  import Help from './Help.svelte';
  import type { Standing } from './objectChanges';

  let { standing, error }: { standing: Standing | undefined; error?: string } = $props();
</script>

{#if standing === 'failing'}
  <span class="test-failing">Failing</span>
{:else if standing === 'error'}
  <span class="test-error"
    >Could not run{#if error}<Help label="could not run">{error}</Help>{/if}</span
  >
{:else if standing === 'passing'}
  <span class="test-passing">Passing</span>
{:else}
  <span class="dim">Running…</span>
{/if}

<style>
  span {
    white-space: nowrap;
  }
</style>
