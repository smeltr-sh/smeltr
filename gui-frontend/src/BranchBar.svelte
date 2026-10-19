<!--
  The strip under the nav while you work on a branch: which branch, that the
  fleet does not run it, and what is waiting on it. The shell draws it on
  every branch but `main`, so no page can be mistaken for what is deployed.
  Open for review unfolds it into the form that opens the change.
-->
<script lang="ts">
  import ReviewBar from './ReviewBar.svelte';
  import Strip from './Strip.svelte';
  import OpenChangeForm from './OpenChangeForm.svelte';
  import type { BranchDto } from './api';

  let { branch }: { branch: string } = $props();

  /** The branch being opened for review, while its form is showing. */
  let opening = $state<BranchDto | null>(null);
</script>

<Strip>
  <div class="strip-row">
    <span class="name">{branch}</span>
    <span class="state">Not deployed</span>
    <div class="review">
      <ReviewBar {branch} opening={opening !== null} onopen={(entry) => (opening = entry)} />
    </div>
  </div>
  {#if opening}
    <div class="strip-row">
      <OpenChangeForm
        entry={opening}
        onclose={() => (opening = null)}
        onopened={() => (opening = null)}
      />
    </div>
  {/if}
</Strip>

<style>
  .name {
    font-family: var(--mono);
    color: var(--ink);
    font-weight: 600;
  }

  .state {
    color: var(--accent);
    font-weight: 600;
  }

  .review {
    display: flex;
    align-items: center;
    gap: 0.8rem;
    margin-left: auto;
  }
</style>
