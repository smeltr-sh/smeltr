<!--
  Where a change stands before it deploys, held at the bottom of the viewport
  so it stays in view while the conversation and the diff scroll: what its
  reviewers said, how its tests came out, what it holds that will not deploy,
  whether it is current with main, and the button that deploys it.
-->
<script lang="ts">
  import StateMark from './StateMark.svelte';
  import Locked from './Locked.svelte';
  import type { Reviews, TestsTally } from './review';
  import { count } from './time';

  let {
    reviews,
    tests,
    testsLoading,
    behind,
    leftOut = 0,
    bringingCurrent,
    bringCurrentDisabled,
    onbringcurrent,
    approvable,
    deploying,
    deployDisabledReason,
    ondeploy,
    held = false,
    height = $bindable(0),
  }: {
    reviews: Reviews;
    tests: TestsTally;
    testsLoading: boolean;
    /** Commits `main` has that the branch does not. */
    behind: number;
    /** Samples no test names, which the deploy leaves out. */
    leftOut?: number;
    bringingCurrent: boolean;
    bringCurrentDisabled: boolean;
    onbringcurrent: () => void;
    /** Whether Approve & deploy is offered at all. */
    approvable: boolean;
    deploying: boolean;
    /** Why Approve & deploy is disabled, or '' when it is not. */
    deployDisabledReason: string;
    ondeploy: () => void;
    /** A rollback holds the fleet, so approving merges without deploying. */
    held?: boolean;
    /** The dock's own height, so the page can leave that much room below its content. */
    height?: number;
  } = $props();
</script>

<Locked refusedInDemo>
  <div class="dock" bind:clientHeight={height}>
    <div class="dock-row">
      <span class="item">
        {#if reviews.state === 'changes-requested'}
          <span class="mark req" aria-hidden="true">!</span>
          <span>{reviews.names.join(', ')} requested changes</span>
        {:else if reviews.state === 'approved'}
          <StateMark state="ok" size={17} />
          <span>Approved by {reviews.names.join(', ')}</span>
        {:else}
          <span class="mark" aria-hidden="true"></span>
          <span class="dim">No reviews yet</span>
        {/if}
      </span>

      <span class="item">
        {#if testsLoading}
          <span class="mark" aria-hidden="true"></span>
          <span class="dim">Running tests&hellip;</span>
        {:else if tests.total === 0}
          <span class="mark" aria-hidden="true"></span>
          <span class="dim">No tests</span>
        {:else if tests.failed > 0}
          <span class="mark req" aria-hidden="true">!</span>
          <span>{tests.failed} of {count(tests.total, 'test')} fail</span>
        {:else if tests.couldNotRun > 0}
          <span class="mark" aria-hidden="true"></span>
          <span>{tests.couldNotRun} of {count(tests.total, 'test')} could not run</span>
        {:else}
          <StateMark state="ok" size={17} />
          <span>{tests.passed} of {count(tests.total, 'test')}</span>
        {/if}
      </span>

      {#if leftOut > 0}
        <span class="item">
          <!-- Nothing to fix: what stays on the branch is said, not warned about. -->
          <span class="mark" aria-hidden="true"></span>
          <span>{count(leftOut, 'sample')} left out</span>
        </span>
      {/if}

      <span class="item">
        {#if behind > 0}
          <span class="mark req" aria-hidden="true">!</span>
          <span>Behind <span class="mono">main</span> by {count(behind, 'commit')}</span>
          <button
            type="button"
            class="compact"
            disabled={bringCurrentDisabled}
            onclick={onbringcurrent}
          >
            {bringingCurrent ? 'Bringing current…' : 'Bring current'}
          </button>
        {:else}
          <StateMark state="ok" size={17} />
          <span>Current with <span class="mono">main</span></span>
        {/if}
      </span>

      {#if approvable}
        <span class="act">
          {#if deployDisabledReason}<span class="blocked">{deployDisabledReason}</span>{/if}
          <button
            type="button"
            class="primary"
            disabled={deploying || deployDisabledReason !== ''}
            title={deployDisabledReason || undefined}
            onclick={ondeploy}
          >
            {deploying
              ? held
                ? 'Approving…'
                : 'Deploying…'
              : held
                ? 'Approve'
                : 'Approve & deploy'}
          </button>
        </span>
      {/if}
    </div>
  </div>
</Locked>

<style>
  /* The page's bottom bar, drawn as `SaveBar` draws its own (STYLE_GUIDE.md, "Bottom save bar"). */
  .dock {
    position: fixed;
    left: 0;
    right: 0;
    /* Above the agent panel while it is open. */
    bottom: var(--agent-panel, 0);
    z-index: 15;
    background: var(--panel);
    border-top: 1px solid var(--border);
  }

  .dock-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem 1.2rem;
    max-width: var(--measure, var(--width-page));
    margin-inline: auto;
    padding: 0.6rem 1.5rem;
    font-size: var(--text-label);
  }

  .item {
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
  }

  .mark {
    flex-shrink: 0;
    width: 1.1rem;
    height: 1.1rem;
    border-radius: 50%;
    display: inline-grid;
    place-items: center;
    box-sizing: border-box;
    border: 1px solid var(--border);
    font-size: var(--text-micro);
    font-weight: 700;
    color: var(--on-accent);
  }
  .mark.req {
    border-color: var(--accent);
    background: var(--accent);
  }

  .act {
    margin-left: auto;
    display: inline-flex;
    align-items: center;
    gap: 0.75rem;
  }

  /* What holds the deploy back: a condition that blocks the task (STYLE_GUIDE.md, "Critical"). */
  .blocked {
    color: var(--accent);
    font-weight: 600;
    font-size: var(--text-note);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
