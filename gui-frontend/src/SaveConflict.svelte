<!--
  A save the leader refused because the branch moved (W9). Both versions are
  on screen: theirs is a commit and can be read here, yours is in this editor
  and nowhere else, which is why nothing on this panel reloads without being
  asked to.

  Two ways out, and no third. Reloading works from their commit and drops what
  is on screen. Taking it to a branch keeps both.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import { commitDiff, createBranch, setBranch, type SaveConflict } from './api';
  import { shortCommit } from './time';

  let {
    conflict,
    onDiscard,
    onRetry,
  }: {
    conflict: SaveConflict;
    /** Reload the page from the branch head, dropping the refused change. */
    onDiscard: () => void;
    /** Save the change again, now that the branch has been switched. True
     * when it landed. */
    onRetry: (branch: string) => Promise<boolean>;
  } = $props();

  let diff = $state('');
  let files = $state<string[]>([]);
  let diffFailed = $state(false);
  let naming = $state(false);
  let newName = $state('');
  let busy = $state(false);
  let forkError = $state('');

  async function readTheirs() {
    if (!conflict.now) return;
    try {
      const res = await commitDiff(conflict.base, conflict.now);
      if (!res.ok) {
        diffFailed = true;
        return;
      }
      diff = res.diff;
      files = res.files.map((f) => f.path);
    } catch (e) {
      diffFailed = true;
      reported(e);
    }
  }
  readTheirs();

  async function fork() {
    const name = newName.trim();
    if (!name || busy) return;
    busy = true;
    forkError = '';
    try {
      const started = await createBranch(name);
      // A refusal is the error bar's to say.
      if (!started.ok) return;
      // From here the editor is on the new branch, so the retry is a first
      // save onto it rather than another attempt at the one that moved.
      setBranch(name);
      if (await onRetry(name)) location.reload();
      else forkError = 'the change did not save onto the new branch';
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  function lineKind(line: string): string {
    if (line.startsWith('+++') || line.startsWith('---')) return 'file';
    if (line.startsWith('+')) return 'add';
    if (line.startsWith('-')) return 'remove';
    if (line.startsWith('@@') || line.startsWith('diff --git')) return 'file';
    return '';
  }
</script>

<section class="conflict">
  <header>
    <span class="sev">Moved</span>
    <div>
      <p class="lead">This branch moved while you were editing.</p>
      <p>
        Your copy is from <span class="mono">{shortCommit(conflict.base)}</span>; the branch is
        {#if conflict.now}
          now at <span class="mono">{shortCommit(conflict.now)}</span>.
        {:else}
          gone.
        {/if}
      </p>
      <p class="critical">Your edit was not saved and is only on this screen.</p>
    </div>
  </header>

  {#if conflict.now}
    <div class="theirs">
      <div class="theirs-head">
        <strong>What they committed</strong>
        <span class="dim">{files.join(' · ')}</span>
      </div>
      {#if diffFailed}
        <!-- The error bar says why. -->
      {:else if diff}
        <pre>{#each diff.split('\n') as line}<span class={lineKind(line)}
              >{line}
</span>{/each}</pre>
      {:else}
        <p class="dim">Reading it&hellip;</p>
      {/if}
    </div>
  {/if}

  <InlineError message={forkError} />

  {#if naming}
    <form class="actions" onsubmit={(e) => (e.preventDefault(), fork())}>
      <input placeholder="a name for the branch" bind:value={newName} disabled={busy} />
      <button class="primary" type="submit" disabled={busy || newName.trim() === ''}>
        Start it and save
      </button>
      <button type="button" onclick={() => (naming = false)} disabled={busy}> Cancel </button>
    </form>
  {:else}
    <div class="actions">
      <button onclick={onDiscard}>
        Discard mine, reload {conflict.now ? shortCommit(conflict.now) : 'the branch'}
      </button>
      <button class="primary" onclick={() => (naming = true)}> Put my edit on a new branch </button>
    </div>
  {/if}
</section>

<style>
  .conflict {
    border: 1px solid var(--warn);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 0.75rem;
    margin-bottom: 1rem;
  }

  header {
    display: flex;
    gap: 0.75rem;
    align-items: baseline;
  }

  .sev {
    flex-shrink: 0;
    width: 62px;
    color: var(--warn);
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .lead {
    margin: 0 0 0.25rem;
    font-weight: 600;
  }

  .dim {
    color: var(--dim);
    margin: 0;
    font-size: var(--text-note);
  }

  .mono {
    font-family: var(--mono);
  }

  .theirs {
    margin: 0.75rem 0 0 calc(62px + 0.75rem);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
    overflow: hidden;
  }

  .theirs-head {
    display: flex;
    flex-wrap: wrap;
    gap: 0.6rem;
    align-items: baseline;
    padding: 0.4rem 0.6rem;
    border-bottom: 1px solid var(--border);
    font-size: var(--text-label);
  }

  .theirs-head .dim {
    font-family: var(--mono);
    font-size: var(--text-micro);
  }

  pre {
    margin: 0;
    padding: 0.5rem 0.6rem;
    max-height: 18rem;
    overflow: auto;
    font-family: var(--mono);
    font-size: var(--text-note);
    line-height: 1.5;
  }

  pre .add {
    color: var(--added);
  }

  pre .remove {
    color: var(--removed);
  }

  pre .file {
    color: var(--dim);
  }

  .actions {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    margin: 0.75rem 0 0 calc(62px + 0.75rem);
  }

  .actions input {
    font-family: var(--mono);
    width: min(16rem, 100%);
  }

  /* Below this the sev-column indent leaves too little room for a commit
     hash and a button on the same line; both boxes go full width instead. */
  @media (max-width: 480px) {
    .theirs,
    .actions {
      margin-left: 0;
    }
  }

  header .critical {
    margin: 0;
  }
</style>
