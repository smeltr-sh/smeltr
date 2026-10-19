<!--
  Opening a branch for review: the title and description it will carry,
  written from what changed and the author's to edit, beside what the change
  touches and how its tests do. Nothing is created until Open for review.

  The branch bar unfolds into this, so it renders on the bar's tint.
-->
<script lang="ts">
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import Locked from './Locked.svelte';
  import {
    SAVED_EVENT,
    branchDiff,
    configAt,
    listUsers,
    openChange,
    runTests,
    type BranchDto,
  } from './api';
  import { tick } from 'svelte';
  import { deployBlocker } from './access';
  import { changeText, type ScopeChange } from './changeText';
  import { scopeOf } from './objectChanges';
  import { session } from './session';
  import { count } from './time';

  let {
    entry,
    onclose,
    onopened,
  }: {
    /** The branch being opened, as the branch list answered it. */
    entry: BranchDto;
    onclose: () => void;
    onopened: () => void;
  } = $props();

  let title = $state('');
  let description = $state('');
  let drafting = $state(true);
  let busy = $state(false);
  let error = $state('');

  let files = $state<{ count: number; added: number; removed: number } | null>(null);
  /** `errored` could not run, so it is neither a pass nor a failure. */
  let tests = $state<{ passed: number; failed: number; errored: number; total: number } | null>(
    null,
  );
  let testsError = $state('');
  /** Who reviews: everybody enabled who may approve this change, as the leader judges it
   * (`deployBlocker`), but the author. Null when the roster cannot be read, as it cannot by somebody
   * who is not an administrator. */
  let reviewers = $state<{ id: string; name: string }[] | null>(null);

  /** Every scope this branch touched, against `main`. A change that spans
   * two groups is two comparisons, and the text says so. */
  async function scopeChanges(): Promise<ScopeChange[]> {
    const scopes = [...new Set(entry.touches.map(scopeOf))].sort();
    const pairs = await Promise.all(
      scopes.map(async (scope) => {
        const [before, after] = await Promise.all([
          configAt('main', scope),
          configAt(entry.name, scope),
        ]);
        return { scope, before: before.config, after: after.config };
      }),
    );
    return pairs.filter((pair): pair is ScopeChange => !!pair.before && !!pair.after);
  }

  async function draft() {
    try {
      const text = changeText(await scopeChanges());
      title = text.subject;
      description = text.body;
    } catch (e) {
      error = unreported(e);
    } finally {
      drafting = false;
    }
    // The title was disabled while drafting, so autofocus could not take.
    await tick();
    titleInput?.focus();
  }

  /** Which summary is newest, so a slower earlier one cannot land over it. */
  let summaries = 0;

  async function summarize() {
    const mine = ++summaries;
    tests = null;
    testsError = '';
    const [diff, run] = await Promise.allSettled([
      branchDiff(entry.name),
      runTests({ touched: true }, entry.name),
    ]);
    if (mine !== summaries) return;
    if (diff.status === 'fulfilled' && diff.value.ok) {
      const f = diff.value.files;
      files = {
        count: f.length,
        added: f.reduce((n, x) => n + x.added, 0),
        removed: f.reduce((n, x) => n + x.removed, 0),
      };
    }
    if (run.status === 'fulfilled' && run.value.ok) {
      const ran = run.value.results;
      tests = {
        passed: ran.filter((t) => t.verdict?.passed).length,
        failed: ran.filter((t) => t.verdict && !t.verdict.passed).length,
        errored: ran.filter((t) => !t.verdict).length,
        total: ran.length,
      };
    } else {
      testsError = 'tests could not be run';
    }
  }

  async function loadReviewers() {
    try {
      reviewers = (await listUsers())
        .filter(
          (u) =>
            u.enabled && deployBlocker(entry.touches, u.access) === '' && u.id !== session().userId,
        )
        .map((u) => ({ id: u.id, name: u.displayName }));
    } catch {
      reviewers = null;
    }
  }

  $effect(() => {
    void draft();
    void summarize();
    void loadReviewers();
  });

  // A save from the page below moves the branch: count again, but keep what the author wrote.
  $effect(() => {
    const again = () => void summarize();
    addEventListener(SAVED_EVENT, again);
    return () => removeEventListener(SAVED_EVENT, again);
  });

  let ready = $derived(!drafting && !busy && title.trim() !== '');

  async function submit(e: SubmitEvent) {
    e.preventDefault();
    if (!ready) return;
    busy = true;
    error = '';
    // Read now: `onopened` closes the form, and with it goes `entry`.
    const name = entry.name;
    try {
      const res = await openChange(name, title.trim(), description.trim());
      if (res.ok) {
        location.hash = `#/changes/review/${encodeURIComponent(name)}`;
        onopened();
      } else {
        error = res.errors.map((x) => x.message).join('; ');
      }
    } catch (x) {
      error = unreported(x);
    } finally {
      busy = false;
    }
  }

  let form: HTMLFormElement;
  let titleInput: HTMLInputElement | undefined;

  /** Escape leaves the form, but only from inside it: another dialog's Escape is that dialog's. */
  function onkeydown(e: KeyboardEvent) {
    if (e.key !== 'Escape') return;
    // While the open is on its way the form stays, and so does the page behind
    // it, wherever a disabled button left focus.
    if (busy) return e.preventDefault();
    if (!form.contains(document.activeElement)) return;
    e.preventDefault();
    onclose();
  }
</script>

<svelte:window {onkeydown} />

<form class="open-form" bind:this={form} onsubmit={submit}>
  <div class="left">
    <label class="field">
      <span>Title</span>
      <!-- svelte-ignore a11y_autofocus -->
      <input
        id="open-change-title"
        bind:this={titleInput}
        bind:value={title}
        disabled={drafting}
        autofocus
      />
    </label>
    <div class="field">
      <span>Reviewers</span>
      <div class="reviewers">
        {#if reviewers === null}
          <span class="dim">every administrator and deployer</span>
        {:else if reviewers.length === 0}
          <span class="dim">no other administrator or deployer</span>
        {:else}
          {#each reviewers as r (r.id)}<span class="reviewer">{r.name}</span>{/each}
        {/if}
      </div>
    </div>
    <div class="facts">
      {#if tests}
        {#if tests.total === 0}
          <span class="dim">no tests reach this change</span>
        {:else if tests.passed === tests.total}
          <span class="pass">&#10003; {tests.passed} of {count(tests.total, 'test')} pass</span>
        {:else}
          {#if tests.failed}
            <span class="fail">{tests.failed} of {count(tests.total, 'test')} fail</span>
          {/if}
          {#if tests.errored}
            <span class="dim">{count(tests.errored, 'test')} could not run</span>
          {/if}
        {/if}
      {:else if testsError}
        <span class="dim">{testsError}</span>
      {:else}
        <span class="dim">running tests&hellip;</span>
      {/if}
      {#if files}
        <span class="dim files">
          {count(files.count, 'file')} &middot;
          <span class="added">+{files.added}</span>
          <span class="removed">&minus;{files.removed}</span>
        </span>
      {/if}
    </div>
  </div>
  <label class="field">
    <span>Description</span>
    <textarea id="open-change-description" bind:value={description} disabled={drafting} rows="6"
    ></textarea>
  </label>
  <div class="actions">
    <InlineError message={error} inline />
    <button type="button" onclick={onclose} disabled={busy}>Cancel</button>
    <Locked refusedInDemo>
      <button type="submit" class="primary" disabled={!ready}>
        {busy ? 'Opening…' : 'Open for review'}
      </button>
    </Locked>
  </div>
</form>

<style>
  .open-form {
    flex: 1 1 auto;
    min-width: 0;
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
    gap: 1rem 1.25rem;
    padding-block: 0.6rem 1rem;
  }

  .left {
    display: grid;
    gap: 0.75rem;
    align-content: start;
  }

  .field {
    display: grid;
    gap: 0.3rem;
  }

  .field > span {
    font-size: var(--text-label);
    font-weight: 600;
  }

  .reviewers {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    font-size: var(--text-note);
  }

  .reviewer {
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0.1rem 0.6rem;
    background: var(--field);
  }

  textarea {
    resize: vertical;
    min-height: 8rem;
    font-family: var(--font-body);
  }

  .facts {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem 1rem;
    font-size: var(--text-note);
  }

  .files {
    margin-left: auto;
  }

  .dim {
    color: var(--dim);
  }

  .pass,
  .added {
    color: var(--added);
  }

  .fail {
    color: var(--error);
  }

  .removed {
    color: var(--removed);
  }

  .actions {
    grid-column: 1 / -1;
    display: flex;
    justify-content: flex-end;
    align-items: center;
    gap: 0.5rem;
  }

  .actions :global(.error) {
    margin-right: auto;
    font-size: var(--text-note);
  }

  @media (max-width: 720px) {
    .open-form {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
