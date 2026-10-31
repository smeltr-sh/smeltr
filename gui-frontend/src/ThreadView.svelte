<!--
  One comment thread where it sits: each comment, then a reply box and
  Resolve or Reopen. A resolved thread folds to one line that opens it.
-->
<script lang="ts">
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import Locked from './Locked.svelte';
  import { commentOnChange, errorText, resolveThread, type BranchMutationResult } from './api';
  import Avatar from './Avatar.svelte';
  import type { Thread } from './threads';
  import { ago } from './time';

  let {
    branch,
    thread,
    showRoot = true,
    folds = true,
    onchanged,
  }: {
    branch: string;
    thread: Thread;
    /** False where the comment that started it is already shown above. */
    showRoot?: boolean;
    /** False where the caller folds a resolved thread itself. */
    folds?: boolean;
    /** A reply, resolve or reopen landed: the timeline has it. */
    onchanged: () => Promise<void> | void;
  } = $props();

  let open = $state(false);
  let reply = $state('');
  let busy = $state(false);
  let actError = $state('');

  let folded = $derived(folds && thread.resolved && !open);
  let comments = $derived(showRoot ? [thread.root, ...thread.replies] : thread.replies);

  async function act(call: () => Promise<BranchMutationResult>) {
    busy = true;
    actError = '';
    try {
      const res = await call();
      if (res.ok) {
        reply = '';
        await onchanged();
      } else {
        actError = errorText(res.errors);
      }
    } catch (err) {
      actError = unreported(err);
    } finally {
      busy = false;
    }
  }

  function submit(e: SubmitEvent) {
    e.preventDefault();
    if (busy || !reply.trim()) return;
    void act(() => commentOnChange(branch, reply, { replyTo: thread.id }));
  }

  function settle() {
    void act(() => resolveThread(branch, thread.id, !thread.resolved));
  }
</script>

<div class="thread">
  {#if folds && thread.resolved}
    <button type="button" class="fold" aria-expanded={open} onclick={() => (open = !open)}>
      Resolved thread by {thread.settledBy?.author ?? thread.root.author}
    </button>
  {/if}
  {#if !folded}
    {#each comments as c (c.id ?? c.time)}
      <div class="c">
        <Avatar name={c.author} />
        <div class="c-main">
          <div class="c-head">
            <span class="who">{c.author}</span>
            <span class="dim">{ago(c.time)}</span>
            {#if c === thread.root && thread.outdated}<span class="outdated">outdated</span>{/if}
          </div>
          <div class="text">{c.text ?? ''}</div>
        </div>
      </div>
    {/each}
    <Locked refusedInDemo
      ><form class="reply" onsubmit={submit}>
        <input type="text" aria-label="Reply" placeholder="Reply" bind:value={reply} />
        {#if reply.trim()}
          <button type="submit" disabled={busy}>Reply</button>
        {/if}
        <button type="button" disabled={busy} onclick={settle}
          >{thread.resolved ? 'Reopen' : 'Resolve'}</button
        >
      </form></Locked
    >
    <InlineError message={actError} />
  {/if}
</div>

<style>
  .thread {
    display: grid;
    gap: 0.5rem;
    min-width: 0;
  }
  .fold {
    justify-self: start;
    border: 0;
    background: none;
    padding: 0;
    color: var(--dim);
    font-size: var(--text-note);
    cursor: pointer;
  }
  .fold:hover,
  .fold:focus-visible {
    color: var(--ink);
  }
  .c {
    display: flex;
    gap: 0.5rem;
    align-items: flex-start;
    min-width: 0;
  }
  .c-main {
    min-width: 0;
  }
  .c-head {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.4rem;
    font-size: var(--text-note);
  }
  .who {
    font-weight: 600;
    color: var(--ink);
    font-size: var(--text-label);
  }
  .outdated {
    font-size: var(--text-micro);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0 0.45rem;
    color: var(--dim);
  }
  /* What a person wrote keeps the lines they wrote it in. */
  .text {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    color: var(--ink);
    font-size: var(--text-body);
    line-height: 1.5;
  }
  .reply {
    display: flex;
    gap: 0.5rem;
  }
  .reply input {
    flex: 1 1 auto;
    min-width: 0;
  }
</style>
