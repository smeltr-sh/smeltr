<!--
  A change's conversation: what happened to it, oldest first, headed by the
  card that opened it, and a box to comment, review, close or reopen it.
  The page reads the timeline, which its dock reads too.

  The heading card shows the change's description as it stands now rather
  than as it was opened with: a rename rewrites the description, and the
  rename's own line in the timeline says that it happened.

  A thread shows in the place of the comment that started it, its replies
  and resolutions folded in. One on an object or a line opens with a quote
  of what it sits on; a resolved one folds to a line that opens it.
-->
<script lang="ts">
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import Locked from './Locked.svelte';
  import {
    closeChange,
    commentOnChange,
    errorText,
    openChange,
    reviewChange,
    type BranchMutationResult,
    type ChangeDto,
    type TimelineEvent,
  } from './api';
  import { conversation, eventVerb } from './conversation';
  import ConversationEntry from './ConversationEntry.svelte';
  import ConversationEvent from './ConversationEvent.svelte';
  import type { ObjectChange } from './objectChanges';
  import { canSubmit, type ComposerChoice } from './review';
  import ThreadView from './ThreadView.svelte';
  import { anchorLabel, displayList, threads, type Thread } from './threads';
  import { session } from './session';
  import { ago, shortCommit } from './time';

  let {
    branch,
    change,
    events,
    loading,
    objects,
    lines,
    onposted,
    onstatechanged,
  }: {
    branch: string;
    change: ChangeDto;
    /** The change's timeline, oldest first. */
    events: TimelineEvent[];
    loading: boolean;
    /** The objects the change touched, which name what a thread sits on. */
    objects: ObjectChange[];
    /** Each changed file's new-side lines the change's diff shows, by number. */
    lines: Map<string, Map<number, string>>;
    /** A comment, reply, review or resolution landed: the timeline has something new. */
    onposted: () => Promise<void> | void;
    /** The change was closed or reopened: everything on the page may have moved. */
    onstatechanged: () => Promise<void> | void;
  } = $props();

  let body = $state('');
  let choice = $state<ComposerChoice>('comment');
  let busy = $state(false);
  let postError = $state('');

  let shown = $derived(conversation(events));
  let items = $derived(displayList(shown.rest, threads(events)));
  /** The resolved threads opened from their folded line, by id. */
  let unfolded = $state<Record<string, boolean>>({});
  /** The opening as the timeline tells it, or as the change itself does while the timeline is unread. */
  let opener = $derived(shown.opened?.author ?? change.author);
  let openedAt = $derived(shown.opened?.time ?? change.opened);
  let closed = $derived(change.state === 'closed');

  /** Who is writing, when the session names someone. */
  const me = session().displayName || session().username || '';
  const myId = session().userId;

  /** The leader refuses a verdict on a closed change, and from the change's own author. */
  let reviewable = $derived(!closed && !(myId !== undefined && myId === change.authorId));
  let sending = $derived<ComposerChoice>(reviewable ? choice : 'comment');

  async function act(call: () => Promise<BranchMutationResult>, after: () => Promise<void> | void) {
    busy = true;
    postError = '';
    try {
      const res = await call();
      if (res.ok) {
        body = '';
        choice = 'comment';
        await after();
      } else {
        postError = errorText(res.errors);
      }
    } catch (err) {
      postError = unreported(err);
    } finally {
      busy = false;
    }
  }

  function submit(e: SubmitEvent) {
    e.preventDefault();
    if (busy || !canSubmit(sending, body)) return;
    const verdict = sending;
    void act(
      () =>
        verdict === 'comment' ? commentOnChange(branch, body) : reviewChange(branch, verdict, body),
      onposted,
    );
  }

  /** The text in the box is the reason. */
  function close() {
    void act(() => closeChange(branch, body), onstatechanged);
  }

  function reopen() {
    void act(() => openChange(branch, change.title, change.description), onstatechanged);
  }
</script>

{#snippet eventEntry(ev: TimelineEvent)}
  {#if ev.kind === 'comment'}
    <ConversationEntry author={ev.author} text={ev.text ?? ''}>
      {#snippet head()}
        <span class="who">{ev.author}</span>
        <span class="dim">&middot; {ago(ev.time)}</span>
      {/snippet}
    </ConversationEntry>
  {:else if ev.kind === 'approved' || ev.kind === 'changes-requested'}
    {@const requested = ev.kind === 'changes-requested'}
    <!-- A request for changes holds the deploy back, so it reads as critical; an approval does not. -->
    <ConversationEntry author={ev.author} text={ev.text} accent={requested}>
      {#snippet head()}
        <span class="who">{ev.author}</span>
        {#if requested}
          <span class="verdict requested-word">requested changes</span>
        {:else}
          <span class="verdict approved-word">approved</span>
        {/if}
        <span class="dim">&middot; {ago(ev.time)}</span>
      {/snippet}
    </ConversationEntry>
  {:else if ev.kind !== 'opened'}
    <ConversationEvent>
      <span class="who">{ev.author}</span>
      {eventVerb(ev.kind)}
      {#if ev.kind === 'renamed'}
        <span class="ink">{ev.title}</span>
      {:else if ev.kind === 'saved'}
        {#if ev.commit}<span class="mono">{shortCommit(ev.commit)}</span>{/if}
        <span class="ink">{ev.text ?? ''}</span>
      {/if}
      &middot; {ago(ev.time)}
      {#if ev.kind === 'closed' && ev.text}
        <span class="reason text">{ev.text}</span>
      {/if}
    </ConversationEvent>
  {/if}
{/snippet}

{#snippet threadCard(t: Thread)}
  {@const where = t.anchor ? anchorLabel(t.anchor, objects, lines) : null}
  {@const resolver = t.settledBy?.author ?? t.root.author}
  {#if t.resolved && !unfolded[t.id]}
    <ConversationEvent>
      <button
        type="button"
        class="fold"
        aria-expanded="false"
        onclick={() => (unfolded = { ...unfolded, [t.id]: true })}
        >Resolved thread by {resolver}</button
      >
      &middot; {ago(t.settledBy?.time ?? t.root.time)}
    </ConversationEvent>
  {:else if !where && t.replies.length === 0 && !t.resolved}
    {@render eventEntry(t.root)}
  {:else}
    <ConversationEntry author={t.root.author}>
      {#snippet head()}
        <span class="who">{t.root.author}</span>
        <span class="dim">&middot; {ago(t.root.time)}</span>
        {#if t.outdated}<span class="outdated">outdated</span>{/if}
        {#if t.resolved}
          <button
            type="button"
            class="fold end"
            aria-expanded="true"
            onclick={() => (unfolded = { ...unfolded, [t.id]: false })}
            >Resolved by {resolver}</button
          >
        {/if}
      {/snippet}
      <div class="thread-body">
        {#if where}
          <div class="quote">
            <div class="quote-head">
              <span class="mono quote-title">{where.title}</span>
              {#if where.detail}<span class="dim">{where.detail}</span>{/if}
            </div>
            <!-- The line under an outdated comment is not the one it was written on. -->
            {#if where.code !== undefined && !t.outdated}
              <div class="quote-code mono">{where.code}</div>
            {/if}
          </div>
        {/if}
        <div class="text">{t.root.text ?? ''}</div>
        <ThreadView {branch} thread={t} showRoot={false} folds={false} onchanged={onposted} />
      </div>
    </ConversationEntry>
  {/if}
{/snippet}

<section class="conversation" aria-label="Conversation">
  <ConversationEntry author={opener} text={change.description}>
    {#snippet head()}
      <span class="who">{opener}</span>
      <span class="dim">opened this change &middot; {ago(openedAt)}</span>
    {/snippet}
  </ConversationEntry>

  {#if loading}
    <p class="dim note">Loading&hellip;</p>
  {/if}

  {#each items as item, i (i)}
    {#if item.kind === 'thread'}
      {@render threadCard(item.thread)}
    {:else}
      {@render eventEntry(item.event)}
    {/if}
  {/each}

  <ConversationEntry author={me || undefined}>
    <form class="composer" onsubmit={submit}>
      <textarea rows="4" aria-label="Comment" placeholder="Comment" bind:value={body}></textarea>
      {#if reviewable}
        <div class="verdict-pick" role="radiogroup" aria-label="Review">
          <label><input type="radio" value="comment" bind:group={choice} /> Comment</label>
          <label><input type="radio" value="approve" bind:group={choice} /> Approve</label>
          <label
            ><input type="radio" value="request-changes" bind:group={choice} /> Request changes</label
          >
        </div>
      {/if}
      <InlineError message={postError} />
      <Locked refusedInDemo
        ><div class="actions">
          {#if closed}
            <button type="button" disabled={busy} onclick={reopen}>Reopen change</button>
          {:else}
            <button type="button" disabled={busy} onclick={close}>Close change</button>
          {/if}
          <button type="submit" class="primary" disabled={busy || !canSubmit(sending, body)}>
            {busy ? 'Sending…' : 'Submit'}
          </button>
        </div></Locked
      >
    </form>
  </ConversationEntry>
</section>

<style>
  .conversation {
    display: grid;
    gap: 0.75rem;
  }
  .dim {
    color: var(--dim);
  }
  .mono {
    font-family: var(--mono);
  }
  .note {
    font-size: var(--text-note);
    margin: 0;
  }
  .ink {
    color: var(--ink);
  }
  .who {
    font-weight: 600;
    color: var(--ink);
  }

  /* What a person wrote keeps the lines they wrote it in. */
  .text {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    color: var(--ink);
    font-size: var(--text-body);
    line-height: 1.5;
  }

  .reason {
    display: block;
    font-size: var(--text-note);
  }

  .composer textarea {
    width: 100%;
    box-sizing: border-box;
    resize: vertical;
    margin-bottom: 0.5rem;
  }
  .composer .actions {
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 0.5rem;
  }
  .composer :global(.error) {
    margin-bottom: 0.5rem;
  }
  .verdict-pick {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem 1rem;
    margin-bottom: 0.6rem;
    font-size: var(--text-label);
  }
  .verdict-pick label {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
  }

  .outdated {
    font-size: var(--text-micro);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0 0.45rem;
    color: var(--dim);
  }
  .fold {
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
  .fold.end {
    margin-left: auto;
  }
  .thread-body {
    display: grid;
    gap: 0.6rem;
  }
  .quote {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--field);
    font-size: var(--text-note);
    min-width: 0;
  }
  .quote-head {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    padding: 0.4rem 0.6rem;
    overflow-wrap: anywhere;
  }
  .quote-title {
    font-weight: 600;
    color: var(--ink);
  }
  .quote-code {
    border-top: 1px solid var(--border);
    padding: 0.4rem 0.6rem;
    color: var(--ink);
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }

  .verdict {
    font-weight: 600;
  }
  .requested-word {
    color: var(--accent);
  }
  .approved-word {
    color: var(--added);
  }
</style>
