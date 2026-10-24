<!--
  The app's one dialog: a question asked before something that cannot be taken
  back happens (`ask`), or a notice the page must not be allowed to miss
  (`tell`). One instance, mounted by App, so a page raises either without
  holding a dialog of its own.

  A notice is the same chrome with nothing to decide -- it is here rather than
  in a second component because what makes it work is what makes a question
  work: a backdrop that stops the page, so the news cannot be walked past.

  A native `<dialog>`, per STYLE_GUIDE.md "Dialogs".
-->
<script module lang="ts">
  export type Question = {
    /** The header rule: what is being asked, in a word or two. */
    title: string;
    /** Why, and what it costs. */
    body: string;
    /** The long form of the body, behind a `?` beside it. */
    help?: string;
    /** The answer's label. */
    action: string;
    /** The refusal's label, where "Cancel" would undersell what refusing
     * actually does (discards something rather than simply closing the
     * dialog). Plain "Cancel" otherwise. */
    cancelLabel?: string;
    /** The answer destroys something, so it carries `--error`. */
    danger?: boolean;
    /** Nothing to decide: one button, and no refusal to put beside it. */
    notice?: boolean;
    /** Places the notice's news names, each reached by clicking rather than
     * read only: a save that failed on three steps says which. Clicking one
     * closes the dialog and runs its own action, generic to whatever that is. */
    links?: { label: string; onClick: () => void }[];
    /** Named changes, read only -- marked the way an added, changed or
     * removed row already reads elsewhere in the app (`RecordFields.svelte`).
     * For a question the dialog cannot itself resolve by browsing, unlike
     * `links`. */
    diff?: { kind: 'added' | 'removed' | 'changed'; summary: string }[];
  };

  type Raised = { question: Question; settle: (ok: boolean) => void };

  let pending = $state<Raised | null>(null);

  /** Notices raised while something else holds the dialog, shown in turn as it
   * clears. A question is refused instead: it asks about the moment it was
   * raised in, and answering it later answers about a page that has moved on.
   * A notice is news, and stays true however long it waits. */
  const waiting: Raised[] = [];

  /** Ask, and resolve to the answer -- `null` rather than `false` when the
   * question was never shown at all. The backdrop blocks the page, so a
   * second question cannot be raised over the first; one that arrives
   * anyway is refused rather than queued behind a dialog nobody asked for.
   * Most callers only care whether to proceed, and `null` is as falsy as
   * `false` there; one that must tell "declined" from "never asked" (a
   * caller that would otherwise discard something on a refusal it never
   * showed) reads the `null` itself. */
  export function ask(question: Question): Promise<boolean | null> {
    if (pending) return Promise.resolve(null);
    return new Promise((settle) => {
      pending = { question, settle };
    });
  }

  /** Say something the page cannot be allowed to miss, and resolve once it
   * has been read. For news a line of text somewhere on the page would lose:
   * a save that did not happen, which the author would otherwise navigate
   * away from believing their work is stored.
   *
   * Never dropped. A failed save is most likely exactly when another dialog is
   * open -- the request was in flight while the author moved on -- and that is
   * the case the notice exists for, so it queues rather than being refused. */
  export function tell(notice: {
    title: string;
    body: string;
    action?: string;
    links?: { label: string; onClick: () => void }[];
  }): Promise<void> {
    const question: Question = {
      title: notice.title,
      body: notice.body,
      action: notice.action ?? 'Close',
      notice: true,
      links: notice.links,
    };
    return new Promise<void>((resolve) => {
      const raised: Raised = { question, settle: () => resolve() };
      if (pending) waiting.push(raised);
      else pending = raised;
    });
  }
</script>

<script lang="ts">
  import Dialog from './Dialog.svelte';
  import Help from './Help.svelte';
  import JumpLinks from './JumpLinks.svelte';

  /* Escape and Cancel both land here saying no. The dialog stays open across a handover, so a
     queued notice replaces the content rather than reopening the dialog under the reader. */
  function answer(ok: boolean) {
    pending?.settle(ok);
    pending = waiting.shift() ?? null;
  }
</script>

<Dialog
  open={pending !== null}
  title={pending?.question.title ?? ''}
  oncancel={() => answer(false)}
  onsubmit={() => answer(true)}
>
  {#if pending}
    <p>
      {pending.question.body}{#if pending.question.help}<Help label={pending.question.title}
          >{pending.question.help}</Help
        >{/if}
    </p>
    {#if pending.question.links && pending.question.links.length > 0}
      <JumpLinks
        links={pending.question.links.map((link) => ({
          label: link.label,
          onClick: () => {
            link.onClick();
            answer(true);
          },
        }))}
      />
    {/if}
    {#if pending.question.diff && pending.question.diff.length > 0}
      <ul class="diff">
        {#each pending.question.diff as row}
          <li>
            <span class="mark {row.kind}"
              >{row.kind === 'added' ? '+' : row.kind === 'removed' ? '−' : '~'}</span
            >
            <span class="diff-summary">{row.summary}</span>
          </li>
        {/each}
      </ul>
    {/if}
  {/if}
  {#snippet actions()}
    <!-- A notice has no refusal: there is nothing to decide, so the one button is the focus. -->
    {#if pending && !pending.question.notice}
      <button type="button" onclick={() => answer(false)}
        >{pending.question.cancelLabel ?? 'Cancel'}</button
      >
    {/if}
    <button type="submit" class={pending?.question.danger ? 'danger' : 'primary'}>
      {pending?.question.action}
    </button>
  {/snippet}
</Dialog>

<style>
  .diff {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    margin: 0.6rem 0 0;
    padding: 0;
    list-style: none;
  }

  .diff li {
    display: flex;
    align-items: baseline;
    gap: 0.45rem;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.35rem 0.55rem;
  }

  .diff .mark {
    font-family: var(--mono);
    font-weight: 600;
    flex: none;
  }

  .diff .mark.added {
    color: var(--added);
  }

  .diff .mark.changed {
    color: var(--changed);
  }

  .diff .mark.removed {
    color: var(--removed);
  }

  .diff-summary {
    font-size: var(--text-note);
    color: var(--ink);
  }

  p {
    margin: 0;
    color: var(--ink);
    font-size: var(--text-body);
    line-height: 1.5;
  }
</style>
