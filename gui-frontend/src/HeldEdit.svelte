<!--
  The file moved under an edit that outlived the page it was typed on
  (`edits.ts`). `user-stories.md`, W9: an open editor notices when what it is
  working from moves, and says so rather than writing over it.

  A panel, not a dialog, for the reason `SaveConflict` is one: it waits as long
  as the reader needs. A dialog could not -- `Confirm.ask` refuses a second
  question outright and answers `null`, and a question that another question
  can throw away is not a guard.

  What is on screen while this is up is the file, not the held edit. The reader
  is choosing whether to bring the edit back over it, so the thing they are
  choosing against is the thing they can see -- the same call `offerDraftRestore`
  makes for a pipeline draft.
-->
<script lang="ts">
  import type { ChangeRow } from './diff';

  let {
    what,
    rows,
    onrestore,
    ondiscard,
  }: {
    /** The file, named the way the page names it: "sources", "routes". */
    what: string;
    /** What the edit would change about the file as it stands now. Empty where
     * the page cannot say -- the sentence still holds without it. */
    rows: ChangeRow[];
    onrestore: () => void;
    ondiscard: () => void;
  } = $props();
</script>

<section class="held">
  <header>
    <span class="sev">moved</span>
    <div>
      <strong>{what} changed while your edits were waiting</strong>
      <p class="critical">
        <strong>Bring my edits back</strong> puts your copy of {what} over the file as it stands, still
        unsaved. If you then save, the changes saved since you made your edits are undone.
      </p>
    </div>
  </header>

  {#if rows.length > 0}
    <div class="rows">
      <div class="rows-head"><strong>What your edits would change</strong></div>
      {#each rows as row, i (i)}
        <div class="row">
          <span class="kind {row.kind}">{row.kind}</span>
          <span class="summary">{row.summary}</span>
        </div>
      {/each}
    </div>
  {/if}

  <div class="actions">
    <button onclick={ondiscard}>Discard mine, keep what is saved</button>
    <button class="primary" onclick={onrestore}>Bring my edits back</button>
  </div>
</section>

<style>
  .held {
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

  header .critical {
    margin: 0.35rem 0 0;
  }

  .rows {
    margin: 0.75rem 0 0 calc(62px + 0.75rem);
    border-top: 1px solid var(--border);
    padding-top: 0.5rem;
  }

  .rows-head {
    font-size: var(--text-note);
    margin-bottom: 0.35rem;
  }

  .row {
    display: flex;
    gap: 0.5rem;
    align-items: baseline;
    font-size: var(--text-note);
    padding: 0.1rem 0;
  }

  .kind {
    flex-shrink: 0;
    width: 68px;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }

  .kind.added {
    color: var(--added);
  }

  .kind.removed {
    color: var(--error);
  }

  .kind.changed {
    color: var(--warn);
  }

  /* The kind is a label; the summary names the keys and values, so it alone is mono. */
  .summary {
    color: var(--ink);
    font-family: var(--mono);
  }

  .actions {
    display: flex;
    gap: 0.5rem;
    align-items: center;
    flex-wrap: wrap;
    margin: 0.75rem 0 0 calc(62px + 0.75rem);
  }
</style>
