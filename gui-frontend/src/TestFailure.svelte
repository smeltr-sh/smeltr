<!--
  Why a test fails: how the sample changed under it, and the first row that differs from what it locks.
-->
<script lang="ts">
  import { type Verdict } from './api';
  import { diffObjects, formatDiffValue, type DiffEntry } from './diff';
  import { count } from './time';
  import { outcomeText } from './objectChanges';
  import { differenceNote } from './testDiff';

  let {
    verdict,
    routeName,
  }: {
    /** A failing test's verdict. */
    verdict: Verdict;
    /** The name shown for route `id`, in the scope that holds the test. */
    routeName: (id: string) => string;
  } = $props();

  /** What a failing verdict's first difference is, laid out for the card: a headline sentence, and
   * either the changed fields (both sides made a document) or a plain note plus whichever side is
   * present -- a drop, or a row that made more or fewer documents than the test locks. */
  type FailInfo = {
    headline: string;
    note: string | null;
    entries: DiffEntry[];
    expectedLine: string | null;
    actualLine: string | null;
  };

  /** "held 3 records when locked, now 4": the sample itself changed under the test. */
  function rowsInText(v: Verdict): string | null {
    if (!v.rowsIn) return null;
    return `The sample held ${count(v.rowsIn.locked, 'record')} when locked, now ${v.rowsIn.now}.`;
  }

  function failInfo(v: Verdict): FailInfo | null {
    const named = routeName;
    const d = v.firstDifference;
    if (!d) return null;
    const headline = `Row ${d.row} differs from what the test locks.`;
    const { expected, actual } = d;
    const note = differenceNote(d);
    if (note === null)
      return {
        headline,
        note,
        entries: diffObjects(expected!.values!, actual!.values!).filter(
          (e) => e.kind !== 'unchanged',
        ),
        expectedLine: null,
        actualLine: null,
      };
    return {
      headline,
      note,
      entries: [],
      expectedLine: expected ? outcomeText(expected, named) : null,
      actualLine: actual ? outcomeText(actual, named) : null,
    };
  }

  let info = $derived(failInfo(verdict));
  let rowsIn = $derived(rowsInText(verdict));
</script>

{#if rowsIn || info}
  <div class="fail-card">
    {#if rowsIn}<p class="fail-headline">{rowsIn}</p>{/if}
    {#if info}<p class="fail-headline">{info.headline}</p>{/if}
    {#if info?.note}
      <p>{info.note}</p>
      {#if info.expectedLine !== null}
        <div class="mono diff-line removed">
          expected&emsp;{info.expectedLine}
        </div>
      {/if}
      {#if info.actualLine !== null}
        <div class="mono diff-line added">
          actual&emsp;&emsp;{info.actualLine}
        </div>
      {/if}
    {:else if info}
      {#each info.entries as entry}
        <div class="mono diff-line">
          <span class="diff-key">{entry.key}:</span>
          {#if entry.kind === 'changed'}
            <span class="dim">expected</span>
            <span class="removed">{formatDiffValue(entry.before)},</span>
            <span class="dim">got</span>
            <span class="added">{formatDiffValue(entry.after)}</span>
          {:else if entry.kind === 'removed'}
            <span class="removed">{formatDiffValue(entry.before)}</span>
          {:else}
            <span class="added">{formatDiffValue(entry.after)}</span>
          {/if}
        </div>
      {/each}
    {/if}
  </div>
{/if}

<style>
  .fail-card {
    margin: 0.35rem 0 0.6rem 0.9rem;
    padding: 0.55rem 0.7rem;
    border: 1px solid var(--error);
    border-radius: var(--radius-card);
    background: color-mix(in oklch, var(--error) 6%, transparent);
  }
  .fail-headline {
    margin: 0 0 0.35rem;
    font-size: var(--text-label);
  }
  .diff-line {
    font-size: var(--text-note);
    padding: 0.1rem 0;
    overflow-wrap: anywhere;
  }
  .diff-key {
    color: var(--dim);
    margin-right: 0.4rem;
  }
  .added {
    color: var(--added);
  }
  .removed {
    color: var(--removed);
  }
</style>
