<!--
  The records a pipeline's preview made, beside its steps, in the records grid (`RecordsGrid`):
  each row the sample's record before the steps and after them, or either side of the step the
  preview pauses at. The editor runs the preview and owns the pause; this pane only shows what came
  back, and asks the editor for an expression's rows (`preview`).

  The editor's own controls sit in it: `sampleMenu` beside the find box, `stage` (the failed
  stage and the pause) over the grid, and `head` above everything.
-->
<script module lang="ts">
  /** A step's chip in the find box: the rows it dropped or failed. */
  export type StepChip = { kind: 'droppedBy' | 'failedAt'; step: string };

  /** A pause's two runs, each side of its step. */
  export type PauseRun = {
    before: PreviewRow[];
    after: PreviewRow[];
    declared: { before: Declared | null; after: Declared | null };
  };
</script>

<script lang="ts">
  import Button, { INCOMPLETE } from './Button.svelte';
  import { untrack, type Snippet } from 'svelte';
  import { stepName, type Step } from './pipeline';
  import RecordsGrid, { type GridRow } from './RecordsGrid.svelte';
  import ChipPicker from './ChipPicker.svelte';
  import { count } from './time';
  import { type Declared } from './recordDiff';
  import {
    sampleSource,
    type LockChoice,
    type PreviewResult,
    type PreviewRow,
    type SampleRecordDto,
  } from './api';

  let {
    pipelineId,
    steps,
    signature,
    sampleRows,
    selectedSample,
    lastSample,
    result,
    lastGoodResult,
    gridRows,
    attribution,
    stepNumbers,
    pause,
    pauseRun,
    declared,
    canLock,
    onlock,
    onsaveoutput,
    saveinput,
    onjump,
    onassign,
    preview,
    head,
    sampleMenu,
    stage,
  }: {
    /** What the field choice and the recent expressions are remembered under. */
    pipelineId: string;
    steps: Step[];
    /** Changes whenever the steps a preview runs do, so an expression is run again. */
    signature: string;
    sampleRows: SampleRecordDto[];
    selectedSample: string;
    /** The sample the rows on screen came from. */
    lastSample: string;
    result: PreviewResult | null;
    lastGoodResult: PreviewResult | null;
    /** The rows the grid shows: the pause's, once run, or else the whole pipeline's. */
    gridRows: PreviewRow[] | null;
    /** Each row's attribution, by row. */
    attribution: Map<number, PreviewRow> | null;
    /** Each step by id, with its number in the step list. */
    stepNumbers: Map<string, { step: Step; number: number }>;
    /** Where the records stop: the pipeline up to and including that step. */
    pause: { after: Step[] } | null;
    pauseRun: PauseRun | null;
    /** The whole pipeline's types, each side. */
    declared: { before: Declared | null; after: Declared | null };
    canLock: boolean;
    /** Locks the fields and rows on screen as a test, by `PreviewRow.row`; absent where the pipeline is read
     * only. */
    onlock?: (choice: LockChoice) => void;
    /** Saves the output rows on screen as a sample, by `PreviewRow.row`; absent where no sample may be saved. */
    onsaveoutput?: (rows: Set<number>) => void;
    /** The input's record count and its save action, given while the input is a capture not saved yet. The output's
     * save button then opens a menu of both. */
    saveinput?: { records: number; onsave: () => void };
    onjump: (step: Step) => void;
    /** Writes a field in an eval step; absent where the pipeline is read only. */
    onassign?: (path: string) => void;
    /** The rows `filter` keeps of `sample` run through `steps`. */
    preview: (steps: Step[], sample: string, filter: string) => Promise<PreviewResult>;
    head?: Snippet;
    sampleMenu: Snippet;
    stage: Snippet;
  } = $props();

  let grid = $state<RecordsGrid | undefined>();

  /** Keeps the rows a step dropped or failed, on top of what the find box already keeps. */
  export function addChip(c: StepChip) {
    const number = stepNumbers.get(c.step)?.number ?? '?';
    grid?.addChip({
      kind: 'rows',
      key: `${c.kind === 'droppedBy' ? 'dropped by' : 'failed at'} ${c.step}`,
      label: c.kind === 'droppedBy' ? `⌫ Dropped by step ${number}` : `Failed at step ${number}`,
      failing: c.kind === 'failedAt',
      keeps: (row) =>
        c.kind === 'droppedBy'
          ? attribution?.get(row.key)?.droppedBy === c.step
          : (attribution?.get(row.key)?.exceptions.some((e) => e.step === c.step) ?? false),
    });
  }

  /** Empties the find box, for records it was not typed over. */
  export function clearFind() {
    grid?.clearFind();
  }

  // What the record's source set is shown on every row, read or not, so an
  // author sees what there is to read. It is never delivered, so the
  // pipeline's output does not carry it.
  function withSource(record: unknown, row: number): unknown {
    const source = sampleRows[row] && sampleSource(sampleRows[row]);
    if (!source || record === null || typeof record !== 'object') return record;
    return { __source: source, ...record };
  }
  // The sample's side once per sample rather than once per run, so the grid reads its fields once.
  let samples = $derived(sampleRows.map((r, i) => withSource(r, i)));

  /** The steps that touched each field of `row`, in the order they ran. A
   * list is compared whole, so a field inside one takes the list's. */
  function touchedOn(row: number) {
    const touched = attribution?.get(row)?.steps;
    return (path: string) => {
      let at = path;
      while (touched && !(at in touched)) {
        const up = at.replace(/(\.[^.[]*|\[[^\]]*\])$/, '');
        if (up === at || up === '') break;
        at = up;
      }
      return (touched?.[at] ?? []).flatMap((id) => {
        const t = stepNumbers.get(id);
        return t ? [t] : [];
      });
    };
  }

  // The pause's runs by row, so a row finds its sides without a search.
  let pausedByRow = $derived.by(() => {
    const d = pause ? pauseRun : null;
    if (!d) return null;
    const byRow = (rows: PreviewRow[]) => new Map(rows.map((r) => [r.row, r]));
    return { before: byRow(d.before), after: byRow(d.after) };
  });

  /** The types each side of the step shown: the pause's, or the whole pipeline's. */
  let shownDeclared = $derived(pause && pauseRun ? pauseRun.declared : declared);

  /** Each row as the grid draws it, from its sample row: the record before and after the step
   * shown, or before and after the whole pipeline. */
  let rows = $derived(
    gridRows?.map((row): GridRow => ({
      key: row.row,
      label: String(row.row),
      before: pausedByRow
        ? withSource(pausedByRow.before.get(row.row)?.json ?? null, row.row)
        : (samples[row.row] ?? withSource({}, row.row)),
      after: pausedByRow
        ? withSource(pausedByRow.after.get(row.row)?.json ?? null, row.row)
        : withSource(row.json, row.row),
      dropped: row.json === null,
      exceptions: row.exceptions.map((a) => {
        const by = a.step ? stepNumbers.get(a.step) : undefined;
        return {
          message: a.message,
          by: by && { label: stepName(by.step, by.number), onjump: () => onjump(by.step) },
        };
      }),
    })) ?? null,
  );

  /** The record the steps write, from the last run that compiled: with no sample, a run that
   * read no records. */
  let dictionary = $derived(
    (result?.ok ? result.dictionary : undefined) ?? lastGoodResult?.dictionary,
  );
  /** The fields the schemas the pipeline produces require or recommend and its rows lack. */
  let lacking = $derived(
    (result?.ok ? result.missing : undefined) ?? lastGoodResult?.missing ?? [],
  );

  // Paused, the expression reads the records where they stand.
  let evaluate = $derived.by(() => {
    const [sample, cut] = [lastSample, pause?.after];
    void signature;
    return sample
      ? (expression: string) => preview(cut ?? untrack(() => steps), sample, expression)
      : null;
  });

  // The records pane ends at the bottom of the window, wherever it starts, so only the steps make the
  // page taller than the window. What the page holds below the pane is left room for too.
  let rightCol = $state<HTMLElement | undefined>();
  let paneTop = $state(0);
  let paneAfter = $state(0);
  $effect(() => {
    const pane = rightCol;
    if (!pane) return;
    const place = () => {
      const top = pane.getBoundingClientRect().top;
      const columns = pane.parentElement!.getBoundingClientRect().bottom;
      paneTop = Math.max(0, top);
      // The page ends where its content does, short of the window or past it.
      paneAfter = Math.max(0, document.documentElement.getBoundingClientRect().bottom - columns);
    };
    place();
    // And as the page settles: what loads above the pane moves it down.
    const settled = new ResizeObserver(place);
    settled.observe(document.body);
    settled.observe(pane);
    window.addEventListener('scroll', place, { capture: true, passive: true });
    window.addEventListener('resize', place);
    return () => {
      settled.disconnect();
      window.removeEventListener('scroll', place, { capture: true });
      window.removeEventListener('resize', place);
    };
  });

  /** The menu of the input's and the output's saves is open. */
  let savingOpen = $state(false);
</script>

<section
  class="right-col"
  bind:this={rightCol}
  style:--pane-top={`${paneTop}px`}
  style:--pane-after={`${paneAfter}px`}
>
  {@render head?.()}

  {#if !selectedSample}{@render stage()}{/if}

  <RecordsGrid
    bind:this={grid}
    {rows}
    chosen={selectedSample !== ''}
    declared={shownDeclared}
    remember={pipelineId}
    openOn={lastSample || undefined}
    {dictionary}
    {lacking}
    {evaluate}
    touched={touchedOn}
    {onjump}
    {onassign}
  >
    {#snippet bar()}{@render sampleMenu()}{/snippet}
    {#snippet above()}{#if selectedSample}{@render stage()}{/if}{/snippet}
    {#snippet empty()}
      {#if !result}
        <p class="dim">Run the pipeline to see output rows here.</p>
      {/if}
    {/snippet}
    {#snippet actions(kept, shown)}
      <!-- The rows on screen that have output, by `PreviewRow.row`: a dropped row has none. -->
      {@const outputShown = new Set(kept.filter((r) => !r.dropped).map((r) => r.key))}
      {#if onsaveoutput && saveinput}
        <ChipPicker kind="columns" compact bind:open={savingOpen}>
          {#snippet face()}Save as sample <span aria-hidden="true">▾</span>{/snippet}
          <!-- Choosing closes the menu (`ChipPicker` leaves that to its caller). -->
          <div onclick={() => (savingOpen = false)} role="presentation">
            <button type="button" class="pick" onclick={saveinput.onsave}
              >Save input as sample <span class="dim"
                >Your {count(saveinput.records, 'record')}</span
              ></button
            >
            <Button
              type="button"
              class="pick"
              onclick={() => onsaveoutput(outputShown)}
              disabled={outputShown.size === 0 ? INCOMPLETE : ''}
              >Save output as sample <span class="dim"
                >The {count(outputShown.size, 'row')} on screen</span
              ></Button
            >
          </div>
        </ChipPicker>
      {:else if onsaveoutput}
        <Button
          type="button"
          onclick={() => onsaveoutput(outputShown)}
          disabled={outputShown.size === 0 ? INCOMPLETE : ''}
        >
          Save output as sample
        </Button>
      {/if}
      {#if onlock}
        <span class="dim"
          >The {count(shown.length, 'field')} on {count(kept.length, 'row')} on screen</span
        >
        <Button
          type="button"
          class="primary"
          data-guide="lock-test"
          onclick={() => onlock({ fields: shown, rows: kept.map((r) => r.key) })}
          disabled={!canLock ? INCOMPLETE : ''}
        >
          Lock this as a test
        </Button>
      {/if}
    {/snippet}
  </RecordsGrid>
</section>

<style>
  /* A grid item is `min-width: auto` by default, so a record wider than the
     column would push past it rather than scroll inside its own half. */
  .right-col {
    min-width: 0;
    overflow-x: auto;
  }

  .right-col {
    display: flex;
    flex-direction: column;
    gap: 0;
  }

  /* Beside the steps, the records scroll on their own: the 500th record and the step that set it
     are on screen together. */
  @media (min-width: 1001px) {
    .right-col {
      position: sticky;
      top: var(--clear-top, 0px);
      /* Its grid and dictionary scroll themselves; a short pane must not clip a menu opened in it. */
      overflow: visible;
      /* From where it stands on screen to the bottom of the window, clear of what the page holds
         below it and of the bars held over the page: the Save bar, and the agent panel under it. */
      max-height: calc(
        100vh - max(var(--pane-top), var(--clear-top, 0px)) - var(--pane-after) -
          var(--clear-bottom, 0px) - var(--agent-panel, 0px)
      );
    }

    /* The grid is what scrolls, so the controls above it and its header row stay put. */
    .right-col :global(.records),
    .right-col :global(.results) {
      display: flex;
      flex-direction: column;
      flex: 1;
      min-height: 0;
    }

    /* What the editor passes in is its own markup, so these reach past this component's. */
    .right-col > :global(:not(.records)),
    .right-col :global(.records > :not(.results)) {
      flex-shrink: 0;
    }

    .right-col :global(.grid-wrap) {
      overflow: auto;
      min-height: 0;
    }
  }
</style>
