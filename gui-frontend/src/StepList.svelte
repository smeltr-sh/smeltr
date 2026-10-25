<script lang="ts">
  import type { DotAnswer, DotAt } from './api';
  import StepEditor, { saveExpanded } from './StepEditor.svelte';
  import {
    newStep,
    STEP_TYPES,
    STEP_LABELS,
    STEP_SUMMARIES,
    type Step,
    type StepType,
    type PreviewError,
  } from './pipeline';
  import type { GrokPatternDto, LookupTableDto } from './api';
  import type { OpenProposal } from './proposal';
  import Dialog from './Dialog.svelte';
  import { editable } from './access';
  import TypeCatalog, { type CatalogType } from './TypeCatalog.svelte';
  import { STEP_GLYPHS } from './stepGlyphs';

  let {
    steps,
    errorsByStep = new Map(),
    warningsByStep = new Map(),
    pipelineIds = [],
    pipelineNames = {},
    lookupTables = [],
    grokPatterns = [],
    parseInput = '_raw',
    selectedStep = null,
    onSelectStep = () => {},
    onPropose = () => {},
    proposal = null,
    askAt,
  }: {
    steps: Step[];
    errorsByStep?: Map<Step, PreviewError[]>;
    /** What compiles but is likely a mistake, by step, as `errorsByStep` places errors. */
    warningsByStep?: Map<Step, PreviewError[]>;
    pipelineIds?: string[];
    pipelineNames?: Record<string, string>;
    lookupTables?: LookupTableDto[];
    /** The bundle's grok patterns, `shared/`'s and the scope's own, which a grok step may name. */
    grokPatterns?: GrokPatternDto[];
    /** The field a new parse step reads. */
    parseInput?: string;
    /** The step driving the diff view, threaded down so a nested step can
     * be selected. */
    selectedStep?: Step | null;
    onSelectStep?: (step: Step | null) => void;
    /** Ask a parser step to propose its declared fields from the sample. */
    onPropose?: (step: Step) => void;
    /** The field proposal open on some parser step, shown in that step's card. */
    proposal?: OpenProposal | null;
    /** Asks the engine what may follow a dot in an expression at `at` (`afterDot`). */
    askAt?: (at: DotAt) => (text: string, dot: number) => Promise<DotAnswer | null>;
  } = $props();

  /** Each step type's group in the catalog: what it acts on. */
  const ACTS_ON: Record<StepType, string> = {
    parser: 'Fields',
    eval: 'Fields',
    rename: 'Fields',
    lookup: 'Fields',
    code: 'Fields',
    drop: 'Records',
    unroll: 'Records',
    if: 'Flow',
    call: 'Flow',
    comment: 'Notes',
  };

  const TYPES: CatalogType<StepType>[] = STEP_TYPES.map((type) => ({
    type,
    label: STEP_LABELS[type],
    category: ACTS_ON[type],
    glyph: STEP_GLYPHS[type],
    help: STEP_SUMMARIES[type],
  }));

  /** Where the step the catalog adds goes, as an index into `steps`; null while it is shut. */
  let addingAt = $state<number | null>(null);

  function add(type: StepType) {
    const step = newStep(type, parseInput);
    saveExpanded(step.id, true);
    steps.splice(addingAt ?? steps.length, 0, step);
    addingAt = null;
  }
</script>

<div class="step-list">
  {#each steps as step, i (step)}
    {#if editable()}
      <div class="insert">
        <button type="button" onclick={() => (addingAt = i)} aria-label="Insert step">+ step</button
        >
      </div>
    {/if}
    <StepEditor
      {steps}
      index={i}
      {errorsByStep}
      {warningsByStep}
      {pipelineIds}
      {pipelineNames}
      {lookupTables}
      {grokPatterns}
      {parseInput}
      {selectedStep}
      {onSelectStep}
      {onPropose}
      {proposal}
      {askAt}
    />
  {/each}

  {#if editable()}
    <div class="add-row">
      <button type="button" onclick={() => (addingAt = steps.length)} aria-label="Add step"
        >+ step</button
      >
    </div>
  {/if}
</div>

<Dialog open={addingAt !== null} title="add step" wide oncancel={() => (addingAt = null)}>
  <TypeCatalog types={TYPES} cards onpick={add} />
  {#snippet actions()}
    <button type="button" onclick={() => (addingAt = null)}>Cancel</button>
  {/snippet}
</Dialog>

<style>
  .step-list {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }

  .add-row {
    display: flex;
    gap: 0.5rem;
    align-items: center;
  }

  /* The gap above a step, where a step can go in. It takes none of the list's height: its negative margins give
     back what it fills, so it lies over the gap the list leaves, and above the first step, over the list's edge.
     It shows on hover or focus. Where nothing hovers it would be an invisible target, so it is not there at all,
     and + step at the end still adds. */
  .insert {
    position: relative;
    height: 0.6rem;
    margin: -0.6rem 0;
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1;
    opacity: 0;
  }
  .insert:hover,
  .insert:focus-within {
    opacity: 1;
  }
  .insert::before {
    content: '';
    position: absolute;
    left: 0;
    right: 0;
    border-top: 1px dashed var(--accent);
  }
  @media (hover: none) {
    .insert {
      display: none;
    }
  }

  .add-row button,
  .insert button {
    background: transparent;
    color: var(--ink);
    border: 1px dashed var(--dim);
    border-radius: var(--radius);
    padding: 0.3rem 0.7rem;
    font-size: var(--text-label);
    cursor: pointer;
    flex: none;
    white-space: nowrap;
  }

  /* Opaque, so the dashed rule stops at its edges. */
  .insert button {
    position: relative;
    background: var(--field);
    padding: 0 0.7rem;
  }
</style>
