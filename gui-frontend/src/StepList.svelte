<script lang="ts">
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
    pipelineIds = [],
    pipelineNames = {},
    lookupTables = [],
    grokPatterns = [],
    parseInput = '_raw',
    selectedStep = null,
    onSelectStep = () => {},
    onPropose = () => {},
    proposal = null,
  }: {
    steps: Step[];
    errorsByStep?: Map<Step, PreviewError[]>;
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

  let adding = $state(false);

  function add(type: StepType) {
    const step = newStep(type, parseInput);
    saveExpanded(step.id, true);
    steps.push(step);
    adding = false;
  }
</script>

<div class="step-list">
  {#each steps as step, i (step)}
    <StepEditor
      {steps}
      index={i}
      {errorsByStep}
      {pipelineIds}
      {pipelineNames}
      {lookupTables}
      {grokPatterns}
      {parseInput}
      {selectedStep}
      {onSelectStep}
      {onPropose}
      {proposal}
    />
  {/each}

  {#if editable()}
    <div class="add-row">
      <button type="button" onclick={() => (adding = true)} aria-label="Add step">+ step</button>
    </div>
  {/if}
</div>

<Dialog open={adding} title="add step" wide oncancel={() => (adding = false)}>
  <TypeCatalog types={TYPES} cards onpick={add} />
  {#snippet actions()}
    <button type="button" onclick={() => (adding = false)}>Cancel</button>
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

  .add-row button {
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
</style>
