<script module lang="ts">
  const STEP_EXPANDED_KEY_PREFIX = 'step-expanded:';

  function loadExpanded(id: string): boolean {
    try {
      return localStorage.getItem(STEP_EXPANDED_KEY_PREFIX + id) === '1';
    } catch {
      return false;
    }
  }

  /** Remembers whether step `id` is open, so it opens that way when it mounts. */
  export function saveExpanded(id: string, value: boolean) {
    try {
      localStorage.setItem(STEP_EXPANDED_KEY_PREFIX + id, value ? '1' : '0');
    } catch {
      // Best-effort: a blocked or full localStorage just forgets across reloads.
    }
  }
</script>

<script lang="ts">
  import type { AskAt } from './api';
  import InlineError from './InlineError.svelte';
  import StepList from './StepList.svelte';
  import ValidatedField from './ValidatedField.svelte';
  import Help from './Help.svelte';
  import Advanced from './Advanced.svelte';
  import Collapsible from './Collapsible.svelte';
  import Toggle from './Toggle.svelte';
  import Field from './Field.svelte';
  import Nested from './Nested.svelte';
  import Locked from './Locked.svelte';
  import SeparatorInput from './SeparatorInput.svelte';
  import ChipPicker from './ChipPicker.svelte';
  import MoreIcon from './MoreIcon.svelte';
  import DisableIcon from './DisableIcon.svelte';
  import { flip } from 'svelte/animate';
  import { glide } from './glide';
  import FieldRow from './FieldRow.svelte';
  import TypePicker from './TypePicker.svelte';
  import DatetimeFormat from './DatetimeFormat.svelte';
  import {
    STEP_LABELS,
    stepSummary,
    MATCH_KINDS,
    MULTIPLICITIES,
    MULTIPLICITY_LABELS,
    type Step,
    type FieldSpec,
    type LookupOutput,
    type LookupOutputType,
    type PreviewError,
    STR_OUTPUT,
    SPLIT_DOTS_DEFAULT,
    PARSER_FORMAT_NAMES,
    splitsDots,
    setSplitDots,
    declaredTypeGroups,
    isDeclared,
    lookupTypeGroups,
    typeKey,
    withTypeKey,
    OBJECT_FIELD_TYPE_GROUPS,
    type TypeKey,
  } from './pipeline';
  import { fieldNotes, type OpenProposal, type Suggestion } from './proposal';
  import { fieldError } from './errorMarks';
  import { ago, count } from './time';
  import { parsePath, pushKey } from './names';
  import { SvelteMap } from 'svelte/reactivity';
  import StepGlyph from './StepGlyph.svelte';
  import { errorText, grokCaptures, type GrokPatternDto, type LookupTableDto } from './api';
  import { sharedGrokSuggestions } from './grokPatterns';
  import { beginReconcile, endReconcile } from './grokFields';
  import { unfinishedRows } from './unfinished.svelte';
  import { editable } from './access';

  let {
    steps,
    index,
    errorsByStep = new Map(),
    warningsByStep = new Map(),
    pipelineIds = [],
    pipelineNames = {},
    lookupTables = [],
    grokPatterns = [],
    parseInput = '_raw',
    selectedStep = null,
    ran = null,
    counts = new Map(),
    onCount = () => {},
    onSelectStep = () => {},
    onPropose = () => {},
    proposal = null,
    askAt,
  }: {
    steps: Step[];
    index: number;
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
    selectedStep?: Step | null;
    /** The steps the records have been through, while they are paused; the rest are dimmed. */
    ran?: Set<Step> | null;
    /** How many records each step dropped and failed, in the records shown. */
    counts?: Map<Step, { dropped: number; failed: number }>;
    /** Filter the records to those the step dropped or failed. */
    onCount?: (step: Step, kind: 'dropped' | 'failed') => void;
    onSelectStep?: (step: Step | null) => void;
    /** Ask the parser step to propose its declared fields from the sample. */
    onPropose?: (step: Step) => void;
    /** The field proposal open on some parser step, shown in that step's card. */
    proposal?: OpenProposal | null;
    /** Asks the engine what may follow a dot in an expression at `at` (`afterDot`). */
    askAt?: AskAt;
  } = $props();

  const readOnly = !editable();
  let step = $derived(steps[index]);
  let stepErrors = $derived(errorsByStep.get(step) ?? []);
  /** The plain messages every field's `errors` prop takes -- the substring
   * heuristic's fallback, kept for every error `assignmentIndex` cannot
   * place any more precisely than "somewhere in this step". */
  let stepErrorMessages = $derived(stepErrors.map((e) => e.message));
  let stepWarningMessages = $derived([
    ...new Set((warningsByStep.get(step) ?? []).map((e) => e.message)),
  ]);
  /** Where an error naming an assignment by row number points after rows
   * above it were removed; null once its own row is gone. */
  const moved = new SvelteMap<PreviewError, number | null>();
  const rowOf = (e: PreviewError) => (moved.has(e) ? moved.get(e) : e.assignmentIndex);
  const unfinished = unfinishedRows();
  /** Focus left a box of `row`: blank, it is now a mistake the preview shows.
   * The remove button is neither box. */
  function leave(row: { target: string; expr: string }, from: EventTarget | null) {
    if (!(from instanceof HTMLInputElement || from instanceof HTMLTextAreaElement)) return;
    unfinished?.left(row, from.closest('.target-field') ? 'target' : 'expr');
  }
  /** Every message about this step, written once in its list: its compile
   * errors, then what a field-name box's own check finds. The boxes only
   * mark where. */
  let listedErrors = $derived([
    ...new Set([
      ...stepErrorMessages,
      ...nameFields(step)
        .map(nameError)
        .filter((m) => m !== null),
    ]),
  ]);
  function removeAssignment(i: number) {
    for (const e of stepErrors) {
      const row = rowOf(e);
      if (row === i) moved.set(e, null);
      else if (row != null && row > i) moved.set(e, row - 1);
    }
    if (step.type === 'eval') step.assignments.splice(i, 1);
  }
  let isSelected = $derived(selectedStep === step);
  // A broken step stays bright: its error is what to read.
  let stepCounts = $derived(counts.get(step));
  let notRun = $derived(ran !== null && !ran.has(step) && stepErrors.length === 0);
  function toggleSelected() {
    onSelectStep(isSelected ? null : step);
  }

  // Open/closed is a per-step preference, remembered across reloads by the
  // step's own id; a step never seen before opens closed, except one just
  // added (`StepList`), which opens to be filled in. A one-time read:
  // this component remounts when `step` changes (`StepList` keys its
  // `{#each}` by step object), so there is no later value to react to. An if
  // card is always open, so it reads no preference.
  // svelte-ignore state_referenced_locally
  let expanded = $state(step.type !== 'if' ? loadExpanded(step.id) : true);
  let patternDefinitionsOpen = $state(false);

  function setExpanded(value: boolean) {
    expanded = value;
    saveExpanded(step.id, value);
  }

  // A step with a compile error is shown expanded, so the error is visible.
  // This does not overwrite the remembered preference -- it is a temporary
  // override for as long as the error stands.
  $effect(() => {
    if (stepErrors.length > 0) expanded = true;
  });

  // -- lookup: column pickers ----------------------------------------------
  //
  // A step picks a shared table by id. A csv/inline table's `headers` feed
  // the column pickers; an mmdb table, or an unknown one, gets free text.
  function tableOf(tableId: string): LookupTableDto | undefined {
    return lookupTables.find((t) => t.id === tableId);
  }
  function knownColumns(tableId: string): string[] | null {
    const t = tableOf(tableId);
    return t && t.type !== 'mmdb' ? t.headers : null;
  }

  /** The table's columns with the key column first, so its row can be told apart. */
  function orderedColumns(columns: string[], keyColumn: string): string[] {
    return columns.includes(keyColumn)
      ? [keyColumn, ...columns.filter((h) => h !== keyColumn)]
      : columns;
  }

  /** Why a box that holds a name refuses its text, or null. A blank box is not
   * yet a name, and the compiler says so if it stays blank. */
  function nameError(text: string): string | null {
    if (text === '') return null;
    const parsed = parsePath(text);
    return parsed.ok ? null : parsed.message;
  }

  /** The text of every box whose own check is `nameError`, as the template
   * gives them one. */
  function nameFields(step: Step): string[] {
    switch (step.type) {
      case 'eval':
        return step.assignments.map((a) => a.target);
      case 'rename':
        return step.pairs.flatMap((p) => [p.from, p.to]);
      case 'parser':
        return [
          step.input,
          ...(step.kind.type === 'csv' || step.kind.type === 'grok'
            ? []
            : step.declared.map((d) => d.name)),
        ];
      case 'lookup':
        return [step.input, ...step.outputs.map((o) => o.field)];
      default:
        return [];
    }
  }

  /** Which parsers offer "split keys at dots". A CSV column's name is
   * already a path, and the header text comes from it. */

  /** A lookup output as `key` types it. */
  function outputTyped(ty: LookupOutputType, key: TypeKey): LookupOutputType {
    // The picker offers a lookup output only what `lookupTypeGroups` lists, each one value.
    return withTypeKey(ty, key) as LookupOutputType;
  }

  /** A known column's row in the grid: `skip` leaves it out of the outputs, any type puts it in. */
  function setColumnType(outputs: LookupOutput[], column: string, key: TypeKey) {
    const i = outputs.findIndex((o) => o.source === column);
    if (key === 'skip') {
      if (i !== -1) outputs.splice(i, 1);
    } else if (i === -1) {
      const ty = outputTyped({ ...STR_OUTPUT }, key);
      outputs.push({ source: column, field: column, default: '', ty });
    } else {
      outputs[i].ty = outputTyped(outputs[i].ty, key);
    }
  }

  // The More menu, which holds the controls a narrow head folds away.
  let actionsOpen = $state(false);

  /** A choice from the More menu: act on it, then close the menu. */
  function choose(action: () => void) {
    action();
    actionsOpen = false;
  }

  function remove() {
    steps.splice(index, 1);
  }
  function moveUp() {
    if (index === 0) return;
    const [s] = steps.splice(index, 1);
    steps.splice(index - 1, 0, s);
  }
  function moveDown() {
    if (index === steps.length - 1) return;
    const [s] = steps.splice(index, 1);
    steps.splice(index + 1, 0, s);
  }

  /** Swaps a declared field with its neighbor. Only meaningful where position
   * is significant -- a CSV column's index -- so the caller only offers this
   * for CSV. */
  function moveDeclared(declared: FieldSpec[], i: number, dir: -1 | 1) {
    const j = i + dir;
    if (j < 0 || j >= declared.length) return;
    [declared[i], declared[j]] = [declared[j], declared[i]];
  }
  /** A field or column as `+` adds it. */
  function newField(): FieldSpec {
    return { name: '', ty: { kind: 'named', name: 'str' } };
  }

  /** A grok step's captures, as the compiled pattern reports them. `null` until the first answer arrives. */
  let captures: string[] | null = $state(null);
  /** Why a half-typed pattern does not compile. The fields on screen stay as they are while it says something. */
  let captureError: string | null = $state(null);

  /**
   * Fill a grok step's field names in from its pattern, keeping the type already chosen for a name that survives.
   *
   * Naming a capture is how a grok field is declared, so the two cannot be edited into disagreement -- which is what
   * makes the compiler's refusal of either direction unreachable from here. `captures` comes from the server because a
   * definition can mint one the pattern text never names, and only the compiled matcher sees those.
   */
  function reconcile(declared: FieldSpec[], caps: string[], split: boolean): FieldSpec[] {
    const chosen = new Map(declared.map((f) => [f.name, f.ty]));
    // A capture's name is the parser's key: one key, or under splitDots a path split at its dots.
    const names = caps.map((c) => (split ? c.split('.') : [c]).reduce(pushKey, ''));
    return names.map((name) => ({ name, ty: chosen.get(name) ?? { kind: 'named', name: 'str' } }));
  }

  // Debounced: the author is typing a pattern, and every keystroke is a half-written one. The whole window counts as
  // owed, timer included -- a save landing before the timer fires would serialize the same stale list as one landing
  // mid-request.
  let timer: ReturnType<typeof setTimeout> | undefined;
  $effect(() => {
    if (step.type !== 'parser' || step.kind.type !== 'grok') {
      captures = null;
      captureError = null;
      return;
    }
    const { patterns, customPatterns } = step.kind;
    const split = splitsDots(step.kind);
    const key = JSON.stringify([patterns, customPatterns, split, grokPatterns]);

    beginReconcile();
    let settled = false;
    const settle = () => {
      if (settled) return;
      settled = true;
      endReconcile();
    };

    clearTimeout(timer);
    timer = setTimeout(() => {
      grokCaptures(patterns, customPatterns, grokPatterns)
        .then((r) => {
          // Another keystroke landed while this was in flight; its answer is the current one.
          if (step.type !== 'parser' || step.kind.type !== 'grok') return;
          if (
            JSON.stringify([
              step.kind.patterns,
              step.kind.customPatterns,
              splitsDots(step.kind),
              grokPatterns,
            ]) !== key
          )
            return;
          captureError = r.ok ? null : errorText(r.errors) || 'these patterns do not compile';
          if (!r.ok) return;
          captures = r.captures;
          step.declared = reconcile(step.declared, r.captures, split);
        })
        // Unreachable server, or a rejection that is not JSON. Say so: the fields are read-only, so there is nothing
        // the author can do here but edit the pattern again, which is what retries it.
        .catch((e: unknown) => {
          captureError = `${e instanceof Error ? e.message : String(e)} -- edit a pattern to try again`;
        })
        .finally(settle);
    }, 250);

    // A further edit supersedes this reconciliation; the effect that replaces it owes its own.
    return () => {
      clearTimeout(timer);
      settle();
    };
  });

  /** What one definition contributes, for its summary line: the captures its own text names. */
  function bringsOf(pattern: string): string {
    const named = [...pattern.matchAll(/%\{[A-Za-z0-9_]+:([^}]+)\}/g)].map((m) => m[1]);
    const unique = [...new Set(named)];
    return unique.length ? `brings ${unique.join(', ')}` : 'brings no field';
  }

  type ParserStep = Extract<Step, { type: 'parser' }>;

  /** What a parser's Advanced holds that is not at its default, as its header names it. */
  function advancedValues(ps: ParserStep): { label: string; value: string }[] {
    const kind = ps.kind;
    const values: { label: string; value: string }[] = [];
    if (ps.input !== parseInput) values.push({ label: 'input', value: ps.input });
    if (ps.filter.trim() !== '') values.push({ label: 'filter', value: ps.filter });
    if (kind.type === 'grok' && kind.customPatterns.length > 0)
      values.push({ label: 'pattern definitions', value: String(kind.customPatterns.length) });
    if (kind.type === 'keyValue') {
      if (kind.quote !== '"')
        values.push({ label: 'quote char', value: JSON.stringify(kind.quote) });
      if (kind.nextKey === false)
        values.push({ label: 'values run to the next key', value: 'off' });
    }
    if (splitsDots(kind) !== SPLIT_DOTS_DEFAULT[kind.type])
      values.push({ label: 'split keys at dots', value: splitsDots(kind) ? 'on' : 'off' });
    return values;
  }

  /** How many of a parser's Advanced boxes mark an error, as each box would. */
  function advancedErrors(ps: ParserStep): number {
    const input = fieldError(ps.input, stepErrorMessages, nameError(ps.input));
    const filter = fieldError(ps.filter, stepErrorMessages, null);
    return (input ? 1 : 0) + (filter ? 1 : 0);
  }

  /** Whether the open field proposal is this step's. */
  let suggesting = $derived(proposal?.step === step);

  /** The name a suggestion declares: the author's edit of it, where it has a field to edit. */
  function suggestedName(s: Suggestion): string {
    return s.field?.name ?? s.row.name;
  }
  /** Whether a suggestion is declared already, as the rows named it or as the author renamed it. */
  function suggestionDeclared(ps: ParserStep, s: Suggestion): boolean {
    return isDeclared(ps.declared, s.row.name) || isDeclared(ps.declared, suggestedName(s));
  }
  /** The proposal's fields worth showing: those not declared yet, and a declared one that rows failed. */
  function shownSuggestions(ps: ParserStep, all: Suggestion[]): Suggestion[] {
    return all.filter(
      (s) => !suggestionDeclared(ps, s) || !!s.row.sample?.failed || !!s.row.production?.failed,
    );
  }
  /** Declares a suggestion as the author left it. A copy, so editing the suggestion does not edit the field. */
  function addSuggestion(ps: ParserStep, s: Suggestion) {
    if (s.field && !suggestionDeclared(ps, s)) ps.declared.push($state.snapshot(s.field));
  }
  /** What a suggestion's rows said of it: each source's notes, the source named where there are two. A source the
   * field is absent from is no warning: the other source has it. */
  function suggestionNotes(
    s: Suggestion,
    fromSample: boolean,
    fromProduction: boolean,
  ): { text: string; warn: boolean }[] {
    const both = fromSample && fromProduction;
    const of = (f: typeof s.row.sample, missing: string, source: string) =>
      f
        ? fieldNotes(f).map((n) => ({ text: both ? `${source}: ${n}` : n, warn: true }))
        : both
          ? [{ text: missing, warn: false }]
          : [];
    return [
      ...(fromSample ? of(s.row.sample, 'not in the sample', 'From the sample') : []),
      ...(fromProduction ? of(s.row.production, 'not in production', 'In production') : []),
    ];
  }
</script>

{#snippet stepErrorList()}
  {#if listedErrors.length > 0 || stepWarningMessages.length > 0}
    <div class="step-errors" class:warn={listedErrors.length === 0}>
      {#each listedErrors as message}
        {@render stepMessage(message, false)}
      {/each}
      {#each stepWarningMessages as message}
        {@render stepMessage(message, true)}
      {/each}
    </div>
  {/if}
{/snippet}

{#snippet stepMessage(message: string, warn: boolean)}
  <div class="step-error-msg" class:warn>
    {@render glyph(warn)}
    <span>{message}</span>
  </div>
{/snippet}

{#snippet glyph(warn: boolean)}
  <svg class="error-glyph" class:warn viewBox="0 0 24 24" fill="none" aria-hidden="true">
    <path d="M12 3.5 L22 20.5 H2 Z" fill="currentColor" />
    <rect x="11" y="9" width="2" height="6" rx="1" class="cutout" />
    <rect x="11" y="16.5" width="2" height="2" rx="1" class="cutout" />
  </svg>
{/snippet}

{#snippet more()}
  <div class="more">
    <ChipPicker
      kind="step"
      compact
      ariaLabel="More actions"
      title="More actions"
      bind:open={actionsOpen}
    >
      {#snippet face()}
        <MoreIcon />
      {/snippet}
      {#if step.type !== 'if' && step.type !== 'comment'}
        <button
          type="button"
          class="pick"
          onclick={() => choose(() => (step.disabled = !step.disabled))}
        >
          {step.disabled ? 'Enable' : 'Disable'}
        </button>
      {/if}
      <button type="button" class="pick" onclick={() => choose(moveUp)}>Move up</button>
      <button type="button" class="pick" onclick={() => choose(moveDown)}>Move down</button>
      <button type="button" class="pick remove" onclick={() => choose(remove)}>Delete</button>
    </ChipPicker>
  </div>
{/snippet}

<!-- The fields the open proposal read, under the step's own: each as Add would declare it, its name and type the
     author's to change first. -->
{#snippet suggestions(ps: ParserStep, open: OpenProposal)}
  {@const p = open.proposal}
  {@const shown = shownSuggestions(ps, p.suggestions)}
  {@const addable = shown.filter((s) => s.field && !suggestionDeclared(ps, s))}
  {@const fromSample = p.result !== null}
  {@const production = p.production}
  <div class="suggestions" role="group" aria-label="Suggested fields">
    <div class="suggestions-head">
      <span class="suggestions-source">
        {#if p.loading}
          reading the sample…
        {:else if p.result || production}
          {@const sampleRowCount = p.result?.fields[0]?.rows}
          <strong>Suggested</strong> from {#if p.result}<code>{open.sampleName}</code
            >{sampleRowCount === undefined ? '' : `, ${count(sampleRowCount, 'row')}`}{production
              ? ', and '
              : ''}{/if}{#if production}production, {count(production.rows, 'row')}, last seen {ago(
              production.lastSeenMs / 1000,
            )}{/if}
        {/if}
      </span>
      {#if addable.length > 0}
        <button
          type="button"
          class="compact"
          onclick={() => addable.forEach((s) => addSuggestion(ps, s))}>Add all</button
        >
      {/if}
      <button type="button" class="compact" aria-label="Done" title="Done" onclick={open.onClose}
        >✕︎</button
      >
    </div>
    <InlineError message={p.error} />
    {#each shown as s (s.row.name)}
      {@const declared = suggestionDeclared(ps, s)}
      <div class="suggestion" role="group" aria-label={s.row.name}>
        {#if s.field && !declared}
          {@const field = s.field}
          {#snippet add()}
            <button type="button" class="compact" onclick={() => addSuggestion(ps, s)}>Add</button>
          {/snippet}
          <FieldRow
            bind:ty={field.ty}
            groups={declaredTypeGroups(false)}
            label={field.name}
            end={add}
          >
            {#snippet name()}
              <input type="text" aria-label="suggested field name" bind:value={field.name} />
            {/snippet}
          </FieldRow>
        {:else}
          <div class="suggestion-fixed">
            <code>{suggestedName(s)}</code>
            <code>{s.row.suggested}</code>
            {#if declared}<span class="dim">already declared</span>{/if}
          </div>
        {/if}
        {#each suggestionNotes(s, fromSample, production !== null) as note (note.text)}
          <div class="suggestion-note" class:warn={note.warn}>{note.text}</div>
        {/each}
      </div>
    {/each}
  </div>
{/snippet}

{#snippet controls()}
  <div class="controls">
    {#if !readOnly && step.type !== 'if' && step.type !== 'comment'}
      <button
        type="button"
        class="fold"
        class:active={step.disabled}
        onclick={() => (step.disabled = !step.disabled)}
        aria-label={step.disabled ? 'Enable this step' : 'Disable this step'}
        title={step.disabled ? 'Enable this step' : 'Disable this step'}><DisableIcon /></button
      >
    {/if}
    <button
      type="button"
      class:active={isSelected}
      onclick={toggleSelected}
      aria-label={isSelected ? 'Show the whole pipeline' : 'Pause the records after this step'}
      title={isSelected ? 'Show the whole pipeline' : 'Pause the records after this step'}
      >{isSelected ? '●︎' : '◎︎'}</button
    >
    {#if !readOnly}
      <button type="button" class="fold" onclick={moveUp} aria-label="Move up" title="Move up"
        >↑︎</button
      >
      <button type="button" class="fold" onclick={moveDown} aria-label="Move down" title="Move down"
        >↓︎</button
      >
      <button type="button" class="fold remove" onclick={remove} aria-label="Delete" title="Delete"
        >✕︎</button
      >
      {@render more()}
    {/if}
  </div>
{/snippet}

{#if step.type === 'if'}
  <div
    id={`step-${step.id}`}
    class="card if-card"
    class:has-error={stepErrors.length > 0}
    class:diff-selected={isSelected}
    class:not-run={notRun}
  >
    <div class="card-head if-head">
      <span class="type-badge">
        <StepGlyph type="if" />
        If
      </span>
      <div class="condition">
        <Locked>
          <ValidatedField
            placeholder="condition, e.g. status >= 400"
            bind:value={step.condition}
            errors={stepErrorMessages}
            warnings={stepWarningMessages}
            autocomplete="condition"
            ask={askAt?.({ step: step.id })}
            wrap
          />
        </Locked>
      </div>
      {@render controls()}
    </div>
    {@render stepErrorList()}
    <div class="branches">
      <div class="branch">
        <div class="branch-label">then</div>
        <StepList
          steps={step.then}
          {errorsByStep}
          {warningsByStep}
          {pipelineIds}
          {pipelineNames}
          {lookupTables}
          {grokPatterns}
          {parseInput}
          {selectedStep}
          {ran}
          {counts}
          {onCount}
          {onSelectStep}
          {onPropose}
          {proposal}
          {askAt}
        />
      </div>
      <div class="branch">
        <div class="branch-label">else</div>
        <StepList
          steps={step.else}
          {errorsByStep}
          {warningsByStep}
          {pipelineIds}
          {pipelineNames}
          {lookupTables}
          {grokPatterns}
          {parseInput}
          {selectedStep}
          {ran}
          {counts}
          {onCount}
          {onSelectStep}
          {onPropose}
          {proposal}
          {askAt}
        />
      </div>
    </div>
  </div>
{:else}
  <div
    id={`step-${step.id}`}
    class="card"
    class:open={expanded}
    class:has-error={stepErrors.length > 0}
    class:comment-card={step.type === 'comment'}
    class:diff-selected={isSelected}
    class:not-run={notRun}
    class:step-disabled={step.type !== 'comment' && step.disabled}
  >
    <!-- The controls sit beside the button that opens the step: a button cannot hold
         another, and the More menu is one. -->
    <div class="card-head">
      <button
        class="expander"
        type="button"
        data-guide={step.type}
        aria-expanded={expanded}
        onclick={() => setExpanded(!expanded)}
      >
        <span class="type-badge" class:comment-badge={step.type === 'comment'}>
          <StepGlyph type={step.type} />
          <span class="type-name">{STEP_LABELS[step.type]}</span>
        </span>
        <span class="summary">{stepSummary(step)}</span>
        {#if stepErrors.length > 0 || stepWarningMessages.length > 0}
          {@render glyph(stepErrors.length === 0)}
        {/if}
      </button>
      {#if stepCounts && (stepCounts.dropped > 0 || stepCounts.failed > 0)}
        <span class="counts">
          {#if stepCounts.dropped > 0}
            <button type="button" class="count link-button" onclick={() => onCount(step, 'dropped')}
              >&minus;{stepCounts.dropped} dropped</button
            >
          {/if}
          {#if stepCounts.failed > 0}
            <button
              type="button"
              class="count failed link-button"
              onclick={() => onCount(step, 'failed')}>{stepCounts.failed} failed</button
            >
          {/if}
        </span>
      {/if}
      {@render controls()}
    </div>

    {#if expanded}
      <div class="body">
        {#if step.type === 'comment'}
          <Field optional label="Text">
            <textarea rows="2" bind:value={step.text}></textarea>
          </Field>
        {:else}
          <!-- A parser's filter is under its Advanced: nearly every parse reads every record. -->
          {#if step.type !== 'drop' && step.type !== 'parser'}
            <Field optional label="filter">
              <ValidatedField
                placeholder="true"
                bind:value={step.filter}
                errors={stepErrorMessages}
                warnings={stepWarningMessages}
                autocomplete="condition"
                ask={askAt?.({ step: step.id })}
                wrap
              />
            </Field>
          {/if}

          {#if step.type === 'eval'}
            <div class="rows">
              <div class="rows-label"><span class="field-name">assignments</span></div>
              {#each step.assignments as _, i}
                {@const rowErrors = stepErrors.filter((e) => rowOf(e) === i)}
                {@const targetError = rowErrors.find((e) => e.field === 'target')}
                {@const assignmentError = rowErrors.find((e) => e.field !== 'target')}
                <!-- The demo's guide points at an assignment that does not compile. -->
                <div
                  class="row-pair"
                  data-guide={assignmentError ? 'assignment-error' : undefined}
                  onfocusout={(e) => leave(step.assignments[i], e.target)}
                >
                  <span class="target-field">
                    <Locked>
                      <ValidatedField
                        placeholder="target field"
                        bind:value={step.assignments[i].target}
                        errors={stepErrorMessages}
                        warnings={stepWarningMessages}
                        forced={nameError(step.assignments[i].target) ??
                          targetError?.message ??
                          null}
                      />
                    </Locked>
                  </span>
                  <span class="eq">=</span>
                  <Locked>
                    <ValidatedField
                      placeholder="expression"
                      bind:value={step.assignments[i].expr}
                      errors={stepErrorMessages}
                      warnings={stepWarningMessages}
                      forced={assignmentError?.message ?? null}
                      autocomplete="assignment"
                      ask={askAt?.({ step: step.id })}
                    />
                  </Locked>
                  {#if !readOnly}
                    <button
                      type="button"
                      class="ctl remove"
                      onclick={() => removeAssignment(i)}
                      aria-label="Remove assignment"
                      title="Remove assignment">✕︎</button
                    >
                  {/if}
                </div>
              {/each}
              {#if !readOnly}
                <button
                  type="button"
                  class="add-small"
                  onclick={() => step.assignments.push({ target: '', expr: '' })}
                  >+ assignment</button
                >
              {/if}
            </div>
          {:else if step.type === 'code'}
            <Field required label="body">
              <ValidatedField
                multiline
                rows={6}
                placeholder={'let n = status; n += 1; msg = `status=${n}`;'}
                bind:value={step.body}
                errors={stepErrorMessages}
                warnings={stepWarningMessages}
                autocomplete="code"
                ask={askAt?.({ step: step.id })}
              />
            </Field>
          {:else if step.type === 'drop'}
            <Field optional label="drop when this is true" help="Leave it blank to drop nothing.">
              <ValidatedField
                placeholder="e.g. status == 500"
                bind:value={step.condition}
                errors={stepErrorMessages}
                warnings={stepWarningMessages}
                autocomplete="condition"
                ask={askAt?.({ step: step.id })}
                wrap
              />
            </Field>
          {:else if step.type === 'rename'}
            <div class="rows">
              <div class="rows-label"><span class="field-name">pairs</span></div>
              {#each step.pairs as _, i}
                <div class="row-pair">
                  <Locked>
                    <ValidatedField
                      placeholder="from"
                      bind:value={step.pairs[i].from}
                      errors={stepErrorMessages}
                      warnings={stepWarningMessages}
                      forced={nameError(step.pairs[i].from)}
                    />
                  </Locked>
                  <span class="eq">→︎</span>
                  <Locked>
                    <ValidatedField
                      placeholder="to"
                      bind:value={step.pairs[i].to}
                      errors={stepErrorMessages}
                      warnings={stepWarningMessages}
                      forced={nameError(step.pairs[i].to)}
                    />
                  </Locked>
                  {#if !readOnly}
                    <button
                      type="button"
                      class="ctl remove"
                      onclick={() => step.pairs.splice(i, 1)}
                      aria-label="Remove pair"
                      title="Remove pair">✕︎</button
                    >
                  {/if}
                </div>
              {/each}
              {#if !readOnly}
                <button
                  type="button"
                  class="add-small"
                  onclick={() => step.pairs.push({ from: '', to: '' })}>+ pair</button
                >
              {/if}
            </div>
          {:else if step.type === 'parser'}
            {@const ps = step}
            {@const parserKind = step.kind}
            {@const isCsv = parserKind.type === 'csv'}
            {@const isGrok = parserKind.type === 'grok'}
            <Field label="format">
              <select
                value={step.kind.type}
                onchange={(e) => {
                  const t = (e.target as HTMLSelectElement).value;
                  ps.kind =
                    t === 'json'
                      ? { type: 'json' }
                      : t === 'csv'
                        ? { type: 'csv', delimiter: ',' }
                        : t === 'keyValue'
                          ? { type: 'keyValue', pairSep: ' ', kvSep: '=', quote: '"' }
                          : { type: 'grok', patterns: [], customPatterns: [] };
                  // A `skip` describes a column, and only CSV has columns. Left behind it would be sent on the next
                  // save and refused by the loader, with the row it came from no longer on screen to correct.
                  if (t !== 'csv') ps.declared = ps.declared.filter((f) => f.ty.kind !== 'skip');
                }}
              >
                {#each Object.entries(PARSER_FORMAT_NAMES) as [type, name] (type)}
                  <option value={type}>{name}</option>
                {/each}
              </select>
            </Field>

            {#if step.kind.type === 'csv'}
              {@const csvKind = step.kind}
              <Nested>
                <Field required label="delimiter">
                  <SeparatorInput bind:value={csvKind.delimiter} />
                </Field>
              </Nested>
            {:else if step.kind.type === 'keyValue'}
              <Nested>
                <div class="kind-fields">
                  <Field required label="pair separator">
                    <SeparatorInput bind:value={step.kind.pairSep} />
                  </Field>
                  <Field required label="key/value separator">
                    <SeparatorInput bind:value={step.kind.kvSep} />
                  </Field>
                </div>
              </Nested>
            {:else if step.kind.type === 'grok'}
              {@const grokKind = step.kind}
              <Nested>
                <div class="rows">
                  <div class="rows-label"><span class="field-name">patterns</span></div>
                  {#each grokKind.patterns as _, i}
                    <div class="row-pair">
                      <Locked>
                        <ValidatedField
                          placeholder={'%{IP:client} %{WORD:action}'}
                          bind:value={grokKind.patterns[i]}
                          autocomplete="grok"
                          grokNamed={sharedGrokSuggestions(grokPatterns)}
                        />
                      </Locked>
                      {#if !readOnly}
                        <button
                          type="button"
                          class="ctl remove"
                          onclick={() => grokKind.patterns.splice(i, 1)}
                          aria-label="Remove pattern"
                          title="Remove pattern">✕︎</button
                        >
                      {/if}
                    </div>
                  {/each}
                  {#if !readOnly}
                    <button
                      type="button"
                      class="add-small"
                      onclick={() => grokKind.patterns.push('')}>+ pattern</button
                    >
                  {/if}
                </div>
              </Nested>
            {/if}

            <div class="rows">
              <div class="rows-label">
                <span class="field-name"
                  >{isCsv ? 'columns' : 'declared fields'}
                  {#if isCsv}
                    <Help label="columns">
                      One row per column, in the record's order. To leave a column out, set its type
                      to skip; removing it would shift every column after it.
                    </Help>
                  {:else if !isGrok}
                    <Help label="declared fields">
                      Declare a field to read it by name in a later step. Keys you do not declare
                      still pass through to the output.
                    </Help>
                  {/if}</span
                >
              </div>
              {#if isGrok && captureError}
                <small class="note"
                  >These patterns do not compile yet, so the fields below are the last ones that did
                  — {captureError}</small
                >
              {/if}
              {#each ps.declared as field, i (field)}
                <div class="glide-row" animate:flip={{ duration: glide() }}>
                  {#snippet fieldActions()}
                    {#if isCsv}
                      <button
                        type="button"
                        class="pick"
                        disabled={i === 0}
                        onclick={() => moveDeclared(ps.declared, i, -1)}>Move up</button
                      >
                      <button
                        type="button"
                        class="pick"
                        disabled={i === ps.declared.length - 1}
                        onclick={() => moveDeclared(ps.declared, i, 1)}>Move down</button
                      >
                      <button
                        type="button"
                        class="pick"
                        onclick={() => ps.declared.splice(i, 0, newField())}
                        >Add column above</button
                      >
                    {/if}
                    <button
                      type="button"
                      class="pick remove"
                      onclick={() => ps.declared.splice(i, 1)}>Delete</button
                    >
                  {/snippet}
                  {#snippet objectFields()}
                    {@const objTy = field.ty}
                    {#if objTy.kind === 'object' || objTy.kind === 'arrayOfObjects'}
                      {#each objTy.fields as sub, j (sub)}
                        {#snippet subActions()}
                          <button
                            type="button"
                            class="pick remove"
                            onclick={() => objTy.fields.splice(j, 1)}>Delete</button
                          >
                        {/snippet}
                        <FieldRow
                          bind:ty={
                            () => ({ kind: 'named', name: sub.ty }),
                            (t) => {
                              if (t.kind === 'named') sub.ty = t.name;
                            }
                          }
                          groups={OBJECT_FIELD_TYPE_GROUPS}
                          disabled={readOnly}
                          label={sub.name || 'object field'}
                          actions={subActions}
                        >
                          {#snippet name()}
                            <input
                              disabled={readOnly}
                              type="text"
                              placeholder="name"
                              aria-label="object field name"
                              bind:value={sub.name}
                            />
                          {/snippet}
                        </FieldRow>
                      {/each}
                      {#if !readOnly}
                        <button
                          type="button"
                          class="add-small"
                          onclick={() => objTy.fields.push({ name: '', ty: 'str' })}>+ field</button
                        >
                      {/if}
                      {#if objTy.kind === 'object'}
                        <label class="inline rows-label">
                          <Toggle disabled={readOnly} bind:checked={objTy.open} ariaLabel="open" />
                          <span class="field-name">
                            open
                            <Help label="open">
                              Closed, the object holds only the fields above and the parse drops
                              every other key. Open, the other keys stay in the output.
                            </Help>
                          </span>
                        </label>
                      {/if}
                    {/if}
                  {/snippet}
                  <FieldRow
                    bind:ty={ps.declared[i].ty}
                    groups={declaredTypeGroups(isCsv)}
                    number={isCsv ? i + 1 : undefined}
                    disabled={readOnly}
                    label={field.name || (isCsv ? 'column' : 'field')}
                    actions={isGrok ? undefined : fieldActions}
                    nested={field.ty.kind === 'object' || field.ty.kind === 'arrayOfObjects'
                      ? objectFields
                      : undefined}
                  >
                    {#snippet name()}
                      {#if isGrok}
                        <span class="derived-name" title="from the pattern">{field.name}</span>
                      {:else}
                        <Locked>
                          <ValidatedField
                            placeholder="name"
                            bind:value={ps.declared[i].name}
                            errors={stepErrorMessages}
                            warnings={stepWarningMessages}
                            forced={isCsv ? null : nameError(ps.declared[i].name)}
                          />
                        </Locked>
                      {/if}
                    {/snippet}
                  </FieldRow>
                </div>
              {/each}
              {#if !readOnly && !isGrok}
                <div class="adds">
                  <button
                    type="button"
                    class="add-small"
                    onclick={() => ps.declared.push(newField())}
                    >+ {isCsv ? 'column' : 'field'}</button
                  >
                  <!-- Only a dynamic parser's fields come from what it reads (docs/gui-design.md, "The workspace"):
                       CSV's are its columns and grok's its captures, which the step already names. -->
                  {#if !isCsv}
                    <button
                      type="button"
                      class="add-small"
                      class:on={suggesting}
                      onclick={() => onPropose(ps)}>Suggest fields</button
                    >
                  {/if}
                </div>
              {/if}
            </div>

            {#if suggesting && proposal}
              {@render suggestions(ps, proposal)}
            {/if}

            <Advanced values={advancedValues(ps)} errors={advancedErrors(ps)}>
              <Field optional label="input field">
                <ValidatedField
                  bind:value={ps.input}
                  errors={stepErrorMessages}
                  warnings={stepWarningMessages}
                  forced={nameError(ps.input)}
                />
              </Field>
              <Field optional label="filter">
                <ValidatedField
                  placeholder="true"
                  bind:value={ps.filter}
                  errors={stepErrorMessages}
                  warnings={stepWarningMessages}
                  autocomplete="condition"
                  ask={askAt?.({ step: step.id })}
                  wrap
                />
              </Field>
              {#if parserKind.type === 'grok'}
                {@const grokKind = parserKind}
                <Collapsible bind:open={patternDefinitionsOpen}>
                  {#snippet summary()}
                    pattern definitions
                  {/snippet}
                  {#snippet actions()}
                    <Help label="pattern definitions">
                      Your own named patterns, for when no built-in grok pattern fits. A pattern
                      uses one as <code>%{'{'}NAME{'}'}</code>.
                    </Help>
                  {/snippet}
                  {#each grokKind.customPatterns as def, i}
                    <details class="definition" open={i === 0}>
                      <summary>
                        <span class="def-name">{def.name || 'unnamed'}</span>
                        <span class="def-brings">{bringsOf(def.pattern)}</span>
                      </summary>
                      <div class="definition-body">
                        <div class="row-pair">
                          <input
                            disabled={readOnly}
                            type="text"
                            placeholder="NAME"
                            aria-label="definition name"
                            bind:value={grokKind.customPatterns[i].name}
                          />
                          {#if !readOnly}
                            <button
                              type="button"
                              class="ctl remove"
                              onclick={() => grokKind.customPatterns.splice(i, 1)}
                              aria-label="Remove pattern definition"
                              title="Remove pattern definition">✕︎</button
                            >
                          {/if}
                        </div>
                        <Locked>
                          <ValidatedField
                            placeholder={'%{WORD:vendor},%{GREEDYDATA:rest}'}
                            ariaLabel={(def.name || 'unnamed') + ' pattern'}
                            bind:value={grokKind.customPatterns[i].pattern}
                            autocomplete="grok"
                            grokNamed={sharedGrokSuggestions(grokPatterns)}
                          />
                        </Locked>
                      </div>
                    </details>
                  {/each}
                  {#if !readOnly}
                    <button
                      type="button"
                      class="add-small"
                      onclick={() => grokKind.customPatterns.push({ name: '', pattern: '' })}
                      >+ pattern definition</button
                    >
                  {/if}
                </Collapsible>
              {/if}
              {#if parserKind.type === 'keyValue'}
                {@const kvKind = parserKind}
                <Field
                  optional
                  label="quote char"
                  help="Wraps a value that contains the pair separator: it runs from this character to the next one. Leave it empty if values are never quoted."
                >
                  <input type="text" maxlength="1" bind:value={kvKind.quote} />
                </Field>
                <label class="inline">
                  <Toggle
                    disabled={readOnly}
                    bind:checked={
                      () => kvKind.nextKey !== false,
                      (on) => {
                        if (on) delete kvKind.nextKey;
                        else kvKind.nextKey = false;
                      }
                    }
                    ariaLabel="values run to the next key"
                  />
                  <span class="field-name">
                    values run to the next key
                    <Help label="values run to the next key">
                      On by default, <code>level=info debug x=1</code> produces<br />
                      <code>{'{'}"level": "info debug", "x": "1"{'}'}</code>.<br />
                      Off, it produces<br />
                      <code>{'{'}"level": "info", "debug": "true", "x": "1"{'}'}</code>.
                    </Help>
                  </span>
                </label>
              {/if}
              <label class="inline">
                <Toggle
                  disabled={readOnly}
                  bind:checked={() => splitsDots(parserKind), (on) => setSplitDots(parserKind, on)}
                  ariaLabel="split keys at dots"
                />
                <span class="field-name">
                  split keys at dots
                  <Help label="split keys at dots">
                    {#if parserKind.type === 'json'}
                      Off by default for JSON: the key <code>geo.city</code> is one key,
                      <code>{'{'}"geo.city": …{'}'}</code>. On, it splits into a path,
                      <code>{'{'}"geo": {'{'}"city": …{'}'}{'}'}</code>.
                    {:else if parserKind.type === 'csv'}
                      Off by default for CSV: the column <code>src.ip</code> is one key,
                      <code>{'{'}"src.ip": …{'}'}</code>. On, it splits into a path,
                      <code>{'{'}"src": {'{'}"ip": …{'}'}{'}'}</code>.
                    {:else if parserKind.type === 'keyValue'}
                      On by default for Key/value: <code>a.b=1</code> becomes
                      <code>a: {'{'}b: "1"{'}'}</code>, its only way to nest. Off,
                      <code>a.b</code> is one key.
                    {:else}
                      On by default for grok: <code>%{'{'}IP:src.ip{'}'}</code> becomes
                      <code>src: {'{'}ip: …{'}'}</code>. Off, <code>src.ip</code> is one key.
                    {/if}
                  </Help>
                </span>
              </label>
            </Advanced>
          {:else if step.type === 'lookup'}
            {@const columns = knownColumns(step.table)}
            {@const isMmdb = tableOf(step.table)?.type === 'mmdb'}
            <Field required label="input field">
              <ValidatedField
                bind:value={step.input}
                errors={stepErrorMessages}
                warnings={stepWarningMessages}
                forced={nameError(step.input)}
              />
            </Field>

            <Field
              label="lookup table"
              note={lookupTables.length === 0
                ? 'No lookup tables yet — add one on the Lookup Tables page.'
                : undefined}
            >
              <select
                value={step.table}
                class:field-error={step.table !== '' && !tableOf(step.table)}
                onchange={(e) => {
                  step.table = (e.target as HTMLSelectElement).value;
                  if (tableOf(step.table)?.type === 'mmdb') {
                    step.matchKind = 'cidr';
                    step.keyColumn = '';
                  }
                }}
              >
                <option value="">(choose a table)</option>
                {#if step.table !== '' && !tableOf(step.table)}
                  <option value={step.table}>{step.table} (unknown table!)</option>
                {/if}
                {#each lookupTables as t}
                  <option value={t.id}>{t.id} ({t.type})</option>
                {/each}
              </select>
            </Field>

            <Field label="match kind">
              <select bind:value={step.matchKind} disabled={isMmdb}>
                {#each MATCH_KINDS as k}
                  <option value={k}>{k}</option>
                {/each}
              </select>
            </Field>
            {#if !isMmdb}
              <Field label="match effort">
                <select bind:value={step.multiplicity}>
                  {#each MULTIPLICITIES as m}
                    <option value={m}>{MULTIPLICITY_LABELS[m]}</option>
                  {/each}
                </select>
              </Field>

              {#if columns}
                <Field label="key / match column">
                  <select bind:value={step.keyColumn}>
                    <option value="">(choose a column)</option>
                    {#each columns as h}
                      <option value={h}>{h}</option>
                    {/each}
                  </select>
                </Field>
              {:else}
                <Field required label="key / match column">
                  <input type="text" bind:value={step.keyColumn} />
                </Field>
              {/if}
            {/if}

            {#if columns}
              <div class="rows">
                <div class="rows-label">
                  <span class="field-name"
                    >enrichment columns
                    <Help label="enrichment columns">
                      On a match, each column not typed <em>skip</em> is written onto the row as its output
                      field. On a miss, the field gets its default on miss; a blank default is null.
                    </Help></span
                  >
                </div>
                <div class="output-header">
                  <span>column</span>
                  <span>output field</span>
                  <span>type</span>
                  <span>default on miss</span>
                </div>
                {#each orderedColumns(columns, step.keyColumn) as h, ci (ci)}
                  {@const existing = step.outputs.find((o) => o.source === h)}
                  {@const isKey = h === step.keyColumn}
                  <div class="output-row" class:skipped={!existing}>
                    <span class="output-col-name"
                      >{h}{#if isKey}<span class="key-tag">key</span>{/if}</span
                    >
                    {#if existing}
                      <Locked>
                        <ValidatedField
                          placeholder="output field name"
                          bind:value={existing.field}
                          errors={stepErrorMessages}
                          warnings={stepWarningMessages}
                          forced={nameError(existing.field)}
                        />
                      </Locked>
                    {:else}
                      <input type="text" placeholder={h} disabled />
                    {/if}
                    <TypePicker
                      value={existing ? typeKey(existing.ty) : 'skip'}
                      groups={lookupTypeGroups(true)}
                      disabled={readOnly}
                      ariaLabel={`${h} type`}
                      onpick={(key) => setColumnType(step.outputs, h, key)}
                    />
                    {#if existing && !isKey}
                      <input
                        disabled={readOnly}
                        type="text"
                        placeholder="null"
                        bind:value={existing.default}
                      />
                    {:else}
                      <span></span>
                    {/if}
                    {#if existing && existing.ty.kind === 'datetime'}
                      {@const dt = existing.ty}
                      <div class="fmt-row">
                        <DatetimeFormat bind:format={dt.format} disabled={readOnly} />
                      </div>
                    {/if}
                  </div>
                {/each}
              </div>
            {:else}
              <div class="rows">
                <div class="rows-label">
                  <span class="field-name"
                    >outputs
                    <Help label="outputs">
                      {#if isMmdb}
                        On a match, each dotted path into the MMDB record, such as
                        <code>country.iso_code</code>, is written onto the row as its output field.
                      {:else}
                        On a match, each table column is written onto the row as its output field.
                      {/if}
                      On a miss, the field gets its default; a blank default is null.
                    </Help></span
                  >
                </div>
                {#each step.outputs as o, i}
                  <div class="row-pair">
                    <input
                      disabled={readOnly}
                      type="text"
                      placeholder={isMmdb ? 'country.iso_code' : 'column name'}
                      bind:value={step.outputs[i].source}
                    />
                    <span class="eq">→︎</span>
                    <Locked>
                      <ValidatedField
                        placeholder="field name"
                        bind:value={step.outputs[i].field}
                        errors={stepErrorMessages}
                        warnings={stepWarningMessages}
                        forced={nameError(step.outputs[i].field)}
                      />
                    </Locked>
                    <TypePicker
                      value={typeKey(o.ty)}
                      groups={lookupTypeGroups(false)}
                      disabled={readOnly}
                      ariaLabel={`${o.field || 'output'} type`}
                      onpick={(key) => (o.ty = outputTyped(o.ty, key))}
                    />
                    <input
                      disabled={readOnly}
                      type="text"
                      placeholder="default on miss (blank = null)"
                      bind:value={step.outputs[i].default}
                    />
                    {#if !readOnly}
                      <button
                        type="button"
                        class="ctl remove"
                        onclick={() => step.outputs.splice(i, 1)}
                        aria-label="Remove output"
                        title="Remove output">✕︎</button
                      >
                    {/if}
                  </div>
                  {#if o.ty.kind === 'datetime'}
                    {@const dt = o.ty}
                    <div class="fmt-row">
                      <DatetimeFormat bind:format={dt.format} disabled={readOnly} />
                    </div>
                  {/if}
                {/each}
                {#if !readOnly}
                  <button
                    type="button"
                    class="add-small"
                    onclick={() =>
                      step.outputs.push({ source: '', field: '', default: '', ty: STR_OUTPUT })}
                    >+ output</button
                  >
                {/if}
              </div>
            {/if}
          {:else if step.type === 'call'}
            <Field
              label="pipeline to inline here"
              note={pipelineIds.length === 0
                ? 'No other pipelines to call yet — create one on the Pipelines page first.'
                : undefined}
            >
              <select bind:value={step.pipeline}>
                <option value="">(choose a pipeline)</option>
                {#each pipelineIds as p}
                  <option value={p}>{pipelineNames[p] || p}</option>
                {/each}
              </select>
            </Field>
          {:else if step.type === 'unroll'}
            <Field required label="array field to unroll">
              <input type="text" bind:value={step.field} />
            </Field>
            <label class="inline">
              <Toggle
                disabled={readOnly}
                bind:checked={step.keepParentFields}
                ariaLabel="keep the parent record's fields on every element"
              />
              <span class="field-name">
                keep the parent record's fields on every element
                <Help label="keep the parent record's fields">
                  On, every other field, <code>_raw</code> included, is copied onto each row the array
                  fans out into, so memory grows with the square of the array's length.
                </Help>
              </span>
            </label>
          {/if}
        {/if}
      </div>
    {/if}
    {@render stepErrorList()}
  </div>
{/if}

<style>
  /* A card lays itself out by its own width: the divider sets how wide the
     steps are, and every if step narrows its branches further. The control
     box is set once here, for every control in the card and the More menu's
     chip, which ChipPicker draws. */
  /* At rest a step is its surface alone: `--field` on the panel draws it.
     Open, it takes its border and the accent rail down its leading edge. */
  .card {
    border: 1px solid transparent;
    border-radius: var(--radius-card);
    /* The card's surface, which the head's floating controls fade in from. */
    --card-surface: var(--field);
    background: var(--card-surface);
    container: step / inline-size;
    --control-width: 1.6rem;
    --control-height: 1.4rem;
  }

  /* `:where` keeps this below an error's edge and a comment's border, which
     outrank being open. */
  .card:where(.open) {
    border-color: var(--border);
    box-shadow: inset 3px 0 0 var(--accent);
  }

  /* An if step holds steps, so it is their panel. */
  .if-card {
    border-color: var(--border);
    --card-surface: var(--panel);
  }

  /* An error turns the card's leading edge --error. This rule comes after
     .if-card so it wins that edge. */
  .has-error {
    border-left: 3px solid var(--error);
  }

  .comment-card {
    --card-surface: var(--comment-bg);
    border-style: dashed;
    border-color: var(--comment-border);
  }

  /* Skipped by the compiler entirely (`FunctionKind`, disabled), so its
     glyph, summary and body read the same way a disabled button does --
     present, but visibly not in play -- without needing the card open to
     see the toggle that says so. */
  .step-disabled {
    opacity: 0.55;
  }

  /* The step the records are paused after: the pause chip's accent. */
  .diff-selected {
    --card-surface: color-mix(in oklab, var(--accent) 10%, var(--field));
  }
  .diff-selected:not(.has-error) {
    border-color: color-mix(in srgb, var(--accent) 60%, var(--border));
  }
  .comment-card.diff-selected {
    --card-surface: color-mix(in oklab, var(--accent) 10%, var(--comment-bg));
  }

  /* Steps the paused records have not been through yet. */
  .not-run {
    opacity: 0.55;
  }

  .card-head {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0.7rem;
  }

  /* The whole head but its controls opens the step: the expander reaches out
     over the head's padding and the gap before the controls, and pads its
     content back in. */
  .expander {
    display: flex;
    align-items: center;
    align-self: stretch;
    gap: 0.5rem;
    flex: 1;
    min-width: 0;
    margin: -0.5rem -0.5rem -0.5rem -0.7rem;
    padding: 0.5rem 0.5rem 0.5rem 0.7rem;
    background: none;
    border: none;
    text-align: left;
    font: inherit;
    color: inherit;
    cursor: pointer;
  }

  .condition {
    display: flex;
    flex: 1;
    min-width: 0;
  }

  /* An if step's condition takes a row of its own under its badge and controls, as another step's fields do. */
  .if-head {
    flex-wrap: wrap;
  }

  .if-head .condition {
    order: 1;
    flex-basis: 100%;
  }

  .if-head .controls {
    margin-left: auto;
  }

  .more {
    display: none;
  }

  /* Five controls take 9.4rem and never shrink, and the summary needs room
     beside them. At 18.75rem and under, ◎ stays out and the rest fold into
     the More menu, the badge shows its glyph and keeps its name only for a
     screen reader. In
     rem, so the fold moves with the reader's text size, as the controls do. */
  @container step (max-width: 18.75rem) {
    .controls .fold {
      display: none;
    }

    .more {
      display: block;
    }

    .type-name {
      position: absolute;
      width: 1px;
      height: 1px;
      overflow: hidden;
      clip-path: inset(50%);
      white-space: nowrap;
    }
  }

  .type-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.3em;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    flex-shrink: 0;
  }

  .type-badge :global(svg) {
    height: 1.15em;
    width: auto;
  }

  /* The same cutout technique the logo itself uses (STYLE_GUIDE.md, Logo):
     a background-colored shape punched through a filled one, so the mark
     reads without a second color. In the header it sits on the card's own
     panel; in the message band below it sits on that band's own tint, which
     is close enough to the panel color for the cutout to still read clean. */
  .error-glyph {
    width: 15px;
    height: 15px;
    flex-shrink: 0;
    color: var(--error);
  }

  .error-glyph .cutout {
    fill: var(--field);
  }

  .comment-badge {
    color: var(--comment-ink);
  }

  .comment-card .summary {
    font-family: inherit;
    color: var(--comment-ink);
    font-style: italic;
  }

  /* What the step did to the records shown, each a filter to those records. */
  .counts {
    display: flex;
    flex: none;
  }

  .count {
    flex: none;
    padding: 0 0.3rem;
    font-size: var(--text-note);
    color: var(--dim);
  }

  .count.failed {
    color: var(--error);
    font-weight: 600;
  }

  .summary {
    flex: 1;
    font-family: var(--mono);
    font-size: var(--text-label);
    color: var(--ink);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .controls {
    display: flex;
    gap: 0.35rem;
    flex-shrink: 0;
  }

  /* Controls arrive when wanted: on the head's hover or focus, while the
     step is open, and while one of them is on or has its menu open. On the
     head, not the card, so a step nested in an if does not wake the if's.
     Where nothing hovers they stay, since the More menu only takes their
     place when the card is narrow. A closed step's summary runs the head's
     whole width, and its controls arrive over the summary's end, fading in
     from the card's surface, so they cost the summary nothing while they are
     away. An if's head keeps them in line, since its condition is a field. */
  @media (hover: hover) {
    .card:not(.open)
      > .card-head:not(:hover, :focus-within, :has(.active, [aria-expanded='true']))
      > .controls {
      opacity: 0;
      pointer-events: none;
    }

    .card:not(.open, .if-card) > .card-head {
      position: relative;
    }

    /* The counts step aside for the controls, rather than sit under them. */
    .card:not(.open, .if-card)
      > .card-head:is(:hover, :focus-within, :has(.active, [aria-expanded='true']))
      > .counts {
      margin-right: 10.6rem;
    }

    .card:not(.open, .if-card) > .card-head > .controls {
      position: absolute;
      right: 0.7rem;
      padding-left: 1.5rem;
      background: linear-gradient(to right, transparent, var(--card-surface) 1.2rem);
    }
  }

  /* Every control in a step looks like a control, wherever it sits: the
     head's controls, and each row's delete, marked `.ctl`. Without this a row
     delete fell through to the browser's own button chrome wearing an error
     color. */
  .controls button,
  .ctl {
    background: none;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    color: var(--dim);
    font-size: var(--text-note);
    cursor: pointer;
    /* A fixed box, centered on its glyph, rather than padding around
       whatever width that glyph happens to render at: "◎︎" and "●︎" are
       not the same width in the font, and sizing to content made the
       button -- and everything after it in the row -- shift when this one
       toggled. */
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: var(--control-width);
    height: var(--control-height);
    padding: 0;
  }

  .controls button.active {
    border-color: var(--ink);
    color: var(--ink);
    font-weight: 600;
  }

  .remove {
    color: var(--error);
    border-color: var(--error);
  }

  /* A tinted band, not a card-filling wash: it reads as "this step says
     something" without drowning the badge and summary above it in color. */
  /* Inset in the card's own padding, so it lines up with the fields above. */
  .step-errors {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    margin: 0 0.7rem 0.7rem;
    padding: 0.55rem 0.7rem;
    background: color-mix(in oklab, var(--error) 12%, var(--panel));
    border-radius: var(--radius);
  }

  .step-error-msg {
    display: flex;
    gap: 0.5rem;
    align-items: flex-start;
    font-size: var(--text-note);
    color: var(--error);
    line-height: 1.45;
  }

  .step-error-msg .error-glyph {
    margin-top: 0.1rem;
  }

  .step-errors.warn {
    background: color-mix(in oklab, var(--warn) 12%, var(--panel));
  }

  .step-error-msg.warn,
  .error-glyph.warn {
    color: var(--warn);
  }

  .body {
    padding: 0 0.7rem 0.7rem;
    display: flex;
    flex-direction: column;
    gap: 0.55rem;
  }

  label {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    font-size: var(--text-label);
    color: var(--dim);
  }

  .inline {
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 0.4rem;
  }

  /* The label register: the field's name, and the `?` that belongs to it. */
  .field-name {
    display: flex;
    align-items: center;
    font-weight: 600;
  }

  /* The note register: one line stating what a field takes. Weight 400 --
     a sentence, not a second label -- and dim, since a note may stand
     outside a dim-colored label (a rows-label's own caption). */
  .note {
    font-size: var(--text-note);
    font-weight: 400;
    color: var(--dim);
    line-height: 1.4;
  }

  input[type='text'],
  select,
  textarea {
    font-family: var(--mono);
  }

  textarea {
    resize: vertical;
  }

  select.field-error {
    border-color: var(--error);
    color: var(--error);
  }

  .kind-fields {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    gap: 0.6rem;
  }

  /* A `.glide-row` is a declared field and what sits under it, one box, so they glide together when a column
     moves; it spaces them as the rows are spaced. */
  .rows,
  .glide-row {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }

  .rows-label {
    font-size: var(--text-note);
    color: var(--dim);
  }

  .row-pair {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }

  /* Free to shrink, so a row fits the card an if step's branch gives it. */
  .row-pair input {
    flex: 1;
    min-width: 0;
  }

  /* An assignment's target is almost always shorter than its expression, so
     it stops growing at 220px of a wide row and the expression (flex: 1 via
     ValidatedField's .field-wrap) takes the rest. A narrow row splits evenly:
     a 220px basis would leave the expression nothing. */
  .row-pair .target-field {
    display: flex;
    flex: 1 1 0;
    max-width: 220px;
    min-width: 0;
  }

  /* A grok field's name follows the pattern, so it is shown rather than typed. */
  .derived-name {
    flex: 1;
    font-family: var(--mono);
    font-size: 0.8rem;
    padding: 0.3rem 0.4rem;
    border: 1px dashed var(--border);
    border-radius: var(--radius);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .definition {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
  }

  .definition summary {
    cursor: pointer;
    padding: 0.3rem 0.45rem;
    display: flex;
    gap: 0.45rem;
    align-items: baseline;
  }

  .def-name {
    font-family: var(--mono);
    font-size: 0.78rem;
  }

  .def-brings {
    color: var(--dim);
    font-size: 0.72rem;
  }

  .definition-body {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    padding: 0 0.45rem 0.45rem;
  }

  .output-header,
  .output-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(0, max-content) minmax(0, 1fr);
    align-items: center;
    gap: 0.5rem;
  }

  /* A skipped column keeps its row, dimmed, so setting a type brings it back where it was. */
  .output-row.skipped .output-col-name,
  .output-row.skipped input {
    opacity: 0.55;
  }

  /* Body face, though it sits inside the mono column name: it labels the
     value, it is not part of it. */
  .key-tag {
    margin-left: 0.35rem;
    font-family: var(--font-body);
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  /* A datetime's format folds out under its row, so the grid keeps its columns. */
  .fmt-row {
    grid-column: 2 / -1;
  }

  .output-header {
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .output-row input[type='text'] {
    width: 100%;
    box-sizing: border-box;
  }

  /* Its column can be narrower than the name, so a long one wraps in place. */
  .output-col-name {
    font-family: var(--mono);
    font-size: var(--text-label);
    overflow-wrap: anywhere;
  }

  .eq {
    color: var(--dim);
    font-size: var(--text-label);
  }

  .adds {
    display: flex;
    gap: 0.5rem;
  }

  .add-small.on {
    background: var(--panel);
  }

  /* Dashed in the accent: what is here is offered, not yet declared. */
  .suggestions {
    border: 1px dashed var(--accent);
    border-radius: var(--radius-card);
    padding: 0.5rem;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    background: color-mix(in oklab, var(--accent) 4%, var(--field));
  }

  .suggestions-head {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: var(--text-note);
    color: var(--dim);
  }

  .suggestions-source {
    flex: 1;
    min-width: 0;
  }

  .suggestions-source strong {
    color: var(--ink);
    font-weight: 600;
  }

  .suggestions-head button {
    white-space: nowrap;
  }

  .suggestion {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }

  .suggestion-fixed {
    display: flex;
    gap: 0.6rem;
    align-items: baseline;
    font-size: var(--text-note);
  }

  .suggestion-note {
    margin: -0.15rem 0 0.1rem 0.2rem;
    font-size: var(--text-note);
    color: var(--dim);
  }

  .suggestion-note.warn {
    color: var(--warn);
  }

  .add-small {
    align-self: flex-start;
    background: transparent;
    color: var(--ink);
    border: 1px dashed var(--dim);
    border-radius: var(--radius);
    padding: 0.2rem 0.55rem;
    font-size: var(--text-note);
    cursor: pointer;
  }

  /* THEN above ELSE, until the if step is wide enough to hold them side by
     side with about 16.5rem apiece: room for a step whose summary still
     reads. `minmax(0, …)` because a plain `1fr` will not shrink below its
     widest step, and it counted that step's summary at full length. */
  .branches {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 0.6rem;
    padding: 0 0.5rem 0.5rem;
  }

  @container step (min-width: 36rem) {
    .branches {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }

  .branch {
    min-width: 0;
    border-left: 2px solid var(--border);
    padding-left: 0.5rem;
  }

  .branch-label {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    margin-bottom: 0.35rem;
  }
</style>
