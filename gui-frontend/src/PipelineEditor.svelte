<!--
  One pipeline's builder: loaded from `GET /api/pipelines/:id` (blank for
  `id === 'new'`), run in-process against a saved sample through
  `/api/preview`, and saved with `PUT /api/pipelines/:id`, which the
  backend validates against the whole bundle.

  The records come from a saved sample, so what a preview ran over has a
  name and is in the branch's history; the sample menu also saves a
  capture or pasted records as one. The editor opens on the sample named in
  the URL, else on the one this pipeline last ran over in this browser.
-->
<script lang="ts">
  import InlineError from './InlineError.svelte';
  import { untrack } from 'svelte';
  import StepList from './StepList.svelte';
  import Connections from './Connections.svelte';
  import SectionShell from './SectionShell.svelte';
  import JsonEditPage from './JsonEditPage.svelte';
  import Toggle from './Toggle.svelte';
  import SaveBar from './SaveBar.svelte';
  import {
    buildFileConfig,
    stepToDto,
    stepsFromDto,
    parsePipelineDtoJson,
    errorsByStep,
    orderedStepErrors,
    unknownLookupTables,
    truncateSteps,
    flatSteps,
    STEP_LABELS,
    stepsRun,
    parseInputFor,
    stepSummary,
    diffSteps,
    type Step,
    type StepDiffRow,
    declareProposed,
    PROPOSED_FORMAT_NAMES,
  } from './pipeline';
  import {
    productionOf,
    proposalRows,
    suggestionsOf,
    type FieldProposal,
    type OpenProposal,
  } from './proposal';
  import { SvelteMap, SvelteSet } from 'svelte/reactivity';
  import { fieldsSettled } from './grokFields';
  import { provideUnfinishedRows } from './unfinished.svelte';
  import RecordFields from './RecordFields.svelte';
  import SearchBox from './SearchBox.svelte';
  import RemovableChip from './RemovableChip.svelte';
  import { formatDiffValue } from './diff';
  import {
    countStates,
    declaredRecord,
    fieldAt,
    leafMap,
    pathsOf,
    splitPath,
    type Declared,
  } from './recordDiff';
  import {
    sharedConfig,
    getPipeline,
    putPipeline,
    getConfig,
    listBranches,
    configAt,
    undoChanges,
    currentBranch,
    currentScope,
    MAIN_BRANCH,
    listSamples,
    listTests,
    getSample,
    proposeOnboard,
    SHARED_SCOPE,
    previewFailure,
    previewPipeline,
    afterDot,
    type AskAt,
    errorText,
    type ApiError,
    errorMessage,
    type GrokPatternDto,
    type LookupTableDto,
    type PipelineDto,
    type Connected,
    type SampleRecordDto,
    type SampleSummary,
    type TestSummary,
    type SaveConflict,
    type PreviewRow,
    type PreviewResult,
    type ParserProposal,
    parsersIn,
    sampleSource,
  } from './api';
  import FieldsMenu from './FieldsMenu.svelte';
  import ChipPicker from './ChipPicker.svelte';
  import JumpLinks from './JumpLinks.svelte';
  import { setFunctionUsage } from './usage.svelte';
  import { ask } from './Confirm.svelte';
  import { errors, reported, unreported } from './errors.svelte';
  import { canonicalJson } from './objectChanges';
  import SaveConflictPanel from './SaveConflict.svelte';
  import HeldEdit from './HeldEdit.svelte';
  import { hold, verdictFor, drop } from './edits';
  import type { ChangeRow } from './diff';
  import AddSampleDialog from './AddSampleDialog.svelte';
  import SampleMenu from './SampleMenu.svelte';
  import PencilIcon from './PencilIcon.svelte';
  import { mintPipelineId } from './ids';
  import { announcePipelineNames, newPipelineHash, withPipeline, type Making } from './nav';
  import { ago } from './time';
  import { editable, mayCapture } from './access';
  import SortButton from './SortButton.svelte';
  import FoldAllButton from './FoldAllButton.svelte';
  import { TableSort } from './sort.svelte';

  let {
    id,
    showJson = false,
    sample = '',
    for: making,
    back,
  }: {
    id: string;
    showJson?: boolean;
    sample?: string;
    /** What a route being made takes: a new pipeline starts on a sample of it, with the steps proposed from it. */
    for?: Making;
    /** Where Save returns to, told the pipeline saved. */
    back?: string;
  } = $props();

  const readOnly = !editable();

  // The id this pipeline is saved under: empty until a new one's first
  // save, and distinct from the route param `id`, which may be `new`.
  let loadedId = $state('');
  let newId = $state('');
  let pipelineId = $derived(loadedId || newId);
  let pipelineName = $state('New pipeline');
  let steps: Step[] = $state([]);
  let loading = $state(true);
  let loadFailed = $state(false);

  // -- unsaved-edit protection ------------------------------------------
  //
  // `editSignature` is what's in the editor, name included; `savedSignature`
  // is what the backend last confirmed, set once on load and again on every
  // successful save. The gap between them (`isDirty`) drives the
  // leave-warning and the draft this pipeline is backed up under while that
  // gap is open. `signature` is the narrower half auto-run tracks: the steps
  // the preview is sent, so neither a rename nor typing in a row still being
  // written (`unfinished.svelte.ts`) re-runs it.
  function signatureOf(theSteps: Step[]): string {
    return JSON.stringify(theSteps.map(stepToDto));
  }
  function editSignatureOf(name: string, theSteps: Step[]): string {
    return name + '::' + signatureOf(theSteps);
  }
  const unfinished = provideUnfinishedRows();
  let signature = $derived(signatureOf(unfinished.hold(steps).steps));
  let editSignature = $derived(editSignatureOf(pipelineName, steps));
  let savedSignature = $state('');
  // What `savedSignature` names, kept whole (not just its signature) so
  // discarding an edit has something to revert to. Cloned in and out: `steps`
  // is mutated in place as the editor is used, and a shared reference would
  // let that mutate the very snapshot discarding is meant to fall back on.
  let savedPipelineName = $state('');
  let savedSteps: Step[] = $state([]);
  // `loading` guards the window between a route change and the fetch it
  // starts landing: `steps` still holds whatever the previous route left
  // there (or the "New pipeline" default) while `savedSignature` has not
  // yet been set for the id now in the URL, which would otherwise read as
  // dirty -- backward, and specifically for a pipeline nothing has
  // touched yet.
  let isDirty = $derived(!loading && editSignature !== savedSignature);
  // A brand new pipeline that nothing has touched yet is not dirty (its
  // edit and saved signatures already match), but it has never been
  // written either -- an empty pipeline is a real, useful pass-through
  // one, so Save has to reach it without an otherwise unnecessary edit.
  let neverSaved = $derived(!loading && id === 'new' && loadedId === '');

  /** The file this editor holds unsaved work for (`edits.ts`). Derived, because
   * this component is reused across a route change rather than remounted, so
   * the pipeline it is holding for moves under it. */
  let FILE = $derived(`pipelines/${id}`);
  /** The commit the loaded pipeline was read at, and what a save carries. Null
   * for a pipeline that does not exist yet: there is nothing to be stale
   * against, which is how a first write already behaves. */
  let readAtCommit: string | null = $state(null);
  /** Set when an edit held from an earlier visit was made against a different
   * pipeline than the one just read. */
  let moved: { value: string; rows: ChangeRow[] } | null = $state(null);

  /** What is held, in the two forms a record needs: the steps to put back,
   * and the signature to compare by, in the form `savedSignature` takes. */
  function snapshotFor(name: string, theSteps: Step[]) {
    return {
      value: JSON.stringify({ pipelineName: name, steps: theSteps }),
      signature: editSignatureOf(name, theSteps),
    };
  }

  /** A held draft. */
  type Draft = { pipelineName: string; steps: Step[] };

  /** What to do about an edit held from an earlier visit, now that this
   * editor's own read has landed. Restore says nothing: the pipeline is what
   * the edit was made against. Anything else leaves the pipeline on screen and
   * puts the panel up, so the reader chooses against what they can see. */
  function adoptHeld() {
    const verdict = verdictFor(FILE, savedSignature);
    moved = null;
    if (verdict.kind === 'none') return;
    const draft = JSON.parse(verdict.held.value) as Draft;
    if (verdict.kind === 'restore') {
      pipelineName = draft.pipelineName;
      steps = draft.steps;
      return;
    }
    const rows = diffSteps(steps, draft.steps);
    if (draft.pipelineName !== pipelineName) {
      rows.unshift({
        kind: 'changed',
        summary: `renamed from "${pipelineName}" to "${draft.pipelineName}"`,
      });
    }
    moved = { value: verdict.held.value, rows };
  }

  // The edit, held for the tab's life rather than this component's. This is the
  // only thing that writes the record while the id stays put, so there is no
  // list of paths that must remember to clear it: a clean editor holds nothing.
  // A new pipeline's first save is the one exception: the key moves from `new`
  // to the id that save wrote, so that save drops the record it saved under by
  // hand. The label is the name box, since the key holds an id.
  $effect(() => {
    if (loading || moved) return;
    hold(FILE, {
      ...snapshotFor(pipelineName, steps),
      baseline: savedSignature,
      readAtCommit,
      label: pipelineName,
    });
  });

  // Every other saved pipeline in this scope, and its lookup tables: what a
  // preview sends.
  let ownPipelines: PipelineDto[] = $state([]);
  let ownLookupTables: LookupTableDto[] = $state([]);
  let ownGrokPatterns: GrokPatternDto[] = $state([]);
  // A group runs with `shared/` merged in, so a `call` step may target a
  // shared pipeline and a lookup step read a shared table. The leader merges
  // them into a group's preview itself, so they are offered, never sent.
  let sharedPipelines: PipelineDto[] = $state([]);
  let sharedLookupTables: LookupTableDto[] = $state([]);
  let sharedGrokPatterns: GrokPatternDto[] = $state([]);
  // What a `call` step may target (a self-call is a cycle).
  let otherPipelines = $derived([...sharedPipelines, ...ownPipelines]);
  let pipelineIds = $derived(otherPipelines.map((p) => p.id));
  let pipelineNames = $derived(Object.fromEntries(otherPipelines.map((p) => [p.id, p.name])));
  let lookupTables = $derived([...sharedLookupTables, ...ownLookupTables]);
  let grokPatterns = $derived([...sharedGrokPatterns, ...ownGrokPatterns]);
  let configLoaded = $state(false);

  // The trail names a pipeline by what was last saved, here and on a locked
  // test reached from here (`nav.ts`, `trail`).
  $effect(() => {
    if (loading) return;
    announcePipelineNames({ ...pipelineNames, [id]: savedPipelineName });
  });
  // Read on the same branch as the scope's own, so the two halves are one
  // branch's view.
  Promise.all([getConfig(), sharedConfig()]).then(([res, shared]) => {
    if (res.config) {
      ownPipelines = res.config.pipelines.filter((p) => p.id !== id);
      ownLookupTables = res.config.lookupTables;
      ownGrokPatterns = res.config.grokPatterns ?? [];
    }
    if (shared) {
      sharedPipelines = shared.pipelines;
      sharedLookupTables = shared.lookupTables;
      sharedGrokPatterns = shared.grokPatterns ?? [];
    }
    configLoaded = true;
  });

  // Enforced only once the tables have loaded: before that every
  // reference would read as unknown.
  let badLookupTables = $derived(
    configLoaded
      ? unknownLookupTables(
          steps,
          lookupTables.map((t) => t.id),
        )
      : [],
  );

  // Autocomplete ranks the functions this bundle calls a little higher.
  $effect(() => setFunctionUsage(otherPipelines));

  /** By value: each hash makes `making` afresh, and the switch to raw JSON and back must not start the pipeline over. */
  const madeFor = $derived(making?.source);

  $effect(() => {
    if (id === 'new') {
      loadedId = '';
      newId = mintPipelineId();
      result = lastGoodResult = null;
      pipelineName = madeFor ? `${madeFor}-parse` : 'New pipeline';
      steps = [];
      loading = false;
      loadFailed = false;
      // Literals, not a read-back of `pipelineName`/`steps`: reading them
      // here, right after this same effect just wrote them, would make the
      // effect depend on its own output and re-run itself indefinitely.
      savedSignature = editSignatureOf('New pipeline', []);
      savedPipelineName = 'New pipeline';
      savedSteps = [];
      readAtCommit = null;
      // Outside tracking too, for the same reason -- its own reads of
      // `steps`/`pipelineName`/`editSignature` would otherwise attach to
      // this effect as well.
      untrack(() => adoptHeld());
      return;
    }
    // The URL just caught up with a save. The state in hand is newer
    // than a refetch, and a Loading flash would remount JsonEditPage
    // before its "Saved." shows.
    if (id === loadedId) {
      loading = false;
      loadFailed = false;
      return;
    }
    loading = true;
    loadFailed = false;
    result = lastGoodResult = null;
    getPipeline(id)
      .then((res) => {
        if (res.ok && res.pipeline) {
          const loadedSteps = stepsFromDto(res.pipeline.steps);
          loadedId = res.pipeline.id;
          pipelineName = res.pipeline.name;
          steps = loadedSteps;
          savedSignature = editSignatureOf(res.pipeline.name, loadedSteps);
          savedPipelineName = res.pipeline.name;
          savedSteps = $state.snapshot(loadedSteps);
          readAtCommit = res.commit ?? null;
          untrack(() => adoptHeld());
        } else {
          loadFailed = true;
        }
      })
      .catch((e) => {
        loadFailed = true;
      })
      .finally(() => {
        loading = false;
      });
  });

  // -- changed on this branch -------------------------------------------
  //
  // The pipeline as read against the pipeline where the branch left `main`.
  // Both sides are read through the same endpoint, so they compare equal
  // whenever the pipeline is.

  const editingBranch = currentBranch();
  /** The commit the branch left `main` at; null on `main` and until read. */
  let branchBase = $state<string | null>(null);
  if (editingBranch !== MAIN_BRANCH) {
    listBranches()
      .then((res) => {
        branchBase = res.branches.find((b) => b.name === editingBranch)?.base ?? null;
      })
      .catch(() => {
        // Without the base there is nothing to compare against, and no badge.
      });
  }
  let changedOnBranch = $state(false);
  /** Whether the pipeline was there when the branch left `main`; undoing a
   * pipeline that was not removes it. */
  let existedAtBase = $state(true);
  let undoBusy = $state(false);
  let undoError = $state('');
  let compares = 0;

  $effect(() => {
    const base = branchBase;
    const at = readAtCommit;
    const pipelineId = loadedId;
    const mine = ++compares;
    if (!base || !at || !pipelineId || loading) {
      changedOnBranch = false;
      return;
    }
    const scope = currentScope();
    Promise.all([configAt(editingBranch, scope, base), configAt(editingBranch, scope, at)])
      .then(([was, now]) => {
        if (mine !== compares) return;
        const before = was.config?.pipelines.find((p) => p.id === pipelineId);
        const after = now.config?.pipelines.find((p) => p.id === pipelineId);
        existedAtBase = before !== undefined;
        changedOnBranch = canonicalJson(before ?? null) !== canonicalJson(after ?? null);
      })
      .catch(() => {
        if (mine === compares) changedOnBranch = false;
      });
  });

  async function undoPipeline() {
    if (!readAtCommit) return;
    const ok = await ask({
      title: 'undo changes',
      body: `Undo the changes to ${savedPipelineName}, as a new save on ${editingBranch}.`,
      action: 'Undo changes',
      danger: true,
    });
    if (!ok) return;
    undoBusy = true;
    undoError = '';
    try {
      const res = await undoChanges(
        editingBranch,
        currentScope(),
        { section: 'pipelines', id: loadedId },
        readAtCommit,
      );
      if (!res.ok) {
        undoError =
          errorText(res.errors) ||
          (res.conflict ? 'The branch moved since this pipeline was read.' : 'Not undone.');
      } else if (!existedAtBase) {
        location.hash = '#/build/pipelines';
      } else {
        // Clearing what was loaded is what makes the loader read it again.
        loadedId = '';
      }
    } catch (e) {
      undoError = unreported(e);
    } finally {
      undoBusy = false;
    }
  }

  let saving = $state(false);
  /** Save was pressed with no name: refused here, before the leader is asked. */
  let unnamed = $state(false);
  let saveMessage = $state('');
  let conflict = $state<SaveConflict | null>(null);

  /** A refused save's errors, kept whole rather than flattened, so the ones
   * naming a step can be shown on it. */
  let saveErrors = $state<ApiError[]>([]);

  /** The ids an error might name this pipeline by: the one a save writes, and
   * the route's while that is a real id. Never the name, which another
   * pipeline -- an inlined callee among them -- may share. */
  let myIds = $derived([pipelineId, id === 'new' ? '' : id].filter((v) => v !== ''));

  /** A save error belongs to a step only when it names this pipeline and a
   * step this pipeline actually has. An error from an inlined callee names
   * one it does not, and stays in the message above the editor rather than
   * landing on whichever step sits nearby. */
  function onAStep(e: ApiError): boolean {
    return (
      e.step !== null &&
      e.pipeline !== null &&
      myIds.includes(e.pipeline) &&
      errorsByStep(steps, [e]).size > 0
    );
  }

  let saveErrorsByStep = $derived(errorsByStep(steps, saveErrors.filter(onAStep)));

  /** A save that fails on steps scrolls to the first one rather than leaving
   * the author to find it: the same `scrollIntoView` idiom Sources and
   * Destinations already use for a URL-driven focus. An effect, not a direct
   * call from `save()` -- the target step may have just switched from
   * collapsed to expanded in this same update, and the DOM has to catch up
   * first for the scroll to land in the right place. */
  let jumpToStepId = $state<string | null>(null);
  $effect(() => {
    // A step's id is a string like any other and a hand-edited pipeline can
    // legally give one "" -- null, not falsy, is what "nothing to jump to" means.
    if (jumpToStepId === null) return;
    document
      .getElementById(`step-${jumpToStepId}`)
      ?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    jumpToStepId = null;
  });

  /** Reverts to what was last loaded or saved, dropping every edit since --
   * the save bar's own way out of "unsaved", short of a page refresh. */
  function discard() {
    pipelineName = savedPipelineName;
    steps = $state.snapshot(savedSteps);
    moved = null;
  }

  /** Save under the pipeline's stable ID, preserving callers through a rename. */
  async function save(): Promise<boolean> {
    // A slow save can outlive the page it was for: this component is reused
    // across a route change rather than remounted, so `steps` may belong to
    // a different pipeline by the time the response lands. Jumping then
    // would send the reader wherever a step happens to share that id in
    // whatever is now on screen -- captured here to tell the two apart.
    const savingId = id;
    if (pipelineName.trim() === '') {
      unnamed = true;
      return false;
    }
    if (badLookupTables.length > 0) {
      return false;
    }
    // A grok step's fields follow its pattern, and that answer is asynchronous: serializing between the edit and the
    // answer would send a `declared` list the pattern disagrees with, which the compiler refuses.
    await fieldsSettled();
    saving = true;
    saveMessage = '';
    saveErrors = [];
    conflict = null;
    try {
      // Snapshotted here, not re-read after the request resolves: an edit
      // made while the request is in flight must stay unsaved (it was not
      // in the body just sent), and re-reading the live `pipelineName`/
      // `steps` on success would instead mark it saved and clear the draft
      // protecting it.
      const submittedName = pipelineName;
      const submittedSteps = $state.snapshot(steps);
      const targetId = pipelineId;
      const res = await putPipeline(targetId, {
        id: targetId,
        name: submittedName,
        steps: submittedSteps.map(stepToDto),
      });
      if (res.ok) {
        loadedId = targetId;
        saveMessage = 'Saved.';
        savedSignature = editSignatureOf(submittedName, submittedSteps);
        savedPipelineName = submittedName;
        savedSteps = $state.snapshot(submittedSteps);
        readAtCommit = res.commit ?? null;
        // Clear the draft under the URL this save was submitted from,
        // including `#/build/pipelines/new` on a new pipeline's first save.
        drop(`pipelines/${savingId}`);
        if (back) location.hash = withPipeline(back, targetId);
        else if (id !== targetId) {
          location.hash = `#/build/pipelines/${encodeURIComponent(targetId)}${showJson ? '/json' : ''}`;
        }
        return true;
      }
      if (res.conflict) {
        // Its own panel, which offers the retry this dialog could not.
        conflict = res.conflict;
        errors.shown(res.errors);
      } else {
        saveErrors = res.errors;
        const broken = id === savingId ? orderedStepErrors(steps, saveErrorsByStep) : [];
        if (broken.length > 0) jumpToStepId = broken[0].step.id;
        // The bar lists what no step takes, and links to each step that holds the rest.
        errors.claim(
          res.errors,
          () =>
            orderedStepErrors(steps, saveErrorsByStep).flatMap(({ step, messages }) =>
              messages.map((message) => ({
                where: stepSummary(step),
                message,
                go: () => (jumpToStepId = step.id),
              })),
            ),
          res.errors.filter((e) => !onAStep(e)),
        );
      }
      return false;
    } catch {
      // apiJson reported it to the error bar.
      return false;
    } finally {
      saving = false;
    }
  }

  async function saveFromJson(text: string): Promise<{ ok: boolean; errors: string }> {
    let parsed: { pipelineName: string; steps: Step[] };
    try {
      parsed = parsePipelineDtoJson(text);
    } catch (e) {
      return { ok: false, errors: errorMessage(e) };
    }
    const targetId = pipelineId;
    const res = await putPipeline(targetId, {
      id: targetId,
      name: parsed.pipelineName,
      steps: parsed.steps.map(stepToDto),
    });
    if (res.ok) {
      pipelineName = parsed.pipelineName;
      steps = parsed.steps;
      loadedId = targetId;
      savedSignature = editSignatureOf(parsed.pipelineName, parsed.steps);
      savedPipelineName = parsed.pipelineName;
      savedSteps = $state.snapshot(parsed.steps);
      readAtCommit = res.commit ?? null;
      drop(`pipelines/${id}`);
      if (back) location.hash = withPipeline(back, targetId);
      else if (id !== targetId) {
        location.hash = `#/build/pipelines/${encodeURIComponent(targetId)}/json`;
      }
      return { ok: true, errors: '' };
    }
    // The leader's refusal is the error bar's to say.
    return { ok: false, errors: '' };
  }

  function jsonDraft(): string {
    return JSON.stringify(
      { id: pipelineId, name: pipelineName, steps: steps.map(stepToDto) },
      null,
      2,
    );
  }

  // The library to choose from, the choice, and that sample's records --
  // held so a row's "before" can show the record it started as without
  // asking the preview endpoint to echo its own input back.
  let samples: SampleSummary[] = $state([]);
  let selectedSample = $state('');
  let sampleRows: SampleRecordDto[] = $state([]);
  let capturing = $state(false);
  /** The dialog is open on its catalog, for a sample pasted, uploaded or captured. */
  let adding = $state(false);

  let result = $state<PreviewResult | null>(null);
  // The grid's own state: the last run that actually produced rows, kept
  // on screen through a failed one so a syntax slip mid-edit does not take
  // the sample away from under whoever is fixing it. `result` still drives
  // the error itself -- this only decides what the grid below it shows.
  let lastGoodResult = $state<PreviewResult | null>(null);
  let running = $state(false);
  let autoRun = $state(true);
  let requestError = $state('');
  /** The sample the rows on screen came from. */
  let lastSample = $state('');

  async function loadSamples() {
    try {
      const res = await listSamples();
      samples = res.samples;
      // `sample` came in from the URL (a sample row's "Run in a pipeline"
      // link, or a pipeline opened from the list while it held one). With
      // none, or a stale one not on this branch, the editor opens on the
      // last one this pipeline ran over, else on none: which records to run is
      // the author's to choose.
      lastUsed = readLastUsed();
      const opening = sample || lastUsed;
      if (!selectedSample && opening && samples.some((s) => s.id === opening)) {
        await chooseSample(opening);
      }
    } catch (e) {
      reported(e);
    }
  }
  // Samples and what a preview shows are production records, an editor's to read: a viewer is
  // shown the steps alone.
  if (!readOnly) loadSamples();

  // The tests of this pipeline, for the samples they lock. A failure only
  // leaves the padlocks off.
  let tests: TestSummary[] = $state([]);
  if (!readOnly)
    listTests()
      .then((res) => (tests = res.tests))
      .catch(() => {});
  let lockedBy = $derived.by(() => {
    const by = new Map<string, string[]>();
    for (const t of tests)
      if (pipelineId && t.pipelines.includes(pipelineId))
        by.set(t.sample, [...(by.get(t.sample) ?? []), t.name]);
    return by;
  });

  async function captured(id: string) {
    capturing = false;
    adding = false;
    captureAt = null;
    await loadSamples();
    await chooseSample(id);
    if (making) await proposeFrom(sampleRows);
  }

  // The sample a pipeline last ran over, remembered per pipeline in this browser: the editor
  // opens on it, and the sample menu lists it first.
  const sampleKey = (pipeline: string) => `smeltr-workspace-sample:${pipeline}`;
  let lastUsed = $state('');
  function readLastUsed(): string {
    try {
      return window.localStorage.getItem(sampleKey(pipelineId)) ?? '';
    } catch {
      return '';
    }
  }

  async function chooseSample(id: string) {
    const pipeline = untrack(() => pipelineId);
    selectedSample = id;
    if (id) {
      lastUsed = id;
      try {
        window.localStorage.setItem(sampleKey(pipeline), id);
      } catch {
        // Storage refused: the editor opens on no sample next time.
      }
    }
    sampleRows = [];
    // The rows on screen were the last sample's.
    lastGoodResult = null;
    lastSample = '';
    outputQuery = '';
    chips = [];
    if (!id) return;
    try {
      const res = await getSample(id);
      if (res.sample) sampleRows = res.sample.records;
    } catch (e) {
      reported(e);
    }
  }

  // 'load' fails before compilation ever runs -- a blank assignment, say --
  // and can name a step just as a compile error can; 'sample' cannot (there
  // is no pipeline in it to have one), so it is left out.
  let compileErrorsByStep = $derived(
    result && !result.ok && (result.stage === 'compile' || result.stage === 'load')
      ? errorsByStep(steps, result.errors)
      : new Map(),
  );
  let warningsByStep = $derived(
    result?.ok && result.warnings ? errorsByStep(steps, result.warnings) : new Map(),
  );
  // A refused save's errors stay on the steps until the next save, over the preview's.
  let stepListErrors = $derived(saveErrors.length > 0 ? saveErrorsByStep : compileErrorsByStep);
  /** The steps showing the preview's errors, which the box above the sample jumps to. */
  let brokenSteps = $derived(
    stepListErrors === compileErrorsByStep ? orderedStepErrors(steps, compileErrorsByStep) : [],
  );
  /** The preview's errors no step shows. */
  let unplacedErrors = $derived.by(() => {
    if (!result || result.ok) return [];
    const placed = new Set([...stepListErrors.values()].flat());
    return result.errors.filter((e) => !placed.has(e));
  });

  /** One `/api/preview` call over `stepsSubset`: the whole tree for a
   * Run, a truncated one for each side of a per-step diff. Goes through
   * `previewPipeline`, like every other config call, so it carries the
   * branch being edited. The rows still being written stay out of it.
   *
   * `atMicros` pins `now()`, which matters for the two sides of a diff:
   * without it each side reads its own clock, and a `parseTime` given no
   * reference of its own -- a yearless syslog stamp takes its year from
   * `now()` -- can land on a different instant either side and be drawn as
   * a change the step never made. */
  async function fetchPreview(
    stepsSubset: Step[],
    sample: string,
    atMicros?: number,
    filter?: string,
  ): Promise<PreviewResult> {
    const held = unfinished.hold(stepsSubset);
    const config = JSON.stringify(
      buildFileConfig(
        pipelineId,
        pipelineName,
        held.steps,
        ownPipelines,
        ownLookupTables,
        ownGrokPatterns,
      ),
    );
    const res = await previewPipeline({
      config,
      pipeline: pipelineId,
      sample: sample || undefined,
      // Always: whether an internal field is shown is the Columns choice.
      showInternal: true,
      atMicros,
      filter,
    });
    return { ...res, errors: res.errors.map(held.restore) };
  }

  /** Asks the engine what may follow a dot in a step's expression, over the steps as they are being written and the
   * sample's columns. */
  const askAt: AskAt = (at) => (text, dot) =>
    afterDot({
      config: JSON.stringify(
        buildFileConfig(
          pipelineId,
          pipelineName,
          unfinished.hold(steps).steps,
          ownPipelines,
          ownLookupTables,
          ownGrokPatterns,
        ),
      ),
      pipeline: pipelineId,
      sample: lastSample || undefined,
      at,
      text,
      dot,
    });

  async function run() {
    running = true;
    requestError = '';
    const sample = selectedSample;
    try {
      const res = await fetchPreview(steps, sample);
      // A run of a sample put away while it ran answers for nothing on screen.
      if (sample !== selectedSample) return;
      result = res;
      if (res.ok) lastGoodResult = res;
      lastSample = sample;
    } catch (e) {
      requestError = unreported(e);
    } finally {
      running = false;
    }
  }

  // Locking a test needs a saved pipeline to attach it to, a sample to name,
  // and a run that actually produced the output being locked.
  let canLockTest = $derived(
    loadedId !== '' && selectedSample !== '' && result !== null && result.ok,
  );

  function goLockTest() {
    location.hash = `#/build/tests/lock/${encodeURIComponent(loadedId)}/${encodeURIComponent(selectedSample)}`;
  }

  // The output is not an action. It follows the steps and the sample, one
  // run behind the typing rather than one button behind it. Runs compile from
  // `steps` in memory, so nothing is saved to see them. With no sample a run
  // still compiles, for the steps' errors and the data dictionary.
  const RECOMPUTE_MS = 400;
  $effect(() => {
    const key = signature;
    void selectedSample;
    // A viewer is shown the steps alone.
    if (readOnly || !autoRun || key === '') return;
    const timer = setTimeout(() => void run(), RECOMPUTE_MS);
    return () => clearTimeout(timer);
  });

  // -- the divider ----------------------------------------------------------
  //
  // The steps column takes half the width when the work is the steps and is
  // narrow when it is inside one of them. Drag it, or double-click to snap
  // between the two. `null` is the half.

  const STEPS_NARROW = 240;
  const STEPS_MIN = 200;
  let stepsWidth = $state<number | null>(null);

  function drag(e: PointerEvent) {
    const bar = e.currentTarget as HTMLElement;
    const left = bar.parentElement?.getBoundingClientRect().left ?? 0;
    bar.setPointerCapture(e.pointerId);
    const move = (m: PointerEvent) => {
      stepsWidth = Math.max(STEPS_MIN, m.clientX - left);
    };
    const stop = () => {
      bar.removeEventListener('pointermove', move);
      bar.removeEventListener('pointerup', stop);
    };
    bar.addEventListener('pointermove', move);
    bar.addEventListener('pointerup', stop);
  }

  function snap(e: MouseEvent) {
    const half = ((e.currentTarget as HTMLElement).parentElement?.clientWidth ?? 0) / 2;
    stepsWidth = (stepsWidth ?? half) > (half + STEPS_NARROW) / 2 ? STEPS_NARROW : null;
  }

  // -- the pause ----------------------------------------------------------------
  //
  // The records stop at a step. ◎ on a step pauses them after it; a compile error pauses them
  // before the first broken step, where the pipeline still compiles. Paused, the grid shows the
  // pipeline truncated after that step and each record's fold shows what that one step did, each
  // side a real preview run (`truncateSteps`). Unpaused, it shows the whole pipeline against the
  // records as they arrived.

  // Every field, or only the ones this step touched. A mapping step rewrites
  // most of a record, so the filter is what makes a later step's handful of
  // changes findable without reading the whole thing.
  let changedOnly = $state(false);
  let showTypes = $state(true);

  /** The step ◎ paused at. */
  let selectedStep: Step | null = $state(null);
  /** The first step the preview's compile errors name. */
  let brokenStep = $derived(orderedStepErrors(steps, compileErrorsByStep)[0]?.step ?? null);

  type Pause = {
    /** The step the records stand after; null for the records as they arrived. */
    step: Step | null;
    /** Its place in the step list, from 1; 0 for the records as they arrived. */
    number: number;
    /** The steps that have run by then, which the step list leaves undimmed. */
    run: Set<Step>;
    /** The pipeline right before and right after `step`. */
    before: Step[];
    after: Step[];
    /** ◎ chose it, so ✕ clears it. A compile error's lasts as long as the error. */
    chosen: boolean;
  };

  /** ◎'s pause, unless the steps it runs do not compile: then the compile error's. */
  let pause = $derived.by((): Pause | null => {
    const flat = flatSteps(steps);
    const run = selectedStep && stepsRun(steps, selectedStep, true);
    if (selectedStep && run && !(brokenStep && run.has(brokenStep)))
      return {
        step: selectedStep,
        number: flat.indexOf(selectedStep) + 1,
        run,
        before: truncateSteps(steps, selectedStep, false)!,
        after: truncateSteps(steps, selectedStep, true)!,
        chosen: true,
      };
    if (!brokenStep) return null;
    const ran = stepsRun(steps, brokenStep, false)!;
    // The last step that ran: an `if` holding the broken step, rather than a branch skipped.
    let at: Step | null = null;
    for (const s of flat.slice(0, flat.indexOf(brokenStep))) if (ran.has(s)) at = s;
    return {
      step: at,
      number: at ? flat.indexOf(at) + 1 : 0,
      run: ran,
      before: at ? truncateSteps(steps, at, false)! : [],
      after: truncateSteps(steps, brokenStep, false)!,
      chosen: false,
    };
  });

  let pauseRun = $state<{
    before: PreviewRow[];
    after: PreviewRow[];
    declared: { before: Declared | null; after: Declared | null };
  } | null>(null);
  let pauseLoading = $state(false);
  let errorsOpen = $state(false);
  /** A failed stage as the error chip names it. */
  const stageName = (stage: string | null) =>
    stage ? stage[0].toUpperCase() + stage.slice(1) : 'Preview';
  let pauseError = $state('');

  function onSelectStep(step: Step | null) {
    selectedStep = step;
  }

  let pauseAsked = 0;
  async function loadPause() {
    pauseError = '';
    const asked = ++pauseAsked;
    if (!pause || !lastSample) {
      pauseRun = null;
      return;
    }
    const { before, after } = pause;
    pauseLoading = true;
    try {
      // One instant for both sides, so the only difference between them is
      // the step.
      const at = Date.now() * 1000;
      const [beforeRes, afterRes] = await Promise.all([
        fetchPreview(before, lastSample, at),
        fetchPreview(after, lastSample, at),
      ]);
      // A later pause answers for the screen.
      if (asked !== pauseAsked) return;
      const failed = !beforeRes.ok ? beforeRes : !afterRes.ok ? afterRes : null;
      if (failed) {
        pauseError = previewFailure(failed);
        pauseRun = null;
        return;
      }
      pauseRun = {
        before: beforeRes.rows,
        after: afterRes.rows,
        declared: { before: declaredOf(beforeRes), after: declaredOf(afterRes) },
      };
    } catch (e) {
      if (asked !== pauseAsked) return;
      pauseError = unreported(e);
      pauseRun = null;
    } finally {
      if (asked === pauseAsked) pauseLoading = false;
    }
  }

  // Tracks `pause` and `lastSample`: both read before the first `await`.
  $effect(() => {
    loadPause();
  });

  // -- a pipeline for a route being made ---------------------------------------
  //
  // It starts on a capture of what the route takes, and each capture made for it brings the steps `leader::onboard`
  // proposes from it.

  /** Where a capture for the route being made starts: its source, filtered to what it takes. */
  let captureTarget = $derived(
    making && {
      kind: 'source' as const,
      id: making.source,
      scope: currentScope(),
      group: making.group ?? (currentScope() === SHARED_SCOPE ? '' : currentScope()),
      ...(making.condition ? { filter: making.condition } : {}),
    },
  );
  /** A capture the sample menu started, at one of the pipeline's sources: in place of the route's. */
  let captureAt = $state<ReturnType<typeof sourceTarget> | null>(null);
  function sourceTarget(c: Connected) {
    return {
      kind: 'source' as const,
      id: c.id,
      scope: c.scope,
      group: c.scope === SHARED_SCOPE ? '' : c.scope,
    };
  }

  /** What the proposal read, said over the steps it made. */
  let proposed = $state('');
  /** Why no proposal was made. Its calls are quiet: this is said beside the samples. */
  let proposeError = $state('');

  async function proposeFrom(records: SampleRecordDto[]) {
    proposeError = '';
    try {
      const p = await proposeOnboard(records);
      // Each capture's proposal replaces the last one's, a proposal of no steps too.
      steps = stepsFromDto(p.steps ?? []);
      proposed = '';
      if (p.structured) {
        proposed = 'The captured records arrive as fields, so they need no parser.';
        return;
      }
      if (!steps.length) {
        if (p.of > 0) proposed = 'No format read these records, so they need a parser of your own.';
        return;
      }
      const parser = steps.find((s) => s.type === 'parser');
      // The parser declares what it read, as Propose would add it.
      const upTo =
        parser?.type === 'parser' && parser.declared.length === 0
          ? truncateSteps(steps, parser, true)
          : null;
      if (parser?.type === 'parser' && upTo) {
        const res = await fetchPreview(upTo, selectedSample);
        const read = res.ok ? res.parsers.at(-1) : undefined;
        if (read) declareProposed(parser.declared, read.fields);
      }
      if (p.format)
        proposed = `${p.read} of ${p.of} records read as ${PROPOSED_FORMAT_NAMES[p.format]}`;
    } catch (e) {
      proposeError = unreported(e);
    }
  }

  // svelte-ignore state_referenced_locally
  if (making && !readOnly && mayCapture(captureTarget)) capturing = true;

  // -- parser field proposal -------------------------------------------------
  //
  // What a parser step would declare, read off a fresh preview truncated to
  // just that step (`truncateSteps(..., true)`) -- the same request
  // `loadPause` makes for the "after" side of a pause. One
  // proposal open at a time; asking for a different step replaces it.

  let proposalStep: Step | null = $state(null);
  let proposal = $state<FieldProposal | null>(null);
  let selectedSampleName = $derived(
    samples.find((s) => s.id === selectedSample)?.name ?? selectedSample,
  );

  async function proposeFields(step: Step) {
    proposalStep = step;
    const upTo = truncateSteps(steps, step, true);
    if (upTo === null) {
      // The step is no longer in the tree: a stale request.
      proposal = null;
      return;
    }
    proposal = { loading: true, error: '', result: null, production: null, suggestions: [] };
    // What production saw is offered beside the sample, never in its way: a leader that cannot say shows the sample.
    const production = parsersIn(currentScope())
      .then((shapes) => productionOf(shapes, pipelineId, step.id))
      .catch(() => null);
    let error = '';
    let result: ParserProposal | null = null;
    if (selectedSample) {
      try {
        const res = await fetchPreview(upTo, selectedSample);
        if (res.ok) result = res.parsers.at(-1) ?? null;
        else error = previewFailure(res);
      } catch (e) {
        error = unreported(e);
      }
    }
    const seen = await production;
    if (proposalStep !== step) return; // superseded by a later request
    if (!selectedSample && !seen) error = 'choose a sample first';
    proposal = {
      loading: false,
      error,
      result,
      production: seen,
      suggestions: suggestionsOf(proposalRows(result?.fields ?? [], seen)),
    };
  }

  function closeProposal() {
    proposalStep = null;
    proposal = null;
  }

  let openProposal: OpenProposal | null = $derived(
    proposalStep && proposal
      ? { step: proposalStep, proposal, sampleName: selectedSampleName, onClose: closeProposal }
      : null,
  );

  // What the record's source set is shown on every row, read or not, so an
  // author sees what there is to read. It is never delivered, so the
  // pipeline's output does not carry it.
  function withSource(record: unknown, row: number): unknown {
    const source = sampleRows[row] && sampleSource(sampleRows[row]);
    if (!source || record === null || typeof record !== 'object') return record;
    return { __source: source, ...record };
  }

  /** The record type a preview compiled, or null where it did not. */
  function declaredOf(res: PreviewResult | null): Declared | null {
    const d = res?.dictionary?.compiled;
    return d ? declaredRecord(d.fields) : null;
  }

  // The sample's own types, as its source reads it before any step: what the
  // whole pipeline's "before" side declares.
  let sampleDeclared = $state<Declared | null>(null);

  $effect(() => {
    const sample = lastSample;
    sampleDeclared = null;
    if (!sample) return;
    untrack(() => fetchPreview([], sample)).then(
      (res) => {
        if (lastSample === sample) sampleDeclared = res.ok ? declaredOf(res) : null;
      },
      () => {},
    );
  });

  let runDeclared = $derived(declaredOf(lastGoodResult));

  /** The rows the grid shows: the pause's, once run, or else the whole pipeline's. */
  let gridRows = $derived(pause && pauseRun ? pauseRun.after : (lastGoodResult?.rows ?? null));

  // The pause's runs by row, so a row finds its sides without a search.
  let pausedByRow = $derived.by(() => {
    const d = pause ? pauseRun : null;
    if (!d) return null;
    const byRow = (rows: PreviewRow[]) => new Map(rows.map((r) => [r.row, r]));
    return { before: byRow(d.before), after: byRow(d.after) };
  });

  /** The record on either side of the step shown: the selected step, or the
   * whole pipeline from its sample row. */
  function rowSides(row: PreviewRow): {
    before: unknown;
    after: unknown;
    declared: { before: Declared | null; after: Declared | null };
  } {
    if (pausedByRow && pauseRun) {
      return {
        before: withSource(pausedByRow.before.get(row.row)?.json ?? null, row.row),
        after: withSource(pausedByRow.after.get(row.row)?.json ?? null, row.row),
        declared: pauseRun.declared,
      };
    }
    return {
      before: withSource(sampleRows[row.row] ?? {}, row.row),
      after: withSource(row.json, row.row),
      declared: { before: sampleDeclared, after: runDeclared },
    };
  }

  // Each row's leaves, both sides, by path: what a cell, the search and the
  // sort read, without diffing a row nobody is looking at. The sample's side
  // is read once per sample rather than once per run.
  let sampleMaps = $derived(sampleRows.map((r, i) => leafMap(withSource(r, i))));
  type RowMaps = { before: Map<string, unknown>; after: Map<string, unknown> };
  let rowsMaps: RowMaps[] = $derived(
    (gridRows ?? []).map((row) => {
      if (pausedByRow) {
        const sides = rowSides(row);
        return { before: leafMap(sides.before), after: leafMap(sides.after) };
      }
      return {
        before: sampleMaps[row.row] ?? leafMap(withSource({}, row.row)),
        after: leafMap(withSource(row.json, row.row)),
      };
    }),
  );
  let allColumns = $derived(pathsOf(rowsMaps.flatMap((m) => [m.before, m.after])));

  /** What a cell shows: what the field became, or what it was if the step took it away. */
  function cellValue(m: RowMaps, path: string): unknown {
    return m.after.has(path) ? m.after.get(path) : m.before.get(path);
  }

  // How many records hold each field, after the step shown.
  let fieldCounts = $derived.by(() => {
    const counts = new Map<string, number>();
    for (const m of rowsMaps)
      for (const p of m.after.keys()) counts.set(p, (counts.get(p) ?? 0) + 1);
    return counts;
  });

  /** The record the steps write, from the last run that compiled: with no sample, a run that
   * read no records. */
  let dictionary = $derived(
    (result?.ok ? result.dictionary : undefined) ?? lastGoodResult?.dictionary,
  );
  let declaredTypes = $derived(new Map(dictionary?.compiled.fields ?? []));

  /** Every field the records hold, and every field the pipeline declares that none of them
   * do, which the Fields menu lists as held by none. A declared struct a record fills is its
   * fields, not one of its own. */
  let menuFields = $derived.by(() => {
    const paths = new Set(allColumns);
    for (const [path] of dictionary?.compiled.fields ?? []) {
      if (path.includes('*')) continue;
      if (allColumns.some((c) => c.startsWith(path + '.') || c.startsWith(path + '['))) continue;
      paths.add(path);
    }
    return [...paths].sort().map((path) => ({
      path,
      type: declaredTypes.get(path) ?? null,
      count: fieldCounts.get(path) ?? 0,
    }));
  });

  // What the reader chose for each field they touched, and the default for the
  // rest: a step added along the way brings its fields with it, instead of
  // arriving invisible behind a choice made before they existed. The default is
  // the fields most records hold, up to a grid's worth, so it moves with the
  // step and sample shown; a choice does not. A field under a `__`
  // key is internal, smeltr's own and never delivered, so it starts hidden,
  // and an unfolded row shows it only while its column is shown. A root key
  // the path has to quote (`["__a.b"]`) counts as well.
  const DEFAULT_FIELDS = 50;
  const startsHidden = (path: string) => path.startsWith('__') || path.startsWith('["__');
  let shownByDefault = $derived(
    new Set(
      menuFields
        .filter((f) => !startsHidden(f.path))
        .sort((x, y) => y.count - x.count || (x.path < y.path ? -1 : 1))
        .slice(0, DEFAULT_FIELDS)
        .map((f) => f.path),
    ),
  );
  let chosenFields = $state(new SvelteMap<string, boolean>());
  const isShown = (path: string) => chosenFields.get(path) ?? shownByDefault.has(path);
  let columns = $derived(allColumns.filter(isShown));

  const unfoldedPath = (path: string) => !startsHidden(path) || isShown(path);

  function showFields(paths: string[], show: boolean) {
    for (const path of paths) chosenFields.set(path, show);
  }

  // The field choice is the reader's own, remembered per pipeline in this browser.
  const fieldsKey = (pipeline: string) => `smeltr-workspace-fields:${pipeline}`;
  $effect(() => {
    const key = fieldsKey(pipelineId);
    untrack(() => {
      chosenFields.clear();
      try {
        const saved: unknown = JSON.parse(window.localStorage.getItem(key) ?? '{}');
        if (saved && typeof saved === 'object' && !Array.isArray(saved))
          for (const [path, shown] of Object.entries(saved))
            if (typeof shown === 'boolean') chosenFields.set(path, shown);
      } catch {
        // Nothing usable stored: the default stands.
      }
    });
  });
  $effect(() => {
    const saved = JSON.stringify(Object.fromEntries(chosenFields));
    try {
      if (saved === '{}') window.localStorage.removeItem(untrack(() => fieldsKey(pipelineId)));
      else
        window.localStorage.setItem(
          untrack(() => fieldsKey(pipelineId)),
          saved,
        );
    } catch {
      // Storage refused: the choice lasts as long as the page.
    }
  });

  // A record is a line until asked for, and any number can be open at once:
  // comparing two is reading down the page rather than remembering the last.
  let expanded = $state(new SvelteSet<number>());

  function toggleRow(row: number) {
    if (expanded.has(row)) expanded.delete(row);
    else expanded.add(row);
  }

  // Every current row, rather than a count: `expanded` keeps the rows an
  // author opened across a re-preview, so after a smaller sample its size can
  // exceed the rows on screen and a count would leave `Fold all` unreachable.
  let allOpen = $derived(
    gridRows !== null && gridRows.length > 0 && gridRows.every((r) => expanded.has(r.row)),
  );

  function toggleAll() {
    if (allOpen) expanded.clear();
    else for (const row of gridRows ?? []) expanded.add(row.row);
  }

  // A sample opens on a record rather than a wall of lines: the first that
  // failed, which is what needs reading, or else the first. Once per sample,
  // so a re-run as the steps are typed leaves the reader's choice alone.
  let openedFor = '';
  $effect(() => {
    const rows = gridRows;
    const sample = lastSample;
    if (!rows || !sample || sample === openedFor) return;
    openedFor = sample;
    const first = rows.find((r) => r.exceptions.length > 0) ?? rows[0];
    if (first) untrack(() => expanded.add(first.row));
  });

  /** What the step did to a row's shown fields, for the head of its record. */
  function rowMarks(m: RowMaps) {
    return countStates(
      pathsOf([m.before, m.after])
        .filter(unfoldedPath)
        .map((path) => fieldAt(m.before, m.after, path)!),
    );
  }

  // -- the window -------------------------------------------------------------
  //
  // A thousand rows of five hundred cells is more than a page draws and stays
  // responsive, so the grid draws the rows near the viewport and a spacer
  // stands for the rest. A folded row is one line, as tall as the first one
  // drawn; an opened row's record is measured once it is drawn.

  const OVERSCAN = 20;
  let tbody = $state<HTMLElement | undefined>();
  let rowHeight = $state(24);
  let openHeights: Record<number, number> = $state({});
  /** How wide the grid shows, which an opened record wraps to however wide its columns run. */
  let paneWidth = $state(0);
  /** The viewport, from the top of the grid's body. */
  let viewport = $state({ top: 0, bottom: 1080 });

  function measure() {
    if (!tbody) return;
    const top = tbody.getBoundingClientRect().top;
    // What shows of the grid: its own box where it scrolls itself, the window where the page does.
    const box = tbody.closest('.grid-wrap')!.getBoundingClientRect();
    viewport = {
      top: Math.max(0, box.top) - top,
      bottom: Math.min(window.innerHeight, box.bottom) - top,
    };
    const first = tbody.querySelector<HTMLElement>('tr.grid-row');
    if (first && first.offsetHeight > 0) rowHeight = first.offsetHeight;
  }

  $effect(() => {
    // Again whenever the rows change, since what sits above the grid may have too.
    void gridOrder;
    if (!tbody) return;
    measure();
    // Captured, so the grid hears whichever ancestor it is that scrolls.
    window.addEventListener('scroll', measure, { capture: true, passive: true });
    window.addEventListener('resize', measure);
    return () => {
      window.removeEventListener('scroll', measure, { capture: true });
      window.removeEventListener('resize', measure);
    };
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

  let drawn = $derived.by(() => {
    const rows = gridRows ?? [];
    const n = gridOrder.length;
    const tops = new Array<number>(n + 1);
    tops[0] = 0;
    for (let k = 0; k < n; k++) {
      const row = rows[gridOrder[k]].row;
      tops[k + 1] = tops[k] + rowHeight + (expanded.has(row) ? (openHeights[row] ?? 0) : 0);
    }
    let first = 0;
    while (first < n && tops[first + 1] <= viewport.top) first++;
    let last = first;
    while (last < n && tops[last] < viewport.bottom) last++;
    const start = Math.max(0, first - OVERSCAN);
    const end = Math.min(n, last + OVERSCAN);
    return { start, end, above: tops[start], below: tops[n] - tops[end] };
  });

  // Widths fixed per column, from the values a column holds, so a column does
  // not jump as rows scroll past. Read off the first rows of the run rather
  // than all of them: enough to size by, and the same whichever are drawn.
  const COLUMN_CH = 28;
  const COLUMN_MIN_CH = 6;
  const SIZED_ROWS = 200;
  let columnWidths = $derived.by(() => {
    const sized = rowsMaps.slice(0, SIZED_ROWS);
    return columns.map((path) => {
      let w = COLUMN_MIN_CH;
      for (const m of sized) {
        if (w >= COLUMN_CH) break;
        if (m.after.has(path) || m.before.has(path))
          w = Math.max(w, formatDiffValue(cellValue(m, path)).length);
      }
      return Math.min(w, COLUMN_CH);
    });
  });
  /** The row number, with room for a failing row's mark. */
  let indexWidth = $derived(String(gridRows?.length ?? 0).length + 2);
  let gridWidth = $derived(
    `max(100%, calc(${columnWidths.reduce((a, b) => a + b, 2 + indexWidth)}ch + ${columns.length + 2}rem))`,
  );

  // Keyed apart from the field paths: a record may carry a field named `row`, but
  // no field has an empty path.
  const ROW_KEY = '';
  const gridSort = new TableSort<string>();
  // -- the find box ----------------------------------------------------------
  //
  // A keyword by default: a row stays when one of its shown cells holds the
  // text. Text with an operator or a call in it is tried as an expression over
  // the pipeline's output (`PreviewRequest.filter`). One that compiles to a
  // Bool filters the rows on the leader; anything else stays a keyword, so a
  // bare word like `slow` is searched for rather than read as a field. Chips
  // beside the text keep the rows that are failing, dropped, changed by a step
  // or hold a field, each on top of the text and of each other.

  type FindChip = { kind: 'failing' | 'dropped' | 'changed' } | { kind: 'has'; path: string };
  let chips = $state<FindChip[]>([]);
  const chipKey = (c: FindChip) => (c.kind === 'has' ? `has ${c.path}` : c.kind);

  const hasChip = (c: FindChip) => chips.some((x) => chipKey(x) === chipKey(c));

  function addChip(c: FindChip) {
    if (!hasChip(c)) chips = [...chips, c];
  }

  function toggleChip(c: FindChip) {
    if (hasChip(c)) chips = chips.filter((x) => chipKey(x) !== chipKey(c));
    else chips = [...chips, c];
  }

  // Whether a step changed a row, kept per run: it diffs every field of the
  // row, so it is asked only of rows a chip or a count needs.
  let changedRows = new Map<number, boolean>();
  let changedFor: typeof rowsMaps | undefined;
  function rowChanged(i: number): boolean {
    if (changedFor !== rowsMaps) {
      changedRows = new Map();
      changedFor = rowsMaps;
    }
    let known = changedRows.get(i);
    if (known === undefined) {
      const m = rowsMaps[i];
      known = pathsOf([m.before, m.after]).some(
        (p) => fieldAt(m.before, m.after, p)!.state !== 'unchanged',
      );
      changedRows.set(i, known);
    }
    return known;
  }

  function chipKeeps(c: FindChip, i: number): boolean {
    if (c.kind === 'has') return rowsMaps[i].after.has(c.path);
    const row = gridRows![i];
    if (c.kind === 'failing') return row.exceptions.length > 0;
    if (c.kind === 'dropped') return row.json === null;
    return rowChanged(i);
  }

  let failingCount = $derived((gridRows ?? []).filter((r) => r.exceptions.length > 0).length);

  const CHIP_LABELS = {
    failing: '⚠ Failing',
    dropped: '⌫ Dropped',
    changed: '~ Changed by a step',
  } as const;

  // What the find box offers: the kinds of row a chip keeps, with how many there are, and the
  // expressions this pipeline was last filtered by, each narrowed to what is typed.
  type FindItem =
    | { kind: keyof typeof CHIP_LABELS; count: number; section?: undefined }
    | { kind: 'recent'; expression: string; section: string };
  let findItems = $derived.by((): FindItem[] => {
    const rows = gridRows ?? [];
    const q = outputQuery.trim().toLowerCase();
    const counts = {
      failing: () => failingCount,
      dropped: () => rows.filter((r) => r.json === null).length,
      changed: () => rows.filter((_, i) => rowChanged(i)).length,
    };
    const kinds = (Object.keys(CHIP_LABELS) as (keyof typeof CHIP_LABELS)[])
      .filter((kind) => !chips.some((c) => c.kind === kind))
      .filter((kind) => CHIP_LABELS[kind].toLowerCase().includes(q))
      .map((kind) => ({ kind, count: counts[kind]() }))
      .filter((k) => k.count > 0);
    const recent = recentExpressions
      .filter((e) => e !== outputQuery.trim() && e.toLowerCase().includes(q))
      .map((expression) => ({ kind: 'recent' as const, expression, section: 'Recent' }));
    return [...kinds, ...recent];
  });

  function pickFind(item: FindItem): boolean {
    if (item.kind === 'recent') outputQuery = item.expression;
    else addChip({ kind: item.kind });
    return true;
  }

  const RECENT_EXPRESSIONS = 5;
  const expressionsKey = (pipeline: string) => `smeltr-workspace-expressions:${pipeline}`;
  let recentExpressions = $state<string[]>([]);
  $effect(() => {
    const key = expressionsKey(pipelineId);
    untrack(() => {
      try {
        recentExpressions = JSON.parse(window.localStorage.getItem(key) ?? '[]') as string[];
      } catch {
        recentExpressions = [];
      }
    });
  });

  /** Keeps the expression the rows are filtered by, once the box is left with it. */
  function rememberExpression() {
    const q = outputQuery.trim();
    if (!q || expressionRows === null) return;
    recentExpressions = [q, ...recentExpressions.filter((e) => e !== q)].slice(
      0,
      RECENT_EXPRESSIONS,
    );
    try {
      window.localStorage.setItem(expressionsKey(pipelineId), JSON.stringify(recentExpressions));
    } catch {
      // Storage refused: they last as long as the page.
    }
  }

  let outputQuery = $state('');
  /** The row numbers the expression kept, or null while the search is a keyword. */
  let expressionRows = $state<Set<number> | null>(null);
  const looksLikeExpression = (q: string) => /[=<>!&|()]/.test(q);

  $effect(() => {
    const [q, sample] = [outputQuery.trim(), lastSample];
    void signature;
    // Paused, the expression reads the records where they stand.
    const cut = pause?.after;
    if (!q || !sample || !looksLikeExpression(q)) {
      expressionRows = null;
      return;
    }
    let current = true;
    const timer = setTimeout(async () => {
      try {
        const res = await fetchPreview(cut ?? untrack(() => steps), sample, undefined, q);
        if (current) expressionRows = res.ok ? new Set(res.rows.map((r) => r.row)) : null;
      } catch {
        if (current) expressionRows = null;
      }
    }, RECOMPUTE_MS);
    return () => {
      current = false;
      clearTimeout(timer);
    };
  });

  function matches(i: number): boolean {
    const row = gridRows?.[i];
    if (!row) return false;
    if (!chips.every((c) => chipKeeps(c, i))) return false;
    const q = outputQuery.trim().toLowerCase();
    if (!q) return true;
    if (expressionRows) return expressionRows.has(row.row);
    const m = rowsMaps[i];
    return columns.some(
      (path) =>
        (m.after.has(path) || m.before.has(path)) &&
        formatDiffValue(cellValue(m, path)).toLowerCase().includes(q),
    );
  }

  // Indices, not rows: `rowsMaps` is position-parallel to the run's rows.
  let gridOrder = $derived.by(() => {
    const rows = gridRows ?? [];
    const by: Record<string, (i: number) => number | string | undefined> = {
      [ROW_KEY]: (i) => rows[i].row,
    };
    for (const path of columns)
      by[path] = (i) => {
        const m = rowsMaps[i];
        if (!m.after.has(path) && !m.before.has(path)) return undefined;
        const v = cellValue(m, path);
        return typeof v === 'number' ? v : formatDiffValue(v);
      };
    return gridSort.apply(rows.map((_, i) => i).filter(matches), by);
  });
</script>

{#snippet stageChips()}
  {#if (result && !result.ok) || (pause && selectedSample)}
    <!-- Above the grid, not instead of it: a pipeline that does not compile still shows the
         records as far as it does. A step's errors are written on the step; here they are listed
         to jump to. -->
    <div class="stage">
      {#if result && !result.ok}
        {@const failed = result}
        <ChipPicker kind="error" align="left" bind:open={errorsOpen}>
          {#snippet face()}⚠ {stageName(failed.stage)} failed · {failed.errors.length} ▾{/snippet}
          {#snippet children()}
            <div class="stage-errors">
              {#if unplacedErrors.length > 0}
                <ul>
                  {#each unplacedErrors as e}
                    <li>{e.message}</li>
                  {/each}
                </ul>
              {/if}
              {#if brokenSteps.length > 0}
                <JumpLinks
                  links={brokenSteps.flatMap(({ step, messages }) =>
                    messages.map((label) => ({
                      label,
                      onClick: () => {
                        errorsOpen = false;
                        jumpToStepId = step.id;
                      },
                    })),
                  )}
                />
              {/if}
            </div>
          {/snippet}
        </ChipPicker>
      {/if}
      {#if pause && selectedSample}
        <RemovableChip
          accent
          label="Paused"
          removeLabel="Show the whole pipeline"
          onremove={pause.chosen ? () => onSelectStep(null) : undefined}
        >
          ◉ {pause.step
            ? `After step ${pause.number} · ${STEP_LABELS[pause.step.type].toUpperCase()}`
            : 'Input records'}
        </RemovableChip>
        {#if pauseLoading}<span class="dim">loading…</span>{/if}
      {/if}
      {#if result && !result.ok && !pause && lastGoodResult && selectedSample}
        <span class="dim">Showing the sample from before this.</span>
      {/if}
    </div>
    <InlineError message={pauseError} />
  {/if}
{/snippet}

{#if loading}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else if loadFailed}
  <SectionShell />
{:else if showJson}
  <JsonEditPage
    title={`Edit JSON — ${pipelineName}`}
    help={`The pipeline as the editor holds it, unsaved edits included, in the form of pipelines/${id}.json.`}
    initialText={jsonDraft()}
    backHash={making
      ? newPipelineHash(making, back)
      : `#/build/pipelines/${encodeURIComponent(id)}`}
    onSave={saveFromJson}
  />
{:else}
  <SectionShell>
    <header class="head">
      <input
        id="pipeline-name"
        class="title"
        type="text"
        bind:value={pipelineName}
        aria-label="pipeline name"
        disabled={readOnly}
      />
      {#if !readOnly}
        <label class="edit-name" for="pipeline-name" title="Edit pipeline name"
          ><PencilIcon /></label
        >
      {/if}
      {#if changedOnBranch}<span class="changed-badge">changed on this branch</span>{/if}
      <span class="grow"></span>
      {#if changedOnBranch && !readOnly}
        {#if isDirty || neverSaved}<span class="dim">Save or discard your edits first.</span>{/if}
        <button type="button" disabled={undoBusy || isDirty || neverSaved} onclick={undoPipeline}
          >{undoBusy ? 'Undoing…' : 'Undo changes'}</button
        >
      {/if}
      <button
        type="button"
        onclick={() =>
          (location.hash = making
            ? newPipelineHash(making, back, true)
            : `#/build/pipelines/${encodeURIComponent(id)}/json`)}
      >
        Raw JSON
      </button>
    </header>
    <InlineError message={undoError} />

    {#if id !== 'new'}<Connections kind="pipeline" {id} />{/if}

    {#if moved}
      {@const held = moved.value}
      <HeldEdit
        what="this pipeline"
        rows={moved.rows}
        onrestore={() => {
          const draft = JSON.parse(held) as Draft;
          pipelineName = draft.pipelineName;
          steps = draft.steps;
          moved = null;
        }}
        ondiscard={() => {
          // Clean again, so the effect above forgets the record.
          moved = null;
        }}
      />
    {/if}

    {#if conflict}
      <SaveConflictPanel {conflict} onDiscard={() => location.reload()} onRetry={() => save()} />
    {/if}

    <div
      class="columns"
      style={`grid-template-columns: ${readOnly ? '1fr' : `${stepsWidth === null ? `minmax(${STEPS_MIN}px, 1fr)` : `${stepsWidth}px`} 11px minmax(0, 1fr)`}`}
    >
      <section class="editor">
        {#if proposed}<p class="proposed">{proposed}</p>{/if}
        <div class="builder">
          <StepList
            {steps}
            errorsByStep={stepListErrors}
            {warningsByStep}
            {pipelineIds}
            {pipelineNames}
            {lookupTables}
            {grokPatterns}
            parseInput={parseInputFor(sampleRows[0])}
            selectedStep={pause?.step ?? null}
            ran={pause?.run ?? null}
            {onSelectStep}
            onPropose={readOnly ? undefined : proposeFields}
            proposal={openProposal}
            {askAt}
          />
        </div>

        {#if !readOnly}
          <div class="run-row">
            <button type="button" onclick={run} disabled={running}>
              {running ? 'Running…' : 'Run now'}
            </button>
            <label class="inline-checkbox">
              <Toggle bind:checked={autoRun} ariaLabel="recompute as I type" />
              <span class="field-name">recompute as I type</span>
            </label>
          </div>
        {/if}

        {#if badLookupTables.length > 0}
          <InlineError>
            can't save: unknown lookup table(s) {badLookupTables.join(', ')} -- pick from the dropdown
            above or add them on the <a href="#/build/lookups">Lookup Tables</a> page
          </InlineError>
        {/if}
        {#if saveMessage}<p class="save-message">{saveMessage}</p>{/if}
        <InlineError
          message={unnamed && pipelineName.trim() === '' ? 'the pipeline needs a name' : ''}
        />
        <InlineError message={requestError} />
      </section>

      {#if !readOnly}
        <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
        <div
          class="divider"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize the columns"
          onpointerdown={drag}
          ondblclick={snap}
        >
          <span class="grip" aria-hidden="true">&#8596;</span>
        </div>

        <section
          class="right-col"
          bind:this={rightCol}
          style:--pane-top={`${paneTop}px`}
          style:--pane-after={`${paneAfter}px`}
        >
          <InlineError message={proposeError} />
          <AddSampleDialog
            open={capturing || adding}
            start={capturing ? 'capture' : undefined}
            target={captureAt ?? captureTarget}
            oncancel={() => {
              capturing = false;
              adding = false;
              captureAt = null;
            }}
            onsaved={captured}
          />

          {#if !selectedSample}{@render stageChips()}{/if}

          {#snippet fieldsMenu(records: number | null)}
            <FieldsMenu
              fields={menuFields}
              {records}
              shown={isShown}
              {dictionary}
              onshow={showFields}
              ondefault={() => chosenFields.clear()}
              onhas={(path) => {
                showFields([path], true);
                addChip({ kind: 'has', path });
              }}
            />
          {/snippet}

          <div class="records">
            <div class="output-search">
              <SampleMenu
                {samples}
                selected={selectedSample}
                {lastUsed}
                records={selectedSample && lastGoodResult && lastSample === selectedSample
                  ? lastGoodResult.rows.length
                  : null}
                {lockedBy}
                pipeline={loadedId}
                mayCapture={(c) => mayCapture(sourceTarget(c))}
                onpick={chooseSample}
                onadd={() => (adding = true)}
                oncapture={(c) => {
                  captureAt = sourceTarget(c);
                  capturing = true;
                }}
              />
              {#if gridRows && gridRows.length > 0}
                <SearchBox
                  bind:query={outputQuery}
                  items={findItems}
                  onpick={pickFind}
                  onleave={rememberExpression}
                  id="find"
                  label="Search the output"
                  placeholder="Search the output, or filter with an expression such as status >= 500"
                  mono={expressionRows !== null}
                >
                  {#snippet before()}<span aria-hidden="true">⌕</span
                    >{#each chips as c (chipKey(c))}<RemovableChip
                        removeLabel={`remove ${chipKey(c)}`}
                        onremove={() => toggleChip(c)}
                        ><span class="find-chip"
                          >{c.kind === 'has' ? `has ${c.path}` : CHIP_LABELS[c.kind]}</span
                        ></RemovableChip
                      >{/each}{/snippet}
                  {#snippet after()}{#if outputQuery.trim()}<span
                        class="mode"
                        class:expression={expressionRows !== null}
                        >{expressionRows !== null ? 'ƒ expression' : 'keyword'}</span
                      >{/if}{/snippet}
                  {#snippet option(item)}
                    {#if item.kind === 'recent'}
                      <code>{item.expression}</code>
                    {:else}
                      <span>{CHIP_LABELS[item.kind]}</span>
                      <span></span>
                      <span class="dim">{item.count}</span>
                    {/if}
                  {/snippet}
                </SearchBox>
                {#if outputQuery.trim() || chips.length > 0}
                  <span class="dim count">{gridOrder.length} of {gridRows.length} rows</span>
                {/if}
                {#if failingCount > 0}
                  <button
                    type="button"
                    class="failing error-button"
                    aria-pressed={chips.some((c) => c.kind === 'failing')}
                    onclick={() => toggleChip({ kind: 'failing' })}>⚠ {failingCount} failing</button
                  >
                {/if}
                {@render fieldsMenu(gridRows.length)}
              {:else if !selectedSample && dictionary}
                <!-- No records to count, but the steps compiled: what they declare. -->
                <span class="grow"></span>
                {@render fieldsMenu(null)}
              {/if}
            </div>
            {#if selectedSample}
              {@render stageChips()}
              <div class="output-actions">
                <button
                  type="button"
                  class="toggle"
                  class:on={changedOnly}
                  aria-pressed={changedOnly}
                  onclick={() => (changedOnly = !changedOnly)}
                >
                  Changed only
                </button>
                <button type="button" onclick={() => (showTypes = !showTypes)}>
                  {showTypes ? 'Hide types' : 'Show types'}
                </button>
                <span class="grow"></span>
                {#if !readOnly}
                  <button
                    type="button"
                    class="primary"
                    data-guide="lock-test"
                    onclick={goLockTest}
                    disabled={!canLockTest}
                  >
                    Lock this as a test
                  </button>
                {/if}
              </div>

              <div class="results">
                {#if !gridRows}
                  {#if !result}
                    <p class="dim">Run the pipeline to see output rows here.</p>
                  {/if}
                {:else if gridRows.length === 0}
                  <p class="dim">No sample rows.</p>
                {:else}
                  <!-- Fixed widths, so a column holds still as rows scroll past
                   (`columnWidths`); a value wider than its column ends in an
                   ellipsis, and opening the row shows it whole. -->
                  <div class="grid-wrap" bind:clientWidth={paneWidth}>
                    <table class="grid" style:width={gridWidth}>
                      <colgroup>
                        <col style:width="calc(2ch + 1rem)" />
                        <col style:width={`calc(${indexWidth}ch + 1rem)`} />
                        <!-- The last column takes what the pane has to spare. -->
                        {#each columnWidths as w, k (columns[k])}
                          <col
                            style:width={k < columns.length - 1 ? `calc(${w}ch + 1rem)` : undefined}
                          />
                        {/each}
                      </colgroup>
                      <thead>
                        <tr>
                          <th class="fold-cell"
                            ><FoldAllButton icon {allOpen} what="records" onclick={toggleAll} /></th
                          >
                          <th class="index-cell" aria-sort={gridSort.aria(ROW_KEY)}
                            ><SortButton sort={gridSort} key={ROW_KEY}>#</SortButton></th
                          >
                          {#each columns as path (path)}
                            {@const name = splitPath(path)}
                            <th class="cell" title={path} aria-sort={gridSort.aria(path)}
                              ><SortButton sort={gridSort} key={path}
                                ><span class="head-name"
                                  ><span class="path-prefix">{name.prefix}</span>{name.leaf}</span
                                ></SortButton
                              ></th
                            >
                          {/each}
                        </tr>
                      </thead>
                      <tbody bind:this={tbody}>
                        {#if drawn.above > 0}
                          <tr class="spacer" aria-hidden="true" style:height={`${drawn.above}px`}
                            ><td colspan={columns.length + 2}></td></tr
                          >
                        {/if}
                        {#each gridOrder.slice(drawn.start, drawn.end) as i (gridRows[i].row)}
                          {@const row = gridRows[i]}
                          {@const m = rowsMaps[i]}
                          {@const open = expanded.has(row.row)}
                          {@const failing = row.exceptions.length > 0}
                          <tr
                            class="grid-row"
                            class:dropped={row.json === null}
                            class:open
                            class:failing
                          >
                            <td class="fold-cell">
                              <button
                                type="button"
                                class="fold"
                                aria-expanded={open}
                                aria-label={`row ${row.row}`}
                                onclick={() => toggleRow(row.row)}>{open ? '▾︎' : '▸︎'}</button
                              >
                            </td>
                            <td class="index-cell"
                              >{#if failing}<span class="warn">⚠</span>{/if}{row.row}</td
                            >
                            {#each columns as path (path)}
                              {@const field = fieldAt(m.before, m.after, path)}
                              <td class="cell">
                                {#if !field}
                                  <span class="dim">&mdash;</span>
                                {:else if field.state === 'removed'}
                                  <!-- What the step took away is what the grid is
                                   being read for. The value stands, struck
                                   through, rather than an em dash saying only
                                   that something used to be here. -->
                                  <span class="state-removed">{formatDiffValue(field.before)}</span>
                                {:else}
                                  <span class="state-{field.state}"
                                    >{formatDiffValue(field.after)}</span
                                  >
                                {/if}
                              </td>
                            {/each}
                          </tr>

                          {#if open}
                            {@const sides = rowSides(row)}
                            {@const marks = rowMarks(m)}
                            <!-- Named apart from its row, which is named by its fold button. -->
                            <tr
                              class="expanded-row"
                              aria-label={`record ${row.row}`}
                              bind:offsetHeight={openHeights[row.row]}
                            >
                              <td class="expanded-cell" colspan={columns.length + 2}>
                                <div class="row-head-wrap">
                                  <div class="row-head">
                                    <span class="row-index">row {row.row}</span>
                                    <!-- Beside the row number: pushing it to the far
                                     side needed the row-head to reliably span the
                                     visible width, which a `colspan` cell wider
                                     than the viewport does not give it. -->
                                    {#if marks.added > 0}<span class="mark added"
                                        >+{marks.added}</span
                                      >{/if}
                                    {#if marks.removed > 0}<span class="mark removed"
                                        >&minus;{marks.removed}</span
                                      >{/if}
                                    {#if marks.changed > 0}<span class="mark changed"
                                        >~{marks.changed}</span
                                      >{/if}
                                    {#if row.json === null}
                                      <span class="badge dropped-badge">dropped</span>
                                    {/if}
                                  </div>
                                  {#each row.exceptions as a}
                                    <p class="exception">{a.message}</p>
                                  {/each}
                                </div>
                                <div
                                  class="record-body"
                                  style:max-width={`calc(${paneWidth}px - 1.5rem)`}
                                >
                                  <RecordFields
                                    before={sides.before}
                                    after={sides.after}
                                    keep={unfoldedPath}
                                    declared={sides.declared}
                                    {changedOnly}
                                    {showTypes}
                                  />
                                </div>
                              </td>
                            </tr>
                          {/if}
                        {/each}
                        {#if drawn.below > 0}
                          <tr class="spacer" aria-hidden="true" style:height={`${drawn.below}px`}
                            ><td colspan={columns.length + 2}></td></tr
                          >
                        {/if}
                      </tbody>
                    </table>
                  </div>
                {/if}
              </div>
            {/if}
          </div>
        </section>
      {/if}
    </div>
  </SectionShell>
  <SaveBar
    dirty={isDirty || neverSaved}
    {saving}
    onsave={save}
    ondiscard={isDirty ? discard : undefined}
  />
{/if}

<style>
  .dim {
    color: var(--dim);
  }

  .head {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    row-gap: 0.5rem;
    gap: 0.5rem;
    margin-bottom: 1rem;
  }

  .edit-name {
    display: inline-flex;
    color: var(--dim);
    cursor: text;
  }

  .grow {
    flex-grow: 1;
  }

  .changed-badge {
    font-size: var(--text-micro);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    border: 1px solid var(--accent);
    border-radius: 999px;
    padding: 0 0.45rem;
    color: var(--accent);
  }

  /* The name is the page's title, so it is set like one and edited in place
     rather than in a field of its own below it. */
  .title {
    font-family: var(--font-display);
    font-size: var(--text-title);
    font-weight: 600;
    color: var(--ink);
    background: none;
    border: 1px solid transparent;
    border-radius: var(--radius);
    padding: 0.15rem 0.35rem;
    min-width: min(18rem, 100%);
  }

  .title:hover {
    border-color: var(--border);
  }

  .divider {
    cursor: col-resize;
    justify-self: center;
    display: flex;
    justify-content: center;
    width: 11px;
    height: 100%;
    min-height: 4rem;
    background: linear-gradient(var(--border), var(--border)) center / 1px 100% no-repeat;
  }

  .divider:hover {
    background: linear-gradient(var(--accent), var(--accent)) center / 3px 100% no-repeat;
  }

  /* A line nobody knows to pull is a line. The mark says which way it goes,
     faintly, and follows the page down so it is there wherever the reader is
     in a long pipeline. */
  .grip {
    position: sticky;
    top: 1.25rem;
    align-self: flex-start;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 15px;
    height: 22px;
    margin-left: -2px;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--bg);
    color: var(--dim);
    font-size: var(--text-micro);
    line-height: 1;
    user-select: none;
  }

  .divider:hover .grip {
    border-color: var(--ink);
    color: var(--ink);
  }

  .columns {
    display: grid;
    gap: 0.75rem;
    margin-top: 0.5rem;
    align-items: start;
  }

  /* Below this the two halves stack, and the divider has nothing to divide. */
  @media (max-width: 1000px) {
    .columns {
      grid-template-columns: 1fr !important;
    }

    .divider {
      display: none;
    }
  }

  .editor-head {
    display: flex;
    align-items: center;
    gap: 0.8rem;
    margin-bottom: 0.9rem;
  }

  .stage {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin: 0.6rem 0 0.4rem;
    font-size: var(--text-label);
  }

  .stage-errors {
    width: min(32rem, 80vw);
    padding: 0.5rem;
    color: var(--error);
    font-size: var(--text-label);
  }

  .stage-errors ul {
    margin: 0;
    padding-left: 1.2rem;
  }

  /* The steps sit on the panel, so a step at rest is drawn by its own
     `--field` surface (STYLE_GUIDE.md, "Cards"). */
  .builder {
    margin-bottom: 0.8rem;
    padding: 0.5rem;
    border-radius: var(--radius-card);
    background: var(--panel);
  }

  .save-message {
    color: var(--ink);
    font-size: var(--text-label);
  }

  /* What the proposal read, over the steps it made. */
  .proposed {
    margin: 0 0 0.5rem;
    color: var(--dim);
    font-size: var(--text-label);
  }

  label {
    display: block;
    font-size: var(--text-body);
    font-weight: 600;
    margin: 0 0 0.35rem;
  }

  /* The label register: the field's name, weight 600. */
  .field-name {
    display: flex;
    align-items: center;
    font-weight: 600;
  }

  .output-search {
    display: flex;
    align-items: center;
    gap: 0.7rem;
  }
  .mode {
    flex: none;
    font-size: var(--text-note);
    color: var(--dim);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0 0.5rem;
    white-space: nowrap;
  }
  .mode.expression {
    color: var(--accent);
    border-color: var(--accent);
    font-family: var(--mono);
  }
  .count {
    font-size: var(--text-note);
    white-space: nowrap;
  }

  .find-chip {
    color: var(--ink);
    font-size: var(--text-note);
  }

  /* The failing rows are what to read first, so their count stands out until read. */
  .failing {
    flex: none;
  }

  .add-small {
    align-self: flex-start;
    background: none;
    color: var(--ink);
    border: 1px dashed var(--dim);
    border-radius: var(--radius);
    padding: 0.15rem 0.5rem;
    font-size: var(--text-micro);
    cursor: pointer;
  }

  .run-row {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin-top: 1rem;
    flex-wrap: wrap;
  }

  .inline-checkbox {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    font-size: var(--text-label);
    font-weight: 400;
    color: var(--dim);
    margin: 0;
  }

  /* A grid item is `min-width: auto` by default, so a step wider than the
     column would push into the output rather than scroll inside its own
     half. Both halves are told they may be narrower than their content. */
  .editor,
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
    .records,
    .results {
      display: flex;
      flex-direction: column;
      flex: 1;
      min-height: 0;
    }

    /* Only what fills the pane scrolls: a scroller around the controls would clip their menus. */
    .dictionary {
      overflow-y: auto;
      min-height: 0;
    }

    .right-col > :not(.records),
    .records > :not(.results, .dictionary) {
      flex-shrink: 0;
    }

    .grid-wrap {
      overflow: auto;
      min-height: 0;
    }
  }

  .output-actions {
    display: flex;
    align-items: center;
    margin-top: 0.75rem;
  }

  .results {
    margin-top: 1rem;
    min-height: 200px;
  }

  .row {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.6rem 0.8rem;
    margin-bottom: 0.6rem;
  }

  .row.dropped {
    opacity: 0.7;
  }

  /* A rule under it, not just a gap, so the row it names and the fields
     below it read as three things -- the folded row above, this head, the
     record -- rather than one tinted block. On the wrapper, not `.row-head`
     itself, so it spans the cell's full width rather than shrinking to the
     sticky content's `width: max-content`. */
  .row-head-wrap {
    padding: 0.4rem 0;
    margin-bottom: 0.4rem;
    border-bottom: 1px solid var(--border);
  }

  .row-head {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .row-index {
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--ink);
  }

  .badge {
    font-size: var(--text-micro);
    font-weight: 600;
    padding: 0.15rem 0.5rem;
    border-radius: var(--radius);
    max-width: 100%;
    white-space: normal;
    background: color-mix(in srgb, var(--error) 15%, transparent);
    color: var(--error);
  }

  .badge.dropped-badge {
    background: color-mix(in srgb, var(--dim) 20%, transparent);
    color: var(--dim);
  }

  /* A record is one line until it is asked for. The whole grid scrolls
     sideways rather than dropping columns: a field stays where it was, and
     the alphabetical order means it stays findable once scrolled to. */
  /* The bar sits at the bottom of the padding box, so without this it is
     drawn over the last row rather than under it. */
  .grid-wrap {
    overflow-x: auto;
    padding-bottom: 0.85rem;
    scrollbar-gutter: stable;
  }

  /* At least the width of the panel, so a pipeline that has produced one
     field spends the whole panel on it rather than leaving it empty; wider
     than the panel when the columns need it, which is what scrolls. */
  .grid {
    border-collapse: collapse;
    table-layout: fixed;
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .grid th,
  .grid td {
    text-align: left;
    font-weight: inherit;
    padding: 0.15rem 0.5rem;
    border-bottom: 1px solid var(--border);
  }

  .grid thead th {
    color: var(--dim);
    font-size: var(--text-micro);
    position: sticky;
    top: 0;
    /* Above the open record's body, which is sticky too and later in the page. */
    z-index: 1;
    background: var(--bg);
  }

  /* Tinted the same as the record beneath it, so the two read as one open
     group -- but still a hairline apart, or the row that was clicked and
     the record it opened would run together into one shape. */
  .grid-row.open > td {
    background: color-mix(in srgb, var(--ink) 8%, transparent);
  }

  .grid-row.dropped .cell {
    opacity: 0.55;
  }

  .fold {
    border: none;
    background: none;
    padding: 0;
    color: var(--dim);
    font: inherit;
    cursor: pointer;
  }

  .index-cell {
    color: var(--dim);
    text-align: right;
  }

  .cell {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* The rail marks how far the open record reaches, and stays put while the
     grid scrolls sideways under it. A record the step changed takes the
     added color, as a diff does. */
  .expanded-cell {
    background: color-mix(in oklch, var(--ink) 6%, transparent);
    box-shadow: inset 3px 0 0 var(--border);
    padding: 0.5rem 0.5rem 0.5rem 0.75rem;
  }

  /* The record follows the left edge as the grid scrolls sideways, and wraps
     to what shows of it rather than to the width of every column. */
  .record-body {
    position: sticky;
    left: 0.75rem;
  }

  /* A row that failed is the one to read, so it is the one that stands out. */
  .grid-row.failing > td {
    background: color-mix(in srgb, var(--error) 10%, transparent);
  }

  .grid-row.failing .index-cell,
  .warn {
    color: var(--error);
  }

  .warn {
    margin-right: 0.4ch;
  }

  /* Why the row failed, under its head, in the words the step gave. */
  .exception {
    position: sticky;
    left: 0.75rem;
    width: max-content;
    max-width: 100%;
    margin: 0.3rem 0 0;
    padding-left: 0.5rem;
    border-left: 2px solid var(--error);
    color: var(--error);
    white-space: normal;
  }

  .spacer td {
    padding: 0;
    border: 0;
  }

  /* The header's name gives way to an ellipsis rather than widening its
     column; the whole path is its title. */
  .head-name {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  /* The head of an open record follows the left edge rather than scrolling
     away: this cell is as wide as the whole table. A floor rather than
     shrink-to-fit, so the row's own identity and its summary have room to
     sit at opposite ends instead of crowding together on the left. */
  .expanded-cell .row-head {
    position: sticky;
    left: 0.75rem;
    width: max-content;
  }

  /* The nesting recedes so the eye lands on the field's own name. */
  .path-prefix {
    opacity: 0.6;
  }

  .toggle.on {
    border-color: var(--ink);
    color: var(--ink);
    font-weight: 600;
  }

  .state-added {
    color: var(--added);
  }

  .state-changed {
    color: var(--changed);
  }

  .state-removed {
    color: var(--removed);
    text-decoration: line-through;
  }

  /* What this step did to the row, before reading a line of it. */
  .mark {
    font-family: var(--mono);
    font-size: var(--text-micro);
    font-weight: 600;
  }

  .mark.added {
    color: var(--added);
  }

  .mark.changed {
    color: var(--changed);
  }

  .mark.removed {
    color: var(--removed);
  }
</style>
