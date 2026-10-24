<!--
  One pipeline's builder: loaded from `GET /api/pipelines/:id` (blank for
  `id === 'new'`), run in-process against a saved sample through
  `/api/preview`, and saved with `PUT /api/pipelines/:id`, which the
  backend validates against the whole bundle.

  The sample is chosen from the library, never typed here: records are
  authored on the Samples page, so what a preview ran over has a name and
  is in the branch's history. Arriving with one named in the URL selects it
  on load instead of defaulting to the first in the library.
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
  import { SvelteSet } from 'svelte/reactivity';
  import { fieldsSettled } from './grokFields';
  import { provideUnfinishedRows } from './unfinished.svelte';
  import RecordFields from './RecordFields.svelte';
  import { formatDiffValue } from './diff';
  import {
    cellOf,
    columnsFor,
    countStates,
    declaredRecord,
    diffRecords,
    splitPath,
    type Declared,
    type FieldLeaf,
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
    errorText,
    type ApiError,
    errorMessage,
    type GrokPatternDto,
    type LookupTableDto,
    type PipelineDto,
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
  import Help from './Help.svelte';
  import AddSampleDialog from './AddSampleDialog.svelte';
  import SamplePicker from './SamplePicker.svelte';
  import PencilIcon from './PencilIcon.svelte';
  import { mintPipelineId } from './ids';
  import { announcePipelineNames, newPipelineHash, withPipeline, type Making } from './nav';
  import { ago, count } from './time';
  import { editable, mayCapture } from './access';
  import SortButton from './SortButton.svelte';
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
  let includeSampled = $state(false);
  let copiedMapping = $state(false);

  async function copyMapping(mapping: unknown) {
    try {
      await navigator.clipboard.writeText(JSON.stringify(mapping, null, 2));
      copiedMapping = true;
      setTimeout(() => (copiedMapping = false), 1500);
    } catch {
      // No clipboard access: the mapping is on screen to select by hand.
    }
  }

  async function loadSamples() {
    try {
      const res = await listSamples();
      samples = res.samples;
      // `sample` came in from the URL (a sample row's "Run in a pipeline"
      // link, or a pipeline opened from the list while it held one). With
      // none, or a stale one not on this branch, the editor opens on the
      // picker: which records to run is the author's to choose.
      if (!selectedSample && sample && samples.some((s) => s.id === sample)) {
        await chooseSample(sample);
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
    await loadSamples();
    await chooseSample(id);
    if (making) await proposeFrom(sampleRows);
  }

  async function chooseSample(id: string) {
    selectedSample = id;
    sampleRows = [];
    // The rows on screen were the last sample's.
    lastGoodResult = null;
    lastSample = '';
    outputQuery = '';
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

  // -- per-step diff --------------------------------------------------------
  //
  // No step selected: before is the record as it arrived, after is the whole
  // pipeline's output. A step selected: before/after are the pipeline
  // truncated to either side of that step (`truncateSteps`), each a real
  // preview run, so the diff shows what that one step did.

  // Every field, or only the ones this step touched. A mapping step rewrites
  // most of a record, so the filter is what makes a later step's handful of
  // changes findable without reading the whole thing.
  let changedOnly = $state(false);
  let showTypes = $state(true);

  let selectedStep: Step | null = $state(null);
  let selectedDiff: {
    before: PreviewRow[];
    after: PreviewRow[];
    declared: { before: Declared | null; after: Declared | null };
  } | null = $state(null);
  let selectedDiffLoading = $state(false);
  let selectedDiffError = $state('');

  function onSelectStep(step: Step | null) {
    selectedStep = step;
  }

  async function loadSelectedDiff() {
    selectedDiffError = '';
    if (!selectedStep || !lastSample) {
      selectedDiff = null;
      return;
    }
    const beforeSteps = truncateSteps(steps, selectedStep, false);
    const afterSteps = truncateSteps(steps, selectedStep, true);
    if (beforeSteps === null || afterSteps === null) {
      // The step is no longer in the tree: a stale selection.
      selectedStep = null;
      selectedDiff = null;
      return;
    }
    selectedDiffLoading = true;
    try {
      // One instant for both sides, so the only difference between them is
      // the step.
      const at = Date.now() * 1000;
      const [beforeRes, afterRes] = await Promise.all([
        fetchPreview(beforeSteps, lastSample, at),
        fetchPreview(afterSteps, lastSample, at),
      ]);
      const failed = !beforeRes.ok ? beforeRes : !afterRes.ok ? afterRes : null;
      if (failed) {
        selectedDiffError = previewFailure(failed);
        selectedDiff = null;
        return;
      }
      selectedDiff = {
        before: beforeRes.rows,
        after: afterRes.rows,
        declared: { before: declaredOf(beforeRes), after: declaredOf(afterRes) },
      };
    } catch (e) {
      selectedDiffError = unreported(e);
      selectedDiff = null;
    } finally {
      selectedDiffLoading = false;
    }
  }

  // Tracks `selectedStep`, `steps` and `lastSample`: all read before the
  // first `await`.
  $effect(() => {
    loadSelectedDiff();
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
  // `loadSelectedDiff` makes for the "after" side of a per-step diff. One
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

  /** The record on either side of the step shown: the selected step, or the
   * whole pipeline from its sample row. */
  function rowSides(row: PreviewRow): {
    before: unknown;
    after: unknown;
    declared: { before: Declared | null; after: Declared | null };
  } {
    if (selectedStep && selectedDiff) {
      const before = selectedDiff.before.find((r) => r.row === row.row);
      const after = selectedDiff.after.find((r) => r.row === row.row);
      return {
        before: withSource(before?.json ?? null, row.row),
        after: withSource(after?.json ?? null, row.row),
        declared: selectedDiff.declared,
      };
    }
    const before = sampleRows[row.row] ?? {};
    return {
      before: withSource(before, row.row),
      after: withSource(row.json, row.row),
      declared: { before: sampleDeclared, after: runDeclared },
    };
  }

  function rowFields(row: PreviewRow): FieldLeaf[] {
    const { before, after } = rowSides(row);
    return diffRecords(before, after);
  }

  // Read once per run, not once per row per column: the columns are the union
  // over every row, so the grid needs all of them in hand anyway.
  let rowsFields = $derived(lastGoodResult ? lastGoodResult.rows.map(rowFields) : []);
  let allColumns = $derived(columnsFor(rowsFields));

  // What differs from each column's default, rather than what is shown: a
  // step added along the way brings its fields with it, instead of arriving
  // invisible behind a choice made before they existed. A field under a `__`
  // key is internal, smeltr's own and never delivered, so it starts hidden,
  // and an unfolded row shows it only while its column is shown. A root key
  // the path has to quote (`["__a.b"]`) counts as well.
  const startsHidden = (path: string) => path.startsWith('__') || path.startsWith('["__');
  let toggledColumns = $state(new SvelteSet<string>());
  const isShown = (path: string) => startsHidden(path) === toggledColumns.has(path);
  let columns = $derived(allColumns.filter((c) => isShown(c.path)));

  const unfoldedPath = (path: string) => !startsHidden(path) || isShown(path);
  const unfolded = (f: FieldLeaf) => unfoldedPath(f.path);

  function toggleColumn(path: string) {
    if (toggledColumns.has(path)) toggledColumns.delete(path);
    else toggledColumns.add(path);
  }

  function showAllColumns() {
    toggledColumns.clear();
    for (const c of allColumns) if (startsHidden(c.path)) toggledColumns.add(c.path);
  }

  let columnsOpen = $state(false);

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
    lastGoodResult !== null &&
      lastGoodResult.rows.length > 0 &&
      lastGoodResult.rows.every((r) => expanded.has(r.row)),
  );

  function toggleAll() {
    if (allOpen) expanded.clear();
    else for (const row of lastGoodResult?.rows ?? []) expanded.add(row.row);
  }

  const dictionarySort = new TableSort<'field' | 'type'>();
  const sortDictionary = (fields: [string, string][]) =>
    dictionarySort.apply(fields, { field: ([name]) => name, type: ([, type]) => type });

  // Keyed apart from the field paths: a record may carry a field named `row`, but
  // no field has an empty path.
  const ROW_KEY = '';
  const gridSort = new TableSort<string>();
  // -- the Output tab's search ---------------------------------------------
  //
  // A keyword by default: a row stays when one of its shown cells holds the
  // text. Text with an operator or a call in it is tried as an expression over
  // the pipeline's output (`PreviewRequest.filter`). One that compiles to a
  // Bool filters the rows on the leader; anything else stays a keyword, so a
  // bare word like `slow` is searched for rather than read as a field.

  let paneTab = $state<'output' | 'dictionary'>('output');
  /** The record the steps write, from the last run that compiled: with no sample, a run that
   * read no records. */
  let dictionary = $derived(
    (result?.ok ? result.dictionary : undefined) ?? lastGoodResult?.dictionary,
  );
  let outputQuery = $state('');
  /** The row numbers the expression kept, or null while the search is a keyword. */
  let expressionRows = $state<Set<number> | null>(null);
  const looksLikeExpression = (q: string) => /[=<>!&|()]/.test(q);

  $effect(() => {
    const [q, sample] = [outputQuery.trim(), lastSample];
    void signature;
    if (!q || !sample || !looksLikeExpression(q)) {
      expressionRows = null;
      return;
    }
    let current = true;
    const timer = setTimeout(async () => {
      try {
        const res = await fetchPreview(
          untrack(() => steps),
          sample,
          undefined,
          q,
        );
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
    const q = outputQuery.trim().toLowerCase();
    if (!q) return true;
    const row = lastGoodResult?.rows[i];
    if (!row) return false;
    if (expressionRows) return expressionRows.has(row.row);
    return columns.some((col) => {
      const field = cellOf(rowsFields[i] ?? [], col.path);
      if (!field) return false;
      const v = field.state === 'removed' ? field.before : field.after;
      return formatDiffValue(v).toLowerCase().includes(q);
    });
  }

  // Indices, not rows: `rowsFields` is position-parallel to the run's rows.
  let gridOrder = $derived.by(() => {
    const rows = lastGoodResult?.rows ?? [];
    const by: Record<string, (i: number) => number | string | undefined> = {
      [ROW_KEY]: (i) => rows[i].row,
    };
    for (const col of columns)
      by[col.path] = (i) => {
        const field = cellOf(rowsFields[i] ?? [], col.path);
        if (!field) return undefined;
        const v = field.state === 'removed' ? field.before : field.after;
        return typeof v === 'number' ? v : formatDiffValue(v);
      };
    return gridSort.apply(rows.map((_, i) => i).filter(matches), by);
  });
</script>

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
            {pipelineIds}
            {pipelineNames}
            {lookupTables}
            {grokPatterns}
            parseInput={parseInputFor(sampleRows[0])}
            {selectedStep}
            {onSelectStep}
            onPropose={readOnly ? undefined : proposeFields}
            proposal={openProposal}
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

        <section class="right-col">
          <InlineError message={proposeError} />
          <AddSampleDialog
            open={capturing}
            start="capture"
            target={captureTarget}
            oncancel={() => (capturing = false)}
            onsaved={captured}
          />

          {#if selectedSample}
            <div class="sample-line">
              <button
                type="button"
                class="compact"
                onclick={() => {
                  chooseSample('');
                  paneTab = 'output';
                }}>← Back</button
              >
              <span class="dim">Sample</span>
              <strong>{selectedSampleName}</strong>
              {#if lastGoodResult && lastSample === selectedSample}
                <span class="dim">{count(lastGoodResult.rows.length, 'row')}</span>
              {/if}
            </div>
          {/if}

          <div class="subtabs" role="tablist" aria-label="Output">
            <button
              type="button"
              role="tab"
              aria-selected={paneTab === 'output'}
              onclick={() => (paneTab = 'output')}
              >Output{#if selectedSample && lastGoodResult}
                <span class="pill">{lastGoodResult.rows.length}</span>{/if}</button
            >
            <button
              type="button"
              role="tab"
              aria-selected={paneTab === 'dictionary'}
              onclick={() => (paneTab = 'dictionary')}
              >Data dictionary{#if dictionary}
                <span class="pill">{dictionary.compiled.fields.length}</span>{/if}</button
            >
          </div>

          {#if result && !result.ok}
            <!-- Above the grid, not instead of it: a syntax slip mid-edit
                 should not take the sample away from whoever is fixing it. -->
            <!-- A step's errors are written on the step; here it is only named. -->
            <div class="stage-error">
              <strong>{result.stage} failed</strong>
              {#if unplacedErrors.length > 0}
                <ul>
                  {#each unplacedErrors as e}
                    <li>{e.message}</li>
                  {/each}
                </ul>
              {/if}
              {#if brokenSteps.length > 0}
                <JumpLinks
                  links={brokenSteps.map(({ step }) => ({
                    label: stepSummary(step),
                    onClick: () => (jumpToStepId = step.id),
                  }))}
                />
              {/if}
            </div>
            {#if lastGoodResult && selectedSample}
              <p class="dim stale-note">Showing the sample from before this.</p>
            {/if}
          {/if}

          <div role="tabpanel">
            {#if paneTab === 'dictionary'}
              {#if dictionary}
                {@const d = dictionary}
                {@const dict = includeSampled && d.sampled ? d.sampled : d.compiled}
                <div class="dictionary">
                  <div class="run-row">
                    {#if d.sampled}<label class="inline-checkbox">
                        <Toggle
                          bind:checked={includeSampled}
                          ariaLabel="include fields seen in the sample"
                        />
                        <span class="field-name">include fields seen in the sample</span>
                      </label>{/if}
                    <span class="grow"></span>
                    <button type="button" onclick={() => copyMapping(dict.elasticsearch)}>
                      {copiedMapping ? 'Copied' : 'Copy as Elasticsearch mapping'}
                    </button>
                    <Help label="Elasticsearch mapping">
                      The mapping leaves out a <code>variant</code> field. Give the field a type to include
                      it.
                    </Help>
                  </div>
                  <table class="proposal-table">
                    <thead>
                      <tr>
                        <th aria-sort={dictionarySort.aria('field')}
                          ><SortButton sort={dictionarySort} key="field">Field</SortButton></th
                        >
                        <th aria-sort={dictionarySort.aria('type')}
                          ><SortButton sort={dictionarySort} key="type">Type</SortButton></th
                        >
                      </tr>
                    </thead>
                    <tbody>
                      {#each sortDictionary(dict.fields) as [name, type]}
                        <tr><td class="mono">{name}</td><td class="mono">{type}</td></tr>
                      {/each}
                    </tbody>
                  </table>
                  <details>
                    <summary>Elasticsearch mapping</summary>
                    <pre class="mono">{JSON.stringify(dict.elasticsearch, null, 2)}</pre>
                  </details>
                </div>
              {/if}
            {:else if !selectedSample}
              {#if samples.length === 0}
                <p class="dim">
                  No samples yet. Capture one, or paste or upload records on the
                  <a class="btn" href="#/build">Samples</a> page.
                </p>
              {:else}
                <SamplePicker {samples} {lockedBy} onpick={chooseSample}>
                  {#snippet tools()}
                    <button type="button" onclick={() => (capturing = true)}>Capture</button>
                    <a class="btn" href="#/build">Paste or upload</a>
                  {/snippet}
                </SamplePicker>
              {/if}
            {:else}
              {#if selectedStep}
                <div class="diff-scope">
                  <span class="dim">Diffing:</span>
                  <strong>{stepSummary(selectedStep)}</strong>
                  {#if selectedDiffLoading}<span class="dim">loading…</span>{/if}
                  <button type="button" class="compact" onclick={() => onSelectStep(null)}>
                    show whole pipeline
                  </button>
                </div>
                <InlineError message={selectedDiffError} />
              {/if}

              {#if lastGoodResult && lastGoodResult.rows.length > 0}
                <div class="output-search">
                  <label class="search"
                    ><span aria-hidden="true">⌕</span><input
                      type="search"
                      class:mono={expressionRows !== null}
                      bind:value={outputQuery}
                      placeholder="Search the output, or filter with an expression such as status >= 500"
                      aria-label="Search the output"
                    />{#if outputQuery.trim()}<span
                        class="mode"
                        class:expression={expressionRows !== null}
                        >{expressionRows !== null ? 'ƒ expression' : 'keyword'}</span
                      >{/if}</label
                  >
                  {#if outputQuery.trim()}
                    <span class="dim count"
                      >{gridOrder.length} of {lastGoodResult.rows.length} rows</span
                    >
                  {/if}
                </div>
              {/if}
              <div class="output-actions">
                <button
                  type="button"
                  onclick={toggleAll}
                  disabled={!lastGoodResult || lastGoodResult.rows.length === 0}
                >
                  {allOpen ? 'Fold all' : 'Unfold all'}
                </button>
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
                {#if allColumns.length > 0}
                  <ChipPicker
                    kind="columns"
                    align="left"
                    bind:open={columnsOpen}
                    ariaLabel="Choose columns"
                  >
                    {#snippet face()}
                      Columns{#if columns.length < allColumns.length}
                        <span class="dim">{columns.length} of {allColumns.length}</span>
                      {/if}
                    {/snippet}
                    {#snippet children()}
                      <div class="column-list">
                        {#each allColumns as col (col.path)}
                          {@const name = splitPath(col.path)}
                          {@const shown = isShown(col.path)}
                          <button
                            type="button"
                            class="pick"
                            class:on={shown}
                            aria-pressed={shown}
                            onclick={() => toggleColumn(col.path)}
                          >
                            <span class="tick">{shown ? '✓' : ''}</span>
                            <span class="mono"
                              ><span class="path-prefix">{name.prefix}</span>{name.leaf}</span
                            >
                          </button>
                        {/each}
                      </div>
                      {#if columns.length < allColumns.length}
                        <button type="button" class="pick show-all" onclick={showAllColumns}>
                          Show all {allColumns.length}
                        </button>
                      {/if}
                    {/snippet}
                  </ChipPicker>
                {/if}
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
                {#if !lastGoodResult}
                  {#if !result}
                    <p class="dim">Run the pipeline to see output rows here.</p>
                  {/if}
                {:else if lastGoodResult.rows.length === 0}
                  <p class="dim">No sample rows.</p>
                {:else}
                  <!-- A table, so a column is only as wide as what it holds.
                   `.cell`'s max-width caps that: one long value would otherwise
                   set the width of its column for every row. -->
                  <div class="grid-wrap">
                    <table class="grid">
                      <thead>
                        <tr>
                          <th class="fold-cell"></th>
                          <th class="index-cell" aria-sort={gridSort.aria(ROW_KEY)}
                            ><SortButton sort={gridSort} key={ROW_KEY}>row</SortButton></th
                          >
                          {#each columns as col (col.path)}
                            <!-- A field the step treated the same way in every row
                             says so once, here. Where the rows disagree the
                             header stays plain and each cell speaks for itself. -->
                            {@const name = splitPath(col.path)}
                            <th
                              class="cell state-{col.state ?? 'none'}"
                              aria-sort={gridSort.aria(col.path)}
                              ><SortButton sort={gridSort} key={col.path}
                                ><span class="path-prefix">{name.prefix}</span
                                >{name.leaf}</SortButton
                              ></th
                            >
                          {/each}
                        </tr>
                      </thead>
                      <tbody>
                        {#each gridOrder as i (lastGoodResult.rows[i].row)}
                          {@const row = lastGoodResult.rows[i]}
                          {@const fields = (rowsFields[i] ?? []).filter(unfolded)}
                          {@const marks = countStates(fields)}
                          {@const open = expanded.has(row.row)}
                          <tr class="grid-row" class:dropped={row.json === null} class:open>
                            <td class="fold-cell">
                              <button
                                type="button"
                                class="fold"
                                aria-expanded={open}
                                aria-label={`row ${row.row}`}
                                onclick={() => toggleRow(row.row)}>{open ? '▾︎' : '▸︎'}</button
                              >
                            </td>
                            <td class="index-cell">{row.row}</td>
                            {#each columns as col (col.path)}
                              {@const field = cellOf(fields, col.path)}
                              <td class="cell">
                                {#if !field}
                                  <span class="dim">&mdash;</span>
                                {:else if field.state === 'removed'}
                                  <!-- What the step took away is what the grid is
                                   being read for. The value stands, struck
                                   through, rather than an em dash saying only
                                   that something used to be here. -->
                                  <span class="state-removed">{formatDiffValue(field.before)}</span>
                                {:else if col.state === null}
                                  <span class="state-{field.state}"
                                    >{formatDiffValue(field.after)}</span
                                  >
                                {:else}
                                  {formatDiffValue(field.after)}
                                {/if}
                              </td>
                            {/each}
                          </tr>

                          {#if open}
                            {@const sides = rowSides(row)}
                            <tr class="expanded-row">
                              <td
                                class="expanded-cell"
                                class:changed={marks.added + marks.changed + marks.removed > 0}
                                colspan={columns.length + 2}
                              >
                                <div class="row-head-wrap">
                                  <div class="row-head">
                                    <span class="row-index">row {row.row}</span>
                                    {#each row.exceptions as a}
                                      <span class="badge">{a.message}</span>
                                    {/each}
                                    {#if row.json === null}
                                      <span class="badge dropped-badge">dropped</span>
                                    {/if}
                                    <!-- Beside the row number: pushing it to the far
                                     side needed the row-head to reliably span the
                                     visible width, which a `colspan` cell wider
                                     than the viewport does not give it. -->
                                    {#if marks.added > 0}<span class="mark added"
                                        >+{marks.added}</span
                                      >{/if}
                                    {#if marks.changed > 0}<span class="mark changed"
                                        >~{marks.changed}</span
                                      >{/if}
                                    {#if marks.removed > 0}<span class="mark removed"
                                        >&minus;{marks.removed}</span
                                      >{/if}
                                  </div>
                                </div>
                                <RecordFields
                                  before={sides.before}
                                  after={sides.after}
                                  keep={unfoldedPath}
                                  declared={sides.declared}
                                  {changedOnly}
                                  {showTypes}
                                />
                              </td>
                            </tr>
                          {/if}
                        {/each}
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

  code {
    font-family: var(--mono);
    font-size: 0.9em;
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

  .diff-scope {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: var(--text-label);
    margin: 0.6rem 0 0.4rem;
  }

  .diff-scope button {
    margin-left: auto;
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

  .right-col label {
    margin-top: 0;
  }

  /* The label register: the field's name, weight 600. */
  .field-name {
    display: flex;
    align-items: center;
    font-weight: 600;
  }

  .sample-line {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin-bottom: 0.4rem;
    font-size: var(--text-body);
  }

  /* ChangeDetailPage's tabs. */
  .subtabs {
    display: flex;
    flex-wrap: wrap;
    gap: 1.2rem;
    margin: 0 0 0.9rem;
    border-bottom: 1px solid var(--border);
  }
  .subtabs button {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    background: none;
    padding: 0.35rem 0;
    border-width: 0 0 3px;
    border-radius: 0;
    border-color: transparent;
    color: var(--dim);
    font-size: var(--text-body);
  }
  .subtabs button[aria-selected='true'] {
    color: var(--ink);
    font-weight: 600;
    border-color: var(--accent);
  }
  .pill {
    padding: 0 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--panel);
    color: var(--dim);
    font-size: var(--text-micro);
    font-weight: 500;
  }

  .output-search {
    display: flex;
    align-items: center;
    gap: 0.7rem;
  }
  /* ClusterPage's worker search. */
  .search {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex: 1;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0 0.7rem;
    color: var(--dim);
    font-weight: 400;
  }
  .search input {
    width: 100%;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--ink);
    font: inherit;
    padding: 1px 2px;
  }
  .search input.mono {
    font-family: var(--mono);
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

  .proposal-table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-label);
  }

  .proposal-table th,
  .proposal-table td {
    text-align: left;
    padding: 0.35rem 0.5rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }

  .proposal-table th {
    color: var(--dim);
    font-weight: 700;
    font-size: var(--text-micro);
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }

  .proposal-table tbody tr:last-child td {
    border-bottom: 0;
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

  .output-actions {
    display: flex;
    align-items: center;
    margin-top: 0.75rem;
  }

  .results {
    margin-top: 1rem;
    min-height: 200px;
  }

  .stage-error {
    border: 1px solid var(--error);
    border-radius: var(--radius);
    padding: 0.8rem 1rem;
    color: var(--error);
  }

  .stage-error ul {
    margin: 0.4rem 0 0;
    padding-left: 1.2rem;
  }

  .stale-note {
    margin: 0.6rem 0 0;
    font-size: var(--text-note);
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
    color: var(--dim);
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
    min-width: 100%;
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

  .fold-cell {
    width: 1.2rem;
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

  /* Wide enough for what it holds, up to the point where one value would
     push every other column off the panel. */
  .cell {
    max-width: 14rem;
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

  .expanded-cell.changed {
    background: color-mix(in oklch, var(--added) 7%, transparent);
    box-shadow: inset 3px 0 0 var(--added);
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

  /* A wide record has a column per field, so the list scrolls rather than
     running off the bottom of the window. */
  .column-list {
    max-height: 18rem;
    overflow-y: auto;
  }

  .tick {
    width: 0.7rem;
    flex-shrink: 0;
    color: var(--dim);
  }

  .show-all {
    border-top: 1px solid var(--border);
    color: var(--ink);
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
