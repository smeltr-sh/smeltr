<!--
  One pipeline's builder: loaded from `GET /api/areas/:area/pipelines/:id` (blank for
  `id === 'new'`), run in-process against a saved sample through
  `/api/areas/:area/preview/pipeline`, and saved with `PUT /api/areas/:area/pipelines/:id`, which the
  backend validates against the whole bundle.

  The records come from a saved sample, so what a preview ran over has a
  name and is in the branch's history; the sample menu also saves a
  capture or pasted records as one. The editor opens on the sample named in
  the URL, else on the one this pipeline last ran over in this browser.
-->
<script lang="ts">
  import StateMark from './StateMark.svelte';
  import InlineError from './InlineError.svelte';
  import { tick, untrack } from 'svelte';
  import { loadExpanded, opening, saveExpanded } from './StepEditor.svelte';
  import { store, stored } from './stored';
  import StepList from './StepList.svelte';
  import Connections from './Connections.svelte';
  import PipelineProduces from './PipelineProduces.svelte';
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
    newStep,
  } from './pipeline';
  import {
    productionOf,
    proposalRows,
    suggestionsOf,
    type FieldProposal,
    type OpenProposal,
  } from './proposal';
  import { fieldsSettled } from './grokFields';
  import { provideUnfinishedRows } from './unfinished.svelte';
  import { provideProducedSchemas } from './producedSchemas.svelte';
  import RemovableChip from './RemovableChip.svelte';
  import { declaredRecord, type Declared } from './recordDiff';
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
    type DictionaryOut,
    type Dictionary,
    TAG_FIELD,
    errorText,
    type ApiError,
    errorMessage,
    type GrokPatternDto,
    type SchemaDto,
    type LookupTableDto,
    type PipelineDto,
    type Connected,
    type SampleRecordDto,
    type SampleSummary,
    type TestSummary,
    type SaveConflict,
    type PreviewResult,
    type ParserProposal,
    parsersIn,
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
  import AddSampleDialog from './AddSampleDialog.svelte';
  import RecordsPane, { type PauseRun } from './RecordsPane.svelte';
  import { RECOMPUTE_MS } from './FindBox.svelte';
  import SampleMenu from './SampleMenu.svelte';
  import PencilIcon from './PencilIcon.svelte';
  import { mintPipelineId } from './ids';
  import { announcePipelineNames, newPipelineHash, withPipeline, type Making } from './nav';
  import { ago } from './time';
  import { editable, mayCapture } from './access';

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
  /** The schemas its rows are held to, by id (`PipelineDto.produces`). */
  let produces: string[] = $state([]);
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
  // Produces is added only when it names a schema, so a draft held by a build before Produces still compares.
  function editSignatureOf(name: string, theSteps: Step[], theProduces: string[]): string {
    const produced = theProduces.length > 0 ? '::' + JSON.stringify(theProduces) : '';
    return name + '::' + signatureOf(theSteps) + produced;
  }
  const unfinished = provideUnfinishedRows();
  // What the pipeline produces is what its rows are checked against, so a change to it runs the preview again.
  let signature = $derived(signatureOf(unfinished.hold(steps).steps) + JSON.stringify(produces));
  let editSignature = $derived(editSignatureOf(pipelineName, steps, produces));
  let savedSignature = $state('');
  // What `savedSignature` names, kept whole (not just its signature) so
  // discarding an edit has something to revert to. Cloned in and out: `steps`
  // is mutated in place as the editor is used, and a shared reference would
  // let that mutate the very snapshot discarding is meant to fall back on.
  let savedPipelineName = $state('');
  let savedSteps: Step[] = $state([]);
  let savedProduces: string[] = $state([]);
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
  function snapshotFor(name: string, theSteps: Step[], theProduces: string[]) {
    return {
      value: JSON.stringify({ pipelineName: name, steps: theSteps, produces: theProduces }),
      signature: editSignatureOf(name, theSteps, theProduces),
    };
  }

  /** A held draft. A draft held by a build before Produces has none. */
  type Draft = { pipelineName: string; steps: Step[]; produces?: string[] };

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
      produces = draft.produces ?? [];
      return;
    }
    const rows = diffSteps(steps, draft.steps);
    if (draft.pipelineName !== pipelineName) {
      rows.unshift({
        kind: 'changed',
        summary: `renamed from "${pipelineName}" to "${draft.pipelineName}"`,
      });
    }
    const heldProduces = draft.produces ?? [];
    if (JSON.stringify(heldProduces) !== JSON.stringify(produces)) {
      rows.unshift({
        kind: 'changed',
        summary: `produces ${heldProduces.length > 0 ? heldProduces.join(', ') : 'no schema'}`,
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
      ...snapshotFor(pipelineName, steps, produces),
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
  let ownSchemas: SchemaDto[] = $state([]);
  // A group runs with `shared/` merged in, so a `call` step may target a
  // shared pipeline and a lookup step read a shared table. The leader merges
  // them into a group's preview itself, so they are offered, never sent.
  let sharedPipelines: PipelineDto[] = $state([]);
  let sharedLookupTables: LookupTableDto[] = $state([]);
  let sharedGrokPatterns: GrokPatternDto[] = $state([]);
  let sharedSchemas: SchemaDto[] = $state([]);
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
      ownSchemas = res.config.schemas ?? [];
    }
    if (shared) {
      sharedPipelines = shared.pipelines;
      sharedLookupTables = shared.lookupTables;
      sharedGrokPatterns = shared.grokPatterns ?? [];
      sharedSchemas = shared.schemas ?? [];
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
      produces = [];
      loading = false;
      loadFailed = false;
      // Literals, not a read-back of `pipelineName`/`steps`: reading them
      // here, right after this same effect just wrote them, would make the
      // effect depend on its own output and re-run itself indefinitely.
      savedSignature = editSignatureOf('New pipeline', [], []);
      savedPipelineName = 'New pipeline';
      savedSteps = [];
      savedProduces = [];
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
          const loadedProduces = res.pipeline.produces ?? [];
          pipelineName = res.pipeline.name;
          steps = loadedSteps;
          produces = loadedProduces;
          savedSignature = editSignatureOf(res.pipeline.name, loadedSteps, loadedProduces);
          savedPipelineName = res.pipeline.name;
          savedSteps = $state.snapshot(loadedSteps);
          savedProduces = [...loadedProduces];
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
    const id = jumpToStepId;
    jumpToStepId = null;
    // Opened first, so the scroll lands on the card as it will stand.
    opening.id = id;
    void tick().then(() =>
      document
        .getElementById(`step-${id}`)
        ?.scrollIntoView({ behavior: 'smooth', block: 'center' }),
    );
  });

  /** Reverts to what was last loaded or saved, dropping every edit since --
   * the save bar's own way out of "unsaved", short of a page refresh. */
  function discard() {
    pipelineName = savedPipelineName;
    steps = $state.snapshot(savedSteps);
    produces = [...savedProduces];
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
      const submittedProduces = [...produces];
      const targetId = pipelineId;
      const res = await putPipeline(targetId, {
        id: targetId,
        name: submittedName,
        steps: submittedSteps.map(stepToDto),
        ...(submittedProduces.length > 0 ? { produces: submittedProduces } : {}),
      });
      if (res.ok) {
        loadedId = targetId;
        saveMessage = 'Saved.';
        savedSignature = editSignatureOf(submittedName, submittedSteps, submittedProduces);
        savedPipelineName = submittedName;
        savedSteps = $state.snapshot(submittedSteps);
        savedProduces = submittedProduces;
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
    let parsed: { pipelineName: string; steps: Step[]; produces: string[] };
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
      ...(parsed.produces.length > 0 ? { produces: parsed.produces } : {}),
    });
    if (res.ok) {
      pipelineName = parsed.pipelineName;
      steps = parsed.steps;
      produces = parsed.produces;
      loadedId = targetId;
      savedSignature = editSignatureOf(parsed.pipelineName, parsed.steps, parsed.produces);
      savedPipelineName = parsed.pipelineName;
      savedSteps = $state.snapshot(parsed.steps);
      savedProduces = [...parsed.produces];
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
      {
        id: pipelineId,
        name: pipelineName,
        steps: steps.map(stepToDto),
        ...(produces.length > 0 ? { produces } : {}),
      },
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
  // The name boxes and the Fields menu offer the produced schemas' fields, each marked where the record holds it.
  const produced = provideProducedSchemas();
  $effect(() => {
    const declared = [...sharedSchemas, ...ownSchemas];
    produced.setSchemas(produces.flatMap((sid) => declared.filter((s) => s.id === sid).slice(-1)));
  });
  $effect(() => {
    const d = lastGoodResult?.dictionary;
    produced.held = new Set(
      [...(d?.compiled.fields ?? []), ...(d?.sampled?.fields ?? [])].map(([path]) => path),
    );
  });
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
    return stored(sampleKey(pipelineId)) ?? '';
  }

  async function chooseSample(id: string) {
    const pipeline = untrack(() => pipelineId);
    selectedSample = id;
    if (id) {
      lastUsed = id;
      store(sampleKey(pipeline), id);
    }
    sampleRows = [];
    // The rows on screen were the last sample's.
    lastGoodResult = null;
    lastSample = '';
    records?.clearFind();
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
  /** The preview's warnings no step shows: a field the schemas it produces require and no step writes. */
  let unplacedWarnings = $derived.by(() => {
    if (!result?.ok || !result.warnings) return [];
    const placed = new Set([...warningsByStep.values()].flat());
    return result.warnings.filter((w) => !placed.has(w));
  });
  /** The preview's errors no step shows. */
  let unplacedErrors = $derived.by(() => {
    if (!result || result.ok) return [];
    const placed = new Set([...stepListErrors.values()].flat());
    return result.errors.filter((e) => !placed.has(e));
  });

  /** One `/api/areas/:area/preview/pipeline` call over `stepsSubset`: the whole tree for a
   * Run, a truncated one for each side of a per-step diff. Goes through
   * `previewPipeline`, like every other config call, so it carries the
   * branch being edited. The rows still being written stay out of it.
   *
   * `at` pins `now()`, which matters for the two sides of a diff:
   * without it each side reads its own clock, and a `parseTime` given no
   * reference of its own -- a yearless syslog stamp takes its year from
   * `now()` -- can land on a different instant either side and be drawn as
   * a change the step never made. */
  async function fetchPreview(
    stepsSubset: Step[],
    sample: string,
    {
      at,
      filter,
      attribution,
    }: {
      /** Microseconds since the epoch. */
      at?: number;
      /** An expression the rows must meet (`PreviewRequest.filter`). */
      filter?: string;
      /** Which steps touched, dropped and failed each row. */
      attribution?: boolean;
    } = {},
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
        ownSchemas,
        produces,
      ),
    );
    const res = await previewPipeline({
      config,
      pipeline: pipelineId,
      sample: sample || undefined,
      // Always: whether an internal field is shown is the Columns choice.
      showInternal: true,
      atMicros: at,
      filter,
      attribution,
    });
    return { ...res, errors: res.errors.map(held.restore) };
  }

  /** Previews of the record before a step, by the steps ahead of it and the sample: a step's own edits, which leave
   * those alone, ask nothing. */
  const recordsBefore = new Map<string, Promise<DictionaryOut | null>>();

  /** The record before `step`, from a preview of the steps ahead of it over the sample, as the step diff runs it. */
  async function readRecordBefore(step: Step): Promise<DictionaryOut | null> {
    const before = truncateSteps(steps, step, false);
    if (!before) return null;
    const key = `${lastSample}\n${JSON.stringify(unfinished.hold(before).steps)}`;
    let asked = recordsBefore.get(key);
    if (!asked) {
      if (recordsBefore.size > 32) recordsBefore.clear();
      asked = fetchPreview(before, lastSample).then(
        (res) => {
          // A failed preview, or a refused request, is asked again.
          if (!res.ok || !res.dictionary) recordsBefore.delete(key);
          return res.ok ? (res.dictionary ?? null) : null;
        },
        () => (recordsBefore.delete(key), null),
      );
      recordsBefore.set(key, asked);
    }
    const d = await asked;
    if (!d) return null;
    // The dictionary holds the field a row's exceptions are tagged in, which the record only gets as it leaves. The
    // editor previews under no group's answers, so that field is `TAG_FIELD`.
    const own = (d: Dictionary): Dictionary => ({
      ...d,
      fields: d.fields.filter(([path]) => path !== TAG_FIELD && !path.startsWith(`${TAG_FIELD}.`)),
    });
    return { compiled: own(d.compiled), sampled: d.sampled && own(d.sampled) };
  }

  // Bumped as the steps and the sample settle, so a step showing the record before it asks again.
  let recordsSettled = $state(0);
  $effect(() => {
    void [JSON.stringify(steps), lastSample];
    const timer = setTimeout(() => recordsSettled++, RECOMPUTE_MS);
    return () => clearTimeout(timer);
  });
  let recordBefore = $derived.by(() => {
    void recordsSettled;
    return (step: Step) => readRecordBefore(step);
  });

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
          ownSchemas,
          produces,
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
      const res = await fetchPreview(steps, sample, { attribution: true });
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
  $effect(() => {
    const key = signature;
    void selectedSample;
    // A run before the scope's schemas load finds the produced ones missing: a standard's takes seconds to load.
    void ownSchemas;
    // A viewer is shown the steps alone.
    if (readOnly || !autoRun || key === '') return;
    const timer = setTimeout(() => void run(), RECOMPUTE_MS);
    return () => clearTimeout(timer);
  });

  // -- the divider ----------------------------------------------------------
  //
  // The steps column takes half the width when the work is the steps and is
  // narrow when it is inside one of them. Drag it, or double-click to snap
  // between the two. `null` is the half. The width is the reader's, remembered in this browser.

  const STEPS_NARROW = 240;
  const STEPS_MIN = 200;
  const STEPS_WIDTH_KEY = 'smeltr-workspace-steps-width';
  let stepsWidth = $state<number | null>(Number(stored(STEPS_WIDTH_KEY)) || null);
  $effect(() => store(STEPS_WIDTH_KEY, stepsWidth === null ? null : String(stepsWidth)));

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

  /** The step ◎ paused at. */
  let pauseChoice: Step | null = $state(null);
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
    const run = pauseChoice && stepsRun(steps, pauseChoice, true);
    if (pauseChoice && run && !(brokenStep && run.has(brokenStep)))
      return {
        step: pauseChoice,
        number: flat.indexOf(pauseChoice) + 1,
        run,
        before: truncateSteps(steps, pauseChoice, false)!,
        after: truncateSteps(steps, pauseChoice, true)!,
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

  let pauseRun = $state<PauseRun | null>(null);
  let pauseLoading = $state(false);
  let errorsOpen = $state(false);
  /** A failed stage as the error chip names it. */
  const stageName = (stage: string | null) =>
    stage ? stage[0].toUpperCase() + stage.slice(1) : 'Preview';
  let pauseError = $state('');

  function onPause(step: Step | null) {
    pauseChoice = step;
  }

  let pauseAsked = 0;
  async function loadPause(pause: Pause | null, sample: string) {
    pauseError = '';
    const asked = ++pauseAsked;
    if (!pause || !sample) {
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
        fetchPreview(before, sample, { at }),
        fetchPreview(after, sample, { at, attribution: true }),
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

  // Run as the steps settle, as the whole pipeline's preview is.
  $effect(() => {
    const [p, sample] = [pause, lastSample];
    const timer = setTimeout(() => void loadPause(p, sample), RECOMPUTE_MS);
    return () => clearTimeout(timer);
  });

  // A step deleted while the records are paused after it takes the pause with it.
  $effect(() => {
    if (pauseChoice && !flatSteps(steps).includes(pauseChoice)) pauseChoice = null;
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
      scope: c.area,
      group: c.area === SHARED_SCOPE ? '' : c.area,
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

  /** Writes `path` in the last open eval step, or in a new one at the end, and opens that step for its value. */
  function assignField(path: string) {
    const assignment = { target: path, expr: '' };
    let step = steps.findLast((s) => s.type === 'eval' && loadExpanded(s.id));
    if (step?.type === 'eval') step.assignments.push(assignment);
    else {
      step = { ...newStep('eval'), assignments: [assignment] } as Step;
      saveExpanded(step.id, true);
      steps.push(step);
    }
    jumpToStepId = step.id;
  }

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

  /** The records pane, which the step list's counts add a find chip to. */
  let records = $state<RecordsPane | undefined>();

  /** The rows the grid shows: the pause's, once run, or else the whole pipeline's. */
  let gridRows = $derived(pause && pauseRun ? pauseRun.after : (lastGoodResult?.rows ?? null));

  // Which steps touched each field of a row, dropped it and raised its
  // exceptions: the grid's own run carries it, by row.
  let attribution = $derived(gridRows ? new Map(gridRows.map((r) => [r.row, r])) : null);

  /** Each step by id, with its number in the step list. */
  let stepNumbers = $derived(
    new Map(flatSteps(steps).map((step, i) => [step.id, { step, number: i + 1 }])),
  );

  /** How many of the records shown each step dropped and failed. */
  let stepCounts = $derived.by(() => {
    const counts = new Map<Step, { dropped: number; failed: number }>();
    const of = (id: string | undefined) => {
      const step = id ? stepNumbers.get(id)?.step : undefined;
      if (!step) return null;
      let c = counts.get(step);
      if (!c) counts.set(step, (c = { dropped: 0, failed: 0 }));
      return c;
    };
    for (const r of gridRows ?? []) {
      for (const id of new Set(r.exceptions.map((e) => e.step))) {
        const c = of(id);
        if (c) c.failed++;
      }
      const c = of(attribution?.get(r.row)?.droppedBy);
      if (c) c.dropped++;
    }
    return counts;
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
          {#snippet face()}<StateMark state="error" />
            {stageName(failed.stage)} failed · {failed.errors.length} ▾{/snippet}
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
          onremove={pause.chosen ? () => onPause(null) : undefined}
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
    <PipelineProduces
      bind:produces
      declared={[...sharedSchemas, ...ownSchemas]}
      findings={result?.ok ? (result.schema_findings ?? 0) : undefined}
      {readOnly}
    />

    {#if moved}
      {@const held = moved.value}
      <HeldEdit
        what="this pipeline"
        rows={moved.rows}
        onrestore={() => {
          const draft = JSON.parse(held) as Draft;
          pipelineName = draft.pipelineName;
          steps = draft.steps;
          produces = draft.produces ?? [];
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
            pausedAt={pause?.step ?? null}
            ran={pause?.run ?? null}
            counts={stepCounts}
            onCount={(step, kind) =>
              records?.addChip(
                kind === 'dropped'
                  ? { kind: 'droppedBy', step: step.id }
                  : { kind: 'failedAt', step: step.id },
              )}
            {onPause}
            onPropose={readOnly ? undefined : proposeFields}
            proposal={openProposal}
            {askAt}
            {recordBefore}
          />
        </div>

        {#if unplacedWarnings.length > 0}
          <div class="unplaced-warnings" role="status">
            {#each unplacedWarnings as w, n (n)}
              <div class="unplaced-warning">
                <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
                  <path d="M12 3.5 L22 20.5 H2 Z" fill="currentColor" />
                  <rect x="11" y="9" width="2" height="6" rx="1" class="cutout" />
                  <rect x="11" y="16.5" width="2" height="2" rx="1" class="cutout" />
                </svg>
                <span>{w.message}</span>
              </div>
            {/each}
          </div>
        {/if}

        {#if result?.ok && result.recommended?.length}
          <div class="recommended" role="status">
            {#each result.recommended as told (told)}
              <div class="recommended-note">
                <span aria-hidden="true">ⓘ</span>
                <span>{told}</span>
              </div>
            {/each}
          </div>
        {/if}

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

        <RecordsPane
          bind:this={records}
          {pipelineId}
          {steps}
          {signature}
          {sampleRows}
          {selectedSample}
          {lastSample}
          {result}
          {lastGoodResult}
          {gridRows}
          {attribution}
          {stepNumbers}
          {pause}
          {pauseRun}
          declared={{ before: sampleDeclared, after: runDeclared }}
          canLock={canLockTest}
          onlock={goLockTest}
          onjump={(step) => (jumpToStepId = step.id)}
          onassign={readOnly ? undefined : assignField}
          preview={(steps, sample, filter) => fetchPreview(steps, sample, { filter })}
        >
          {#snippet head()}<InlineError message={proposeError} />{/snippet}
          {#snippet sampleMenu()}
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
          {/snippet}
          {#snippet stage()}{@render stageChips()}{/snippet}
        </RecordsPane>
      {/if}
    </div>
    {#if !readOnly}
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
    {/if}
  </SectionShell>
  <SaveBar
    dirty={isDirty || neverSaved}
    {saving}
    onsave={save}
    ondiscard={isDirty ? discard : undefined}
  />
{/if}

<style>
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
     half. */
  .editor {
    min-width: 0;
    overflow-x: auto;
  }

  /* A warning about the whole pipeline, under its steps: the step-placed ones' look, in a box of its own. */
  .unplaced-warnings {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    margin-top: 0.6rem;
    padding: 0.55rem 0.7rem;
    background: color-mix(in oklab, var(--warn) 12%, var(--panel));
    border-radius: var(--radius);
  }

  .unplaced-warning {
    display: flex;
    gap: 0.5rem;
    align-items: flex-start;
    font-size: var(--text-note);
    color: var(--warn);
    line-height: 1.45;
  }

  .unplaced-warning svg {
    width: 15px;
    height: 15px;
    flex-shrink: 0;
    margin-top: 0.1rem;
  }

  .unplaced-warning .cutout {
    fill: var(--field);
  }

  /* What the schemas recommend and the rows lack: told under the warnings, and no warning itself. */
  .recommended {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    margin-top: 0.4rem;
    padding: 0.55rem 0.7rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }

  .recommended-note {
    display: flex;
    gap: 0.5rem;
    align-items: flex-start;
    font-size: var(--text-note);
    color: var(--dim);
    line-height: 1.45;
  }
</style>
