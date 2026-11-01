<!--
  Branches / review: one branch's change, the diff it carries, and the two
  ways it ends -- closed, keeping the branch, or approved, which squash-merges
  onto main and deletes it. Merging is deploying; there is no separate step.
-->
<script lang="ts">
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import Locked from './Locked.svelte';
  import {
    listBranches,
    bringCurrent,
    changeTimeline,
    commitDiff,
    deployChange,
    history,
    retitleChange,
    leaveBranch,
    listTests,
    branchSchemaFindings,
    runTests,
    errorText,
    scopedKey,
    SAVED_EVENT,
    SHARED_SCOPE,
    MAIN_BRANCH,
    type BranchDto,
    type MergeConflicts,
    type MergeSide,
    type ChangeDto,
    type SchemaFindingDto,
    type ScopedImpact,
    type TestResult,
    type TestSummary,
    type TimelineEvent,
  } from './api';
  import { reviews, testsTally } from './review';
  import { ago, count, shortCommit, pathParts } from './time';
  import ShowAll from './ShowAll.svelte';
  import { nameOf, pipelineNamesAt, type PipelineNames } from './pipelineNames';
  import SectionShell from './SectionShell.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import PageHeader from './PageHeader.svelte';
  import PencilIcon from './PencilIcon.svelte';
  import { ask } from './Confirm.svelte';
  import Help from './Help.svelte';
  import ConflictChoices from './ConflictChoices.svelte';
  import PushState from './PushState.svelte';
  import ChangeConversation from './ChangeConversation.svelte';
  import ChangeTests from './ChangeTests.svelte';
  import ChangeSchemas from './ChangeSchemas.svelte';
  import ChangeDock from './ChangeDock.svelte';
  import { lessMotion } from './glide';
  import ChangeObjects from './ChangeObjects.svelte';
  import { scopeOf, type ObjectChange } from './objectChanges';
  import { deployBlocker } from './access';
  import { threads } from './threads';
  import { newSideLines, parseUnifiedDiff } from './unifiedDiff';

  let { branch }: { branch: string } = $props();

  type Loaded = { branch: BranchDto; change: ChangeDto };

  let loading = $state(true);
  let loadFailed = $state(false);
  let notFound = $state(false);
  let noChange = $state(false);
  let loaded = $state<Loaded | null>(null);
  /** Whoever asked to see every object the change touches, not the first three. */
  let allTouched = $state(false);
  let touched = $derived(
    pathParts(loaded?.branch.touches ?? [], pipelineFileLabel, allTouched ? Infinity : 3),
  );
  /** The remote's URL while one is set. The change is then merged on the host, not approved here. */
  let remote = $state<string | undefined>(undefined);
  // The commit a rollback holds the fleet on, or null. An approval then merges and does not deploy.
  let heldAt = $state<string | null>(null);
  /** The repository's page on the host: the remote URL without its `.git`. */
  let remotePage = $derived(remote?.replace(/\.git$/, ''));
  /** Approve pushes to the remote's main, where one is set. */
  let approvePushes = $state(false);
  /** Whether Approve is offered: always without a remote, and with one only when it pushes to the remote's main. */
  let approvable = $derived(!remote || approvePushes);

  let renaming = $state(false);
  let renameTitle = $state('');
  let renameBody = $state('');
  let renameBusy = $state(false);
  let renameError = $state('');

  let bringingCurrent = $state(false);
  let mergeConflicts = $state<MergeConflicts | null>(null);

  let deploying = $state(false);

  /** Which part of the change the left column shows. */
  const TABS = ['conversation', 'saves', 'tests', 'schemas'] as const;
  let tab = $state<(typeof TABS)[number]>('conversation');
  let dockHeight = $state(0);
  let subtabs = $state<HTMLElement>();

  /** Opens a tab from the dock, bringing its row into view: the dock stays put while the page scrolls. */
  function openTab(t: (typeof TABS)[number]) {
    tab = t;
    subtabs?.scrollIntoView({ block: 'start', behavior: lessMotion() ? 'auto' : 'smooth' });
  }

  let events = $state<TimelineEvent[]>([]);
  let timelineLoading = $state(true);
  let timelineFailed = $state(false);
  let saves = $derived(events.filter((e) => e.kind === 'saved'));
  let reviewState = $derived(reviews(events));
  let threadList = $derived([...threads(events).values()]);

  /** The objects the whole change touched, as the objects pane last read them. */
  let objects = $state<ObjectChange[]>([]);
  /** The whole change's new-side lines, read once a thread sits on a line. */
  let lines = $state<Map<string, Map<number, string>>>(new Map());
  let linesAt = '';

  async function loadLines(from: string, to: string) {
    const at = `${from}..${to}`;
    if (linesAt === at) return;
    linesAt = at;
    try {
      const res = await commitDiff(from, to);
      if (linesAt === at) lines = newSideLines(parseUnifiedDiff(res.diff));
    } catch {
      // A quote without its line is still a quote: the path and number stand.
      if (linesAt === at) linesAt = '';
    }
  }

  $effect(() => {
    const b = loaded?.branch;
    if (b && threadList.some((t) => t.anchor?.kind === 'line')) void loadLines(b.base, b.head);
  });

  async function loadTimeline() {
    timelineFailed = false;
    try {
      const res = await changeTimeline(branch);
      events = res.events ?? [];
      if (!res.ok) timelineFailed = true;
    } catch (e) {
      timelineFailed = true;
      reported(e);
    } finally {
      timelineLoading = false;
    }
  }

  $effect(() => {
    // A save from any page lands on this branch as a timeline event.
    const again = () => void loadTimeline();
    addEventListener(SAVED_EVENT, again);
    return () => removeEventListener(SAVED_EVENT, again);
  });

  // What this change reaches, one entry per scope it reaches anything in,
  // and what its tests (if any) made of it -- kept apart from `results`
  // because something here with no result covering it is reached and
  // untested. A change can reach more than one scope (a `shared/` edit
  // reaches every group), so this runs across the change's own scopes
  // (`runTests`'s doc), never the chip's.
  let touchedByScope = $state<ScopedImpact[]>([]);
  let testResults = $state<TestResult[]>([]);
  // Every test's summary on this branch, by `scopedKey(scope, id)` -- an id
  // is unique only within a scope, and a result's own `pipeline` is absent
  // for an end-to-end test, so which pipelines (and routes) it covers is
  // read from `TestSummary.pipelines`/`.routes` instead, the same derived
  // lists `resultsFor` groups by.
  let testSummaryByKey = $state<Map<string, TestSummary>>(new Map());
  let testsLoading = $state(true);
  let testsError = $state('');
  let tally = $derived(testsTally(testResults));

  /** Every pipeline's name by its id, per scope this change touches, on both sides of it -- `main`'s and the
   * branch's, which wins, so a pipeline it adds, renames or deletes is named. */
  let namesByScope = $state<Map<string, PipelineNames>>(new Map());

  async function loadNames(touches: string[]) {
    const scopes = [...new Set(touches.map(scopeOf))];
    const sides = await Promise.all(
      scopes.map((scope) =>
        Promise.all([pipelineNamesAt(MAIN_BRANCH, scope), pipelineNamesAt(branch, scope)]),
      ),
    );
    namesByScope = new Map(scopes.map((scope, i) => [scope, { ...sides[i][0], ...sides[i][1] }]));
  }

  /** What a reader calls pipeline `id` of `scope`: its name from the configs, or from the scope's
   * `Impact.pipelineNames`, which also names one the change reaches in a scope it does not touch. */
  function pipelineName(scope: string, id: string): string {
    const reached = touchedByScope.find((si) => si.area === scope)?.pipelineNames ?? {};
    return nameOf({ ...reached, ...namesByScope.get(scope) }, id);
  }

  /** A pipeline id as its file's stem, by `bundle.rs`'s `slugify`, which the path is written with. */
  function slugify(id: string): string {
    const slug = id
      .replace(/[^A-Za-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .toLowerCase();
    return slug === '' ? 'pipeline' : slug;
  }

  /** A pipeline file as `pipeline <name>`, or undefined for any other file or a pipeline not yet named. The path
   * names the pipeline by its id's slug, so the name is found by matching that slug among the ids named in the
   * path's own scope, the configs' first and then `Impact.pipelineNames` -- one slug is one pipeline in a bundle
   * (`slugged_pipelines`). */
  function pipelineFileLabel(path: string): string | undefined {
    const file = /^(?:groups\/([^/]+)\/|shared\/)?pipelines\/(.+)\.json$/.exec(path);
    if (!file) return undefined;
    const scope = file[1] ?? SHARED_SCOPE;
    const named = [
      ...Object.entries(namesByScope.get(scope) ?? {}),
      ...touchedByScope
        .filter((si) => si.area === scope)
        .flatMap((si) => Object.entries(si.pipelineNames ?? {})),
    ];
    const hit = named.find(([id]) => slugify(id) === file[2]);
    return hit === undefined ? undefined : `pipeline ${hit[1]}`;
  }

  let schemaFindings = $state<SchemaFindingDto[]>([]);
  /** How many of them are findings: recommended fields the rows lack are shown, not counted. */
  let schemaFound = $derived(schemaFindings.filter((f) => f.breaks).length);
  let schemasLoading = $state(true);
  let schemasError = $state('');

  async function loadSchemaFindings() {
    schemasLoading = true;
    schemasError = '';
    try {
      const res = await branchSchemaFindings(branch);
      if (!res.ok) schemasError = errorText(res.errors);
      else schemaFindings = res.findings;
    } catch (e) {
      schemasError = unreported(e);
    } finally {
      schemasLoading = false;
    }
  }

  async function loadTests() {
    testsLoading = true;
    testsError = '';
    try {
      const [runRes, listRes] = await Promise.all([
        runTests({ touched: true }, branch),
        listTests(branch),
      ]);
      if (!runRes.ok) {
        testsError = errorText(runRes.errors ?? []);
        return;
      }
      touchedByScope = runRes.touched ?? [];
      testResults = runRes.results;
      testSummaryByKey = new Map(listRes.tests.map((t) => [scopedKey(t.area, t.id), t]));
    } catch (e) {
      testsError = unreported(e);
    } finally {
      testsLoading = false;
    }
  }

  async function load() {
    loading = true;
    loadFailed = false;
    notFound = false;
    noChange = false;
    loaded = null;
    try {
      const res = await listBranches();
      remote = res.remote;
      approvePushes = res.approvePushes;
      void history(1)
        .then((h) => (heldAt = h.held?.rolledBack ? (h.deployed ?? null) : null))
        .catch(() => (heldAt = null));
      const found = res.branches.find((b) => b.name === branch);
      if (!found) {
        notFound = true;
        return;
      }
      if (!found.change) {
        noChange = true;
        return;
      }
      loaded = { branch: found, change: found.change };
      timelineLoading = true;
      loadTimeline();
      void loadNames(found.touches);
      loadTests();
      loadSchemaFindings();
    } catch (e) {
      loadFailed = true;
      reported(e);
    } finally {
      loading = false;
    }
  }
  load();

  /** The branch moved under this page (an undo landed): read it again
   * without unloading what is on screen. */
  async function refresh() {
    try {
      const found = (await listBranches()).branches.find((b) => b.name === branch);
      if (!found?.change) return load();
      loaded = { branch: found, change: found.change };
      loadTests();
      loadSchemaFindings();
    } catch (e) {
      loadFailed = true;
      reported(e);
    }
  }

  /** Why Approve & deploy is disabled, or '' when it isn't -- also the
   * button's title, so the reason is never hidden. */
  let deployDisabledReason = $derived.by(() => {
    if (!loaded) return 'No change is open on this branch.';
    if (loaded.change.state === 'closed') return 'This change is closed.';
    if (loaded.branch.behind > 0) {
      return `Behind main by ${count(loaded.branch.behind, 'commit')} — bring the branch current first.`;
    }
    return deployBlocker(loaded.branch.touches);
  });

  /** Seeded from the change itself: renaming starts from what is there,
   * which is usually the generated text needing one word changed. */
  function startRename() {
    if (!loaded) return;
    renameTitle = loaded.change.title;
    renameBody = loaded.change.description;
    renaming = true;
  }

  async function confirmRename() {
    if (renameTitle.trim() === '') return;
    renameBusy = true;
    renameError = '';
    try {
      const res = await retitleChange(branch, renameTitle.trim(), renameBody);
      if (res.ok) {
        renaming = false;
        await load();
      } else {
        renameError = errorText(res.errors);
      }
    } catch (e) {
      renameError = unreported(e);
    } finally {
      renameBusy = false;
    }
  }

  async function confirmBringCurrent(sides?: MergeSide[]) {
    bringingCurrent = true;
    try {
      const res = await bringCurrent(
        branch,
        sides && mergeConflicts ? { conflicts: mergeConflicts, sides } : undefined,
      );
      if (res.ok) {
        mergeConflicts = null;
        await load();
      } else if (res.conflicts) {
        mergeConflicts = res.conflicts;
      } else {
        // The conflicts shown are no longer the question (main or the branch moved), so Bring current asks again.
        mergeConflicts = null;
      }
    } catch (e) {
      reported(e);
    } finally {
      bringingCurrent = false;
    }
  }

  async function confirmDeploy() {
    if (!loaded || deployDisabledReason !== '') return;
    const ok = await ask({
      title: heldAt ? 'approve' : 'approve & deploy',
      body: `Approve ${heldAt ? '' : 'and deploy '}"${loaded.change.title}".`,
      help:
        'This squash-merges the change onto main' +
        (remote ? `, pushes it to the remote's main ` : ' ') +
        `and deletes the branch ${branch}` +
        (heldAt
          ? `. The fleet is held at ${shortCommit(heldAt)} by a rollback, so it does not deploy until main's head is released from History.`
          : ' — merging is deploying, so there is no separate deploy step.'),
      action: heldAt ? 'Approve' : 'Approve & deploy',
    });
    if (!ok) return;
    deploying = true;
    try {
      const res = await deployChange(branch);
      if (res.ok) {
        // Approving merged this branch and deleted it, so nothing may stay
        // pointed at it.
        const moved = leaveBranch(branch);
        // Asked after the approve, not taken from when the page opened: a held
        // fleet does not move, so there is no rollout to watch.
        const after = await history(1).catch(() => null);
        location.hash =
          res.head && after?.deployed === res.head
            ? `#/status?deploying=${encodeURIComponent(res.head)}`
            : '#/changes/history';
        if (moved) location.reload();
      }
    } catch (e) {
      reported(e);
    } finally {
      deploying = false;
    }
  }
</script>

<SectionShell>
  {#if loading}
    <p class="dim">Loading&hellip;</p>
  {:else}
    {#if loadFailed}
      <!-- The error bar says why. -->
    {:else if notFound}
      <PageHeader title={branch} />
      <p class="dim">There is no branch named <span class="mono">{branch}</span>.</p>
    {:else if noChange}
      <PageHeader title={branch} />
      <p class="dim">
        This branch has no change open on it. <a href="#/changes">All branches</a>
      </p>
    {:else if loaded}
      <div class="split">
        <div class="left">
          <PageHeader title={loaded.change.title}>
            {#snippet beside()}
              <Locked refusedInDemo>
                <button
                  type="button"
                  class="icon pencil"
                  onclick={startRename}
                  aria-label="Rename this change"
                  title="Rename"
                >
                  <PencilIcon />
                </button>
              </Locked>
            {/snippet}
          </PageHeader>
          <div class="dim meta">
            <span class="state-pill" class:closed={loaded.change.state === 'closed'}
              >{loaded.change.state === 'closed' ? 'Closed' : 'Open'}</span
            >
            {#if loaded.change.author}<span class="who">{loaded.change.author}</span>
              {loaded.change.state === 'closed' ? 'proposed ' : 'wants to deploy '}{/if}{count(
              loaded.branch.commits,
              'save',
            )} from <span class="mono">{branch}</span>
            <span class="sep">&middot;</span>touches {touched.text}{#if touched.more > 0}
              <span class="sep">&middot;</span><ShowAll
                more={touched.more}
                onshowall={() => (allTouched = true)}
              />{/if}
            <span class="sep">&middot;</span><span class="mono"
              >{shortCommit(loaded.branch.head)}</span
            >
          </div>
          {#if remote}
            <div class="dim meta remote">
              <span
                >remote<Help label="remote"
                  >The leader pushes every save here, follows the host's
                  <span class="mono">main</span> and deploys what lands there.</Help
                ></span
              >
              <a class="mono" href={remotePage} target="_blank" rel="noopener noreferrer"
                >{remotePage}</a
              >
              <PushState branch={loaded.branch} onoverwritten={load} />
            </div>
          {/if}

          {#if renaming}
            <div class="card rename-card">
              <div class="row card-head">
                <h2>
                  Rename this change<Help label="Rename this change"
                    >When the change deploys, its title and description become its entry in History:
                    the title as the commit subject on
                    <code>main</code>, the description as its body.</Help
                  >
                </h2>
              </div>
              <div class="card-body">
                <input aria-label="change title" bind:value={renameTitle} />
                <textarea rows="6" aria-label="change description" bind:value={renameBody}
                ></textarea>
                <InlineError message={renameError} />
                <div class="actions">
                  <button type="button" disabled={renameBusy} onclick={() => (renaming = false)}>
                    Cancel
                  </button>
                  <button
                    type="button"
                    class="primary"
                    disabled={renameBusy || renameTitle.trim() === ''}
                    onclick={confirmRename}
                  >
                    {renameBusy ? 'Saving…' : 'Save'}
                  </button>
                </div>
              </div>
            </div>
          {/if}

          <div class="subtabs" bind:this={subtabs}>
            <SecondaryTabs tabs={TABS} active={tab} onselect={(t) => (tab = t)} ariaLabel="Change">
              {#snippet label(t)}
                {#if t === 'conversation'}Conversation
                {:else if t === 'saves'}Saves <span class="pill"
                    >{timelineLoading ? '…' : saves.length}</span
                  >
                {:else if t === 'tests'}Tests <span class="pill"
                    >{testsLoading ? '…' : `${tally.passed}/${tally.total}`}</span
                  >
                {:else}Schemas <span class="pill">{schemasLoading ? '…' : schemaFound}</span>
                {/if}
              {/snippet}
            </SecondaryTabs>
          </div>

          <div role="tabpanel">
            {#if tab === 'conversation'}
              <ChangeConversation
                {branch}
                change={loaded.change}
                {events}
                loading={timelineLoading}
                {objects}
                {lines}
                onposted={loadTimeline}
                onstatechanged={load}
              />
            {:else if tab === 'saves'}
              {#if timelineFailed}
                <!-- The error bar says why. -->
              {:else if timelineLoading}
                <p class="dim">Loading&hellip;</p>
              {:else if saves.length === 0}
                <p class="dim">No saves yet.</p>
              {:else}
                <ul class="saves">
                  {#each saves as save, i (i)}
                    <li>
                      {#if save.commit}<span class="mono dim">{shortCommit(save.commit)}</span>{/if}
                      <span class="subject">{save.text ?? ''}</span>
                      <span class="dim">{save.author} &middot; {ago(save.time)}</span>
                    </li>
                  {/each}
                </ul>
              {/if}
            {:else if tab === 'schemas'}
              <ChangeSchemas
                {branch}
                findings={schemaFindings}
                loading={schemasLoading}
                error={schemasError}
                {pipelineName}
              />
            {:else}
              <ChangeTests
                {branch}
                {touchedByScope}
                {testResults}
                {testSummaryByKey}
                {testsLoading}
                {testsError}
                {pipelineName}
              />
            {/if}
          </div>
        </div>

        <div class="right">
          {#if mergeConflicts}
            <ConflictChoices
              {branch}
              conflicts={mergeConflicts}
              busy={bringingCurrent}
              onapply={(sides) => confirmBringCurrent(sides)}
              oncancel={() => {
                mergeConflicts = null;
              }}
            />
          {/if}

          {#if loaded.change.state === 'closed'}
            <div class="card">
              <div class="row card-head"><h2>Closed</h2></div>
              <div class="card-body">
                {#if loaded.change.reason}
                  <p class="dim">{loaded.change.reason}</p>
                {:else}
                  <p class="dim">Closed without a reason.</p>
                {/if}
              </div>
            </div>
          {/if}

          <ChangeObjects
            branch={loaded.branch}
            {saves}
            {testResults}
            {testSummaryByKey}
            {testsLoading}
            threads={threadList}
            onundone={refresh}
            ontimeline={loadTimeline}
            onobjects={(c) => (objects = c)}
          />
        </div>
      </div>
      <div class="dock-room" style:height="{dockHeight}px"></div>
      <ChangeDock
        reviews={reviewState}
        tests={tally}
        {testsLoading}
        schemaFindings={schemasLoading ? undefined : schemaFound}
        onopen={openTab}
        behind={loaded.branch.behind}
        leftOut={loaded.branch.leftOut?.length ?? 0}
        {bringingCurrent}
        bringCurrentDisabled={bringingCurrent || mergeConflicts !== null}
        onbringcurrent={() => confirmBringCurrent()}
        {approvable}
        {deploying}
        {deployDisabledReason}
        ondeploy={confirmDeploy}
        held={heldAt !== null}
        bind:height={dockHeight}
      />
    {/if}
  {/if}
</SectionShell>

<style>
  h2 {
    margin: 0;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }
  .mono {
    font-family: var(--mono);
  }

  .meta {
    font-size: var(--text-body);
    margin-bottom: 0.4rem;
  }
  .meta.remote {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.2rem 0.6rem;
  }
  .meta .sep {
    margin: 0 0.3rem;
    opacity: 0.6;
  }

  .card {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    margin-bottom: 1.25rem;
    overflow-x: auto;
  }
  .card-head {
    background: var(--bg);
    border-radius: var(--radius) var(--radius) 0 0;
  }
  .card-body {
    padding: 0.7rem 0.9rem;
  }
  .row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.55rem 0.9rem;
    border-bottom: 1px solid var(--border);
  }
  .row:last-child {
    border-bottom: 0;
  }
  .card-head h2 {
    flex-grow: 1;
  }

  /* Acts on the title, so it sits with the title rather than among the
     page's buttons. Centered, because the row is aligned on a baseline the
     glyph has none of. */
  .pencil {
    align-self: center;
  }

  /* The change and what was said about it on the left, what it does on the right. */
  .split {
    display: grid;
    grid-template-columns: minmax(20rem, 2fr) minmax(0, 3fr);
    gap: 1.5rem;
    align-items: start;
  }
  .split > div {
    min-width: 0;
  }
  .left {
    margin-bottom: 1.25rem;
  }
  @media (max-width: 960px) {
    .split {
      grid-template-columns: minmax(0, 1fr);
    }
  }
  .state-pill {
    display: inline-flex;
    align-items: center;
    margin-right: 0.4rem;
    padding: 0.05rem 0.55rem;
    border: 1px solid var(--added);
    border-radius: 999px;
    background: var(--added);
    color: var(--on-accent);
    font-size: var(--text-note);
    font-weight: 600;
  }
  .state-pill.closed {
    border-color: var(--border);
    background: none;
    color: var(--dim);
  }

  .subtabs {
    margin: 1rem 0 0.9rem;
    /* Room above the row when the dock scrolls it into view. */
    scroll-margin-top: 1rem;
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

  .saves {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .saves li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.2rem 0.6rem;
    padding: 0.45rem 0;
    border-bottom: 1px solid var(--border);
    font-size: var(--text-label);
  }
  .saves li:last-child {
    border-bottom: 0;
  }
  .saves .subject {
    flex: 1 1 10rem;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .saves .dim {
    font-size: var(--text-note);
  }

  .meta .who {
    font-weight: 600;
    color: var(--ink);
  }

  .rename-card input {
    width: 100%;
    box-sizing: border-box;
    margin-bottom: 0.5rem;
  }

  .rename-card textarea {
    width: 100%;
    box-sizing: border-box;
    resize: vertical;
    margin: 0.5rem 0;
  }
  .rename-card .actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
  }
</style>
