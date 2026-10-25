<!--
  What a change did, for review: one card per object it touched, or its files
  as JSON. Either view covers the whole change or one save of it. Comment
  threads sit on the object, step or line they name; a thread on something
  this view does not show is listed at its top.
-->
<script lang="ts">
  import InlineError from './InlineError.svelte';
  import { reported } from './errors.svelte';
  import {
    SHARED_SCOPE,
    commitDiff,
    configAt,
    lookupFileAt,
    sampleAt,
    testAt,
    unreadableAt,
    undoChanges,
    errorText,
    errorMessage,
    type BranchDto,
    type TestResult,
    type TestSummary,
    type CommentAnchor,
    type TimelineEvent,
    type UndoTarget,
  } from './api';
  import {
    LOOKUPS,
    SAMPLES,
    csvRowChanges,
    emptyConfig,
    isRecord,
    fileOf,
    isSectionChange,
    jsonDiff,
    objectChanges,
    scopeOf,
    testsCovering,
    undoTarget,
    type FileSides,
    type ObjectChange,
    type RowChange,
    type SectionChange,
    type StepChange,
  } from './objectChanges';
  import { canEdit } from './access';
  import { describe } from './changeText';
  import { parseCsv } from './lookups';
  import { testsTally } from './review';
  import { count, shortCommit } from './time';
  import { ask } from './Confirm.svelte';
  import Segmented from './Segmented.svelte';
  import ChangeFiles from './ChangeFiles.svelte';
  import CommentComposer from './CommentComposer.svelte';
  import ThreadView from './ThreadView.svelte';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';
  import { anchorLabel, elsewhere, threadsOnObject, type Thread } from './threads';

  let {
    branch,
    saves,
    testResults,
    testSummaryByKey,
    testsLoading,
    threads,
    onundone,
    ontimeline,
    onobjects,
  }: {
    branch: BranchDto;
    /** The change's saves, oldest first, as the timeline gives them. */
    saves: TimelineEvent[];
    testResults: TestResult[];
    testSummaryByKey: Map<string, TestSummary>;
    testsLoading: boolean;
    /** The change's comment threads. */
    threads: Thread[];
    /** An undo landed: the branch has a new head. */
    onundone: () => void;
    /** A comment, reply, resolve or reopen landed: the timeline has it. */
    ontimeline: () => Promise<void> | void;
    /** The objects the whole change touched, each time they are read. */
    onobjects: (changes: ObjectChange[]) => void;
  } = $props();

  let view = $state<'objects' | 'json'>('objects');
  /** `all`, or the index of one save in `saves`. */
  let range = $state('all');

  let withCommit = $derived(saves.filter((s) => s.commit));
  /** The two commits the views compare: the base to the head for the whole
   * change, the commit a save was made on to the save for one save. */
  let span = $derived.by(() => {
    const i = range === 'all' ? -1 : Number(range);
    const save = withCommit[i];
    if (!save?.commit) return { from: branch.base, to: branch.head, whole: true };
    return { from: save.parent ?? branch.base, to: save.commit, whole: false };
  });

  let scopes = $derived([...new Set(branch.touches.map(scopeOf))].sort());

  let changes = $state<ObjectChange[]>([]);
  /** A sample no test names, which a deploy leaves out (`BranchDto.scratch`). Only the whole change says, since
   * one save does not deploy. */
  let scratchPaths = $derived(new Set(branch.scratch ?? []));
  function isScratch(c: ObjectChange): boolean {
    return (
      span.whole &&
      c.section === SAMPLES &&
      scratchPaths.has(`${prefixOf(c.scope)}${SAMPLES}/${c.id}.json`)
    );
  }
  let deployed = $derived(changes.filter((c) => !isScratch(c)));
  let notDeployed = $derived(changes.filter(isScratch));
  let loading = $state(true);
  let loadFailed = $state(false);
  let loads = 0;

  async function sideAt(scope: string, at: string) {
    const res = await configAt(branch.name, scope, at);
    // A scope with no bundle at this commit is one the change creates or removes.
    return res.config ?? emptyConfig();
  }

  function prefixOf(scope: string): string {
    return scope === SHARED_SCOPE ? 'shared/' : `groups/${scope}/`;
  }

  /** How the range changed each file, by path: what tells a created or
   * deleted group from an empty one, and which samples and tests changed.
   * The diff names every changed file whatever path it is asked for, so it is
   * asked for a path no change holds, which keeps its text empty. */
  async function filesChanged(from: string, to: string): Promise<Map<string, string>> {
    const res = await commitDiff(from, to, 'system.json');
    // Without it the objects still show; only the group, sample and test cards are missing.
    if (!res.ok) return new Map();
    return new Map(res.files.map((f) => [f.path, f.status]));
  }

  /** What does not read in the scope's own bundle at `at`, by path in it. */
  async function unreadableOf(scope: string, at: string): Promise<Map<string, string>> {
    const res = await unreadableAt(branch.name, scope, at);
    // Without it the objects still show, though a file that broke reads as its objects removed.
    if (!res.ok) return new Map();
    const prefix = prefixOf(scope);
    return new Map(
      res.files
        .filter((f) => f.path.startsWith(prefix))
        .map((f) => [f.path.slice(prefix.length), f.why]),
    );
  }

  /** The scope's lookup files the range changed, by their path in its bundle. */
  function lookupsOf(scope: string, changed: Map<string, string>): Map<string, string> {
    const prefix = `${prefixOf(scope)}${LOOKUPS}/`;
    return new Map(
      [...changed]
        .filter(([path]) => path.startsWith(prefix))
        .map(([path, status]) => [path.slice(prefixOf(scope).length), status]),
    );
  }

  /** The scope's samples and tests the range changed, read at both ends. */
  async function filesOf(
    scope: string,
    changed: Map<string, string>,
    from: string,
    to: string,
  ): Promise<FileSides[]> {
    const prefix = prefixOf(scope);
    const found = [...changed]
      .filter(([path]) => path.startsWith(prefix))
      .flatMap(([path, status]) => {
        const file = fileOf(path.slice(prefix.length));
        return file ? [{ ...file, status }] : [];
      });
    return Promise.all(
      found.map(async ({ kind, id, status }) => {
        // A side the file is absent from, or that does not read, stays unset.
        const read = async (at: string) =>
          kind === SAMPLES
            ? (await sampleAt(branch.name, scope, id, at)).sample
            : (await testAt(branch.name, scope, id, at)).test;
        const [before, after] = await Promise.all([
          status === 'A' ? undefined : read(from),
          status === 'D' ? undefined : read(to),
        ]);
        return { kind, id, status, before, after } as FileSides;
      }),
    );
  }

  async function loadObjects(from: string, to: string) {
    const mine = ++loads;
    loading = true;
    loadFailed = false;
    try {
      const [sides, changed] = await Promise.all([
        Promise.all(
          scopes.map(async (scope) => {
            const [before, after] = await Promise.all([sideAt(scope, from), sideAt(scope, to)]);
            return { scope, before, after };
          }),
        ),
        filesChanged(from, to),
      ]);
      const pairs = await Promise.all(
        sides.map(async (p) => ({
          ...p,
          manifest: changed.get(`${prefixOf(p.scope)}system.json`),
          files: await filesOf(p.scope, changed, from, to),
          lookups: lookupsOf(p.scope, changed),
          unreadable: {
            before: await unreadableOf(p.scope, from),
            after: await unreadableOf(p.scope, to),
          },
        })),
      );
      if (mine !== loads) return;
      changes = objectChanges(pairs);
      if (span.whole) onobjects(changes);
    } catch (e) {
      if (mine !== loads) return;
      loadFailed = true;
      reported(e);
    } finally {
      if (mine === loads) loading = false;
    }
  }

  $effect(() => {
    void loadObjects(span.from, span.to);
  });

  /** The cards showing their JSON, by `scope section id`. */
  let showingJson = $state<Record<string, boolean>>({});
  let undoing = $state<string | null>(null);
  let undoErrors = $state<Record<string, string>>({});

  type CsvSide = { path: string; delimiter: string };

  /** The CSV files a card's rows come from, at each end of the range, each
   * with its own delimiter: a table's file where the change replaced it, or
   * a changed CSV file itself, read with a comma where no table names it. */
  function csvSides(c: ObjectChange): { before?: CsvSide; after?: CsvSide } | null {
    if (c.rows) return null;
    if (c.section === LOOKUPS) {
      if (!c.id.toLowerCase().endsWith('.csv')) return null;
      const side = (delimiter: string | undefined) => ({ path: c.id, delimiter: delimiter ?? ',' });
      return {
        before: c.status === 'added' ? undefined : side(c.delimiters?.before),
        after: c.status === 'removed' ? undefined : side(c.delimiters?.after),
      };
    }
    if (c.section !== 'lookupTables') return null;
    const csv = (t: unknown) => (isRecord(t) && t.type === 'csv' ? t : undefined);
    const [was, now] = [csv(c.before), csv(c.after)];
    if (!was && !now) return null;
    if (was && now && was.path === now.path) return null;
    const side = (t: Record<string, unknown> | undefined) =>
      t && { path: String(t.path), delimiter: String(t.delimiter ?? ',') };
    return { before: side(was), after: side(now) };
  }

  /** Rows are read on asking: a lookup file can be gigabytes. */
  let comparing = $state<Record<string, boolean>>({});
  let compareErrors = $state<Record<string, string>>({});

  async function compareRowsOf(c: ObjectChange) {
    const sides = csvSides(c);
    if (!sides) return;
    const key = keyOf(c);
    comparing = { ...comparing, [key]: true };
    compareErrors = { ...compareErrors, [key]: '' };
    try {
      const read = async (side: CsvSide | undefined, at: string) => {
        if (side === undefined) return [];
        const res = await lookupFileAt(branch.name, c.scope, side.path, at);
        if (!res.ok || res.text === undefined)
          throw new Error(errorText(res.errors) || `${side.path} did not read`);
        return parseCsv(res.text, side.delimiter);
      };
      const [before, after] = await Promise.all([
        read(sides.before, span.from),
        read(sides.after, span.to),
      ]);
      const { headers, rows } = csvRowChanges(before, after);
      c.headers = headers;
      c.rows = rows;
    } catch (e) {
      compareErrors = { ...compareErrors, [key]: errorMessage(e) };
    } finally {
      comparing = { ...comparing, [key]: false };
    }
  }

  function keyOf(c: ObjectChange): string {
    return `${c.scope} ${c.section} ${c.id}`;
  }

  async function undo(c: ObjectChange, what: UndoTarget) {
    const ok = await ask({
      title: 'undo changes',
      body: `Undo the changes to ${c.label}, as a new save on ${branch.name}.`,
      action: 'Undo changes',
      danger: true,
    });
    if (!ok) return;
    const key = keyOf(c);
    undoing = key;
    undoErrors = { ...undoErrors, [key]: '' };
    try {
      const res = await undoChanges(branch.name, c.scope, what, branch.head);
      if (res.ok) {
        onundone();
      } else {
        const said = errorText(res.errors);
        undoErrors = {
          ...undoErrors,
          [key]:
            said || (res.conflict ? 'The branch moved since this page read it.' : 'Not undone.'),
        };
      }
    } catch (e) {
      undoErrors = { ...undoErrors, [key]: errorMessage(e) };
    } finally {
      undoing = null;
    }
  }

  function testsOf(c: ObjectChange) {
    if (c.section !== 'pipelines' || testsLoading) return null;
    const tally = testsTally(testsCovering(testResults, testSummaryByKey, c.scope, c.id));
    return tally.total > 0 ? tally : null;
  }

  /** Rows of an inline table shown before the rest are counted instead. */
  const ROWS_SHOWN = 100;

  /** Each inline table's sort, by `keyOf`, keyed by column index since its
   * columns come from the table itself. Kept across loads, so a table stays
   * sorted while the range moves. */
  const rowSorts = new Map<string, TableSort<string>>();

  function rowSortOf(key: string): TableSort<string> {
    let sort = rowSorts.get(key);
    if (!sort) rowSorts.set(key, (sort = new TableSort<string>()));
    return sort;
  }

  /** The changed rows in their table's sort, before any are cut: a sort
   * orders every changed row, not only the ones first shown. */
  function sortedRows(sort: TableSort<string>, rows: RowChange[], headers: string[]): RowChange[] {
    return sort.apply(
      rows,
      Object.fromEntries(headers.map((_, j) => [String(j), (r: RowChange) => r.cells[j]])),
    );
  }

  function saveLabel(save: TimelineEvent, i: number): string {
    const subject = save.text ?? '';
    const short = subject.length > 48 ? `${subject.slice(0, 47)}…` : subject;
    return `Save ${i + 1} · ${save.commit ? shortCommit(save.commit) : ''}${short ? ` · ${short}` : ''}`;
  }

  function position(n: number | undefined): string {
    return n === undefined ? '' : String(n).padStart(2, '0');
  }

  /** The one composer open, by `keyOf` and the step's id where it is on a step. */
  let composing = $state<string | null>(null);

  function composerKey(c: ObjectChange, step?: string): string {
    return step === undefined ? keyOf(c) : `${keyOf(c)} ${step}`;
  }

  function anchorOf(c: SectionChange, step?: string): CommentAnchor {
    const anchor: CommentAnchor = {
      kind: 'object',
      scope: c.scope,
      section: c.section,
      objectId: c.id,
    };
    if (step !== undefined) anchor.step = step;
    return anchor;
  }

  function toggleComposer(key: string) {
    composing = composing === key ? null : key;
  }

  async function posted() {
    composing = null;
    await ontimeline();
  }

  let away = $derived(view === 'objects' && !loading ? elsewhere(threads, changes) : []);
</script>

{#snippet commentButton(c: SectionChange, name: string, step?: string)}
  {@const key = composerKey(c, step)}
  <!-- A comment is recorded at the head, so only what the head holds takes one. -->
  {#if span.whole && c.status !== 'removed'}
    <button
      type="button"
      class="compact comment-btn"
      aria-label="Comment on {name}"
      aria-expanded={composing === key}
      onclick={() => toggleComposer(key)}>Comment</button
    >
  {/if}
{/snippet}

{#snippet threadsOn(c: SectionChange, step?: string)}
  {@const here = threadsOnObject(threads, c.scope, c.section, c.id, step)}
  {@const key = composerKey(c, step)}
  {#if here.length > 0 || composing === key}
    <div class="cmt">
      {#each here as t (t.id)}
        <ThreadView branch={branch.name} thread={t} onchanged={ontimeline} />
      {/each}
      {#if composing === key}
        <CommentComposer
          branch={branch.name}
          anchor={anchorOf(c, step)}
          oncancel={() => (composing = null)}
          onposted={posted}
        />
      {/if}
    </div>
  {/if}
{/snippet}

{#snippet stepList(c: SectionChange, steps: StepChange[])}
  <div class="steps">
    {#each steps as step, i (i)}
      <div class="step {step.status}">
        <div class="step-head">
          <span class="mono dim">{position(step.position)}</span>
          <span class="type">{step.type}</span>
          {#if step.status === 'same' && step.summary}
            <span class="mono dim inline-summary" title={step.summary}>{step.summary}</span>
          {/if}
          <span class="grow"></span>
          {#if step.status !== 'same'}
            <span class="badge {step.status}">{step.status}</span>
          {/if}
          {#if step.moved}<span class="dim note">moved</span>{/if}
          {#if step.id !== undefined && step.status !== 'removed'}
            {@render commentButton(
              c,
              step.position === undefined
                ? `removed ${step.type} step`
                : `step ${position(step.position)}`,
              step.id,
            )}
          {/if}
        </div>
        {#if step.status !== 'same' && step.summary}
          <div class="step-body mono" class:struck={step.status === 'removed'}>{step.summary}</div>
        {/if}
        {#if step.id !== undefined}
          {@render threadsOn(c, step.id)}
        {/if}
        {#if step.then?.length || step.else?.length}
          <div class="branches">
            {#if step.then?.length}
              <span class="branch-label">then</span>
              {@render stepList(c, step.then)}
            {/if}
            {#if step.else?.length}
              <span class="branch-label">else</span>
              {@render stepList(c, step.else)}
            {/if}
          </div>
        {/if}
      </div>
    {/each}
  </div>
{/snippet}

{#snippet objectCard(c: ObjectChange, scratch: boolean)}
  {@const key = keyOf(c)}
  {@const tests = span.whole ? testsOf(c) : null}
  {@const target = undoTarget(c)}
  <div class="card">
    <div class="obj-head">
      <span class="badge {c.status}">{c.status}</span>
      {#if c.moved}<span class="dim note">moved</span>{/if}
      <span class="label mono">{c.label}</span>
      <span class="dim note kind">{c.noun} &middot; {c.scope}</span>
      <span class="grow"></span>
      {#if tests}
        <span
          class="note"
          class:ok={tests.passed === tests.total}
          class:bad={tests.passed < tests.total}
          >{tests.passed === tests.total ? '✓ ' : ''}{tests.passed}/{tests.total} tests</span
        >
      {/if}
      {#if csvSides(c)}
        <button
          type="button"
          class="compact"
          disabled={comparing[key]}
          onclick={() => compareRowsOf(c)}>{comparing[key] ? 'Comparing…' : 'Compare rows'}</button
        >
      {/if}
      <button
        type="button"
        class="compact"
        aria-pressed={!!showingJson[key]}
        onclick={() => (showingJson = { ...showingJson, [key]: !showingJson[key] })}>JSON</button
      >
      <!-- A group's own card is read only: neither undo nor a comment names a manifest. A scratch sample
        never reaches main, so it has nothing to undo. -->
      {#if target && span.whole && !scratch && canEdit(c.scope)}
        <button
          type="button"
          class="compact"
          disabled={undoing !== null}
          onclick={() => undo(c, target)}>{undoing === key ? 'Undoing…' : 'Undo changes'}</button
        >
      {/if}
      {#if isSectionChange(c)}{@render commentButton(c, c.label)}{/if}
    </div>
    {#if undoErrors[key]}<div class="card-note">
        <InlineError message={undoErrors[key]} />
      </div>{/if}
    {#if compareErrors[key]}<div class="card-note">
        <InlineError message={compareErrors[key]} />
      </div>{/if}
    {#if isSectionChange(c)}{@render threadsOn(c)}{/if}

    {#if showingJson[key]}
      <div class="json-scroll">
        <pre class="json">{#each jsonDiff(c.before, c.after) as line, i (i)}<div
              class={line.kind}>{line.kind === 'added'
                ? '+'
                : line.kind === 'removed'
                  ? '-'
                  : ' '} {line.text}</div>{/each}</pre>
      </div>
    {:else if !scratch}
      {#if c.fields.length > 0}
        <table class="fields">
          <tbody>
            {#each c.fields as f (f.key)}
              <tr>
                <td class="dim">{f.key}</td>
                <td class="mono">
                  {#if f.kind !== 'added'}<span class:struck={f.kind === 'removed'}
                      >{describe(f.before)}</span
                    >{/if}
                  {#if f.kind === 'changed'}<span class="dim"> &rarr; </span>{/if}
                  {#if f.kind !== 'removed'}<span class:added={c.status === 'added'}
                      >{describe(f.after)}</span
                    >{/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
      {#if c.movesSource}
        <p class="dim note card-note">Deploying this moves the source.</p>
      {/if}
      {#if c.steps && isSectionChange(c)}
        {@render stepList(c, c.steps)}
      {/if}
      {#if c.rows}
        {@const moved = c.rows.filter((r) => r.status !== 'same')}
        {@const same = c.rows.length - moved.length}
        {@const rowSort = rowSortOf(key)}
        <div class="rows-scroll">
          <table class="rowtable">
            <thead>
              <tr
                >{#each c.headers ?? [] as h, i (i)}<th aria-sort={rowSort.aria(String(i))}
                    ><SortButton sort={rowSort} key={String(i)}>{h}</SortButton></th
                  >{/each}</tr
              >
            </thead>
            <tbody>
              {#each sortedRows(rowSort, moved, c.headers ?? []).slice(0, ROWS_SHOWN) as row, i (i)}
                <tr class={row.status}>
                  {#each row.cells as cell, j (j)}<td>{cell}</td>{/each}
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
        {#if moved.length > ROWS_SHOWN || same > 0}
          <p class="dim note card-note">
            {#if moved.length > ROWS_SHOWN}{count(moved.length - ROWS_SHOWN, 'more row')} changed{/if}{#if moved.length > ROWS_SHOWN && same > 0}
              &middot;
            {/if}{#if same > 0}{count(same, 'row')} unchanged{/if}
          </p>
        {/if}
      {/if}
    {/if}
  </div>
{/snippet}

<div class="pane">
  <div class="pane-head">
    <Segmented
      label="View"
      options={[
        { value: 'objects', label: 'Objects' },
        { value: 'json', label: 'JSON' },
      ]}
      bind:value={view}
    />
    {#if view === 'objects' && !loading && !loadFailed}
      <span class="dim note"
        >{count(deployed.length, 'object')}{#if notDeployed.length > 0}
          &middot; {notDeployed.length} not deployed{/if}</span
      >
    {/if}
    <select class="saves" aria-label="Saves" bind:value={range}>
      <option value="all">All saves</option>
      {#each withCommit as save, i (save.commit)}
        <option value={String(i)}>{saveLabel(save, i)}</option>
      {/each}
    </select>
  </div>

  {#if away.length > 0}
    <div class="card elsewhere">
      <h2 class="card-note">Elsewhere</h2>
      {#each away as t (t.id)}
        {@const where = t.anchor ? anchorLabel(t.anchor, changes) : null}
        <div class="away">
          {#if where}
            <div class="note">
              <span class="mono">{where.title}</span>
              {#if where.detail}<span class="dim mono"> &middot; {where.detail}</span>{/if}
            </div>
          {/if}
          <ThreadView branch={branch.name} thread={t} onchanged={ontimeline} />
        </div>
      {/each}
    </div>
  {/if}

  {#if view === 'json'}
    <ChangeFiles
      branch={branch.name}
      from={span.from}
      to={span.to}
      whole={span.whole}
      {threads}
      onchanged={ontimeline}
    />
  {:else if loadFailed}
    <!-- The error bar says why. -->
  {:else if loading}
    <p class="dim">Loading&hellip;</p>
  {:else if changes.length === 0}
    <p class="dim">No objects changed.</p>
  {:else}
    {#each deployed as c (keyOf(c))}
      {@render objectCard(c, false)}
    {/each}
    {#if notDeployed.length > 0}
      <div class="card not-deployed">
        <h2 class="card-note">Not deployed</h2>
        <p class="card-note">
          No test uses these samples, so deploying leaves them out. They stay on <code
            >{branch.name}</code
          >, and go when it is deployed.
        </p>
        {#each notDeployed as c (keyOf(c))}
          {@render objectCard(c, true)}
        {/each}
      </div>
    {/if}
  {/if}
</div>

<style>
  .pane {
    display: grid;
    gap: 1rem;
    align-content: start;
  }
  .pane-head {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.8rem;
  }
  .pane-head .saves {
    margin-left: auto;
    width: auto;
    max-width: 22rem;
  }
  .mono {
    font-family: var(--mono);
  }
  .note {
    font-size: var(--text-note);
  }
  .ok {
    color: var(--added);
  }
  .bad {
    color: var(--error);
  }
  .added {
    color: var(--added);
  }
  .struck {
    text-decoration: line-through;
  }

  .card {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    overflow: hidden;
  }
  .obj-head {
    display: flex;
    flex-wrap: nowrap;
    align-items: center;
    gap: 0.6rem;
    padding: 0.55rem 0.8rem;
    border-bottom: 1px solid var(--border);
  }
  /* One row however narrow the pane: the name and its kind give way, the buttons do not. */
  .obj-head > :global(*) {
    flex-shrink: 0;
    white-space: nowrap;
  }
  .obj-head .label,
  .obj-head .kind {
    flex-shrink: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  /* The kind gives way first: the name is what the card is about, and gives way only past half the row. */
  .obj-head .label {
    flex-shrink: 0;
    max-width: 50%;
    font-size: var(--text-label);
    font-weight: 600;
  }
  .card-note {
    margin: 0;
    padding: 0.5rem 0.8rem;
  }

  .badge {
    font-size: var(--text-micro);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    border: 1px solid currentColor;
    border-radius: 999px;
    padding: 0 0.45rem;
  }
  .badge.added {
    color: var(--added);
  }
  .badge.removed {
    color: var(--removed);
  }
  .badge.changed {
    color: var(--accent);
  }

  .steps {
    display: grid;
    gap: 0.5rem;
    padding: 0.8rem;
  }
  .step {
    display: grid;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--field);
    min-width: 0;
  }
  .step.same {
    opacity: 0.6;
  }
  .step.added {
    border-color: var(--added);
    box-shadow: inset 3px 0 0 var(--added);
  }
  .step.changed {
    border-color: var(--accent);
    box-shadow: inset 3px 0 0 var(--accent);
  }
  .step.removed {
    border-color: var(--removed);
    box-shadow: inset 3px 0 0 var(--removed);
  }
  .step-head {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.35rem 0.6rem;
    font-size: var(--text-note);
    min-width: 0;
  }
  .type {
    font-weight: 600;
    font-size: var(--text-label);
  }
  .inline-summary {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .step-body {
    border-top: 1px solid var(--border);
    padding: 0.4rem 0.6rem;
    font-size: var(--text-note);
    line-height: 1.5;
    color: var(--ink);
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
  .branches {
    border-top: 1px solid var(--border);
    background: var(--panel);
    padding: 0.4rem 0 0;
  }
  .branches .steps {
    padding-top: 0.3rem;
  }
  .branch-label {
    padding: 0 0.8rem;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  /* The comment button arrives on its row's hover or focus, and stays while
     its composer is open; where nothing hovers it is always there. Hidden by
     opacity, so the row keeps its layout. */
  @media (hover: hover) {
    .obj-head:not(:hover, :focus-within) .comment-btn:not([aria-expanded='true']),
    .step-head:not(:hover, :focus-within) .comment-btn:not([aria-expanded='true']) {
      opacity: 0;
    }
  }

  .cmt {
    display: grid;
    gap: 0.6rem;
    padding: 0.6rem;
    border-top: 1px solid var(--border);
    background: var(--panel);
  }
  .obj-head + .cmt,
  .card-note + .cmt {
    border-top: 0;
    border-bottom: 1px solid var(--border);
  }

  .elsewhere h2,
  .not-deployed h2 {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
  }
  /* Its samples sit inside it, each folded to its head. */
  .not-deployed {
    display: grid;
    gap: 0.6rem;
    padding-bottom: 0.8rem;
  }
  .not-deployed > p {
    padding-top: 0;
    padding-bottom: 0;
  }
  .not-deployed > .card {
    margin: 0 0.8rem;
  }
  .not-deployed > .card .obj-head {
    border-bottom: 0;
  }
  .away {
    display: grid;
    gap: 0.4rem;
    padding: 0.6rem 0.8rem;
    border-bottom: 1px solid var(--border);
  }
  .away:last-child {
    border-bottom: 0;
  }

  .fields,
  .rowtable {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-note);
  }
  .fields td,
  .rowtable th,
  .rowtable td {
    text-align: left;
    padding: 0.3rem 0.8rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }
  .fields td:first-child {
    width: 1%;
    white-space: nowrap;
  }
  .fields td.mono {
    overflow-wrap: anywhere;
  }
  .rowtable th {
    color: var(--dim);
    font-weight: 500;
  }
  .rowtable td {
    font-family: var(--mono);
  }
  .rowtable tr.added td {
    background: color-mix(in oklch, var(--added) 12%, transparent);
  }
  .rowtable tr.removed td {
    background: color-mix(in oklch, var(--removed) 12%, transparent);
    text-decoration: line-through;
  }
  .rows-scroll,
  .json-scroll {
    overflow-x: auto;
  }

  .json {
    margin: 0;
    padding: 0.5rem 0;
    font-family: var(--mono);
    font-size: var(--text-note);
    line-height: 1.5;
  }
  .json div {
    padding: 0 0.8rem;
    white-space: pre;
  }
  .json .added {
    color: var(--added);
  }
  .json .removed {
    color: var(--removed);
  }
  .json .same {
    color: var(--dim);
  }
</style>
