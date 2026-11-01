<!--
  Branches: every branch the API lists, plus `main`, which it never does --
  main is what deploys move rather than work in flight, and it moves only by
  a deploy. A change is a branch opened for review, so it is a row here like
  any other branch, carrying its title and its tests.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import { ask } from './Confirm.svelte';
  import Help from './Help.svelte';
  import PushState, { pushWord } from './PushState.svelte';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';
  import Chips, { type ChipItem } from './Chips.svelte';
  import { ownerOf } from './branchNames';
  import { ago, count } from './time';
  import {
    listBranches,
    createBranch,
    deleteBranch,
    currentBranch,
    setBranch,
    leaveBranch,
    listTests,
    runTests,
    scopedKey,
    type BranchDto,
  } from './api';
  import { isOpen, type OpenChange } from './changes';
  import { confirmContextSwitch } from './unsavedGuard';
  import { heldLabel, listHeld } from './edits';

  let branches = $state<BranchDto[]>([]);
  /** The remote's URL while one is set, which is when each branch has a push state to show. */
  let remote = $state<string | undefined>(undefined);
  let loading = $state(true);
  let loadFailed = $state(false);

  const current = currentBranch();

  /** The tests covering what a change reaches, not every test on the
   * branch. `errored` (could not run) and `untested` (a touched pipeline
   * or route with no test) are each counted on their own, never folded
   * into passing or failing. */
  type TestTally = { passing: number; failing: number; errored: number; untested: number };
  // Keyed by branch name; a key absent from the record is a run still in
  // flight. `null` is a run that did not complete -- a state of its own,
  // not a run still going.
  let testTallies: Record<string, TestTally | null> = $state({});
  /** Changes whose tests have been asked for, in flight or done. */
  const testsStarted = new Set<string>();

  /** Test runs to hold in flight at once. Each run previews every touched
   * test's pipeline, so firing one per open change at once can put dozens
   * of concurrent whole-tree runs on the leader. */
  const TEST_RUN_CONCURRENCY = 4;

  /** Runs `worker` over `items`, `limit` at a time. */
  async function pooled<T>(
    items: T[],
    limit: number,
    worker: (item: T) => Promise<void>,
  ): Promise<void> {
    const queue = [...items];
    async function next(): Promise<void> {
      const item = queue.shift();
      if (item === undefined) return;
      await worker(item);
      return next();
    }
    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, next));
  }

  /** Each open change is on its own branch, so its tests are run against
   * that branch rather than whichever one the nav has open for editing --
   * across every scope the change itself reaches (`runTests`'s doc), never
   * just the chip's. Scoped to `touched`, so the tally is the tests
   * covering what the change reaches: a touched pipeline or route with no
   * result counts as untested rather than vanishing from the tally. Ids
   * are unique only within a scope, so every join below keys on
   * `scopedKey(scope, id)` rather than `id` alone -- two groups' same-named
   * pipelines or tests must not collide into one count. */
  async function loadTests(opens: OpenChange[]) {
    await pooled(opens, TEST_RUN_CONCURRENCY, async (b) => {
      try {
        const [runRes, listRes] = await Promise.all([
          runTests({ touched: true }, b.name),
          listTests(b.name),
        ]);
        if (!runRes.ok) {
          testTallies = { ...testTallies, [b.name]: null };
          return;
        }
        const touched = runRes.touched ?? [];
        const summaryByKey = new Map(listRes.tests.map((t) => [scopedKey(t.area, t.id), t]));
        // A result's own `pipeline` is absent for an end-to-end test; its
        // summary's derived list is what says which touched pipeline(s) it
        // actually covers, so a route-only test still counts toward
        // `untested` rather than sitting outside every pipeline it tests.
        const testedPipelines = new Set(
          runRes.results.flatMap((r) => {
            const summary = summaryByKey.get(scopedKey(r.area, r.id));
            const pipelines = summary ? summary.pipelines : r.pipeline ? [r.pipeline] : [];
            return pipelines.map((p) => scopedKey(r.area, p));
          }),
        );
        const testedRoutes = new Set(
          runRes.results.flatMap((r) => {
            const routes = summaryByKey.get(scopedKey(r.area, r.id))?.routes ?? [];
            return routes.map((route) => scopedKey(r.area, route));
          }),
        );
        const passing = runRes.results.filter((r) => r.verdict?.passed).length;
        const failing = runRes.results.filter((r) => r.verdict && !r.verdict.passed).length;
        const errored = runRes.results.filter((r) => r.error).length;
        const untested = touched.reduce(
          (n, si) =>
            n +
            si.pipelines.filter((r) => !testedPipelines.has(scopedKey(si.area, r.id))).length +
            si.routes.filter((r) => !testedRoutes.has(scopedKey(si.area, r.id))).length,
          0,
        );
        testTallies = { ...testTallies, [b.name]: { passing, failing, errored, untested } };
      } catch {
        testTallies = { ...testTallies, [b.name]: null };
      }
    });
  }

  async function load() {
    loading = true;
    loadFailed = false;
    try {
      const res = await listBranches();
      branches = res.branches;
      remote = res.remote;
      // A reload after starting or deleting a branch moves no change's head, so
      // the tests already run stand; a run that did not complete is tried again.
      const untried = branches
        .filter(isOpen)
        .filter((b) => !testsStarted.has(b.name) || testTallies[b.name] === null);
      untried.forEach((b) => testsStarted.add(b.name));
      loadTests(untried);
    } catch (e) {
      loadFailed = true;
      reported(e);
    } finally {
      loading = false;
    }
  }
  load();

  // Switching branches changes what every page's config call reads, so a
  // hard reload is the only way to make the whole app agree with it.
  //
  // Asked before the write, not after: `setBranch` lands whatever the reader
  // answers, and a refusal that stopped only the reload would leave the branch
  // moved under a page still showing the old one. Unsaved work is held per
  // branch and cannot be reached from another (`edits.ts`), so this reload is
  // the end of it.
  async function open(name: string) {
    if (!(await confirmContextSwitch())) return;
    setBranch(name);
    location.assign('#/map');
    location.reload();
  }

  // One write at a time: two branches started at once would race on a name.
  let busy = $state(false);
  let newName = $state('');

  async function start() {
    const name = newName.trim();
    if (!name || busy) return;
    busy = true;
    try {
      const res = await createBranch(name);
      if (res.ok) {
        newName = '';
        await load();
      }
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  /** Deleting takes the commits with it. Nothing else holds them: a branch is
   * where its own work lives until a deploy carries it onto `main`. */
  async function remove(b: BranchDto) {
    const carries =
      b.commits > 0 ? ` It carries ${count(b.commits, 'commit')} main does not have.` : '';
    // Unsaved work is held per branch, so deleting the one in hand takes it
    // with the reload below. Said here rather than in a second dialog: this one
    // is already the question, and `ask` refuses a second while it is up.
    const heldHere = b.name === currentBranch() ? listHeld().map(heldLabel) : [];
    const loses = heldHere.length > 0 ? ` Unsaved edits on it go too: ${heldHere.join(', ')}.` : '';
    if (busy) return;
    const ok = await ask({
      title: 'delete branch',
      body: `Delete the branch ${b.name}.${carries}${loses}`,
      action: 'Delete',
      danger: true,
    });
    if (!ok || busy) return;
    busy = true;
    try {
      const res = await deleteBranch(b.name);
      if (!res.ok) return;
      if (leaveBranch(b.name)) {
        // The branch in hand is gone, so this reload is not refusable -- the
        // question belongs before the delete, which `remove` asks.
        location.reload();
      } else await load();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  function behindText(n: number): string {
    return n === 0 ? 'current' : count(n, 'commit');
  }

  function stateText(b: BranchDto): string {
    if (!b.change) return 'Not opened for review';
    if (b.change.state === 'open') return 'In review';
    if (b.change.state === 'closed') return 'Closed';
    return b.change.state;
  }

  /** What is wrong with a change's tests, worst last when ascending: a run
   * that did not complete is worse than any count, one still going is absent. */
  function testTrouble(name: string): number | undefined {
    if (!(name in testTallies)) return undefined;
    const t = testTallies[name];
    if (t === null) return Number.MAX_SAFE_INTEGER;
    return t.failing + t.errored + t.untested;
  }

  function reviewHash(branch: string): string {
    return `#/changes/review/${encodeURIComponent(branch)}`;
  }

  // The key of the branches with no owner: a slash cannot be in an owner, so it is no owner's name.
  const NO_OWNER = '/';
  let owner = $state<string | null>(null);
  const owners = $derived.by((): ChipItem<string>[] => {
    const counts = new Map<string, number>();
    for (const b of branches) {
      const key = ownerOf(b.name) || NO_OWNER;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    return [...counts]
      .sort(([a], [b]) => (a === NO_OWNER ? 1 : b === NO_OWNER ? -1 : a.localeCompare(b)))
      .map(([key, count]) => ({ key, label: key === NO_OWNER ? 'no owner' : key, count }));
  });
  const shown = $derived(
    owner === null ? branches : branches.filter((b) => (ownerOf(b.name) || NO_OWNER) === owner),
  );

  type Column = 'branch' | 'state' | 'tests' | 'behind' | 'lastSave' | 'remote';
  const sort = new TableSort<Column>();
  // `main` stays on top, outside the sort: it has none of these values.
  const sorted = $derived(
    sort.apply(shown, {
      branch: (b) => b.change?.title ?? b.name,
      state: stateText,
      tests: (b) => testTrouble(b.name),
      behind: (b) => b.behind,
      lastSave: (b) => b.lastSave,
      remote: pushWord,
    }),
  );
</script>

<SectionShell>
  <PageHeader title="Branches">
    {#snippet help()}
      A branch is an editable copy of <code>main</code> as it is now. Opening it for review makes it
      a change, which tells reviewers it is ready. A branch named <code>owner/what</code> is listed under
      that owner.
    {/snippet}
  </PageHeader>

  <form class="start" data-guide="branch-start" onsubmit={(e) => (e.preventDefault(), start())}>
    <input placeholder="a name for new work" bind:value={newName} disabled={busy} />
    <button type="submit" disabled={busy || newName.trim() === ''}>Start a branch</button>
  </form>

  {#if loading}
    <p class="dim">Loading&hellip;</p>
  {:else if loadFailed}
    <!-- The error bar says why. -->
  {:else}
    {#if owners.some((o) => o.key !== NO_OWNER)}
      <div class="tools">
        <span class="dim">Owner</span>
        <Chips
          label="Filter branches by owner"
          allCount={branches.length}
          items={owners}
          bind:value={owner}
        />
      </div>
    {/if}
    <div class="card">
      <table>
        <thead>
          <tr>
            <th aria-sort={sort.aria('branch')}
              ><SortButton {sort} key="branch">Branch</SortButton></th
            >
            <th aria-sort={sort.aria('state')}
              ><SortButton {sort} key="state">Review</SortButton></th
            >
            <th aria-sort={sort.aria('tests')}><SortButton {sort} key="tests">Tests</SortButton></th
            >
            <th aria-sort={sort.aria('behind')}
              ><SortButton {sort} key="behind">Behind</SortButton></th
            >
            <th aria-sort={sort.aria('lastSave')}
              ><SortButton {sort} key="lastSave">Last save</SortButton></th
            >
            {#if remote}
              <th aria-sort={sort.aria('remote')}
                ><SortButton {sort} key="remote">Remote</SortButton></th
              >
            {/if}
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr class:current-row={current === 'main'}>
            <td class="mono">
              main
              {#if current === 'main'}
                <span class="tag accent">open here</span>
              {:else}
                <span class="tag"
                  >read-only<Help label="read-only"
                    >Main is what the fleet runs, so only a deploy changes it.</Help
                  ></span
                >
              {/if}
            </td>
            <td class="dim">&mdash;</td>
            <td class="dim">&mdash;</td>
            <td class="dim">&mdash;</td>
            <td class="dim">&mdash;</td>
            {#if remote}
              <td class="dim">&mdash;</td>
            {/if}
            <td class="right">
              {#if current !== 'main'}
                <button class="compact" onclick={() => open('main')}>Open</button>
              {/if}
            </td>
          </tr>
          {#each sorted as b (b.name)}
            <tr class:current-row={current === b.name}>
              <td>
                {#if b.change}
                  <a href={reviewHash(b.name)}><strong>{b.change.title}</strong></a>
                {:else}
                  {@const who = ownerOf(b.name)}
                  <span class="mono"
                    >{#if who}<span class="dim">{who}/</span>{b.name.slice(
                        who.length + 1,
                      )}{:else}{b.name}{/if}</span
                  >
                {/if}
                {#if current === b.name}
                  <span class="tag accent">open here</span>
                {/if}
                <div class="dim mono sub">
                  {#if b.change}{b.name} &middot;{/if}
                  {count(b.touches.length, 'file')} &middot; {count(b.commits, 'commit')}
                </div>
              </td>
              <td class:dim={!isOpen(b)}>{stateText(b)}</td>
              <td>
                {#if !isOpen(b)}
                  <span class="dim">&mdash;</span>
                {:else if !(b.name in testTallies)}
                  <span class="dim">running&hellip;</span>
                {:else}
                  {@const t = testTallies[b.name]}
                  {#if t === null}
                    <span class="failed">test run failed</span>
                  {:else if t.untested === 0 && t.failing === 0 && t.passing === 0 && t.errored === 0}
                    <span class="dim">no tests</span>
                  {:else}
                    <span class="tally">
                      {#if t.untested > 0}
                        <span class="untested-count">{t.untested} untested</span>
                      {/if}
                      {#if t.failing > 0}
                        <span class="failing-count">{count(t.failing, 'test')} failing</span>
                      {:else if t.passing > 0}
                        <span class="passing-count">{count(t.passing, 'test')} passing</span>
                      {:else if t.errored > 0}
                        <span class="dim">{count(t.errored, 'test')} could not run</span>
                      {/if}
                    </span>
                  {/if}
                {/if}
              </td>
              <td class:dim={b.behind === 0} class:warn={b.behind !== 0}>
                {behindText(b.behind)}
              </td>
              <td class="dim">{ago(b.lastSave)}</td>
              {#if remote}
                <td><PushState branch={b} onoverwritten={load} /></td>
              {/if}
              <td class="right">
                {#if current !== b.name}
                  <!-- The demo's guide points at the Open of a branch just started, which has no commits yet. -->
                  <button
                    class="compact"
                    data-guide={b.commits === 0 ? 'branch-open' : undefined}
                    onclick={() => open(b.name)}
                    disabled={busy}>Open</button
                  >
                {/if}
                <button class="compact" onclick={() => remove(b)} disabled={busy}>Delete</button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</SectionShell>

<style>
  .start {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
  }
  .start input {
    font-family: var(--mono);
    min-width: 16rem;
  }
  .failed {
    color: var(--error);
  }

  .warn {
    color: var(--warn);
  }
  .mono {
    font-family: var(--mono);
  }
  .sub {
    font-size: var(--text-micro);
    margin-top: 0.1rem;
  }
  .passing-count {
    color: var(--added);
    font-weight: 600;
  }
  .failing-count {
    color: var(--error);
    font-weight: 600;
  }
  .untested-count {
    color: var(--warn);
    font-weight: 600;
  }
  .tally {
    display: inline-flex;
    gap: 0.5rem;
  }
  .tools {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.75rem;
    margin-top: 0.75rem;
  }
  .tools + .card {
    margin-top: 0.5rem;
  }
  .card {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    margin-top: 1.25rem;
    overflow-x: auto;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-label);
  }
  th,
  td {
    text-align: left;
    padding: 0.4rem 0.75rem;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  th {
    color: var(--dim);
    font-weight: 700;
    font-size: var(--text-micro);
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  tbody tr:last-child td {
    border-bottom: 0;
  }
  .current-row {
    background: color-mix(in oklch, var(--ink) 6%, transparent);
  }
  .right {
    text-align: right;
  }
  .tag {
    display: inline-flex;
    align-items: center;
    padding: 0.08rem 0.4rem;
    margin-left: 0.35rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    font-size: var(--text-micro);
    color: var(--dim);
  }
  .tag.accent {
    border-color: var(--ink);
    color: var(--ink);
    font-weight: 600;
  }
</style>
