<!--
  Cluster status (`gui-design.md`, "Worker health status"), polled from
  `/api/cluster` (`src/bin/smeltr/cluster.rs`), which the leader serves
  in-process. Nothing here is edited.

  The Workers table is one row per worker with its threads folded away:
  a worker runs a reader per source, a transform pool sized by cores and
  an egress thread per destination, so a flat thread table stops reading
  at a handful of workers. The rollup (`workers.ts`) is the union of the
  credential roster and the live threads, so a worker that never
  heartbeated, or was revoked, still gets a row.

  Groups lead with transform threads: that is the count `thread_limit`
  sizes, and the only one an operator sets.

  Rows in and out (E2) read the leader's own series (`GET /api/flow`,
  `flow.ts`), so the window survives a reload; a gap in a subject's
  history breaks its line rather than dipping to zero. It is drawn from
  one baseline -- in above, out below (`FlowChart.svelte`) -- because the
  two usually agree, and laid over each other the second one drawn hid
  the first. Both the chart and the rate above it are rows per second off
  the last closed bucket: the one covering now is still filling, and its
  running total is not a rate. The ranked table
  compares each source and destination to its own mean at this local
  hour of day over the last thirty days, independent of the chart's own
  window picker. Errors still chart this page's own rolling window: the
  leader keeps no series for `smeltr_rows_errored_total`.
-->
<script lang="ts">
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import Locked from './Locked.svelte';
  import SectionShell from './SectionShell.svelte';
  import { ask } from './Confirm.svelte';
  import PageHeader from './PageHeader.svelte';
  import Help from './Help.svelte';
  import FlowChart from './FlowChart.svelte';
  import Sparkline from './Sparkline.svelte';
  import { pipelineNamesAt, type PipelineNames } from './pipelineNames';
  import { bytes } from './bytes';
  import SortButton from './SortButton.svelte';
  import FoldAllButton from './FoldAllButton.svelte';
  import Chips from './Chips.svelte';
  import { TableSort } from './sort.svelte';
  import {
    errorText,
    getClusterStatus,
    getFlow,
    listWarnings,
    restartWorker,
    conditionLabel,
    MAIN_BRANCH,
    type ClusterHealth,
    type ClusterStatus,
    type FlowResponseDto,
    type WarningDto,
  } from './api';
  import {
    canAsk,
    countWorkers,
    listensAt,
    filterWorkers,
    rollupWorkers,
    runningThreads,
    workerHealth,
    type WorkerHealth,
    type WorkerRow,
    type WorkerStatus,
    onCommit,
    rollout,
  } from './workers';
  import { isOver, workerMemory, type MemoryPart } from './memoryParts';
  import {
    addPoll,
    formatFunction,
    formatSample,
    labelOf,
    rowKey,
    sampleKey,
    sumMetric,
    toRows,
    type BreakdownTotals,
  } from './metrics';
  import {
    BASELINE_WINDOW_MINUTES,
    DEFAULT_WINDOW_MINUTES,
    FLOW_WINDOWS,
    FINEST_BUCKET_MILLIS,
    currentRate,
    denseRates,
    firstBucket,
    furthestFromNormal,
    lastClosedBucket,
    sumSeries,
  } from './flow';
  import { clockTime, count, shortCommit } from './time';

  // The commit a deploy just pointed the fleet at, from the hash: the page follows
  // its rollout until it is dismissed.
  let { deploying }: { deploying?: string } = $props();
  // A panel is a region named by its heading.
  const uid = $props.id();

  let status: ClusterStatus | null = $state(null);
  let loading = $state(true);
  // The status page begins live. Pausing stops every existing poll without changing their cadence.
  let autoRefresh = $state(true);

  // Keyed by worker id, not row index: the list changes between polls.
  let expanded: Record<string, boolean> = $state({});

  const POLL_MS = 2000;

  // Heartbeat counters are per-drain deltas (`Metrics::drain`), so each
  // poll's raw value is already "activity since the last sample". Errors
  // are the only rollup left on this page's own rolling window -- rows in
  // and out now read the leader's own series (`flow.ts`), which survives
  // a reload.
  const MAX_HISTORY = 150; // ~5 minutes at POLL_MS
  type HistoryPoint = { at: number; threads: ClusterHealth['threads'] };
  let history: HistoryPoint[] = $state([]);

  // Totals per label combination, accumulated over the same window as the
  // chart: the sparkline shows the trend, this says which pipeline it
  // belongs to.

  function recordHistory(health: ClusterHealth) {
    history.push({ at: Date.now(), threads: health.threads });
    if (history.length > MAX_HISTORY) history.splice(0, history.length - MAX_HISTORY);
  }

  let selectedGroupId = $state<string | null>(null);
  let selectedWorkerId = $state<string | null>(null);
  // The runtime's parts, folded under its whole.
  let runtimeOpen = $state(false);
  let selectionLabel = $derived(
    selectedWorkerId
      ? `worker ${selectedWorkerId}`
      : selectedGroupId
        ? `group ${selectedGroupId}`
        : 'whole fleet',
  );
  function flowMatchesSelection(series: { group: string; workerId?: string | null }) {
    if (selectedWorkerId) return series.workerId === selectedWorkerId;
    if (selectedGroupId) return series.group === selectedGroupId;
    return true;
  }
  function threadsForSelection(threads: ClusterHealth['threads']) {
    if (selectedWorkerId) return threads.filter((thread) => thread.workerId === selectedWorkerId);
    if (selectedGroupId) return threads.filter((thread) => thread.groupId === selectedGroupId);
    return threads;
  }
  let scopedErroredBy = $derived.by(() =>
    history.reduce(
      (totals, point) =>
        addPoll(totals, threadsForSelection(point.threads), 'smeltr_rows_errored_total', [
          'pipeline',
          'function',
          'kind',
        ]),
      {} as BreakdownTotals,
    ),
  );
  let erroredRows = $derived(toRows(scopedErroredBy));
  const erroredSort = new TableSort<'pipeline' | 'function' | 'kind' | 'rows'>();
  let sortedErroredRows = $derived(
    erroredSort.apply(erroredRows, {
      pipeline: (r) => pipelineName(r.values[0]),
      function: (r) => formatFunction(r.values[1]),
      kind: (r) => r.values[2],
      rows: (r) => r.total,
    }),
  );

  /** Each deployed group's pipeline names on `main`, which is what its workers run, `shared/`'s
   * included. A group absent here has not answered yet. */
  let namesByGroup: Record<string, PipelineNames> = $state({});
  /** Per group, the ids it was read for: once when it is first seen, and again for a pipeline it
   * did not name, which was deployed since. */
  const namesReadFor = new Map<string, Set<string>>();
  function readNames(group: string, forId = '') {
    const readFor = namesReadFor.get(group) ?? new Set<string>();
    if (readFor.has(forId)) return;
    namesReadFor.set(group, readFor.add(forId));
    pipelineNamesAt(MAIN_BRANCH, group).then((names) => (namesByGroup[group] = names));
  }
  /** The groups whose threads reported errors for each pipeline id over the window. */
  let erroredGroups = $derived.by(() => {
    const groups = new Map<string, Set<string>>();
    for (const point of history)
      for (const thread of threadsForSelection(point.threads))
        for (const m of thread.metrics) {
          if (m.metric !== 'smeltr_rows_errored_total') continue;
          const id = labelOf(m, 'pipeline');
          groups.set(id, (groups.get(id) ?? new Set()).add(thread.groupId));
        }
    return groups;
  });
  $effect(() => {
    for (const [id, groups] of erroredGroups)
      for (const g of groups) readNames(g, g in namesByGroup && !namesByGroup[g][id] ? id : '');
  });
  /** A pipeline by the name a group that ran it gives it. */
  function pipelineName(id: string): string {
    for (const g of erroredGroups.get(id) ?? []) {
      const name = namesByGroup[g]?.[id];
      if (name) return name;
    }
    return id;
  }

  let erroredSeries = $derived(
    history.map((point) =>
      sumMetric(threadsForSelection(point.threads), 'smeltr_rows_errored_total'),
    ),
  );
  let totalErrored = $derived(erroredSeries.reduce((a, b) => a + b, 0));
  // What the total actually spans: `history` is a rolling window, not the
  // life of the page.
  const HISTORY_MINUTES = Math.round((MAX_HISTORY * POLL_MS) / 60_000);

  // -- flow (E2): the window-picked chart --------------------------------

  let windowMinutes = $state(DEFAULT_WINDOW_MINUTES);
  let flow: FlowResponseDto | null = $state(null);
  let flowFetchedAt = $state(Date.now());

  function fetchFlow() {
    // Reads `windowMinutes`, so the effect below re-fetches the moment the picker changes
    // rather than waiting for the next tick.
    getFlow(windowMinutes)
      .then((f) => {
        flow = f;
        flowFetchedAt = Date.now();
      })
      .catch(() => {
        // Keep showing the last window rather than blanking a working chart over one bad poll.
      });
  }

  // `$derived.by`: in the expression form TypeScript narrows `flow` to its `null`
  // initializer (as `workerRows` below notes for `status`).
  let fleetIn = $derived.by(() =>
    flow ? sumSeries(flow.rowsIn.filter((series) => flowMatchesSelection(series))) : [],
  );
  let fleetOut = $derived.by(() =>
    flow ? sumSeries(flow.rowsOut.filter((series) => flowMatchesSelection(series))) : [],
  );
  let fleetBytesIn = $derived.by(() =>
    flow ? sumSeries(flow.bytesIn.filter((series) => flowMatchesSelection(series))) : [],
  );
  let fleetBytesOut = $derived.by(() =>
    flow ? sumSeries(flow.bytesOut.filter((series) => flowMatchesSelection(series))) : [],
  );
  let bucketMillis = $derived.by(() => flow?.bucketMillis ?? 0);
  let windowFrom = $derived(flowFetchedAt - windowMinutes * 60_000);
  /** Where the chart stops. The bucket covering now is still filling, so its total is
   * only what has landed so far -- drawn, it is a dip that fills itself in on the next
   * poll. `currentRate` already refuses to read it; the chart ends at the same bucket so
   * the two cannot give different answers about the same instant. */
  let windowTo = $derived(bucketMillis ? lastClosedBucket(bucketMillis, flowFetchedAt) : 0);

  // rows/s, not rows per bucket: the unit the rate line above the chart is in, and the
  // only one that holds across the picker -- a bucket is 10s at sixty minutes and an hour
  // at thirty days.
  let fleetInDense = $derived(
    bucketMillis ? denseRates(fleetIn, bucketMillis, windowFrom, windowTo) : [],
  );
  let fleetOutDense = $derived(
    bucketMillis ? denseRates(fleetOut, bucketMillis, windowFrom, windowTo) : [],
  );
  // Bytes are not drawn -- one scale cannot carry rows and bytes, and a second axis buys
  // less than the reading does. They are read off the same grid, so the hovered slot names
  // one bucket across all four.
  let fleetBytesInDense = $derived(
    bucketMillis ? denseRates(fleetBytesIn, bucketMillis, windowFrom, windowTo) : [],
  );
  let fleetBytesOutDense = $derived(
    bucketMillis ? denseRates(fleetBytesOut, bucketMillis, windowFrom, windowTo) : [],
  );
  let hasFlowData = $derived(
    fleetInDense.some((v) => v !== null) || fleetOutDense.some((v) => v !== null),
  );
  /** The fleet line at the top of the page, which answers before the chart is reached. The
   * flow panel reads its own last slot instead: that is this same closed bucket, off the
   * grid the pointer moves over. */
  let nowOutRate = $derived(
    bucketMillis ? currentRate(fleetOut, bucketMillis, flowFetchedAt) : null,
  );
  /** A rate, or `--` where the bucket that would carry it is missing. Not "0 rows/s": a
   * fleet nobody has heard from and a fleet moving nothing are different answers. A rate
   * under one takes two significant digits, the same count a grid step carries: rounding
   * it would print 0 for a fleet whose chart, right below, draws a line. */
  const rateText = (rate: number | null) =>
    rate === null ? '--' : rate > 0 && rate < 1 ? rate.toPrecision(2) : String(Math.round(rate));

  /** A byte rate, `--` where the bucket is missing, for the same reason `rateText` gives. */
  const bytesText = (rate: number | null) => (rate === null ? '--' : `${bytes(rate)}/s`);

  /** The bucket the pointer is over, or `null` when it is away. `FlowChart` owns finding
   * it; the reading is made here, where the series it does not draw are held. */
  let hoveredSlot: number | null = $state(null);

  /** Which bucket the readout names: the hovered one, or the last the chart drew. That
   * last slot is the bucket `currentRate` reads, so the line and the chart's right edge
   * are one reading and cannot disagree. */
  let readSlot = $derived(hoveredSlot ?? fleetInDense.length - 1);
  let readAt = $derived(
    bucketMillis && readSlot >= 0
      ? clockTime(firstBucket(bucketMillis, windowFrom) + readSlot * bucketMillis)
      : '--',
  );
  const readFrom = (dense: (number | null)[]) => (readSlot >= 0 ? (dense[readSlot] ?? null) : null);

  // -- furthest from normal (Main storyboard): its own fixed, longer window, independent of
  // the chart's picker -- a baseline needs history the picker's window may not cover.

  let baseline: FlowResponseDto | null = $state(null);
  let baselineFetchedAt = $state(Date.now());
  const BASELINE_POLL_MS = 30_000; // an hourly-bucketed baseline does not need a 2s refresh

  function fetchBaseline() {
    getFlow(BASELINE_WINDOW_MINUTES)
      .then((f) => {
        baseline = f;
        baselineFetchedAt = Date.now();
      })
      .catch(reported);
  }

  let ranked = $derived.by(() =>
    baseline
      ? furthestFromNormal(
          baseline.rowsIn.filter((series) => flowMatchesSelection(series)),
          baseline.rowsOut.filter((series) => flowMatchesSelection(series)),
          baseline.bucketMillis,
          baselineFetchedAt,
        )
      : [],
  );
  const rankedSort = new TableSort<'name' | 'kind' | 'group' | 'now' | 'normal' | 'away'>();
  let sortedRanked = $derived(
    rankedSort.apply(ranked, {
      name: (r) => r.subjectId,
      kind: (r) => r.kind,
      group: (r) => r.group,
      now: (r) => r.now,
      normal: (r) => r.normal,
      away: (r) => r.awayPercent,
    }),
  );
  let baselineSourceCount = $derived.by(
    () => baseline?.rowsIn.filter((series) => flowMatchesSelection(series)).length ?? 0,
  );
  let baselineDestCount = $derived.by(
    () => baseline?.rowsOut.filter((series) => flowMatchesSelection(series)).length ?? 0,
  );

  // Derived fresh by the leader on every request and cleared when its fact stops, so these
  // are replaced wholesale each poll and never merged with what was showing.
  let warnings: WarningDto[] = $state([]);
  // Stamped with the answer, so an age reads as of the poll that produced it rather than
  // creeping upward between polls -- the same discipline as `lastSeenMillisAgo`.
  let now = $state(Date.now());
  let failedCount = $derived(warnings.filter((w) => w.severity === 'failed').length);
  let warningCount = $derived(warnings.length - failedCount);

  function since(w: WarningDto, now: number): string {
    if (w.sinceMillis === null) return 'unknown';
    return formatAgo(Math.max(0, now - w.sinceMillis)).replace(' ago', '');
  }

  function refresh() {
    getClusterStatus()
      .then((s) => {
        status = s;
        now = Date.now();
        if (s.health) recordHistory(s.health);
      })
      .catch(reported)
      .finally(() => {
        loading = false;
      });
    listWarnings()
      .then((w) => {
        // Failed first: the ribbon names only the worst one, and a broken thing must never sit
        // behind an advisory one just because the catalog derived it later.
        warnings = w.toSorted(
          (a, b) => Number(b.severity === 'failed') - Number(a.severity === 'failed'),
        );
        now = Date.now();
      })
      .catch(() => {
        // A warning read depends on the durable store, while cluster status can still
        // answer from memory. Do not turn this failure into a failed cluster poll or
        // leave conditions on screen that the latest response did not confirm.
        warnings = [];
      });
  }

  $effect(() => {
    if (!autoRefresh) return;
    refresh();
    const id = setInterval(refresh, POLL_MS);
    return () => clearInterval(id);
  });

  // Reading `windowMinutes` inside `fetchFlow` makes this effect depend on it, so changing
  // the picker re-fetches immediately rather than waiting for the next tick.
  $effect(() => {
    if (!autoRefresh) return;
    fetchFlow();
    const id = setInterval(fetchFlow, POLL_MS);
    return () => clearInterval(id);
  });

  $effect(() => {
    if (!autoRefresh) return;
    fetchBaseline();
    const id = setInterval(fetchBaseline, BASELINE_POLL_MS);
    return () => clearInterval(id);
  });

  function formatAgo(ms: number): string {
    if (ms < 1000) return `${ms}ms ago`;
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s ago`;
    return `${Math.floor(ms / 60_000)}m ago`;
  }

  function commitOrNone(commit: string | null): string {
    return commit ? shortCommit(commit) : 'none';
  }

  function formatCpu(resources: WorkerRow['resources']): string {
    if (resources?.cpuPercent == null) return '--';
    return `${Math.round(resources.cpuPercent)}%`;
  }

  /** Against the container's own limit where one was found, against the whole host otherwise --
   * `title` carries which, since the row itself has no room to say so. */
  function formatMemory(resources: WorkerRow['resources']): string {
    if (!resources) return '--';
    const used = bytes(resources.memoryUsedBytes);
    if (resources.memoryLimitBytes == null) return used;
    const percent = Math.round((resources.memoryUsedBytes / resources.memoryLimitBytes) * 100);
    return `${percent}% (${used} / ${bytes(resources.memoryLimitBytes)})`;
  }

  function memoryTitle(resources: WorkerRow['resources']): string {
    if (!resources || resources.memoryLimitBytes == null) return '';
    return resources.memoryLimited
      ? 'Against this container’s own memory limit'
      : 'No container limit found -- against the host’s total memory';
  }

  function cpuTitle(resources: WorkerRow['resources']): string {
    if (resources?.cpuPercent == null) return '';
    return resources.cpuLimited
      ? 'Against this container’s own CPU quota'
      : 'No container quota found -- against every core the host has';
  }

  /** The capacity itself, for the detail panel: what the table's percentages are measured
   * against. A worker old enough to report the flag but not the figure beside it gets no title,
   * so a tooltip never explains a number the row is showing as `--`. */
  function limitTitle(limited: boolean | undefined, capacity: number | null | undefined): string {
    if (limited === undefined || capacity == null) return '';
    return limited
      ? 'This container’s own limit'
      : 'No container limit found -- the host’s own capacity';
  }

  /** Fractional under a container quota, which is what a k8s `cpu: 500m` limit is. */
  function formatCores(resources: WorkerRow['resources']): string {
    const cores = resources?.cpuLimitCores;
    if (cores == null) return '--';
    return Number.isInteger(cores) ? `${cores}` : cores.toFixed(2);
  }

  function formatWorkerMemory(resources: WorkerRow['resources']): string {
    return resources ? bytes(resources.memoryUsedBytes) : '--';
  }

  function formatMemoryLimit(resources: WorkerRow['resources']): string {
    if (resources?.memoryLimitBytes == null) return '--';
    return bytes(resources.memoryLimitBytes);
  }

  /** Free space leads, because it is what the worker may still write. Total says how much room
   * that is out of. */
  function formatDisk(resources: WorkerRow['resources']): string {
    if (resources?.diskFreeBytes == null || resources.diskTotalBytes == null) return '--';
    return `${bytes(resources.diskFreeBytes)} free of ${bytes(resources.diskTotalBytes)}`;
  }

  /** Absent where nothing measures the part. A reserve over its max is its queue borrowing from the
   * pool, so that is named rather than clamped. */
  function partNow(p: MemoryPart): string {
    if (p.currentBytes === null) return '--';
    const now = bytes(p.currentBytes);
    if (p.peak) return `${now} peak`;
    if (isOver(p)) return `${now} ${p.part === 'reserve' ? 'borrowing' : 'over'}`;
    return now;
  }

  const KIND_LABEL: Record<string, string> = {
    reader: 'reader',
    transform: 'transform',
    egress: 'egress',
    worker: 'the worker itself',
  };

  let silentSeconds = $derived.by(() => (status?.health?.silentAfterMs ?? 0) / 1000);

  const STATUS_LABEL: Record<WorkerStatus, string> = {
    revoked: 'Revoked',
    online: 'Online',
    silent: 'Silent',
    neverHeartbeated: 'Never heartbeated',
    stopped: 'Stopped',
  };

  // `$derived.by`: in the expression form TypeScript narrows `status` to
  // its `null` initializer.
  let workerRows = $derived.by(() =>
    rollupWorkers(
      status?.workers ?? null,
      status?.health?.threads ?? [],
      status?.health?.silentAfterMs ?? 0,
    ),
  );
  const EXPANDED_GROUPS_KEY = 'smeltr-status-expanded-groups';
  function loadExpandedGroups(): Record<string, boolean> {
    if (typeof window === 'undefined') return {};
    try {
      const saved = JSON.parse(window.localStorage.getItem(EXPANDED_GROUPS_KEY) ?? '{}');
      return saved && typeof saved === 'object' ? saved : {};
    } catch {
      return {};
    }
  }
  let expandedGroups: Record<string, boolean> = $state(loadExpandedGroups());
  function saveExpandedGroups() {
    window.localStorage.setItem(EXPANDED_GROUPS_KEY, JSON.stringify(expandedGroups));
  }
  // A group nobody has folded yet starts open.
  function isExpanded(group: string): boolean {
    return expandedGroups[group] ?? true;
  }
  function toggleGroup(group: string) {
    expandedGroups[group] = !isExpanded(group);
    saveExpandedGroups();
  }
  function setAllGroups(expanded: boolean) {
    expandedGroups = {
      ...expandedGroups,
      ...Object.fromEntries(displayedGroups.map(([group]) => [group, expanded])),
    };
    saveExpandedGroups();
  }

  let workerQuery = $state('');
  // Null is All.
  let workerStatusFilter = $state<'attention' | 'online' | null>(null);
  let workerCounts = $derived.by(() => countWorkers(workerRows));
  let healthOf = $derived.by(
    () =>
      new Map<string, WorkerHealth>(workerRows.map((w) => [w.workerId, workerHealth(w, warnings)])),
  );
  let needsAttention = $derived(workerRows.filter((w) => healthOf.get(w.workerId) !== 'healthy'));
  let filteredWorkers = $derived.by(() =>
    workerStatusFilter === 'attention'
      ? filterWorkers(needsAttention, workerQuery, 'all')
      : filterWorkers(workerRows, workerQuery, workerStatusFilter ?? 'all'),
  );
  let selectedWorker = $derived.by(
    () => workerRows.find((w) => w.workerId === selectedWorkerId) ?? null,
  );
  let selectedMemory = $derived(selectedWorker ? workerMemory(selectedWorker.threads) : null);
  let displayedGroups = $derived.by(() => {
    const groups = new Map<string, typeof filteredWorkers>();
    for (const worker of filteredWorkers) {
      const key = worker.groupId ?? 'unassigned';
      groups.set(key, [...(groups.get(key) ?? []), worker]);
    }
    return [...groups.entries()];
  });
  let allGroupsOpen = $derived(
    displayedGroups.length > 0 && displayedGroups.every(([group]) => isExpanded(group)),
  );
  // Sorts workers within their group only: the group row is the band they fold under.
  const workerSort = new TableSort<'worker' | 'running' | 'cpu' | 'memory' | 'lastSeen'>();
  let sortedGroups = $derived(
    displayedGroups.map(
      ([group, workers]) =>
        [
          group,
          workerSort.apply(workers, {
            worker: (w) => `${STATUS_LABEL[w.status]} ${w.workerId}`,
            running: (w) => w.commit,
            cpu: (w) => w.resources?.cpuPercent,
            // Used bytes, not the percent shown: only a worker with a known limit has a percent.
            memory: (w) => w.resources?.memoryUsedBytes,
            lastSeen: (w) => (w.lastSeenMillisAgo === null ? null : -w.lastSeenMillisAgo),
          }),
        ] as const,
    ),
  );
  let fleetRollout = $derived(deploying ? rollout(workerRows, deploying) : null);
  // Online and not yet running the commit being deployed.
  function behind(worker: WorkerRow): boolean {
    return !!deploying && worker.status === 'online' && !onCommit(worker, deploying);
  }

  let totalPartitions = $derived(
    workerRows.reduce((total, worker) => total + worker.partitionsOwned, 0),
  );

  // A restart reaches the worker on its next heartbeat; the button says so until it has.
  let restarting = $state(false);
  let restartError = $state('');

  async function restart(workerId: string) {
    const ok = await ask({
      title: 'restart worker',
      body: `Restart ${workerId}?`,
      action: 'Restart',
    });
    if (!ok) return;
    restarting = true;
    restartError = '';
    try {
      const res = await restartWorker(workerId);
      if (!res.ok) restartError = errorText(res.errors) || 'could not restart the worker';
      refresh();
    } catch (e) {
      restartError = unreported(e);
    } finally {
      restarting = false;
    }
  }

  function refreshNow() {
    refresh();
    fetchFlow();
    fetchBaseline();
  }

  function chooseWindow(minutes: number) {
    windowMinutes = minutes;
    // The live effect reacts to the new window. Paused pages need the explicit fetch.
    if (!autoRefresh) fetchFlow();
  }
</script>

<SectionShell>
  {#if loading}
    <p class="dim">Loading…</p>
  {:else if status === null}
    <PageHeader title="Cluster" />
    <!-- The error bar says why. -->
  {:else}
    {#if warnings.length > 0}
      <a class="issue-ribbon" href="#/status/issues" aria-label="Active fleet issues">
        <strong>▲ {warnings.length} active {warnings.length === 1 ? 'issue' : 'issues'}</strong>
        <span class="issue-divider">|</span>
        <span class="issue-teaser">{warnings[0].subjectId}: {warnings[0].message}</span>
        <span class="issue-more">View all issues &rsaquo;</span>
      </a>
    {/if}

    <header class="fleet-header">
      <div>
        <div class="fleet-title">
          <h1>Fleet status</h1>
          <Help label="Fleet status">
            Silent: none of the worker's threads has reported for {silentSeconds} seconds. Never heartbeated:
            the leader has no report from it at all, usually a worker enrolled but never started. Stopped:
            it said it was shutting down.
          </Help>
        </div>
        <p>
          {workerRows.length} workers across {status.workerGroups?.length ?? 0} groups
          <span>·</span>
          refreshed every {POLL_MS / 1000}s
        </p>
      </div>
      <div class="fleet-totals" aria-label="Fleet totals">
        <span class="total online"><i></i><strong>{workerCounts.online}</strong> online</span>
        <span class="total silent"><i></i><strong>{workerCounts.silent}</strong> silent</span>
        {#if workerCounts.neverHeartbeated > 0}
          <span class="total neverHeartbeated"
            ><i></i><strong>{workerCounts.neverHeartbeated}</strong> never heartbeated</span
          >
        {/if}
        {#if workerCounts.stopped > 0}
          <span class="total stopped"><i></i><strong>{workerCounts.stopped}</strong> stopped</span>
        {/if}
        <span class="total"><strong>{totalPartitions}</strong> partitions</span>
        <span class="total"><strong>{rateText(nowOutRate)}</strong> rows/s</span>
      </div>
    </header>

    {#if deploying && fleetRollout}
      <div class="rollout" role="status">
        <span class="mono">{commitOrNone(deploying)}</span>
        <meter min="0" max={Math.max(fleetRollout.of, 1)} value={fleetRollout.on}></meter>
        <span
          >{fleetRollout.on === fleetRollout.of
            ? `on all ${fleetRollout.of} online ${fleetRollout.of === 1 ? 'worker' : 'workers'}`
            : `${fleetRollout.on} of ${fleetRollout.of} online workers`}</span
        >
        <a class="rollout-close" href="#/status" aria-label="Stop following this deploy">&times;</a>
      </div>
    {/if}

    {#if status.error}
      <InlineError>
        Couldn't read the leader's store: {status.error}. The roster is unavailable.
      </InlineError>
    {/if}

    <section class="fleet-shell" class:has-detail={selectedWorker !== null}>
      <div class="fleet-matrix">
        <div class="matrix-tools">
          <Chips
            label="Filter workers by status"
            allCount={workerRows.length}
            items={[
              { key: 'attention', label: 'Needs attention', count: needsAttention.length },
              { key: 'online', label: 'Online', count: workerCounts.online },
            ]}
            bind:value={workerStatusFilter}
          />
          <FoldAllButton
            allOpen={allGroupsOpen}
            what="worker groups"
            disabled={displayedGroups.length === 0}
            onclick={() => setAllGroups(!allGroupsOpen)}
          />
          <label class="matrix-search"
            ><span aria-hidden="true">⌕</span><input
              type="search"
              bind:value={workerQuery}
              placeholder="Search workers, groups, commits…"
              aria-label="Search workers"
            /></label
          >
          <button title="Refresh now" aria-label="Refresh now" onclick={refreshNow}>↻</button>
        </div>
        <div class="matrix-columns">
          <div>
            <SortButton sort={workerSort} key="worker">State / worker</SortButton>
          </div>
          <div>
            <SortButton sort={workerSort} key="running">Running</SortButton>
          </div>
          <div>
            <SortButton sort={workerSort} key="cpu">CPU</SortButton>
          </div>
          <div>
            <SortButton sort={workerSort} key="memory">Memory</SortButton>
          </div>
          <div>
            <SortButton sort={workerSort} key="lastSeen">Last seen</SortButton>
          </div>
        </div>
        {#if displayedGroups.length > 0}
          {#each sortedGroups as [group, workers] (group)}
            <div
              class="group-row"
              class:selected={selectedGroupId === group && selectedWorkerId === null}
            >
              <button
                class="group-toggle"
                aria-label={isExpanded(group) ? `Collapse ${group}` : `Expand ${group}`}
                aria-expanded={isExpanded(group)}
                onclick={() => toggleGroup(group)}
              >
                <span class="group-caret" class:expanded={isExpanded(group)}>▶</span>
              </button>
              <button
                class="group-select"
                onclick={() => {
                  selectedWorkerId = null;
                  selectedGroupId = selectedGroupId === group ? null : group;
                }}
              >
                <strong>{group}</strong><span
                  >{workers.length} {workers.length === 1 ? 'worker' : 'workers'}</span
                >
                <span class="group-spacer"></span>
                {#if deploying}
                  <!-- The whole group, not just the rows a filter left showing. -->
                  {@const r = rollout(
                    workerRows.filter((w) => (w.groupId ?? 'unassigned') === group),
                    deploying,
                  )}
                  <span class:behind={r.on < r.of}>{r.on}/{r.of} on {commitOrNone(deploying)}</span>
                {/if}
                <span>{workers.reduce((n, w) => n + w.partitionsOwned, 0)} partitions</span>
              </button>
            </div>
            {#if isExpanded(group)}
              {#each workers as worker (worker.workerId)}
                <button
                  class="worker-row"
                  class:selected={selectedWorkerId === worker.workerId}
                  class:attention={worker.status !== 'online' ||
                    healthOf.get(worker.workerId) !== 'healthy'}
                  onclick={() => {
                    selectedGroupId = null;
                    selectedWorkerId = worker.workerId;
                  }}
                >
                  <span class="worker-name"
                    ><i class="state-dot {worker.status} {healthOf.get(worker.workerId)}"
                    ></i><strong>{worker.workerId}</strong></span
                  >
                  <span class="mono" class:behind={behind(worker)}
                    >{commitOrNone(worker.commit)}</span
                  >
                  <span title={cpuTitle(worker.resources)}>{formatCpu(worker.resources)}</span>
                  <span title={memoryTitle(worker.resources)}>{formatMemory(worker.resources)}</span
                  >
                  <span class="last-seen" class:late={worker.status !== 'online'}
                    >{worker.lastSeenMillisAgo === null
                      ? 'Never'
                      : formatAgo(worker.lastSeenMillisAgo)}</span
                  >
                </button>
              {/each}
            {/if}
          {/each}
        {:else}<p class="empty matrix-empty">No workers match this filter.</p>{/if}
      </div>

      {#if selectedWorker}
        <aside class="worker-detail">
          <button
            class="detail-close"
            aria-label="Close worker details"
            onclick={() => {
              selectedWorkerId = null;
            }}>×</button
          >
          <h2>{selectedWorker.workerId}</h2>
          <p class="detail-group">
            {selectedWorker.groupId ?? 'unassigned'} · {selectedWorker.workerId}
          </p>
          <div class="health-callout {selectedWorker.status}">
            <strong
              ><i class="state-dot {selectedWorker.status} {healthOf.get(selectedWorker.workerId)}"
              ></i>{STATUS_LABEL[selectedWorker.status]}</strong
            ><span
              >Last heartbeat<b
                >{selectedWorker.lastSeenMillisAgo === null
                  ? 'Never'
                  : formatAgo(selectedWorker.lastSeenMillisAgo)}</b
              ></span
            >
          </div>
          {#if canAsk(selectedWorker, 'restart')}
            <Locked refusedInDemo
              ><div class="detail-actions">
                <button
                  type="button"
                  class="compact"
                  disabled={restarting || selectedWorker.asked === 'restart'}
                  onclick={() => selectedWorker && restart(selectedWorker.workerId)}
                  >{selectedWorker.asked === 'restart' ? 'Restarting…' : 'Restart'}</button
                >
              </div></Locked
            >
          {/if}
          <InlineError message={restartError} />
          <dl>
            <dt>Commit</dt>
            <dd class="mono">{commitOrNone(selectedWorker.commit)}</dd>
            <dt>Version</dt>
            <dd class="mono">
              {#if selectedWorker.release}
                {selectedWorker.release}
                <Help label="Build">{selectedWorker.smeltrVersion}</Help>
              {:else}
                {selectedWorker.smeltrVersion ?? 'unknown'}
              {/if}
            </dd>
            <dt>Offline policy</dt>
            <dd>
              {selectedWorker.stopAfterUnreachableSecs == null
                ? 'never'
                : `${selectedWorker.stopAfterUnreachableSecs}s`}
            </dd>
            <dt>Group</dt>
            <dd>{selectedWorker.groupId ?? '--'}</dd>
            <dt>Threads</dt>
            <dd>{runningThreads(selectedWorker.threads)}</dd>
            <dt>CPUs</dt>
            <dd
              title={limitTitle(
                selectedWorker.resources?.cpuLimited,
                selectedWorker.resources?.cpuLimitCores,
              )}
            >
              {formatCores(selectedWorker.resources)}
            </dd>
            <dt>Memory</dt>
            <dd
              title={limitTitle(
                selectedWorker.resources?.memoryLimited,
                selectedWorker.resources?.memoryLimitBytes,
              )}
            >
              {formatMemoryLimit(selectedWorker.resources)}
            </dd>
            <dt>Disk</dt>
            <dd title="Free space on the filesystem holding this worker’s state directory">
              {formatDisk(selectedWorker.resources)}
            </dd>
            {#if selectedWorker.listens.length > 0}
              <dt>Receivers</dt>
              <dd>
                {#each selectedWorker.listens as listening (listening.source)}
                  <div>
                    <code>{listening.source}</code> on {listensAt(
                      listening,
                      selectedWorker.hostName,
                    )}{listening.transport === 'udp' ? ' over UDP' : ''}
                  </div>
                {/each}
              </dd>
            {/if}
          </dl>
          {#snippet partRow(p: MemoryPart, nested: boolean)}
            <tr class:nested>
              <td
                >{p.owner === null && p.part === 'runtime' ? 'overhead' : p.part}{#if p.owner}<span
                    class="part-owner">{p.owner.kind} {p.owner.id}</span
                  >{/if}</td
              >
              <td class="num" class:over={isOver(p)}>{partNow(p)}</td>
              <td class="num">{bytes(p.maxBytes)}</td>
            </tr>
          {/snippet}
          {#if selectedMemory}
            <details class="detail-section fold memory-parts">
              <summary>
                <h3>
                  Memory by part
                  <Help label="Memory by part"
                    >Used: memory the worker holds now.<br />Expected max: the expected memory usage
                    if every buffer is full.<br />Limit: system memory limit, expect failures if
                    worker memory reaches this value.</Help
                  >
                </h3>
                <span class="memory-totals">
                  <span>used <b>{formatWorkerMemory(selectedWorker.resources)}</b></span>
                  <span>expected max <b>{bytes(selectedMemory.expectedMaxBytes)}</b></span>
                  <span>limit <b>{formatMemoryLimit(selectedWorker.resources)}</b></span>
                </span>
              </summary>
              <table class="part-table">
                <thead><tr><th>Part</th><th class="num">Now</th><th class="num">Max</th></tr></thead
                >
                <tbody>
                  {#if selectedMemory.supervisor}
                    {@render partRow(selectedMemory.supervisor, false)}
                  {/if}
                  <tr>
                    <td>
                      <button
                        class="group-toggle part-toggle"
                        aria-label={runtimeOpen ? 'Collapse runtime' : 'Expand runtime'}
                        aria-expanded={runtimeOpen}
                        onclick={() => (runtimeOpen = !runtimeOpen)}
                        ><span class="group-caret" class:expanded={runtimeOpen}>▶</span></button
                      >runtime
                    </td>
                    <td class="num" class:over={isOver(selectedMemory.runtime.whole)}>
                      {partNow(selectedMemory.runtime.whole)}
                    </td>
                    <td class="num">{bytes(selectedMemory.runtime.whole.maxBytes)}</td>
                  </tr>
                  {#if runtimeOpen}
                    {#each selectedMemory.runtime.parts as p (`${p.owner?.kind}/${p.owner?.id}/${p.part}`)}
                      {@render partRow(p, true)}
                    {/each}
                  {/if}
                </tbody>
              </table>
            </details>
          {/if}
          <details class="detail-section fold partitions">
            <summary><h3>Owned partitions ({selectedWorker.partitionsOwned})</h3></summary>
            <div class="fold-body">
              {#each selectedWorker.threads.flatMap((t) => t.partitionsOwned) as partition}<span
                  class="partition-chip">{partition.topic}:{partition.partition}</span
                >{:else}<p class="empty">No partitions owned.</p>{/each}
            </div>
          </details>
          <details class="detail-section fold threads">
            <summary><h3>Thread breakdown ({runningThreads(selectedWorker.threads)})</h3></summary>
            <div class="fold-body">
              {#each selectedWorker.threads.filter((t) => t.kind !== 'worker') as thread}<div
                  class="thread-line"
                >
                  <span>{thread.kind ?? 'thread'}</span><b class="mono">{thread.threadId}</b>
                </div>{:else}<p class="empty">No threads reporting.</p>{/each}
            </div>
          </details>
        </aside>
      {/if}
    </section>

    <section class="panel flow" aria-labelledby="{uid}-flow">
      <div class="panel-head">
        <div>
          <h2 id="{uid}-flow">
            Rows in and out — {selectionLabel}
            <Help label="Rows in and out"
              >Above the line, rows per second read from sources; below it, rows sent to
              destinations. A gap in a line is missing history, not zero.</Help
            >
          </h2>
        </div>
        <div class="filters" role="group" aria-label="Chart window">
          {#each FLOW_WINDOWS as w (w.minutes)}
            <button
              type="button"
              class="filter"
              class:on={windowMinutes === w.minutes}
              onclick={() => chooseWindow(w.minutes)}>{w.label}</button
            >
          {/each}
        </div>
      </div>
      {#if flow === null}
        <p class="empty">Loading…</p>
      {:else if !hasFlowData && bucketMillis > FINEST_BUCKET_MILLIS}
        <p class="empty">
          Nothing has aged into this window's buckets yet — wider windows are served from rollups.
        </p>
      {:else if !hasFlowData}
        <p class="empty">No rows recorded in this window yet.</p>
      {:else}
        <p class="rate">
          <span class="at mono">{readAt}</span>
          <span>
            in <strong>{rateText(readFrom(fleetInDense))}</strong> rows/s ·
            <strong>{bytesText(readFrom(fleetBytesInDense))}</strong>
          </span>
          <span>
            out <strong>{rateText(readFrom(fleetOutDense))}</strong> rows/s ·
            <strong>{bytesText(readFrom(fleetBytesOutDense))}</strong> delivered
          </span>
        </p>
        <FlowChart
          inPoints={fleetInDense}
          outPoints={fleetOutDense}
          from={clockTime(firstBucket(bucketMillis, windowFrom))}
          to={clockTime(windowTo)}
          onhover={(slot) => (hoveredSlot = slot)}
        />
      {/if}
    </section>

    <section class="panel ranked">
      <div class="panel-head">
        <div>
          <h2>
            Furthest from normal — {selectionLabel}
            <Help label="Furthest from normal"
              ><b>Normally</b> is a source's or destination's average rows per second at this hour of
              day over the last 30 days, whatever window the chart shows.</Help
            >
          </h2>
          {#if baseline !== null}<p class="note">
              {count(baselineSourceCount, 'source')}, {count(baselineDestCount, 'destination')}.
            </p>{/if}
        </div>
      </div>
      {#if baseline === null}
        <p class="empty">Loading…</p>
      {:else if ranked.length === 0}
        <p class="empty">Not enough history yet to say what any of these usually do.</p>
      {:else}
        <div class="table-wrap">
          <table>
            <thead
              ><tr
                ><th aria-sort={rankedSort.aria('name')}
                  ><SortButton sort={rankedSort} key="name">Name</SortButton></th
                ><th aria-sort={rankedSort.aria('kind')}
                  ><SortButton sort={rankedSort} key="kind">Kind</SortButton></th
                ><th aria-sort={rankedSort.aria('group')}
                  ><SortButton sort={rankedSort} key="group">Group</SortButton></th
                ><th class="num" aria-sort={rankedSort.aria('now')}
                  ><SortButton sort={rankedSort} key="now">Now, rows/s</SortButton></th
                ><th class="num" aria-sort={rankedSort.aria('normal')}
                  ><SortButton sort={rankedSort} key="normal">Normally</SortButton></th
                ><th class="num" aria-sort={rankedSort.aria('away')}
                  ><SortButton sort={rankedSort} key="away">Away by</SortButton></th
                ></tr
              ></thead
            >
            <tbody>
              {#each sortedRanked as r (JSON.stringify([r.group, r.kind, r.subjectId]))}
                <tr>
                  <td class="mono">{r.subjectId}</td><td>{r.kind}</td><td>{r.group}</td>
                  <td class="num">{Math.round(r.now)}</td><td class="num dim"
                    >{Math.round(r.normal)}</td
                  >
                  <td
                    class="num away"
                    class:big={Math.abs(r.awayPercent) >= 50}
                    class:mid={Math.abs(r.awayPercent) >= 20 && Math.abs(r.awayPercent) < 50}
                    >{r.awayPercent >= 0 ? '+' : ''}{Math.round(r.awayPercent)}%</td
                  >
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </section>

    <div class="lower-grid">
      <section class="panel errors">
        <div class="panel-head">
          <div>
            <h2>
              Errors — {selectionLabel}
              <Help label="Errors"
                >Rows that failed to parse, compile or run in a pipeline's function.</Help
              >
            </h2>
            <p class="note">{totalErrored} in the last {HISTORY_MINUTES} minutes.</p>
          </div>
        </div>
        {#if history.length >= 2}
          <Sparkline
            series={[{ label: 'rows errored', color: 'var(--error)', points: erroredSeries }]}
            times={history.map((p) => p.at)}
          />
        {:else}<p class="empty">Collecting data…</p>{/if}
        {#if erroredRows.length > 0}
          <div class="table-wrap">
            <table>
              <thead
                ><tr
                  ><th aria-sort={erroredSort.aria('pipeline')}
                    ><SortButton sort={erroredSort} key="pipeline">Pipeline</SortButton></th
                  ><th aria-sort={erroredSort.aria('function')}
                    ><SortButton sort={erroredSort} key="function">Function</SortButton></th
                  ><th aria-sort={erroredSort.aria('kind')}
                    ><SortButton sort={erroredSort} key="kind">Kind</SortButton></th
                  ><th class="num" aria-sort={erroredSort.aria('rows')}
                    ><SortButton sort={erroredSort} key="rows">Rows</SortButton></th
                  ></tr
                ></thead
              >
              <tbody
                >{#each sortedErroredRows as r (rowKey(r))}<tr
                    ><td>{pipelineName(r.values[0])}</td><td
                      ><code>{formatFunction(r.values[1])}</code></td
                    ><td>{r.values[2]}</td><td class="num">{r.total}</td></tr
                  >{/each}</tbody
              >
            </table>
          </div>
        {:else}<p class="empty">No errors recorded.</p>{/if}
      </section>
    </div>
  {/if}
</SectionShell>

<style>
  .rollout {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
    font-size: var(--text-label);
  }
  .rollout meter {
    width: 10rem;
  }
  .rollout-close {
    margin-left: auto;
    color: var(--dim);
    text-decoration: none;
    font-size: 1.1rem;
  }
  .behind {
    color: var(--warn);
  }
  .issue-ribbon {
    padding: 0.65rem 1rem;
    display: flex;
    align-items: center;
    gap: 1rem;
    color: var(--error);
    background: color-mix(in oklab, var(--error) 8%, var(--panel));
    border: 1px solid color-mix(in oklab, var(--error) 18%, var(--border));
    font-size: var(--text-note);
    text-decoration: none;
  }
  .issue-ribbon:hover {
    background: color-mix(in oklab, var(--error) 14%, var(--panel));
  }
  .issue-ribbon strong {
    flex: none;
  }
  .issue-divider {
    flex: none;
    opacity: 0.45;
  }
  /* The one thing on this row allowed to shrink -- a long message is cut
     with an ellipsis rather than pushing "View all issues" off screen. */
  .issue-teaser {
    min-width: 0;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
  }
  .issue-more {
    flex: none;
    margin-left: auto;
    font-weight: 600;
  }
  .fleet-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 2rem;
    padding: 1.5rem 0 1.25rem;
  }
  .fleet-title {
    display: flex;
    align-items: baseline;
    gap: 0.45rem;
  }
  .fleet-header h1 {
    margin: 0;
    font-family: var(--font-display);
    font-size: 2rem;
    line-height: 1.1;
  }
  .fleet-header p {
    margin: 0.35rem 0 0;
    color: var(--dim);
    font-size: var(--text-note);
  }
  .fleet-header p span {
    padding: 0 0.35rem;
  }
  .fleet-totals {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0;
  }
  .total {
    display: flex;
    gap: 0.45rem;
    align-items: center;
    padding: 0.25rem 1.15rem;
    border-left: 1px solid var(--border);
    font-size: var(--text-note);
    white-space: nowrap;
  }
  .total:first-child {
    border-left: 0;
  }
  .total i,
  .state-dot {
    width: 0.65rem;
    height: 0.65rem;
    border-radius: 50%;
    background: var(--dim);
    display: inline-block;
    flex: none;
  }
  .total.online i,
  .state-dot.online {
    background: var(--added);
  }
  .total.silent i,
  .total.neverHeartbeated i,
  .state-dot.silent,
  .state-dot.neverHeartbeated,
  .state-dot.revoked {
    background: var(--error);
  }
  /* After the heartbeat colors, so an issue on what an online worker runs shows over its green. */
  .state-dot.warning {
    background: var(--warn);
  }
  .state-dot.failed {
    background: var(--error);
  }
  .fleet-shell {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    border: 1px solid var(--border);
    background: var(--panel);
    min-height: 28rem;
  }
  .fleet-shell.has-detail {
    grid-template-columns: minmax(0, 1fr) minmax(20rem, 26rem);
  }
  .fleet-matrix {
    min-width: 0;
  }
  .worker-detail {
    position: relative;
    min-width: 0;
    border-left: 1px solid var(--border);
    padding: 1.15rem 1.25rem;
    background: color-mix(in oklab, var(--panel) 94%, var(--bg));
  }
  .matrix-tools {
    display: flex;
    gap: 0.75rem;
    padding: 0.7rem;
    border-bottom: 1px solid var(--border);
  }
  .matrix-search {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex: 1;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0 0.7rem;
    color: var(--dim);
  }
  .matrix-search input {
    width: 100%;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--ink);
    font: inherit;
    /* The browser's own, which set this box's height before the shared field padding. */
    padding: 1px 2px;
  }
  .matrix-columns,
  .worker-row {
    display: grid;
    grid-template-columns: minmax(12rem, 1.6fr) 1fr 0.55fr 0.65fr 0.85fr;
    align-items: center;
    gap: 0.75rem;
  }
  .matrix-columns {
    padding: 0.45rem 1rem;
    color: var(--dim);
    font-size: var(--text-micro);
    border-bottom: 1px solid var(--border);
  }
  .group-row {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 0;
    padding: 0;
    background: color-mix(in oklab, var(--bg) 55%, var(--panel));
    border-bottom: 1px solid var(--border);
    font-size: var(--text-note);
    font-family: inherit;
    color: inherit;
    text-align: left;
    border: 0;
    cursor: pointer;
  }
  .group-row:hover,
  .group-row.selected {
    background: color-mix(in oklab, var(--ink) 10%, var(--panel));
  }
  .group-toggle,
  .group-select {
    border: 0;
    background: transparent;
    color: inherit;
    font: inherit;
    cursor: pointer;
  }
  .group-toggle {
    align-self: stretch;
    padding: 0 0.3rem 0 1rem;
  }
  .group-caret {
    display: inline-block;
    color: var(--dim);
    font-size: 0.7rem;
    transition: transform 120ms ease;
  }
  .group-caret.expanded {
    transform: rotate(90deg);
  }
  .group-select {
    min-width: 0;
    flex: 1;
    display: flex;
    align-items: center;
    gap: 0.7rem;
    padding: 0.7rem 1rem 0.7rem 0.35rem;
    text-align: left;
  }
  .group-row strong {
    overflow-wrap: anywhere;
    font-size: var(--text-body);
  }
  .group-spacer {
    flex: 1;
  }
  .worker-row {
    width: 100%;
    padding: 0.55rem 1rem;
    border: 0;
    border-bottom: 1px solid var(--border);
    border-radius: 0;
    background: transparent;
    color: var(--ink);
    font: inherit;
    font-size: var(--text-note);
    text-align: left;
    cursor: pointer;
  }
  .worker-row:hover,
  .worker-row.selected {
    background: color-mix(in oklab, var(--ink) 7%, var(--panel));
  }
  .worker-row.attention {
    background: color-mix(in oklab, var(--warn) 8%, var(--panel));
  }
  .worker-row.attention.selected {
    background: color-mix(in oklab, var(--warn) 15%, var(--panel));
  }
  .worker-name {
    min-width: 0;
    display: flex;
    align-items: center;
    gap: 0.8rem;
    padding-left: 1rem;
  }
  .worker-name strong {
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .worker-row > span {
    min-width: 0;
  }
  .worker-row .last-seen {
    font-variant-numeric: tabular-nums;
  }
  /* The heartbeat's own lateness: a worker marked for an issue on what it runs heartbeats on time. */
  .worker-row .last-seen.late {
    color: var(--error);
  }
  .matrix-empty {
    padding: 2rem;
  }
  .detail-close {
    position: absolute;
    right: 1rem;
    top: 1rem;
    border: 0;
    background: none;
    color: var(--dim);
    /* The browser's own, which placed the glyph before the shared button padding. */
    padding: 1px 6px;
    font-size: 1.6rem;
    cursor: pointer;
  }
  .worker-detail h2 {
    max-width: 100%;
    overflow-wrap: anywhere;
    font-family: var(--font-display);
    font-size: 1.6rem;
    margin: 0;
    padding-right: 2rem;
  }
  .detail-group {
    overflow-wrap: anywhere;
    margin: 0.3rem 0 1rem;
    color: var(--dim);
  }
  .health-callout {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.8rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
  }
  .health-callout > strong {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    text-transform: uppercase;
  }
  .health-callout > span {
    padding-left: 1rem;
    border-left: 1px solid var(--border);
    color: var(--dim);
    font-size: var(--text-micro);
  }
  .health-callout b {
    display: block;
    color: var(--ink);
    font-size: var(--text-note);
    margin-top: 0.15rem;
  }
  .health-callout.silent,
  .health-callout.neverHeartbeated,
  .health-callout.revoked,
  .health-callout.silent b,
  .health-callout.neverHeartbeated b,
  .health-callout.revoked b {
    color: var(--error);
  }
  .worker-detail dl {
    display: grid;
    grid-template-columns: 8rem 1fr;
    gap: 0.5rem;
    margin: 1rem 0;
  }
  .worker-detail dt {
    color: var(--dim);
  }
  .worker-detail dd {
    margin: 0;
  }
  .detail-actions {
    margin-top: 0.75rem;
  }
  .detail-section {
    border-top: 1px solid var(--border);
    padding-top: 0.85rem;
    margin-top: 1rem;
  }
  .detail-section h3 {
    margin: 0 0 0.65rem;
    font-size: var(--text-label);
  }
  .partition-chip {
    display: inline-block;
    margin: 0 0.35rem 0.35rem 0;
    padding: 0.25rem 0.55rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--bg);
    font: var(--text-micro) var(--mono);
  }
  .thread-line {
    display: flex;
    justify-content: space-between;
    gap: 0.75rem;
    padding: 0.4rem 0;
    border-bottom: 1px solid var(--border);
    font-size: var(--text-note);
  }
  .thread-line b {
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .fold summary {
    cursor: pointer;
  }
  .fold summary h3 {
    display: inline;
  }
  .fold-body {
    margin-top: 0.65rem;
  }
  .memory-totals {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 0.75rem;
    margin-top: 0.35rem;
    color: var(--dim);
    font-size: var(--text-micro);
  }
  .memory-totals b {
    color: var(--ink);
    font-weight: normal;
  }
  .part-table {
    width: 100%;
    margin-top: 0.65rem;
    font-size: var(--text-note);
  }
  .part-owner {
    display: block;
    color: var(--dim);
    font: var(--text-micro) var(--mono);
    overflow-wrap: anywhere;
  }
  .part-toggle {
    padding: 0 0.3rem 0 0;
  }
  .part-table tr.nested td:first-child {
    padding-left: 1.4rem;
  }
  .part-table td.over {
    color: var(--warn);
  }

  h2 {
    display: flex;
    align-items: center;
    gap: 0.25rem;
    margin: 0;
    font-size: var(--text-section);
  }
  section {
    margin-top: 1.5rem;
  }
  .dim,
  .rate {
    color: var(--dim);
  }
  /* A note or an empty state is a sentence someone reads, so it is ink. */
  .note,
  .empty,
  .sentence {
    color: var(--ink);
  }

  /* The clock and the two readings wrap as three items on a narrow panel rather than
     breaking mid-figure. */
  .rate {
    display: flex;
    flex-wrap: wrap;
    column-gap: 1rem;
    row-gap: 0.1rem;
  }

  /* The clock leads the line and is the one part not dimmed: it says which bucket the
     figures belong to. `.mono` holds its width steady as the reading moves across the
     window, so the figures beside it do not shuffle. */
  .rate .at {
    color: var(--ink);
  }
  .mono,
  code {
    font-family: var(--mono);
  }
  .note,
  .empty,
  .sentence,
  .rate {
    margin: 0.35rem 0 0;
    font-size: var(--text-note);
    line-height: 1.45;
  }
  .error,
  .warning {
    margin: 0 0 1rem;
    font-size: var(--text-body);
  }
  .warning {
    color: var(--warn);
  }
  .live-controls,
  .worker-controls,
  .filters {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
  }
  .filter {
    font: inherit;
    font-size: var(--text-note);
  }
  .panel {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 1rem;
  }
  .panel-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 1rem;
    flex-wrap: wrap;
  }
  .counts {
    color: var(--dim);
    font-family: var(--mono);
    font-size: var(--text-note);
    white-space: nowrap;
  }
  .warning-list {
    margin-top: 0.8rem;
    border-top: 1px solid var(--border);
  }
  .warn-row {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto;
    gap: 0.75rem;
    align-items: start;
    padding: 0.7rem 0;
    border-bottom: 1px solid var(--border);
  }
  .warn-row:last-child {
    border-bottom: 0;
    padding-bottom: 0;
  }
  .sev,
  .status {
    display: inline-block;
    border: 1px solid currentColor;
    border-radius: var(--radius);
    padding: 0.12rem 0.4rem;
    font-size: var(--text-micro);
    font-weight: 600;
    white-space: nowrap;
  }
  .sev-failed,
  .status-revoked {
    color: var(--error);
  }
  .sev-warning,
  .status-stale {
    color: var(--warn);
  }
  .status-online {
    color: var(--added);
  }
  .status-silent {
    color: var(--dim);
  }
  .names {
    display: flex;
    gap: 0.5rem;
    align-items: baseline;
    flex-wrap: wrap;
    font-size: var(--text-label);
  }
  .condition {
    font-weight: 600;
  }
  .age {
    font-size: var(--text-micro);
    white-space: nowrap;
  }
  .summary-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 0.8rem;
  }
  .metric-card {
    min-width: 0;
    padding: 1rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  .metric-label {
    color: var(--dim);
    font-size: var(--text-label);
    font-weight: 600;
  }
  .metric-value {
    margin: 0.45rem 0 0;
    font-family: var(--font-display);
    font-size: 1.8rem;
    font-variant-numeric: tabular-nums;
    line-height: 1;
  }
  .metric-value span {
    color: var(--dim);
    font-family: var(--font-body);
    font-size: var(--text-note);
    font-weight: 400;
  }
  .metric-note {
    min-height: 2.5em;
    margin: 0.45rem 0 0;
    color: var(--dim);
    font-size: var(--text-micro);
    line-height: 1.35;
  }
  .flow .filters {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow: hidden;
    gap: 0;
  }
  .filter {
    padding: 0.3rem 0.6rem;
    color: var(--dim);
    background: transparent;
    border: 0;
    border-right: 1px solid var(--border);
    cursor: pointer;
  }
  .filter:last-child {
    border-right: 0;
  }
  /* The accent belongs to the page's primary action; the chosen window is ink and bold. */
  .filter.on {
    color: var(--ink);
    font-weight: 600;
  }
  .table-wrap {
    margin-top: 0.8rem;
    overflow-x: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-body);
  }
  th,
  td {
    padding: 0.55rem 0.7rem;
    border-bottom: 1px solid var(--border);
    text-align: left;
    white-space: nowrap;
  }
  th {
    color: var(--dim);
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  tbody tr:last-child td {
    border-bottom: 0;
  }
  .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .away.mid {
    color: var(--warn);
  }
  .away.big {
    color: var(--removed);
    font-weight: 600;
  }
  .chip {
    display: inline-block;
    margin: 0.1rem 0.2rem 0.1rem 0;
    padding: 0.1rem 0.35rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    font-family: var(--mono);
    font-size: var(--text-micro);
  }
  .lower-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.8rem;
    align-items: start;
  }
  .lower-grid section {
    min-width: 0;
  }
  @media (max-width: 64rem) {
    .fleet-header {
      align-items: flex-start;
      flex-direction: column;
    }
    .fleet-totals .total:first-child {
      padding-left: 0;
    }
    .fleet-shell.has-detail {
      grid-template-columns: 1fr;
    }
    .worker-detail {
      border-left: 0;
      border-top: 1px solid var(--border);
    }
    .summary-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }
  @media (max-width: 44rem) {
    .matrix-tools {
      align-items: stretch;
      flex-direction: column;
    }
    .matrix-columns {
      display: none;
    }
    .worker-row {
      grid-template-columns: minmax(9rem, 1fr) auto auto;
    }
    .worker-row > :nth-child(2) {
      display: none;
    }
    .worker-row > :nth-child(4) {
      display: none;
    }
    .summary-grid,
    .lower-grid {
      grid-template-columns: 1fr;
    }
    .live-controls,
    .worker-controls {
      width: 100%;
    }
    .worker-search {
      flex: 1 1 12rem;
    }
    .warn-row {
      grid-template-columns: 1fr;
      gap: 0.3rem;
    }
  }
</style>
