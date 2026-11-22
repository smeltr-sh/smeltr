<!--
  Cluster status (`gui-design.md`, "Worker health status"), polled from the
  heartbeats (`/api/health`), the roster (`/api/workers`) and the groups the
  fleet runs (`/api/worker-groups`). Nothing here is edited.

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
  window picker. Errors read the leader's series too, over a range of
  their own.
-->
<script lang="ts">
  import Button, { BUSY } from './Button.svelte';
  import WorkerGroupIcon from './WorkerGroupIcon.svelte';
  import ChevronIcon from './ChevronIcon.svelte';
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import { store, storedJson } from './stored';
  import Locked from './Locked.svelte';
  import SectionShell from './SectionShell.svelte';
  import { ask } from './Confirm.svelte';
  import PageHeader from './PageHeader.svelte';
  import Help from './Help.svelte';
  import Panel from './Panel.svelte';
  import CrossButton from './CrossButton.svelte';
  import FlowChart from './FlowChart.svelte';
  import Sparkline from './Sparkline.svelte';
  import StateMark from './StateMark.svelte';
  import { severityState, type State } from './stateGlyphs';
  import { names } from './names.svelte';
  import { byteUnit, bytes } from './bytes';
  import SortButton from './SortButton.svelte';
  import FoldAllButton from './FoldAllButton.svelte';
  import Chips from './Chips.svelte';
  import Segmented from './Segmented.svelte';
  import RangePicker from './RangePicker.svelte';
  import { chartRange, lastChartRange, rangeOfMinutes } from './map';
  import { TableSort } from './sort.svelte';
  import {
    errorText,
    getHealth,
    listWorkers,
    liveGroups,
    getFlow,
    getSeries,
    listWarnings,
    history as mainHistory,
    restartWorker,
    conditionLabel,
    MAIN_BRANCH,
    type Health,
    type WorkerCredentialDto,
    type WorkerGroupDto,
    type FlowKind,
    type FlowResponseDto,
    type SeriesAnswerDto,
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
  import { formatFunction, formatSample, sampleKey } from './metrics';
  import { FLOW_METRICS, denseAnswer, furthestFromNormal, lastClosedBucket } from './flow';
  import { DEFAULT_VIEW, seriesQuery } from './metricsView';
  import { clockTime, count, shortCommit } from './time';

  // The commit a deploy just pointed the fleet at, from the hash: the page follows
  // its rollout until it is dismissed.
  let { deploying }: { deploying?: string } = $props();
  // A panel is a region named by its heading.
  const uid = $props.id();

  /** The fleet as the last poll read it. A roster or group list that could not be read is `null`, not empty: the
   * error bar says why, and an unread roster must not render as no workers. */
  type Fleet = {
    health: Health;
    workers: WorkerCredentialDto[] | null;
    workerGroups: WorkerGroupDto[] | null;
  };
  let status: Fleet | null = $state(null);
  let loading = $state(true);
  // The status page begins live. Pausing stops every existing poll without changing their cadence.
  let autoRefresh = $state(true);

  // Keyed by worker id, not row index: the list changes between polls.
  let expanded: Record<string, boolean> = $state({});

  const POLL_MS = 2000;

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
  /** The selected worker or group, as the labels a series must hold. */
  function selectionFilter(): [string, string][] {
    if (selectedWorkerId) return [['worker', selectedWorkerId]];
    if (selectedGroupId) return [['group', selectedGroupId]];
    return [];
  }

  // -- errors: the leader's own series, over the panel's own range ----------

  const ERRORED = 'smeltr_rows_errored_total';

  let errorsMinutes = $state(lastChartRange().minutes);
  type ErrorsRead = { at: number; total: SeriesAnswerDto; split: SeriesAnswerDto };
  let errors = $state<ErrorsRead | null>(null);
  /** The request the newest errors fetch made, so an older answer does not land over it. */
  let errorsAsked: string | null = null;

  function fetchErrors() {
    const ask = seriesQuery(
      { ...DEFAULT_VIEW, filter: selectionFilter(), windowMinutes: errorsMinutes },
      ERRORED,
      0,
    );
    errorsAsked = ask;
    const by = (labels: string) => {
      const q = new URLSearchParams(ask);
      q.set('by', labels);
      return q.toString();
    };
    Promise.all([getSeries(ask), getSeries(by('group,pipeline,function,kind'))])
      .then(([total, split]) => {
        if (errorsAsked === ask) errors = { at: Date.now(), total, split };
      })
      .catch(() => {});
  }

  /** The errored rows' line, per second, up to the last complete point. */
  let erroredTo = $derived(errors ? lastClosedBucket(errors.total.pointMillis, errors.at) : 0);
  let erroredSeries = $derived(errors ? denseAnswer(errors.total, erroredTo) : []);
  let erroredTimes = $derived(
    errors
      ? erroredSeries.map((_, i) => errors!.total.fromMillis + i * errors!.total.pointMillis)
      : [],
  );
  /** The errored rows of each pipeline, function and kind over the range; a pipeline is shown by its name. */
  let erroredRows = $derived.by(() => {
    if (!errors) return [];
    const seconds = errors.split.pointMillis / 1000;
    const rows = new Map<
      string,
      { pipeline: string; function: string; kind: string; total: number }
    >();
    for (const s of errors.split.series) {
      const [group, pipeline, fn, kind] = s.values;
      const name = names.of(MAIN_BRANCH, group, 'pipelines', pipeline);
      const key = JSON.stringify([name, fn, kind]);
      const row = rows.get(key) ?? { pipeline: name, function: fn, kind, total: 0 };
      row.total += s.points.reduce((a, [, v]) => a + v, 0) * seconds;
      rows.set(key, row);
    }
    return [...rows.values()].map((r) => ({ ...r, total: Math.round(r.total) }));
  });
  let totalErrored = $derived(erroredRows.reduce((a, r) => a + r.total, 0));
  const erroredSort = new TableSort<'pipeline' | 'function' | 'kind' | 'rows'>();
  let sortedErroredRows = $derived(
    erroredSort.apply(erroredRows, {
      pipeline: (r) => r.pipeline,
      function: (r) => formatFunction(r.function),
      kind: (r) => r.kind,
      rows: (r) => r.total,
    }),
  );

  /** What a person calls a row's pipeline or route: its name in its group on `main`. */
  const nameOf = (r: { group: string; subjectId: string }) =>
    kind === 'route' || kind === 'pipeline'
      ? names.of(MAIN_BRANCH, r.group, `${kind}s`, r.subjectId)
      : r.subjectId;

  // -- flow (E2): the window-picked chart --------------------------------

  let windowMinutes = $state(lastChartRange().minutes);
  /** The table's totals and the sources' normals. */
  let flow: FlowResponseDto | null = $state(null);

  /** What the chart and the table count. */
  let unit: 'rows' | 'bytes' = $state('rows');
  /** What the table lists. */
  let kind: FlowKind = $state('source');
  /** The table row the chart is narrowed to, or `null` for everything the selection holds. */
  let picked: { group: string; subjectId: string } | null = $state(null);

  function fetchFlow() {
    // Reads `windowMinutes`, so the effect below re-fetches the moment it changes rather than
    // waiting for the next tick.
    getFlow(windowMinutes)
      .then((f) => (flow = f))
      .catch(() => {
        // Keep showing the last window rather than blanking a working table over one bad poll.
      });
  }

  /** What the chart reads from `GET /api/series`: the picked row's in and out, or the fleet's,
   * within the selected group or worker. */
  let chartAsk = $derived.by(() => {
    const selection = selectionFilter();
    const filter: [string, string][] = picked
      ? [...selection, ['group', picked.group], [kind, picked.subjectId]]
      : selection;
    return { metrics: FLOW_METRICS[picked ? kind : 'fleet'], filter, selection, windowMinutes };
  });
  type FlowCharts = {
    /** When they were read: the chart ends at the point before the one this falls in. */
    at: number;
    rowsIn: SeriesAnswerDto;
    rowsOut: SeriesAnswerDto;
    bytesIn: SeriesAnswerDto;
    bytesOut: SeriesAnswerDto;
    /** The fleet's rows out within the selection, for the line at the top of the page. */
    fleetOut: SeriesAnswerDto;
  };
  let charts = $state<FlowCharts | null>(null);
  /** The ask the newest fetch is for: an answer to an older one is for a chart no longer shown. */
  let asked: typeof chartAsk | null = null;

  function fetchCharts(ask: typeof chartAsk) {
    asked = ask;
    // On UTC's clock, as `/api/flow` totals the table, so a row's total is its chart's.
    const read = (metric: string, filter: [string, string][]) =>
      getSeries(
        seriesQuery({ ...DEFAULT_VIEW, filter, windowMinutes: ask.windowMinutes }, metric, 0),
      );
    Promise.all([
      ...ask.metrics.map((metric) => read(metric, ask.filter)),
      read(FLOW_METRICS.fleet[1], ask.selection),
    ])
      .then(([rowsIn, rowsOut, bytesIn, bytesOut, fleetOut]) => {
        if (asked === ask)
          charts = { at: Date.now(), rowsIn, rowsOut, bytesIn, bytesOut, fleetOut };
      })
      .catch(() => {});
  }

  // A new window, selection or pick reads the chart at once, paused or not.
  $effect(() => fetchCharts(chartAsk));
  // Likewise the exceptions panel's own range and the selection.
  $effect(fetchErrors);

  function pick(row: { group: string; subjectId: string } | null) {
    picked = row;
  }

  function chooseKind(next: FlowKind) {
    kind = next;
    pick(null);
  }

  let pointMillis = $derived(charts?.rowsIn.pointMillis ?? 0);
  let windowFrom = $derived(charts?.rowsIn.fromMillis ?? 0);
  /** Where the chart stops. The point covering now is still filling, so its total is only
   * what has landed so far -- drawn, it is a dip that fills itself in on the next poll. */
  let windowTo = $derived(charts ? lastClosedBucket(pointMillis, charts.at) : 0);

  /** The fleet line at the top of the page, which answers before the chart is reached: the
   * last point the chart draws. */
  let nowOutRate = $derived(
    charts ? (denseAnswer(charts.fleetOut, windowTo).at(-1) ?? null) : null,
  );
  /** A rate, or `--` where the bucket that would carry it is missing. Not "0 rows/s": a
   * fleet nobody has heard from and a fleet moving nothing are different answers. A rate
   * under one takes two significant digits, the same count a grid step carries: rounding
   * it would print 0 for a fleet whose chart, right below, draws a line. */
  const rateText = (rate: number | null) =>
    rate === null ? '--' : rate > 0 && rate < 1 ? rate.toPrecision(2) : String(Math.round(rate));

  /** A byte rate, `--` where the bucket is missing, for the same reason `rateText` gives. */
  const bytesText = (rate: number | null) => (rate === null ? '--' : `${bytes(rate)}/s`);

  // -- the flow panel's chart and table: the fleet's, or one picked row's, in rows or bytes.

  // Per second: the unit the rate line above the chart is in, and the only one that holds across
  // the picker -- a point is 10s at sixty minutes and six hours at thirty days.
  const chartRates = (answer: SeriesAnswerDto | undefined) =>
    answer ? denseAnswer(answer, windowTo) : [];
  let chartIn = $derived(chartRates(unit === 'rows' ? charts?.rowsIn : charts?.bytesIn));
  let chartOut = $derived(chartRates(unit === 'rows' ? charts?.rowsOut : charts?.bytesOut));
  let hasChartData = $derived(chartIn.some((v) => v !== null) || chartOut.some((v) => v !== null));
  /** Bytes are drawn in the unit the busiest bucket reads best in. */
  let chartScale = $derived(
    unit === 'rows'
      ? { unit: 'rows/s', size: 1 }
      : (({ unit, size }) => ({ unit: `${unit}/s`, size }))(
          byteUnit(Math.max(0, ...chartIn.map((v) => v ?? 0), ...chartOut.map((v) => v ?? 0))),
        ),
  );
  const scaled = (dense: (number | null)[]) =>
    dense.map((v) => (v === null ? null : v / chartScale.size));
  const rateOf = (rate: number | null) =>
    unit === 'rows' ? `${rateText(rate)} rows/s` : bytesText(rate);

  const KINDS: { value: FlowKind; label: string; one: string }[] = [
    { value: 'source', label: 'Sources', one: 'source' },
    { value: 'pipeline', label: 'Pipelines', one: 'pipeline' },
    { value: 'route', label: 'Routes', one: 'route' },
    { value: 'destination', label: 'Destinations', one: 'destination' },
  ];
  /** What each half counts, named for where it is counted (`leader::FlowKind::metrics`). The
   * fleet's out is what destinations delivered, which only a destination's own series carries. */
  const WORDS: Record<FlowKind, [string, string]> = {
    source: ['read', 'sent'],
    pipeline: ['matched', 'sent'],
    route: ['matched', 'sent'],
    destination: ['received', 'delivered'],
  };
  const FLEET_WORDS: [string, string] = ['read', 'delivered'];
  let chartWords = $derived(picked ? WORDS[kind] : FLEET_WORDS);
  type TableRow = {
    group: string;
    subjectId: string;
    source?: string | null;
    pipeline?: string | null;
    in: number;
    out: number;
  };
  /** One row per object the selection holds, its workers' counts added. */
  let tableRows = $derived.by(() => {
    const rows = new Map<string, TableRow>();
    // An answer from before the leader totalled the window has none.
    for (const t of flow?.totals ?? []) {
      if (t.kind !== kind || !flowMatchesSelection(t)) continue;
      const key = JSON.stringify([t.group, t.subjectId]);
      const row = rows.get(key) ?? { group: t.group, subjectId: t.subjectId, in: 0, out: 0 };
      row.source ??= t.source;
      row.pipeline ??= t.pipeline;
      row.in += unit === 'rows' ? t.rowsIn : t.bytesIn;
      row.out += unit === 'rows' ? t.rowsOut : t.bytesOut;
      rows.set(key, row);
    }
    return [...rows.values()].sort((a, b) => b.in - a.in || b.out - a.out);
  });
  /** Only sources' and destinations' rows add up to the selection's: a row goes to every
   * route that matches it, so routes and pipelines can count it more than once. */
  let showsSum = $derived(kind === 'source' || kind === 'destination');
  const totalsSort = new TableSort<'name' | 'group' | 'source' | 'pipeline' | 'in' | 'out'>();
  let sortedTableRows = $derived(
    totalsSort.apply(tableRows, {
      name: nameOf,
      group: (r) => r.group,
      source: (r) => r.source,
      pipeline: (r) => r.pipeline,
      in: (r) => r.in,
      out: (r) => r.out,
    }),
  );
  let sumIn = $derived(tableRows.reduce((a, r) => a + r.in, 0));
  let sumOut = $derived(tableRows.reduce((a, r) => a + r.out, 0));
  const amount = (n: number) => (unit === 'rows' ? Math.round(n).toLocaleString() : bytes(n));
  let unitWord = $derived(unit === 'rows' ? 'Rows' : 'Bytes');
  const isPicked = (r: TableRow) => picked?.group === r.group && picked.subjectId === r.subjectId;

  /** The bucket the pointer is over, or `null` when it is away. `FlowChart` owns finding
   * it; the reading is made here, where the series it does not draw are held. */
  let hoveredSlot: number | null = $state(null);

  /** Which bucket the readout names: the hovered one, or the last the chart drew. That
   * last slot is the bucket `currentRate` reads, so the line and the chart's right edge
   * are one reading and cannot disagree. */
  let readSlot = $derived(hoveredSlot ?? chartIn.length - 1);
  let readAt = $derived(
    pointMillis && readSlot >= 0 ? clockTime(windowFrom + readSlot * pointMillis) : '--',
  );
  const readFrom = (dense: (number | null)[]) => (readSlot >= 0 ? (dense[readSlot] ?? null) : null);

  // -- furthest from normal (Main storyboard): each source's reading against the normal
  // its warnings read, whatever window the chart shows. A source is in the selection when a
  // total the selection holds counts it.

  let normals = $derived.by(() => {
    const read = flow;
    return read === null
      ? []
      : read.normals.filter((n) =>
          read.totals.some(
            (t) =>
              t.kind === 'source' &&
              t.group === n.group &&
              t.subjectId === n.subjectId &&
              flowMatchesSelection(t),
          ),
        );
  });
  let ranked = $derived(furthestFromNormal(normals));
  const rankedSort = new TableSort<'name' | 'group' | 'now' | 'normal' | 'away'>();
  let sortedRanked = $derived(
    rankedSort.apply(ranked, {
      name: (r) => r.subjectId,
      group: (r) => r.group,
      now: (r) => r.now,
      normal: (r) => r.low,
      away: (r) => r.awayPercent,
    }),
  );

  /** Issues the ribbon names; the Issues page lists the rest. */
  const RIBBON_ISSUES = 3;

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
    const read = <T,>(p: Promise<T>) => p.catch(() => null);
    Promise.all([getHealth(), read(listWorkers()), read(liveGroups())])
      .then(([health, workers, workerGroups]) => {
        status = { health, workers, workerGroups };
        now = Date.now();
      })
      .catch(reported)
      .finally(() => {
        loading = false;
      });
    listWarnings()
      .then((w) => {
        // Failed first: the ribbon names only the first few, and a broken thing must never sit
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
    const id = setInterval(() => {
      fetchFlow();
      fetchCharts(chartAsk);
      fetchErrors();
    }, POLL_MS);
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
  function formatConnections(resources: WorkerRow['resources']): string {
    if (resources?.connectionBudget == null) return '--';
    return `up to ${resources.connectionBudget.toLocaleString('en-US')}`;
  }

  function connectionsTitle(resources: WorkerRow['resources']): string {
    const limit = resources?.openFileLimit;
    return limit == null
      ? 'The connections this worker’s receivers may hold together'
      : `The connections this worker’s receivers may hold together, within its open-file limit of ${limit.toLocaleString('en-US')}`;
  }

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

  let silentSeconds = $derived.by(() => (status?.health.silentAfterMs ?? 0) / 1000);

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
      status?.health.threads ?? [],
      status?.health.silentAfterMs ?? 0,
    ),
  );
  const EXPANDED_GROUPS_KEY = 'smeltr-status-expanded-groups';
  function loadExpandedGroups(): Record<string, boolean> {
    const saved = storedJson<unknown>(EXPANDED_GROUPS_KEY, {});
    return saved && typeof saved === 'object' ? (saved as Record<string, boolean>) : {};
  }
  let expandedGroups: Record<string, boolean> = $state(loadExpandedGroups());
  function saveExpandedGroups() {
    store(EXPANDED_GROUPS_KEY, JSON.stringify(expandedGroups));
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
  /** A worker's sign: a failed or warning health, else online is ok and stopped is info. */
  function workerState(w: WorkerRow): State {
    const health = healthOf.get(w.workerId) ?? 'healthy';
    if (health === 'failed' || health === 'warning') return severityState(health);
    return w.status === 'online' ? 'ok' : 'info';
  }

  function workerStateLabel(w: WorkerRow): string {
    const health = healthOf.get(w.workerId) ?? 'healthy';
    return health === 'healthy' || health === 'info'
      ? STATUS_LABEL[w.status]
      : `${STATUS_LABEL[w.status]}, ${health === 'failed' ? 'failing' : 'warning'}`;
  }

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
  // The deployed commit's message, so the card names the change and not only its hash. History's
  // first page: one deployed from further back shows its hash alone.
  let deployingSubject = $state('');
  $effect(() => {
    const target = deploying;
    deployingSubject = '';
    if (!target) return;
    let current = true;
    mainHistory(50)
      .then((h) => {
        const found = h.commits.find(
          (c) => c.commit.startsWith(target) || target.startsWith(c.commit),
        );
        if (current) deployingSubject = found?.subject ?? '';
      })
      .catch(() => {});
    return () => {
      current = false;
    };
  });
  // Online and not yet running the commit being deployed.
  function behind(worker: WorkerRow): boolean {
    return !!deploying && worker.status === 'online' && !onCommit(worker, deploying);
  }

  let totalPartitions = $derived(
    workerRows.reduce((total, worker) => total + worker.partitionsOwned.length, 0),
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
    fetchCharts(chartAsk);
    fetchErrors();
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
        <span class="issue-head">
          <strong
            ><StateMark state={severityState(warnings[0].severity)} />
            {count(warnings.length, 'active issue')}</strong
          >
          <span class="issue-more">View all issues &rsaquo;</span>
        </span>
        {#each warnings.slice(0, RIBBON_ISSUES) as w, i (i)}
          <span class="issue-row">
            <StateMark state={severityState(w.severity)} />
            <span class="issue-teaser ellipsis">{w.subjectId}: {w.message}</span>
          </span>
        {/each}
        {#if warnings.length > RIBBON_ISSUES}
          <span class="issue-overflow">+{count(warnings.length - RIBBON_ISSUES, 'more issue')}</span
          >
        {/if}
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
          {count(workerRows.length, 'worker')} across {count(
            status.workerGroups?.length ?? 0,
            'worker group',
          )}
          <span>·</span>
          refreshed every {POLL_MS / 1000}s
        </p>
      </div>
      <div class="fleet-totals" aria-label="Fleet totals">
        <span class="total"
          ><StateMark state="ok" /><strong>{workerCounts.online}</strong> online</span
        >
        <span class="total"
          ><StateMark state="error" /><strong>{workerCounts.silent}</strong> silent</span
        >
        {#if workerCounts.neverHeartbeated > 0}
          <span class="total"
            ><StateMark state="error" /><strong>{workerCounts.neverHeartbeated}</strong> never heartbeated</span
          >
        {/if}
        {#if workerCounts.stopped > 0}
          <span class="total"
            ><StateMark state="info" /><strong>{workerCounts.stopped}</strong> stopped</span
          >
        {/if}
        <span class="total"><strong>{totalPartitions}</strong> partitions</span>
        <span class="total"><strong>{rateText(nowOutRate)}</strong> rows/s</span>
      </div>
    </header>

    {#if deploying && fleetRollout}
      {@const done = fleetRollout.on === fleetRollout.of}
      <div class="rollout" role="status">
        <Panel title={`${done ? 'Deployed' : 'Deploying'} ${commitOrNone(deploying)}`}>
          {#snippet actions()}
            <CrossButton label="Stop following this deploy" href="#/status" />
          {/snippet}
          <div class="rollout-body">
            {#if deployingSubject}<strong>{deployingSubject}</strong>{/if}
            <meter min="0" max={Math.max(fleetRollout.of, 1)} value={fleetRollout.on}></meter>
            <span
              >{done
                ? `on all ${fleetRollout.of} online ${fleetRollout.of === 1 ? 'worker' : 'workers'}`
                : `${fleetRollout.on} of ${fleetRollout.of} online workers`}</span
            >
          </div>
        </Panel>
      </div>
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
            disabled={displayedGroups.length === 0 ? 'No worker groups are shown.' : ''}
            onclick={() => setAllGroups(!allGroupsOpen)}
          />
          <label class="matrix-search"
            ><span aria-hidden="true">⌕</span><input
              type="search"
              bind:value={workerQuery}
              placeholder="Search workers, worker groups, commits…"
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
                class="icon group-toggle"
                aria-label={isExpanded(group) ? `Collapse ${group}` : `Expand ${group}`}
                aria-expanded={isExpanded(group)}
                onclick={() => toggleGroup(group)}
              >
                <ChevronIcon open={isExpanded(group)} />
              </button>
              <button
                class="group-select"
                onclick={() => {
                  selectedWorkerId = null;
                  selectedGroupId = selectedGroupId === group ? null : group;
                }}
              >
                <WorkerGroupIcon /><strong>{group}</strong><span
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
                <span>{workers.reduce((n, w) => n + w.partitionsOwned.length, 0)} partitions</span>
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
                    ><StateMark state={workerState(worker)} /><strong>{worker.workerId}</strong
                    ></span
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
          <span class="detail-close"
            ><CrossButton
              label="Close worker details"
              onclick={() => {
                selectedWorkerId = null;
              }}
            /></span
          >
          <h2>{selectedWorker.workerId}</h2>
          <p class="detail-group">
            {selectedWorker.groupId ?? 'unassigned'} · {selectedWorker.workerId}
          </p>
          <div class="health-callout {selectedWorker.status}">
            <strong
              ><StateMark
                state={workerState(selectedWorker)}
                label={workerStateLabel(selectedWorker)}
              />{STATUS_LABEL[selectedWorker.status]}</strong
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
                <Button
                  type="button"
                  class="compact"
                  disabled={restarting
                    ? BUSY
                    : selectedWorker.asked === 'restart'
                      ? 'A restart has already been requested for this worker.'
                      : ''}
                  onclick={() => selectedWorker && restart(selectedWorker.workerId)}
                  >{selectedWorker.asked === 'restart' ? 'Restarting…' : 'Restart'}</Button
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
            <dt class="glyphed"><WorkerGroupIcon />Group</dt>
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
            <dt>Connections</dt>
            <dd title={connectionsTitle(selectedWorker.resources)}>
              {formatConnections(selectedWorker.resources)}
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
                        class="icon group-toggle part-toggle"
                        aria-label={runtimeOpen ? 'Collapse runtime' : 'Expand runtime'}
                        aria-expanded={runtimeOpen}
                        onclick={() => (runtimeOpen = !runtimeOpen)}
                        ><ChevronIcon open={runtimeOpen} /></button
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
            <summary><h3>Owned partitions ({selectedWorker.partitionsOwned.length})</h3></summary>
            <div class="fold-body">
              {#each selectedWorker.partitionsOwned as partition}<span class="partition-chip"
                  >{partition.topic}:{partition.partition}</span
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
            {unitWord} in and out — {#if picked}{selectionLabel === 'whole fleet'
                ? ''
                : `${selectionLabel}, `}{KINDS.find((k) => k.value === kind)?.one}
              <span class="picked mono"
                >{selectionLabel === 'whole fleet'
                  ? `${nameOf(picked)} (${picked.group})`
                  : nameOf(picked)}<button
                  type="button"
                  aria-label={selectionLabel === 'whole fleet'
                    ? 'Show the whole fleet'
                    : `Show all of ${selectionLabel}`}
                  onclick={() => pick(null)}>×</button
                ></span
              >{:else}{selectionLabel}{/if}
            <Help label="{unitWord} in and out"
              >Above the line, what came in per second; below it, what went out. A source reads and
              sends on; a pipeline or route matches and sends on; a destination receives and
              delivers. A row goes to every route whose condition matches it, so routes and
              pipelines can add up to more than their sources read, and a row sent to two
              destinations is sent twice. A gap in a line is missing history, not zero.</Help
            >
          </h2>
        </div>
        <div class="controls">
          <Segmented
            label="Count"
            options={[
              { value: 'rows', label: 'Rows' },
              { value: 'bytes', label: 'Bytes' },
            ]}
            bind:value={unit}
          />
          <span class="dim">Over the last {rangeOfMinutes(windowMinutes)?.label}</span>
          <RangePicker
            range={rangeOfMinutes(windowMinutes)?.range ?? ''}
            onpick={(r) => chooseWindow(chartRange(r).minutes)}
          />
        </div>
      </div>
      {#if charts === null}
        <p class="empty">Loading…</p>
      {:else if !hasChartData}
        <p class="empty">No rows recorded in this window yet.</p>
      {:else}
        <p class="rate">
          <span class="at mono">{readAt}</span>
          <span>{chartWords[0]} <strong>{rateOf(readFrom(chartIn))}</strong></span>
          <span>{chartWords[1]} <strong>{rateOf(readFrom(chartOut))}</strong></span>
        </p>
        <FlowChart
          inPoints={scaled(chartIn)}
          outPoints={scaled(chartOut)}
          unit={chartScale.unit}
          words={chartWords}
          label="{unitWord} per second, in above the line and out below it, over the chosen window"
          from={clockTime(windowFrom)}
          to={clockTime(windowTo)}
          onhover={(slot) => (hoveredSlot = slot)}
        />
      {/if}
      <div class="table-head">
        <Segmented
          label="Show by"
          options={KINDS.map(({ value, label }) => ({ value, label }))}
          bind:value={() => kind, chooseKind}
        />
      </div>
      {#if tableRows.length > 0}
        <div class="table-wrap">
          <table>
            <thead
              ><tr
                ><th aria-sort={totalsSort.aria('name')}
                  ><SortButton sort={totalsSort} key="name">Name</SortButton></th
                >
                <th aria-sort={totalsSort.aria('group')}
                  ><SortButton sort={totalsSort} key="group"
                    ><WorkerGroupIcon />Worker group</SortButton
                  ></th
                >
                {#if kind === 'route'}<th aria-sort={totalsSort.aria('source')}
                    ><SortButton sort={totalsSort} key="source">Source</SortButton></th
                  ><th aria-sort={totalsSort.aria('pipeline')}
                    ><SortButton sort={totalsSort} key="pipeline">Pipeline</SortButton></th
                  >{/if}
                <th class="num" aria-sort={totalsSort.aria('in')}
                  ><SortButton sort={totalsSort} key="in">{unitWord} {WORDS[kind][0]}</SortButton
                  ></th
                ><th class="num" aria-sort={totalsSort.aria('out')}
                  ><SortButton sort={totalsSort} key="out">{unitWord} {WORDS[kind][1]}</SortButton
                  ></th
                ></tr
              ></thead
            >
            <tbody>
              {#each sortedTableRows as r (JSON.stringify([r.group, r.subjectId]))}
                <tr class:picked-row={isPicked(r)}>
                  <td class="mono"
                    ><button
                      type="button"
                      class="pick"
                      aria-pressed={isPicked(r)}
                      onclick={() => pick(isPicked(r) ? null : r)}>{nameOf(r)}</button
                    ></td
                  >
                  <td>{r.group}</td>
                  {#if kind === 'route'}<td class="mono">{r.source ?? ''}</td><td class="mono"
                      >{r.pipeline
                        ? names.of(MAIN_BRANCH, r.group, 'pipelines', r.pipeline)
                        : ''}</td
                    >{/if}
                  <td class="num">{amount(r.in)}</td><td class="num">{amount(r.out)}</td>
                </tr>
              {/each}
            </tbody>
            {#if showsSum}
              <tfoot
                ><tr
                  ><td>Total</td><td></td><td class="num">{amount(sumIn)}</td><td class="num"
                    >{amount(sumOut)}</td
                  ></tr
                ></tfoot
              >
            {/if}
          </table>
        </div>
      {:else}
        <p class="empty">No rows recorded in this window yet.</p>
      {/if}
    </section>

    <section class="panel ranked">
      <div class="panel-head">
        <div>
          <h2>
            Furthest from normal — {selectionLabel}
            <Help label="Furthest from normal"
              ><b>Normally</b> is the range of rows per second a source reads at this time of day,
              learned from its past weeks. A source is judged over its last 5 minutes, 30 minutes, 6
              hours and 24 hours: it raises Rows below normal when one of them stays below its
              range, and Rows above normal when its last 30 minutes stays above. The row shows the
              narrowest of those that is below its range, else the last 30 minutes, or the narrowest
              one that can judge the source. <b>Away by</b> is how far Now is past the nearest end of
              that range, as a percent of that end.</Help
            >
          </h2>
          {#if flow !== null}<p class="note">{count(normals.length, 'source')}.</p>{/if}
        </div>
      </div>
      {#if flow === null}
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
                ><th aria-sort={rankedSort.aria('group')}
                  ><SortButton sort={rankedSort} key="group"
                    ><WorkerGroupIcon />Worker group</SortButton
                  ></th
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
              {#each sortedRanked as r (JSON.stringify([r.group, r.subjectId]))}
                {@const away = Math.abs(r.awayPercent)}
                <tr>
                  <td class="mono">{r.subjectId}</td><td>{r.group}</td>
                  <td class="num">{Math.round(r.now)}</td><td class="num dim"
                    >{Math.round(r.low)}–{Math.round(r.high)}</td
                  >
                  <td class="num away" class:big={away >= 50} class:mid={away >= 20 && away < 50}
                    >{r.awayPercent > 0 ? '+' : ''}{Math.round(r.awayPercent)}%</td
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
              Exceptions — {selectionLabel}
              <Help label="Exceptions"
                >Exceptions raised by a pipeline's functions, counted per pipeline, function and
                kind.</Help
              >
            </h2>
            <p class="note">{count(totalErrored, 'exception')}.</p>
          </div>
          <div class="controls">
            <span class="dim">Over the last {rangeOfMinutes(errorsMinutes)?.label}</span>
            <RangePicker
              range={rangeOfMinutes(errorsMinutes)?.range ?? ''}
              onpick={(r) => (errorsMinutes = chartRange(r).minutes)}
            />
          </div>
        </div>
        {#if errors}
          <Sparkline
            series={[{ label: 'exceptions', color: 'var(--error)', points: erroredSeries }]}
            times={erroredTimes}
          />
        {:else}<p class="empty">Loading…</p>{/if}
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
                    ><SortButton sort={erroredSort} key="rows">Exceptions</SortButton></th
                  ></tr
                ></thead
              >
              <tbody
                >{#each sortedErroredRows as r (`${r.pipeline} ${r.function} ${r.kind}`)}<tr
                    ><td>{r.pipeline}</td><td><code>{formatFunction(r.function)}</code></td><td
                      >{r.kind}</td
                    ><td class="num">{r.total.toLocaleString()}</td></tr
                  >{/each}</tbody
              >
            </table>
          </div>
        {:else}<p class="empty">No exceptions recorded.</p>{/if}
      </section>
    </div>
  {/if}
</SectionShell>

<style>
  .rollout {
    margin-bottom: 0.75rem;
  }
  .rollout-body {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    font-size: var(--text-label);
  }
  .rollout-body meter {
    flex: 1;
  }
  .behind {
    color: var(--warn);
  }
  .issue-ribbon {
    padding: 0.65rem 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    color: var(--error);
    background: color-mix(in oklab, var(--error) 8%, var(--panel));
    border: 1px solid color-mix(in oklab, var(--error) 18%, var(--border));
    font-size: var(--text-note);
    text-decoration: none;
  }
  .issue-ribbon:hover {
    background: color-mix(in oklab, var(--error) 14%, var(--panel));
  }
  .issue-head,
  .issue-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .issue-row {
    padding-left: 1.25rem;
  }
  /* The one thing on this row allowed to shrink -- a long message is cut
     with an ellipsis rather than pushing "View all issues" off screen. */
  .issue-teaser {
    min-width: 0;
  }
  .issue-more {
    flex: none;
    margin-left: auto;
    font-weight: 600;
  }
  /* What the ribbon leaves out must not read as nothing left: bold, at the body's size, on its own rule. */
  .issue-overflow {
    padding: 0.4rem 0 0 1.25rem;
    border-top: 1px solid color-mix(in oklab, var(--error) 18%, var(--border));
    font-size: var(--text-body);
    font-weight: 700;
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
  .worker-controls {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
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
  .controls {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .table-head {
    margin: 1rem 0 0.5rem;
  }
  tfoot td {
    font-weight: 600;
    border-top: 1px solid var(--border);
  }
  .pick {
    all: unset;
    cursor: pointer;
  }
  .pick:hover,
  .pick:focus-visible {
    text-decoration: underline;
  }
  tr.picked-row td {
    background: color-mix(in srgb, var(--accent) 14%, transparent);
  }
  tr.picked-row td:first-child {
    box-shadow: inset 3px 0 0 var(--accent);
  }
  .picked {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    padding: 0.05rem 0.2rem 0.05rem 0.5rem;
    border: 1px solid var(--accent);
    border-radius: 999px;
    font-size: 0.85em;
    font-weight: 500;
  }
  .picked button {
    all: unset;
    cursor: pointer;
    padding: 0 0.3rem;
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
