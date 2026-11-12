<!--
  The selected object's details (design doc "The map", The panel), docked beside the map so the
  map stays in view. An overview first -- its throughput and how it moved over the window, then
  what accumulates behind it, then what it connects to -- and a tab for each thing worth reading
  at length: a destination's per-worker buffers, a source's backlog, a pipeline's steps, and the
  routes it sits on.

  What it shows comes from two places: the leader's per-object stats over a window
  (`/api/areas/:area/stats`), read here, and the map's current reading for the node (`/api/map/measurements`),
  handed in by `MapPage.svelte` with the topology it already holds.
-->
<script module lang="ts">
  /** The tabs, in the order they show. A kind gets only the ones it has content for. */
  export type FocusTab = 'overview' | 'buffers' | 'backlog' | 'history' | 'steps' | 'routes';

  export const FOCUS_TABS: FocusTab[] = [
    'overview',
    'buffers',
    'backlog',
    'history',
    'steps',
    'routes',
  ];
</script>

<script lang="ts">
  import Button, { FIRST_PAGE, LAST_PAGE } from './Button.svelte';
  import { CHART_RANGES, chartRange, type SourceGap } from './map';
  import {
    conditionLabel,
    errorMessage,
    errorText,
    getSourcePast,
    getStats,
    markSourcePast,
    type DeliverEdge,
    type MapNode,
    type NodeReadingDto,
    type PastSpan,
    type PipelineStatsDto,
    type RouteEdge,
    type SourcePastResponse,
    type SpreadDto,
    type StatsResponse,
  } from './api';
  import { holds } from './access';
  import { Busy } from './busy.svelte';
  import Segmented from './Segmented.svelte';
  import { dateTime, lasted } from './time';
  import { untrack, type Snippet } from 'svelte';
  import PencilIcon from './PencilIcon.svelte';
  import MapPanel from './MapPanel.svelte';
  import Help from './Help.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import Sparkline from './Sparkline.svelte';
  import ChipPicker from './ChipPicker.svelte';
  import ClockIcon from './ClockIcon.svelte';
  import { byteUnit, bytes } from './bytes';
  import WidenIcon from './WidenIcon.svelte';
  import Figure from './Figure.svelte';
  import Figures from './Figures.svelte';
  import {
    QUEUE_ALARM,
    expectedSlots,
    micros,
    millis,
    percent,
    perSecond as perSecondText,
    queueShare,
    rows,
    shareOfTime,
    slotTimes,
    spreadSlots,
    totalMicros,
    workerPage,
  } from './focus';
  import { has } from './mapTerms';

  /** One object the selection connects to, on its path. */
  type Connected = { node: MapNode; hash: string };

  let {
    node,
    tab,
    reading,
    nowMillis,
    alsoIn,
    connected,
    routes,
    delivers,
    pipelineName,
    tabHash,
    range,
    rangeHash,
    editHash,
    onedit,
    onclose,
    wide = false,
    wideHash,
    controls,
    gap,
    onroute,
  }: {
    node: MapNode;
    tab: FocusTab;
    /** The map's current reading for this node; absent when nothing reported one. */
    reading?: NodeReadingDto;
    /** The leader's clock the reading was taken on. */
    nowMillis?: number;
    /** Other groups this shared object runs in, each with the hash that selects it there. */
    alsoIn: { group: string; hash: string }[];
    /** Everything on the selection's path but itself. */
    connected: Connected[];
    /** This group's routes and delivery rules that touch the node. */
    routes: RouteEdge[];
    delivers: DeliverEdge[];
    /** The name a route's pipeline is shown by; its id when the map has no such pipeline. */
    pipelineName: (id: string) => string;
    tabHash: (tab: FocusTab) => string;
    /** How far back the charts reach, by the name the link carries (`CHART_RANGES`). */
    range?: string;
    rangeHash: (range: string) => string;
    editHash: string;
    /** Runs before the pencil navigates -- a shared object's editor is `shared/`'s, so the caller
     * switches scope here first. */
    onedit?: () => void;
    onclose: () => void;
    /** Whether the panel fills the map's width, which gives every tab its wide layout. */
    wide?: boolean;
    /** The link that widens the panel, or narrows it when it is wide. */
    wideHash: string;
    /** The map's own controls for the selection. */
    controls?: Snippet;
    /** What arrives at a source that no route takes (`map.ts`, gaps). */
    gap?: SourceGap;
    /** Opens a new route on this source, taking what `condition` selects, or all of it. */
    onroute?: (condition?: string) => void;
  } = $props();

  /** What the answer is drawn over. */
  let chart = $derived(chartRange(range));
  let rangeOpen = $state(false);

  /** Workers on one page of the Buffers tab: a fleet's worth would bury the busiest. A wide panel
   * has room for more of them. */
  let workersPerPage = $derived(wide ? 24 : 8);

  let answer = $state<StatsResponse | null>(null);
  let loadError = $state('');
  let loading = $state(true);

  $effect(() => {
    const asked = { group: node.group, kind: node.kind, id: node.authorId, minutes: chart.minutes };
    loading = true;
    loadError = '';
    // Another range's answer drawn under this range's axis would misstate when it was read.
    untrack(() => {
      if (answer && answer.windowMillis !== asked.minutes * 60_000) answer = null;
    });
    // An answer for a subject this view has moved off is not this view's answer.
    const current = () =>
      asked.group === node.group &&
      asked.kind === node.kind &&
      asked.id === node.authorId &&
      asked.minutes === chart.minutes;
    getStats(asked.group, asked.kind, asked.id, asked.minutes)
      .then((r) => {
        if (current()) answer = r;
      })
      .catch((e) => {
        if (current()) loadError = e instanceof Error ? e.message : String(e);
      })
      .finally(() => {
        if (current()) loading = false;
      });
  });

  let source = $derived(answer?.ok ? (answer.source ?? null) : null);
  let pipeline = $derived(answer?.ok ? (answer.pipeline ?? null) : null);
  let destination = $derived(answer?.ok ? (answer.destination ?? null) : null);

  /** Only a Kafka source has a backlog to read. */
  let hasLag = $derived(node.kind === 'source' && node.system === 'kafka');

  let tabs = $derived<{ tab: FocusTab; label: string }[]>([
    { tab: 'overview', label: 'Overview' },
    ...(node.kind === 'destination' ? [{ tab: 'buffers' as const, label: 'Buffers' }] : []),
    ...(hasLag ? [{ tab: 'backlog' as const, label: 'Backlog' }] : []),
    ...(node.kind === 'source' ? [{ tab: 'history' as const, label: 'History' }] : []),
    ...(node.kind === 'pipeline' ? [{ tab: 'steps' as const, label: 'Steps' }] : []),
    { tab: 'routes', label: 'Routes' },
  ]);
  /** A tab this kind does not have reads as its overview, so a hand-edited link still lands. */
  let shown = $derived(tabs.some((t) => t.tab === tab) ? tab : 'overview');

  /** What a source did over the past year that its normal learns from, or not: the History tab. */
  let past = $state<SourcePastResponse | null>(null);
  let pastError = $state('');
  const marking = new Busy();
  /** What a label leaves out, behind its `?`: one text wherever the label appears. */
  const HELP = {
    throughput:
      "This object's rate in this worker group, from the newest closed bucket. Lines on the map show connections, not volume.",
    expected: 'What this source normally reads at this time of day, learned from its past weeks.',
    refused: 'Rows the destination examined and rejected for good; no retry changes that.',
    sendFailed:
      'Rows that could not even be buffered for sending. They are retried and may still arrive.',
    buffers:
      "Bytes each worker holds for this destination against its own budget, from its latest heartbeat. The busiest current worker sets the overview's figure, so one full worker is never averaged away. A figure can pass 100%; only the bar stops there.",
    lag: "Records waiting in Kafka for this source, outside smeltr's own buffers. Lag alone is not backpressure: intake is held only when a worker reports it.",
    steps:
      "In the order they run. The bar is each step's share of the time; the counts are the rows it dropped or errored.",
    routes:
      "A route's condition picks rows from its source; each delivery's condition picks which of them go to that destination. Conditions can overlap, so a source's matches can add up past what it read.",
  };
  const WARNINGS_HELP =
    'Rows below normal warnings of the past year. The windows a warning read are not learned unless marked Normal; marking the one that holds Normal teaches the level the source reads now.';
  const SPELLS_HELP =
    "The source's longest quiet spells of the past year. The longest one learned sets how long the source may go without a row before it warns.";
  /** What an editor in the source's group may say a span was. */
  const WAS: { value: 'normal' | 'outage'; label: string }[] = [
    { value: 'normal', label: 'Normal' },
    { value: 'outage', label: 'Outage' },
  ];

  function readPast() {
    const asked = { group: node.group, id: node.authorId };
    const current = () => asked.group === node.group && asked.id === node.authorId;
    return getSourcePast(asked.group, asked.id)
      .then((answer) => {
        if (!current()) return;
        past = answer.ok ? answer : null;
        pastError = answer.ok ? '' : errorText(answer.errors);
      })
      .catch((e) => {
        if (current()) pastError = errorMessage(e);
      });
  }

  /** In the wide History tab, the strip covers the past year and ends at the leader's now. */
  const YEAR_MILLIS = 365 * 86_400_000;
  let yearEnd = $derived(nowMillis ?? Date.now());
  function acrossYear(at: number): number {
    return Math.min(100, Math.max(0, (100 * (at - (yearEnd - YEAR_MILLIS))) / YEAR_MILLIS));
  }
  /** Where each month of the past year starts on the strip; the newest is left off when it would
   * crowd the strip's end. */
  let months = $derived.by(() => {
    const end = new Date(yearEnd);
    return Array.from(
      { length: 12 },
      (_, i) => new Date(end.getFullYear(), end.getMonth() - 11 + i, 1),
    )
      .map((d) => ({ at: d.getTime(), label: d.toLocaleString('en-US', { month: 'short' }) }))
      .filter((m) => acrossYear(m.at) > 0 && acrossYear(m.at) < 96);
  });

  $effect(() => {
    if (shown !== 'history') return;
    untrack(() => {
      past = null;
      pastError = '';
    });
    readPast();
  });

  function mark(span: PastSpan, was: 'normal' | 'outage') {
    // One mark at a time, each read back before the next, so the list never shows an older answer last.
    if (marking.on) return;
    void marking.run(async () => {
      await markSourcePast(node.group, node.authorId, span, was);
      await readPast();
    });
  }

  /** The series the overview charts: rows per second, bucket by bucket across the window. */
  let rateSpread = $derived(source?.rate ?? pipeline?.rate ?? destination?.rate);
  let ratePoints = $derived(
    rateSpread && answer
      ? spreadSlots(rateSpread, answer.bucketMillis, answer.windowMillis, answer.nowMillis)
      : [],
  );
  /** The source's lag across the window, as the Backlog tab charts it. */
  let lagPoints = $derived(
    source?.lag && answer
      ? spreadSlots(source.lag, answer.bucketMillis, answer.windowMillis, answer.nowMillis)
      : [],
  );
  let rateTimes = $derived(
    answer ? slotTimes(answer.bucketMillis, answer.windowMillis, answer.nowMillis) : [],
  );
  /** The warnings that held in the window, as the chart marks them. */
  let marks = $derived([
    ...(answer?.warnings ?? []).map((w) => ({
      from: w.startedMillis,
      to: w.endedMillis ?? null,
      label: conditionLabel(w.condition),
      failed: w.severity === 'failed',
    })),
    // Some warnings keep no history, so the chart can mark them only while they hold.
    ...(reading?.warnings ?? [])
      .filter(
        (w) =>
          w.sinceMillis !== null && !answer?.warnings?.some((s) => s.condition === w.condition),
      )
      .map((w) => ({
        from: w.sinceMillis!,
        to: null,
        label: conditionLabel(w.condition),
        failed: w.severity === 'failed',
      })),
  ]);

  /** The overview's figure. A pipeline has no reading of its own, so its figure is the last
   * finished bucket of its spread. */
  let overviewRate = $derived(
    node.kind === 'pipeline' ? (ratePoints.at(-1) ?? undefined) : reading?.rate,
  );

  /** How long since the node was last heard from, in whole seconds. */
  let ageSeconds = $derived(
    reading?.lastSeenMillis !== undefined && nowMillis !== undefined
      ? Math.max(0, Math.round((nowMillis - reading.lastSeenMillis) / 1000))
      : null,
  );
  let held = $derived(
    has(reading, 'sourceBlocked') || has(reading, 'sourceRefused') || has(reading, 'sourceWaiting'),
  );
  let lagClimbing = $derived(has(reading, 'lagClimbing'));
  let outsideNormal = $derived(has(reading, 'sourceRowsBelow') || has(reading, 'sourceRowsAbove'));
  let expected = $derived(node.kind === 'source' ? reading?.expected : undefined);
  let chartBand = $derived(
    source?.expected ? expectedSlots(source.expected, rateTimes) : undefined,
  );

  function slotsOf(spread: SpreadDto | undefined): (number | null)[] {
    return spread && answer
      ? spreadSlots(spread, answer.bucketMillis, answer.windowMillis, answer.nowMillis)
      : [];
  }
  /** Bytes per second in the unit the busiest bucket reads best in. */
  function bytesChart(spread: SpreadDto | undefined): { label: string; points: (number | null)[] } {
    const points = slotsOf(spread);
    const { unit, size } = byteUnit(Math.max(0, ...points.map((p) => p ?? 0)));
    return { label: `${unit}/s`, points: points.map((p) => (p === null ? null : p / size)) };
  }
  type Chart = {
    title: string;
    label: string;
    points: (number | null)[];
    band?: ReturnType<typeof expectedSlots>;
  };
  /** Every series the leader sends for this object, side by side in the wide overview; one that
   * read nothing over the window is left out. */
  let charts = $derived<Chart[]>(
    [
      { title: 'Rows per second', label: 'rows/s', points: ratePoints, band: chartBand },
      { title: 'Bytes per second', ...bytesChart(source?.bytesRate ?? destination?.bytesRate) },
      { title: 'Kafka lag', label: 'records', points: lagPoints },
      { title: 'Read time per row', label: 'µs', points: slotsOf(source?.pollPerRow) },
      { title: 'Time per row', label: 'µs', points: slotsOf(pipeline?.perRow) },
      { title: 'Round trip', label: 'ms', points: slotsOf(destination?.roundTrip) },
    ].filter((c) => c.points.some((p) => p !== null)),
  );
  let failures = $derived(reading?.warnings.filter((w) => w.severity === 'failed') ?? []);
  let lesser = $derived(reading?.warnings.filter((w) => w.severity !== 'failed') ?? []);
  let queue = $derived(reading?.queue);
  /** A reading that stopped arriving: heard from once, with no current rate. */
  let stale = $derived(reading !== undefined && reading.rate === undefined && ageSeconds !== null);

  let scope = $derived(
    node.kind === 'source'
      ? 'Input'
      : node.kind === 'destination'
        ? 'Accepted output'
        : 'Rows through',
  );
  let connectedHeading = $derived(
    node.kind === 'source' ? 'Downstream' : node.kind === 'destination' ? 'Upstream' : 'Connected',
  );

  /** The first worker the Buffers tab shows, rather than a page number, so widening or narrowing
   * the panel keeps it on screen. */
  let first = $state(0);
  let workers = $derived(
    workerPage(destination?.queues ?? [], Math.floor(first / workersPerPage), workersPerPage),
  );

  /** Invariant across the step list, so it is summed once rather than once per row. */
  let spent = $derived(pipeline ? totalMicros(pipeline.steps) : 0);

  /** What each of a source's routes matched over the window. */
  let routeShares = $derived(
    source ? new Map(source.routes.map((r) => [r.route, r.matched])) : new Map<string, number>(),
  );

  function rate(perSecond: number | undefined): string {
    return perSecond === undefined ? 'no reading' : `${perSecondText(perSecond)} rows/s`;
  }

  function lagTrend(): string {
    if (lagClimbing) return 'growing';
    return reading?.lag === undefined ? '—' : 'not growing';
  }

  function intake(): string {
    if (held) return 'held';
    return reading?.rate === undefined ? 'unknown' : 'flowing';
  }
</script>

<MapPanel kind={node.kind} group={node.group} title={node.name} mono {onclose}>
  {#snippet actions()}
    <a class="btn icon" href={wideHash} title={wide ? 'Narrow' : 'Widen'}
      ><WidenIcon open={wide} /></a
    >
    <a class="btn icon" href={editHash} onclick={onedit} title="Edit {node.name}"><PencilIcon /></a>
  {/snippet}
  <div class="sub dim">
    {#if node.system}<span>{node.system}</span>{/if}
    {#if pipeline}<span
        >{pipeline.steps.length} {pipeline.steps.length === 1 ? 'step' : 'steps'}</span
      >{/if}
    {#if node.shared}<span class="tag">shared</span>{/if}
  </div>
  {#if alsoIn.length > 0}
    <div class="also dim">
      Also runs in
      {#each alsoIn as other, i (other.group)}{i > 0 ? ', ' : ' '}<a class="link" href={other.hash}
          >{other.group}</a
        >{/each}
    </div>
  {/if}

  <!-- Every warning the attention list can open this panel on, failures first. -->
  {#each failures as w (w.condition)}
    <p class="banner alarm">
      {conditionLabel(w.condition)}
      <Help label={conditionLabel(w.condition)}>{w.message}</Help>
    </p>
  {/each}
  {#if queue && queue.nearCapacity > 0}
    <p class="banner warn">
      Near capacity &middot; {queue.nearCapacity}
      {queue.nearCapacity === 1 ? 'worker' : 'workers'}
    </p>
  {/if}
  {#each lesser as w (w.condition)}
    <p class="banner warn">
      {conditionLabel(w.condition)}
      <Help label={conditionLabel(w.condition)}>{w.message}</Help>
    </p>
  {/each}
  {#if stale && failures.length + lesser.length === 0 && !queue?.nearCapacity}
    <p class="banner dim">Readings {ageSeconds} s old</p>
  {/if}

  {#if gap}
    <section class="gap">
      <h3>Unrouted streams</h3>
      <ul>
        {#each gap.streams as s (s.key)}
          <li class="gap-row">
            <span class="mono">{s.key}</span>
            {#if onroute && s.condition}<button
                type="button"
                data-guide="add-route"
                onclick={() => onroute(s.condition)}>+ Add route</button
              >{/if}
          </li>
        {/each}
        {#if gap.unnamed}
          <li class="gap-row">
            <span>rows under no stream</span>
            {#if onroute}<button type="button" data-guide="add-route" onclick={() => onroute()}
                >+ Add route</button
              >{/if}
          </li>
        {/if}
      </ul>
    </section>
  {/if}

  <!-- Above the readings, so they cannot move it as they arrive. -->
  {#if controls}
    <div class="controls">{@render controls()}</div>
  {/if}

  <div class="tabs">
    <SecondaryTabs
      tabs={tabs.map((t) => ({ label: t.label, hash: tabHash(t.tab) }))}
      active={tabHash(shown)}
      ariaLabel="{node.name} details"
    />
  </div>

  {#if loading && !answer}
    <p class="dim">Reading&hellip;</p>
  {:else if loadError}
    <p class="problem">{loadError}</p>
  {:else if answer && !answer.ok}
    <ul class="problems">
      {#each answer.errors as e}
        <li>{e.message}</li>
      {/each}
    </ul>
  {:else if wide && shown === 'overview'}
    <Figures>
      <Figure label={scope} help={HELP.throughput} tone={outsideNormal ? 'warn' : undefined}
        >{rate(overviewRate)}</Figure
      >
      {#if expected}
        <Figure label="Expected" help={HELP.expected}
          >{perSecondText(expected.low)}–{perSecondText(expected.high)} rows/s</Figure
        >
      {/if}
      {#if ageSeconds !== null}<Figure label="Last seen">{ageSeconds} s ago</Figure>{/if}
      {#if source && hasLag}{@render lagFigures()}{/if}
      {#if pipeline}{@render rowFigures(pipeline)}{/if}
      {#if destination}
        {#if queue?.busiest !== undefined}
          <Figure
            label="Busiest buffer"
            note="{queue.workers} {queue.workers === 1 ? 'worker' : 'workers'}"
            tone={queue.busiest >= QUEUE_ALARM ? 'alarm' : undefined}
            >{percent(queue.busiest)}</Figure
          >
        {/if}
        <Figure label="Delivered" note="of {rows(destination.received)} received"
          >{rows(destination.delivered)}</Figure
        >
        <Figure label="Refused" help={HELP.refused}>{rows(destination.rejected)}</Figure>
        <Figure label="Sends that failed" help={HELP.sendFailed}
          >{rows(destination.sendFailed)}</Figure
        >
        <Figure label="Stalls">{rows(destination.stalls)}</Figure>
        {#if destination.roundTripMs !== null}
          <Figure label="Round trip">{millis(destination.roundTripMs)}</Figure>
        {/if}
      {/if}
    </Figures>
    <section>
      <h3 class="with-range"><span>Over the last {chart.label}</span>{@render rangePicker()}</h3>
      <div class="charts">
        {#each charts as c (c.title)}
          <div>
            <h4>{c.title}</h4>
            {@render overWindow(c.label, c.points, c.band)}
          </div>
        {/each}
      </div>
    </section>
    {#if destination && destination.rejectedBySource.length > 0}
      <section>
        <h3>Refused, by source</h3>
        <dl class="facts">
          {#each destination.rejectedBySource as r (r.source)}
            <dt class="mono">{r.source}</dt>
            <dd class="mono">{rows(r.rejected)}</dd>
          {/each}
        </dl>
      </section>
    {/if}
    {@render connectedChips()}
  {:else if shown === 'overview'}
    <section>
      <h3 class="with-range">
        <span>
          Throughput
          <Help label="Throughput">{HELP.throughput}</Help>
        </span>
        {@render rangePicker()}
      </h3>
      <dl class="facts">
        <dt>{scope}</dt>
        <dd class="mono" class:warn={outsideNormal}>{rate(overviewRate)}</dd>
        {#if expected}
          <dt>
            Expected <Help label="Expected">{HELP.expected}</Help>
          </dt>
          <dd class="mono">{perSecondText(expected.low)}–{perSecondText(expected.high)} rows/s</dd>
        {/if}
        {#if ageSeconds !== null}
          <dt>Last seen</dt>
          <dd class:dim={!stale}>{ageSeconds} s ago</dd>
        {/if}
      </dl>
      {@render overWindow('rows/s', ratePoints, chartBand)}
    </section>

    {#if destination}
      <section>
        <h3>Buffers</h3>
        {#if queue?.busiest !== undefined}
          <div class="summary" class:alarm={queue.busiest >= QUEUE_ALARM}>
            <strong class="mono">{percent(queue.busiest)}</strong>
            <span class="dim">busiest worker</span>
          </div>
          <p class="dim small">
            {queue.fresh} fresh{queue.stale > 0 ? ` · ${queue.stale} stale` : ''} &middot; {queue.workers}
            {queue.workers === 1 ? 'worker' : 'workers'} in the group
          </p>
        {:else}
          <p class="dim small">No current worker has reported this buffer.</p>
        {/if}
        <a class="link small" href={tabHash('buffers')}>Every worker &rarr;</a>
      </section>
      <section>
        <h3>Over the window</h3>
        <dl class="facts">
          <dt>Received</dt>
          <dd class="mono">{rows(destination.received)}</dd>
          <dt>Delivered</dt>
          <dd class="mono">{rows(destination.delivered)}</dd>
          <dt>
            Refused <Help label="Refused">{HELP.refused}</Help>
          </dt>
          <dd class="mono">{rows(destination.rejected)}</dd>
          {#each destination.rejectedBySource as r (r.source)}
            <dt class="indent dim">from {r.source}</dt>
            <dd class="mono dim">{rows(r.rejected)}</dd>
          {/each}
          <dt>
            Sends that failed <Help label="Sends that failed">{HELP.sendFailed}</Help>
          </dt>
          <dd class="mono">{rows(destination.sendFailed)}</dd>
          <dt>Stalls</dt>
          <dd class="mono">{rows(destination.stalls)}</dd>
          {#if destination.roundTripMs !== null}
            <dt>Round trip</dt>
            <dd class="mono">{millis(destination.roundTripMs)}</dd>
          {/if}
        </dl>
      </section>
    {:else if source && hasLag}
      <section>
        <h3>Backlog</h3>
        <dl class="facts">
          <dt>Kafka lag</dt>
          <dd class="mono">{reading?.lag === undefined ? '—' : `${rows(reading.lag)} records`}</dd>
          <dt>Trend</dt>
          <dd class:warn={lagClimbing}>{lagTrend()}</dd>
          <dt>Intake</dt>
          <dd class:alarm={held}>{intake()}</dd>
        </dl>
        <a class="link small" href={tabHash('backlog')}>Backlog &rarr;</a>
      </section>
    {:else if pipeline}
      <section>
        <h3>Over the window</h3>
        <dl class="facts">
          <dt>Rows in</dt>
          <dd class="mono">{rows(pipeline.rowsIn)}</dd>
          <dt>Rows out</dt>
          <dd class="mono">{rows(pipeline.rowsOut)}</dd>
          {#if pipeline.microsPerRow !== undefined}
            <dt>Per row</dt>
            <dd class="mono">{micros(pipeline.microsPerRow)}</dd>
          {/if}
        </dl>
      </section>
    {/if}

    {@render connectedChips()}
  {:else if wide && shown === 'buffers' && destination}
    <section>
      <h3>
        Worker buffers
        <Help label="Worker buffers">{HELP.buffers}</Help>
      </h3>
      {#if workers.total === 0}
        <p class="dim small">
          No worker has reported this buffer. A missing reading is not an empty one.
        </p>
      {:else}
        <table class="readings">
          <thead>
            <tr><th>Worker</th><th colspan="2">Fill</th><th class="num">Held</th><th></th></tr>
          </thead>
          <tbody>
            {#each workers.rows as q (q.worker)}
              {@const share = queueShare(q)}
              {@const near = !q.stale && share >= QUEUE_ALARM}
              <tr>
                <td class="mono">{q.worker}</td>
                <td class="bar-col"
                  ><span class="bar" class:stale={q.stale} aria-hidden="true"
                    ><span
                      class="fill"
                      class:alarm={near}
                      style="width: {Math.min(100, share * 100)}%"
                    ></span></span
                  ></td
                >
                <td class="num mono" class:alarm={near}>{percent(share)}</td>
                <td class="num mono dim">{bytes(q.bytes)} / {bytes(q.budget)}</td>
                <td class="dim">{q.stale ? 'stale' : near ? 'near capacity' : ''}</td>
              </tr>
            {/each}
          </tbody>
        </table>
        {@render workerPager()}
      {/if}
    </section>
  {:else if shown === 'buffers' && destination}
    <section>
      <h3>
        Worker buffers
        <Help label="Worker buffers">{HELP.buffers}</Help>
      </h3>
      {#if workers.total === 0}
        <p class="dim small">
          No worker has reported this buffer. A missing reading is not an empty one.
        </p>
      {:else}
        <ul class="list">
          {#each workers.rows as q (q.worker)}
            {@const share = queueShare(q)}
            {@const near = !q.stale && share >= QUEUE_ALARM}
            <li>
              <div class="row">
                <span class="mono">{q.worker}</span>
                <strong class="mono" class:alarm={near}>{percent(share)}</strong>
              </div>
              <div class="row dim small">
                <span>{bytes(q.bytes)} / {bytes(q.budget)}</span>
                <span>{q.stale ? 'stale' : near ? 'near capacity' : ''}</span>
              </div>
              <span class="bar" class:stale={q.stale} aria-hidden="true">
                <span class="fill" class:alarm={near} style="width: {Math.min(100, share * 100)}%"
                ></span>
              </span>
            </li>
          {/each}
        </ul>
        {@render workerPager()}
      {/if}
    </section>
  {:else if wide && shown === 'backlog' && source}
    <Figures>
      {@render lagFigures()}
      <Figure label="Read over the window">{rows(source.polled)}</Figure>
    </Figures>
    <section>
      <h3 class="with-range"><span>Over the last {chart.label}</span>{@render rangePicker()}</h3>
      <div class="charts stacked">
        <div>
          <h4>Kafka lag</h4>
          {@render overWindow('records', lagPoints)}
        </div>
        <div>
          <h4>Rows per second</h4>
          {@render overWindow('rows/s', ratePoints, chartBand)}
        </div>
      </div>
    </section>
  {:else if shown === 'backlog' && source}
    <section>
      <h3 class="with-range">
        <span>
          Kafka lag
          <Help label="Kafka lag">{HELP.lag}</Help>
        </span>
        {@render rangePicker()}
      </h3>
      <div class="summary">
        <strong class="mono">{reading?.lag === undefined ? '—' : rows(reading.lag)}</strong>
        <span class="dim">records</span>
      </div>
      <dl class="facts">
        <dt>Intake</dt>
        <dd class:alarm={held}>{intake()}</dd>
        <dt>Read over the window</dt>
        <dd class="mono">{rows(source.polled)}</dd>
      </dl>
      {@render overWindow('records', lagPoints)}
    </section>
  {:else if wide && shown === 'steps' && pipeline}
    <Figures>{@render rowFigures(pipeline)}</Figures>
    <section>
      <h3>
        Steps
        <Help label="Steps">{HELP.steps}</Help>
      </h3>
      {#if pipeline.steps.length === 0}
        <p class="dim small">No steps: every row passes through unchanged.</p>
      {:else}
        <table class="readings">
          <thead>
            <tr
              ><th class="num">#</th><th>Step</th><th class="num">Time</th><th colspan="2"
                >Share of the time</th
              ><th class="num">Dropped</th><th class="num">Exceptions</th></tr
            >
          </thead>
          <tbody>
            {#each pipeline.steps as step, i (step.step)}
              {@const share = shareOfTime(step.micros, spent)}
              <tr>
                <td class="num dim">{i + 1}</td>
                <td>{step.step}</td>
                <td class="num mono">{micros(step.micros)}</td>
                <td class="bar-col"
                  ><span class="bar" aria-hidden="true"
                    ><span class="fill" style="width: {(share ?? 0) * 100}%"></span></span
                  ></td
                >
                <td class="num mono">{share === null ? '' : percent(share)}</td>
                <td class="num mono" class:dim={step.dropped === 0}>{rows(step.dropped)}</td>
                <td class="num mono" class:dim={step.errored === 0} class:alarm={step.errored > 0}
                  >{rows(step.errored)}</td
                >
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </section>
    {#if pipeline.perRow}
      <section>
        <h3 class="with-range">
          <span>Time per row, over the last {chart.label}</span>{@render rangePicker()}
        </h3>
        {@render overWindow('µs', slotsOf(pipeline.perRow))}
      </section>
    {/if}
  {:else if shown === 'steps' && pipeline}
    <section>
      <h3>
        Steps
        <Help label="Steps">{HELP.steps}</Help>
      </h3>
      {#if pipeline.steps.length === 0}
        <p class="dim small">No steps: every row passes through unchanged.</p>
      {:else}
        <ul class="list">
          {#each pipeline.steps as step (step.step)}
            {@const share = shareOfTime(step.micros, spent)}
            <li>
              <div class="row">
                <span>{step.step}</span>
                <span class="mono dim small">
                  {micros(step.micros)}{share === null ? '' : ` · ${percent(share)}`}
                  {#if step.dropped > 0}&middot; {rows(step.dropped)} dropped{/if}
                  {#if step.errored > 0}&middot; {rows(step.errored)} errored{/if}
                </span>
              </div>
              <span class="bar" aria-hidden="true">
                <span class="fill" style="width: {(share ?? 0) * 100}%"></span>
              </span>
            </li>
          {/each}
        </ul>
      {/if}
    </section>
  {:else if wide && shown === 'history'}
    {#if pastError}
      <p class="problem">{pastError}</p>
    {:else if !past}
      <p class="dim">Reading&hellip;</p>
    {:else}
      <section>
        <h3>Past year</h3>
        <svg class="year" viewBox="0 0 100 14" preserveAspectRatio="none" aria-hidden="true">
          {#each past.silences as span (`s${span.fromMillis}`)}
            {@render onYear(span, 'spell', 8, 5)}
          {/each}
          {#each past.warnings as span (`w${span.fromMillis}`)}
            {@render onYear(span, 'warned', 1, 6)}
          {/each}
        </svg>
        <div class="months mono dim" aria-hidden="true">
          {#each months as m (m.at)}<span style="left: {acrossYear(m.at)}%">{m.label}</span>{/each}
        </div>
        <div class="legend dim small" aria-hidden="true">
          <span><i class="key warned"></i> Warnings</span>
          <span><i class="key spell"></i> Longest quiet spells</span>
        </div>
      </section>
      <div class="columns">
        {@render pastSpans('Warnings', WARNINGS_HELP, past.warnings)}
        {@render pastSpans('Longest quiet spells', SPELLS_HELP, past.silences)}
      </div>
    {/if}
  {:else if shown === 'history'}
    {#if pastError}
      <p class="problem">{pastError}</p>
    {:else if !past}
      <p class="dim">Reading&hellip;</p>
    {:else}
      {@render pastSpans('Warnings', WARNINGS_HELP, past.warnings)}
      {@render pastSpans('Longest quiet spells', SPELLS_HELP, past.silences)}
    {/if}
  {:else if wide && shown === 'routes'}
    <section>
      <h3>
        Routes
        <Help label="Routes">{HELP.routes}</Help>
      </h3>
      {#if routes.length === 0}
        <p class="dim small">
          {node.kind === 'destination' ? 'Reached only by a fallback.' : 'On no route.'}
        </p>
      {:else}
        <table class="readings">
          <thead>
            <tr
              ><th>Route</th><th>Source</th><th>Condition</th><th>Pipeline</th><th>Delivers to</th
              >{#if source}<th class="num">Matched</th><th class="num">Share</th>{/if}</tr
            >
          </thead>
          <tbody>
            {#each routes as r (r.route)}
              {@const matched = routeShares.get(r.route)}
              <tr>
                <td class="mono">{r.route}</td>
                <td class="mono">{r.source}</td>
                <td class="mono condition-cell">{r.condition ? `if ${r.condition}` : ''}</td>
                <td class="mono">{pipelineName(r.pipeline)}</td>
                <td class="mono">
                  {#each delivers.filter((d) => d.route === r.route) as d, i (i)}
                    <div>
                      {d.destination}{#if d.when}<span class="dim"> when {d.when}</span>{/if}
                    </div>
                  {/each}
                </td>
                {#if source}
                  <td class="num mono">{matched === undefined ? '' : rows(matched)}</td>
                  <td class="num mono"
                    >{matched === undefined
                      ? ''
                      : percent(source.polled > 0 ? matched / source.polled : null)}</td
                  >
                {/if}
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
      {#if source}
        <p class="dim small">{rows(source.unmatched)} of {rows(source.polled)} met Unmatched.</p>
      {/if}
    </section>
  {:else if shown === 'routes'}
    <section>
      <h3>
        Routes
        <Help label="Routes">{HELP.routes}</Help>
      </h3>
      {#if routes.length === 0}
        <p class="dim small">
          {node.kind === 'destination' ? 'Reached only by a fallback.' : 'On no route.'}
        </p>
      {:else}
        <ul class="list">
          {#each routes as r (r.route)}
            {@const matched = routeShares.get(r.route)}
            <li>
              <div class="row">
                <span class="mono">{r.route}</span>
                {#if matched !== undefined && source}
                  <span class="mono dim small"
                    >{rows(matched)} &middot; {percent(
                      source.polled > 0 ? matched / source.polled : null,
                    )}</span
                  >
                {/if}
              </div>
              <div class="small mono dim">{r.source} &rarr; {pipelineName(r.pipeline)}</div>
              {#if r.condition}<div class="condition mono small">if {r.condition}</div>{/if}
              {#each delivers.filter((d) => d.route === r.route) as d, i (i)}
                <div class="small mono dim">&rarr; {d.destination}</div>
                {#if d.when}<div class="condition mono small">when {d.when}</div>{/if}
              {/each}
            </li>
          {/each}
        </ul>
      {/if}
      {#if source}
        <p class="dim small">{rows(source.unmatched)} of {rows(source.polled)} met Unmatched.</p>
      {/if}
    </section>
  {/if}
</MapPanel>

{#snippet connectedChips()}
  {#if connected.length > 0}
    <section>
      <h3>{connectedHeading}</h3>
      <!-- By kind, since a source and the pipeline it feeds often share a name. -->
      {#each ['source', 'pipeline', 'destination'] as kind (kind)}
        {@const of = connected.filter((c) => c.node.kind === kind)}
        {#if of.length > 0}
          <div class="chips">
            <span class="chip-kind uppercase-label">{kind}s</span>
            {#each of as c (c.hash)}
              <a class="chip mono" href={c.hash}>{c.node.name}</a>
            {/each}
          </div>
        {/if}
      {/each}
    </section>
  {/if}
{/snippet}

{#snippet lagFigures()}
  <Figure
    label="Kafka lag"
    help={HELP.lag}
    note={lagTrend()}
    tone={lagClimbing ? 'warn' : undefined}
    >{reading?.lag === undefined ? '—' : rows(reading.lag)}</Figure
  >
  <Figure label="Intake" tone={held ? 'alarm' : undefined}>{intake()}</Figure>
{/snippet}

{#snippet rowFigures(pipeline: PipelineStatsDto)}
  <Figure label="Rows in">{rows(pipeline.rowsIn)}</Figure>
  <Figure label="Rows out">{rows(pipeline.rowsOut)}</Figure>
  {#if pipeline.microsPerRow !== undefined}
    <Figure label="Per row">{micros(pipeline.microsPerRow)}</Figure>
  {/if}
{/snippet}

{#snippet workerPager()}
  {#if workers.pages > 1}
    <div class="pager">
      <Button
        type="button"
        class="icon"
        disabled={workers.page === 0 ? FIRST_PAGE : ''}
        onclick={() => (first = (workers.page - 1) * workersPerPage)}
        aria-label="Previous workers">&lsaquo;</Button
      >
      <span class="small">{workers.start}&ndash;{workers.end} of {workers.total}</span>
      <Button
        type="button"
        class="icon"
        disabled={workers.page === workers.pages - 1 ? LAST_PAGE : ''}
        onclick={() => (first = (workers.page + 1) * workersPerPage)}
        aria-label="Next workers">&rsaquo;</Button
      >
    </div>
  {/if}
{/snippet}

{#snippet onYear(span: PastSpan, kind: string, y: number, height: number)}
  {@const x = acrossYear(span.fromMillis)}
  <rect
    class={kind}
    x={Math.min(x, 99.6)}
    {y}
    width={Math.max(0.4, acrossYear(span.toMillis) - x)}
    {height}
  />
{/snippet}

{#snippet pastSpans(heading: string, help: string, spans: PastSpan[])}
  <section>
    <h3>{heading} <Help label={heading}>{help}</Help></h3>
    {#if spans.length === 0}
      <p class="dim small">None in the past year.</p>
    {:else}
      <ul class="list">
        {#each spans as span (`${span.fromMillis}-${span.toMillis}`)}
          {@const when = dateTime(span.fromMillis / 1000)}
          <li class="row past">
            <span class="mono small"
              ><span class="nowrap">{when} &middot;</span>
              <span class="nowrap">{lasted(span.toMillis - span.fromMillis)}</span></span
            >
            {#if holds(node.group, 'editor')}
              <Segmented
                label="{heading}: {when}"
                options={WAS}
                bind:value={() => (span.learned ? 'normal' : 'outage'), (was) => mark(span, was)}
              />
            {:else}
              <span class="dim small">{span.learned ? 'Normal' : 'Outage'}</span>
            {/if}
          </li>
        {/each}
      </ul>
    {/if}
  </section>
{/snippet}

{#snippet rangePicker()}
  <ChipPicker kind="row" compact ariaLabel="Chart range" title="Chart range" bind:open={rangeOpen}>
    {#snippet face()}<ClockIcon />{/snippet}
    {#each CHART_RANGES as r (r.range)}
      <button
        type="button"
        class="pick"
        class:on={r.range === chart.range}
        onclick={() => {
          rangeOpen = false;
          location.hash = rangeHash(r.range);
        }}>{r.label}</button
      >
    {/each}
  </ChipPicker>
{/snippet}

{#snippet overWindow(
  label: string,
  points: (number | null)[],
  band?: ReturnType<typeof expectedSlots>,
)}
  {#if points.some((p) => p !== null)}
    <Sparkline series={[{ label, color: 'var(--ink)', points }]} {band} times={rateTimes} {marks} />
    <div class="axis mono dim" aria-hidden="true">
      <span>{chart.label} ago</span><span>now</span>
    </div>
  {/if}
{/snippet}

<style>
  .controls {
    margin-top: 1rem;
  }

  h3 {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    margin: 0 0 0.5rem;
    font-size: var(--text-body);
    font-weight: 600;
  }

  section {
    padding: 0.85rem 0;
    border-top: 1px solid var(--border);
  }

  section:first-of-type {
    border-top: 0;
  }

  .sub {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem;
    font-size: var(--text-label);
  }

  .gap ul {
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .gap-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
    padding: 0.2rem 0;
  }

  .also {
    margin-top: 0.3rem;
    font-size: var(--text-label);
  }

  .tag {
    display: inline-flex;
    align-items: center;
    padding: 0 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .banner {
    margin: 0.6rem 0 0;
    font-size: var(--text-label);
    font-weight: 600;
  }

  .tabs {
    margin: 0.7rem 0 0.2rem;
  }

  .small {
    font-size: var(--text-label);
  }

  .alarm {
    color: var(--error);
  }

  .warn {
    color: var(--warn);
  }

  /* The wide panel's charts, two to a row unless stacked to share one time axis. */
  .charts {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 1rem 1.5rem;
  }

  .charts.stacked {
    grid-template-columns: minmax(0, 1fr);
  }

  .charts h4 {
    margin: 0 0 0.4rem;
    font-size: var(--text-label);
    font-weight: 600;
  }

  .charts :global(.sparkline svg) {
    height: 140px;
  }

  /* Two lists side by side in the wide panel, under one rule. */
  .columns {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0 2rem;
    border-top: 1px solid var(--border);
  }

  .columns > section {
    border-top: 0;
  }

  .readings .bar-col {
    width: 40%;
  }

  /* The bar's percentage, kept beside its bar. */
  .readings .bar-col + td {
    width: 1%;
  }

  .readings .bar {
    margin-top: 0;
  }

  .condition-cell {
    font-size: var(--text-label);
  }

  /* The past year on one strip: warnings above, quiet spells below. */
  .year {
    display: block;
    width: 100%;
    height: 3.5rem;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }

  .warned {
    fill: var(--warn);
    background: var(--warn);
  }

  .spell {
    fill: var(--ink);
    background: var(--ink);
  }

  .months {
    position: relative;
    height: 1.2rem;
    font-size: var(--text-micro);
  }

  .months span {
    position: absolute;
    top: 0.2rem;
  }

  .legend {
    display: flex;
    gap: 1.2rem;
  }

  .key {
    display: inline-block;
    width: 0.7rem;
    height: 0.7rem;
    vertical-align: -0.05rem;
  }

  .with-range {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
  }

  .facts {
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 0.4rem 1rem;
    margin: 0 0 0.6rem;
    font-size: var(--text-label);
  }

  .facts dt {
    display: flex;
    align-items: center;
    gap: 0.25rem;
  }

  .facts .indent {
    padding-left: 0.8rem;
  }

  .facts dd {
    margin: 0;
    text-align: right;
  }

  .axis {
    display: flex;
    justify-content: space-between;
    margin-top: 0.2rem;
    font-size: var(--text-micro);
  }

  .summary {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    margin-bottom: 0.3rem;
  }

  .summary strong {
    font-size: var(--text-section);
  }

  .summary.alarm strong {
    color: var(--error);
  }

  .chips {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.35rem;
    margin-bottom: 0.4rem;
  }

  .chip-kind {
    min-width: 6.5rem;
  }

  .chip {
    padding: 0.2rem 0.45rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    color: var(--ink);
    font-size: var(--text-label);
    text-decoration: none;
  }

  .chip:hover {
    border-color: var(--ink);
  }

  .list {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .list li {
    padding: 0.5rem 0;
    border-top: 1px solid var(--border);
  }

  .list li:first-child {
    border-top: 0;
  }

  .row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 0.6rem;
    font-size: var(--text-label);
  }

  /* Aligns the text with the control beside it, which would otherwise sit lower. */
  .row.past {
    align-items: center;
  }
  .nowrap {
    white-space: nowrap;
  }

  .bar {
    display: block;
    height: 4px;
    margin-top: 0.3rem;
    border-radius: 2px;
    background: var(--panel);
    overflow: hidden;
  }

  .bar.stale {
    opacity: 0.5;
  }

  .fill {
    display: block;
    height: 100%;
    background: var(--dim);
  }

  .fill.alarm {
    background: var(--error);
  }

  .condition {
    margin: 0.15rem 0 0.25rem;
    padding: 0.2rem 0.4rem;
    border-radius: var(--radius);
    background: var(--panel);
    overflow-wrap: anywhere;
  }

  .pager {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding-top: 0.6rem;
  }

  .problems {
    margin: 0.6rem 0 0;
    padding-left: 1rem;
  }

  .problem {
    color: var(--error);
  }
</style>
