<!--
  Any stored counter or gauge (`leader::query`): pick one, filter it, split it by one label, and see it per second or
  in totals over a window, as a chart of the busiest series and a table of every one. What is asked lives in the
  hash (`metricsView.ts`), so a view is a link, and the agent hands one over.
-->
<script lang="ts">
  import { onDestroy } from 'svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Segmented from './Segmented.svelte';
  import Toggle from './Toggle.svelte';
  import SortButton from './SortButton.svelte';
  import MetricsChart from './MetricsChart.svelte';
  import { TableSort } from './sort.svelte';
  import { byteUnit, bytes } from './bytes';
  import { FLOW_WINDOWS } from './flow';
  import { clockTime } from './time';
  import {
    conditionLabel,
    getLogs,
    getSeries,
    getSeriesMetrics,
    history,
    listWarnings,
    type LogRowDto,
    type SeriesAnswerDto,
    type StoredMetricDto,
    type WarningCondition,
  } from './api';
  import {
    WEEK_MINUTES,
    alongside,
    described,
    laidOn,
    fold,
    onGrid,
    seriesQuery,
    toHash,
    toneOf,
    unitOf,
    type Mark,
    type Drawn,
    type MetricsView,
  } from './metricsView';

  let {
    view,
    inline = false,
  }: {
    view: MetricsView;
    /** The chart alone, as the agent shows it in a reply: no page around it, nothing to change, no table. */
    inline?: boolean;
  } = $props();

  const GROUP = 'group';
  const WORKER = 'worker';
  const MINUTE = 60_000;
  const HOUR = 60 * MINUTE;
  const DAY = 24 * HOUR;
  /** How often an open page asks again: a ten-second bucket lands this often at the finest. */
  const REFRESH_MS = 10_000;

  let metrics = $state<StoredMetricDto[] | null>(null);
  let answer = $state<SeriesAnswerDto | null>(null);
  /** The same question a week before, while the view asks for it. */
  let before = $state<SeriesAnswerDto | null>(null);
  /** Deploys and issue beginnings since the window's start, at their times, while the view asks for them. */
  let happened = $state<{ at: number; until?: number; kind: Mark['kind']; text: string }[]>([]);
  let fetchedAt = $state(Date.now());
  let failed = $state<string | null>(null);

  /** The list grows as workers report metrics they had not before, so it is read with every answer. */
  function readMetrics() {
    getSeriesMetrics()
      .then((m) => (metrics = m))
      .catch((e: Error) => (failed = e.message));
  }

  let metric = $derived(metrics?.find((m) => m.name === view.metric) ?? null);

  /** Which metric each drawn series is of, by its name. */
  let metricOf = $state(new Map<string, string>());
  /** Several metrics' answers as one: each series named for its metric, and its split value after it. */
  function together(answers: SeriesAnswerDto[]): SeriesAnswerDto {
    if (answers.length === 1) {
      metricOf = new Map(answers[0].series.map((s) => [s.name, answers[0].metric]));
      return answers[0];
    }
    const of = new Map<string, string>();
    const series = answers.flatMap((a) =>
      a.series.map((s) => {
        const name = s.name ? `${a.metric} · ${s.name}` : a.metric;
        of.set(name, a.metric);
        return { ...s, name };
      }),
    );
    metricOf = of;
    return { ...answers[0], series };
  }

  async function read(asked: MetricsView) {
    readMetrics();
    try {
      const offset = -new Date().getTimezoneOffset();
      const each = (ago: number) =>
        Promise.all(
          [asked.metric ?? '', ...asked.more].map((m) =>
            // Totals of different metrics stacked would add up to nothing.
            getSeries(
              seriesQuery(
                { ...asked, metric: m, draw: asked.more.length ? 'perSecond' : asked.draw },
                offset,
                ago,
              ),
            ),
          ),
        ).then(together);
      const [got, then, marks] = await Promise.all([
        each(0),
        asked.weekBefore ? each(WEEK_MINUTES) : null,
        asked.marks ? readMarks(asked.windowMinutes).catch(() => []) : [],
      ]);
      // A slow answer to an older question must not land over the newer one's.
      if (asked === view) {
        answer = got;
        before = then;
        happened = marks;
        fetchedAt = Date.now();
        failed = null;
      }
    } catch (e) {
      if (asked === view) failed = (e as Error).message;
    }
  }
  $effect(() => {
    read(view);
  });

  /** The deploys on `main` over the last `minutes`, and the issues that held in them, each from when it began until
   * it cleared or until now. The open ones are on the Issues page, which every viewer reads; the cleared ones are in
   * the leader's log, which only an administrator does, so another viewer sees the open ones alone. */
  async function readMarks(minutes: number) {
    const now = Date.now();
    const since = now - minutes * MINUTE;
    const [main, open, cleared] = await Promise.all([
      history(200).catch(() => ({ commits: [] })),
      listWarnings().catch(() => []),
      getLogs(
        new URLSearchParams({
          type: 'cleared',
          typeExact: 'true',
          sinceMillis: String(since),
          limit: '1000',
        }).toString(),
      ).catch(() => ({ rows: [] as LogRowDto[] })),
    ]);
    const issues: { at: number; until?: number; name: string; group: string }[] = [
      ...open.flatMap((w) =>
        w.sinceMillis === null
          ? []
          : [
              {
                at: w.sinceMillis,
                name: issueName(w.condition, w.subjectId),
                group: w.group ?? '',
              },
            ],
      ),
      // A cleared line names when its issue began, so one that began before the window shows from its start.
      ...cleared.rows.map((r) => ({
        at: Number(field(r.fields, 'began_ms')) || r.atMillis,
        until: r.atMillis,
        name: issueName(field(r.fields, 'issue') as WarningCondition, field(r.fields, 'subject')),
        group: field(r.fields, 'group'),
      })),
    ];
    const time = (t: number) =>
      minutes > 24 * 60 ? `${day(t, { weekday: 'short' })} ${clockTime(t)}` : clockTime(t);
    return [
      ...issues.map((it) => ({
        at: it.at,
        until: it.until ?? now,
        kind: 'issue' as const,
        text: `▲ ${it.name}${it.group ? `, group ${it.group}` : ''}, ${
          it.until === undefined
            ? `since ${time(it.at)}`
            : `${time(it.at)}–${new Date(it.at).toDateString() === new Date(it.until).toDateString() ? clockTime(it.until) : time(it.until)}`
        }`,
      })),
      ...main.commits
        .filter((c) => c.deployer && c.time * 1000 >= since)
        .map((c) => ({
          at: c.time * 1000,
          kind: 'deploy' as const,
          text: `◆ Deployed: ${c.subject}`,
        })),
    ];
  }
  /** Field `name` of a log line's `fields`, or empty. */
  function field(fields: unknown[], name: string): string {
    const found = (fields as [string, { value?: unknown }][]).find(([k]) => k === name);
    return found?.[1]?.value ? String(found[1].value) : '';
  }
  /** An issue as the Issues page names its condition, and its subject where it has one. */
  function issueName(condition: WarningCondition, subject: string): string {
    return `${conditionLabel(condition)}${subject ? `: ${subject}` : ''}`;
  }
  // A chart in a reply is read once: a long conversation would otherwise keep asking for each of its charts.
  const timer = inline ? undefined : setInterval(() => read(view), REFRESH_MS);
  onDestroy(() => clearInterval(timer));

  function go(next: Partial<MetricsView>) {
    location.hash = toHash({ ...view, ...next });
  }

  // -- the question -------------------------------------------------------------------------------------------------

  let picking = $state(false);
  /** Which metric the open list picks: 0 the first, k the k-th of `more`, or a new one. */
  let pickingFor = $state<number | 'new'>(0);
  function pick(at: number | 'new') {
    picking = !(picking && pickingFor === at);
    pickingFor = at;
  }
  let search = $state('');
  /** The metrics, in the website's reference groups, narrowed by `search`. */
  let groups = $derived.by(() => {
    const by = new Map<string, { m: StoredMetricDto; what: string }[]>();
    for (const m of metrics ?? []) {
      const { group, what } = described(m.name, Object.keys(m.labels));
      if (search && !`${m.name} ${what}`.toLowerCase().includes(search.toLowerCase())) continue;
      by.set(group, [...(by.get(group) ?? []), { m, what }]);
    }
    return [...by];
  });
  function choose(m: StoredMetricDto) {
    picking = false;
    search = '';
    if (pickingFor !== 0) {
      const more = [...view.more];
      if (pickingFor === 'new') more.push(m.name);
      else more[pickingFor - 1] = m.name;
      go({ more, draw: 'perSecond' });
      return;
    }
    // A split or filter by a label the new metric lacks would empty the chart.
    const has = (label: string) => label in m.labels;
    go({
      metric: m.name,
      by: has(view.by) ? view.by : '',
      filter: view.filter.filter(([label]) => has(label)),
      draw: m.counter ? view.draw : 'perSecond',
    });
  }

  const labelName = (label: string) =>
    label === GROUP
      ? 'Worker group'
      : label === WORKER
        ? 'Worker'
        : label[0].toUpperCase() + label.slice(1);
  /** The labels a series can be split or filtered by: its own, then its group, then its worker where it has one. */
  let labels = $derived(
    metric
      ? [
          ...Object.keys(metric.labels).filter((l) => l !== GROUP && l !== WORKER),
          GROUP,
          ...(WORKER in metric.labels ? [WORKER] : []),
        ]
      : [],
  );

  let adding = $state(false);
  let addLabel = $state('');
  function addFilter(value: string) {
    adding = false;
    if (value) go({ filter: [...view.filter, [addLabel, value]] });
  }

  // -- the answer ---------------------------------------------------------------------------------------------------

  let totals = $derived(answer?.draw === 'totals');
  let count = $derived(
    answer
      ? totals
        ? Math.round((view.windowMinutes * MINUTE) / answer.pointMillis)
        : // The point still filling reads low, so the chart ends at the last closed one.
          Math.max(0, Math.floor((fetchedAt - answer.fromMillis) / answer.pointMillis))
      : 0,
  );
  let all = $derived(answer ? onGrid(answer, count) : []);
  /** A series in another unit than the first metric's is drawn against the right-hand axis. */
  const onRight = (name: string) => unitOf(metricOf.get(name) ?? view.metric ?? '') !== unit;
  let shown = $derived.by(() => {
    const left = all.filter((s) => !onRight(s.name));
    const right = all.filter((s) => onRight(s.name));
    if (!right.length) return fold(all, view.top);
    // Each axis's busiest are drawn on their own; the colors run on from the left's into the right's.
    const rank = (d: Drawn[], from: number) =>
      d.map((s, i) => ({ ...s, slot: s.slot === null ? null : from + i }));
    const l = rank(fold(left, view.top), 0);
    const r = rank(fold(right, view.top), l.length).map((s) => ({ ...s, right: true }));
    return [...l, ...r];
  });
  let rightUnit = $derived(
    shown.find((s) => s.right)
      ? unitOf(metricOf.get(shown.find((s) => s.right)!.name) ?? '')
      : null,
  );
  let rightScale = $derived(
    rightUnit === 'bytes'
      ? byteUnit(
          Math.max(0, ...shown.filter((s) => s.right).flatMap((s) => s.points.map((v) => v ?? 0))),
        )
      : { unit: rightUnit === 'seconds' ? 's' : (rightUnit ?? ''), size: 1 },
  );
  function amountIn(u: string | null, value: number): string {
    if (u === 'bytes') return bytes(value);
    if (u === 'seconds') return `${value.toFixed(value < 10 ? 2 : 0)} s`;
    return value >= 100 || Number.isInteger(value)
      ? Math.round(value).toLocaleString()
      : value.toFixed(2);
  }
  let previous = $derived(
    before && view.weekBefore
      ? alongside(shown, laidOn(before, answer!, count, WEEK_MINUTES * MINUTE))
      : null,
  );
  let marks = $derived(
    view.marks && answer
      ? happened
          .map((h) => ({
            i: (h.at - answer!.fromMillis) / answer!.pointMillis,
            end:
              h.until === undefined
                ? undefined
                : (h.until - answer!.fromMillis) / answer!.pointMillis,
            kind: h.kind,
            text: h.text,
          }))
          // An issue that began before the window shows from its first point.
          .map((m) => (m.end !== undefined ? { ...m, i: Math.max(0, m.i) } : m))
          .filter((m) => m.i >= 0 && m.i < count && (m.end === undefined || m.end >= 0))
      : null,
  );
  let unit = $derived(unitOf(view.metric ?? ''));
  let windowLabel = $derived(
    FLOW_WINDOWS.find((w) => w.minutes === view.windowMinutes)?.label ?? `${view.windowMinutes}m`,
  );
  let pointSeconds = $derived((answer?.pointMillis ?? 1000) / 1000);
  let largest = $derived(
    Math.max(
      0,
      ...(totals
        ? Array.from({ length: count }, (_, i) => shown.reduce((a, s) => a + (s.points[i] ?? 0), 0))
        : shown.filter((s) => !s.right).flatMap((s) => s.points.map((v) => v ?? 0))),
    ),
  );
  let scale = $derived(
    unit === 'bytes' ? byteUnit(largest) : { unit: unit === 'seconds' ? 's' : unit, size: 1 },
  );
  let barWord = $derived(
    answer?.pointMillis === MINUTE ? 'minute' : answer?.pointMillis === HOUR ? 'hour' : 'day',
  );
  let axisUnit = $derived(
    !answer?.counter
      ? scale.unit
      : totals
        ? `${scale.unit || 'count'} per ${barWord}`
        : `${scale.unit || 'count'}/s`,
  );

  /** `value` in the metric's own unit, plain. */
  const amount = (value: number) => amountIn(unit, value);
  const rate = (value: number) => `${amount(value)}/s`;
  /** A point's value as drawn: a rate per second for a counter drawn per second. */
  const drawn = (value: number) => (answer?.counter && !totals ? rate(value) : amount(value));

  const startOf = (i: number) => (answer?.fromMillis ?? 0) + i * (answer?.pointMillis ?? 0);
  const day = (t: number, o: Intl.DateTimeFormatOptions) =>
    new Date(t).toLocaleDateString('en-GB', o);
  let ticks = $derived(
    Array.from({ length: count }, (_, i) => {
      const t = startOf(i);
      const pointMs = answer?.pointMillis ?? 0;
      if (pointMs === DAY) {
        if (count <= 7) return day(t, { weekday: 'short', day: 'numeric' });
        // Every other day, and the month named where it starts and on the first shown.
        if (i % 2 !== 0) return '';
        const d = new Date(t);
        return i === 0 || d.getDate() <= 2
          ? day(t, { day: 'numeric', month: 'short' })
          : String(d.getDate());
      }
      const every = Math.max(1, Math.round(count / 8));
      if (i % every !== 0) return '';
      return view.windowMinutes > 24 * 60
        ? `${day(t, { weekday: 'short' })} ${clockTime(t)}`
        : clockTime(t);
    }),
  );
  function at(i: number): string {
    const t = startOf(i);
    if (totals && i === count - 1)
      return barWord === 'day' ? 'Today so far' : `This ${barWord} so far`;
    if (answer?.pointMillis === DAY)
      return day(t, { weekday: 'short', day: 'numeric', month: 'short' });
    if (totals && barWord === 'hour') return `${clockTime(t)}–${clockTime(t + HOUR)}`;
    return view.windowMinutes > 24 * 60
      ? `${day(t, { weekday: 'short', day: 'numeric', month: 'short' })} ${clockTime(t)}`
      : clockTime(t);
  }

  /** Each series' row: a counter's total and its rate, or a gauge's latest, average and highest. */
  let rows = $derived(
    all.map((s, rank) => {
      const values = s.points.filter((v): v is number => v !== null);
      const total = totals ? s.total : s.total * pointSeconds;
      return {
        name: s.name,
        tone: toneOf(
          view.more.length
            ? (shown.find((d) => d.name === s.name)?.slot ?? null)
            : rank < view.top || all.length <= view.top
              ? rank
              : null,
        ),
        u: unitOf(metricOf.get(s.name) ?? view.metric ?? ''),
        total,
        average: answer?.counter ? total / Math.max(1, count * pointSeconds) : average(values),
        highest: values.length
          ? Math.max(...values) / (answer?.counter && totals ? pointSeconds : 1)
          : 0,
        now: values.at(-1) ?? 0,
      };
    }),
  );
  function average(values: number[]): number {
    return values.length ? values.reduce((a, v) => a + v, 0) / values.length : 0;
  }
  const sort = new TableSort<'name' | 'total' | 'now' | 'average' | 'highest'>();
  let sorted = $derived(
    sort.apply(rows, {
      name: (r) => r.name,
      total: (r) => r.total,
      now: (r) => r.now,
      average: (r) => r.average,
      highest: (r) => r.highest,
    }),
  );
  let sum = $derived(rows.reduce((a, r) => a + r.total, 0));
</script>

{#snippet answered()}
  {#if failed}
    <p class="empty">{failed}</p>
  {:else if metrics !== null && metrics.length === 0}
    <p class="empty">No metrics recorded yet.</p>
  {:else if answer === null}
    <p class="empty">Loading…</p>
  {:else if all.length === 0}
    <p class="empty">Nothing recorded in this window yet.</p>
  {:else}
    <div class="over">
      {#if shown.length > 1}
        <ul class="legend">
          {#each shown as s (`${s.right ? 'r' : ''}${s.slot ?? -1}`)}
            <li>
              <span class="swatch {toneOf(s.slot)}"></span><span class="mono">{s.name}</span
              >{#if s.right}<span class="dim" title="Right-hand axis" aria-label="Right-hand axis"
                  >→</span
                >{/if}
            </li>
          {/each}
        </ul>
      {/if}
      {#if !inline}
        <div class="switches">
          <label
            ><Toggle checked={view.marks} onchange={() => go({ marks: !view.marks })} /> Deploys and issues</label
          >
          <label
            ><Toggle
              checked={view.weekBefore}
              onchange={() => go({ weekBefore: !view.weekBefore })}
            /> A week before</label
          >
        </div>
      {/if}
    </div>
    <MetricsChart
      series={shown.map((s) => ({
        ...s,
        points: s.points.map((v) =>
          v === null ? null : v / (s.right ? rightScale.size : scale.size),
        ),
      }))}
      right={rightUnit === null
        ? null
        : {
            unit: `${rightScale.unit || 'count'}/s`,
            format: (v: number) => `${amountIn(rightUnit, v * rightScale.size)}/s`,
          }}
      bars={totals}
      {ticks}
      unit={axisUnit}
      {at}
      format={(v) => drawn(v * scale.size)}
      previous={previous?.map((p, k) =>
        p.map((v) => (v === null ? null : v / (shown[k]?.right ? rightScale.size : scale.size))),
      ) ?? null}
      {marks}
      label="{view.metric}{view.by
        ? `, by ${labelName(view.by).toLowerCase()}`
        : ''}, over the chosen window"
    />
  {/if}
{/snippet}

{#if inline}
  <figure class="inline" aria-label="Metric">
    <figcaption>
      <span class="mono">{view.metric}</span>
      {#each view.filter as [label, value] (label + value)}
        <span class="chip"
          ><span class="dim">{labelName(label).toLowerCase()}</span>
          <span class="mono">{value}</span></span
        >
      {/each}
      <span class="dim"
        >{view.by ? `by ${labelName(view.by).toLowerCase()} · ` : ''}{windowLabel}{totals
          ? ' · totals'
          : ''}{view.weekBefore ? ' · dashed: a week before' : ''}</span
      >
    </figcaption>
    {@render answered()}
  </figure>
{:else}
  <SectionShell>
    <PageHeader title="Metrics">
      {#snippet help()}
        Every counter and gauge the workers report, kept for 30 days. Pick one, filter it, and split
        it by one label.
      {/snippet}
    </PageHeader>

    <section class="panel" aria-label="Metric">
      <div class="ask">
        <div class="picker">
          <button
            type="button"
            class="metric-btn"
            aria-haspopup="listbox"
            aria-expanded={picking && pickingFor === 0}
            onclick={() => pick(0)}
            ><span class="mono">{view.metric}</span><span class="caret" aria-hidden="true">▾</span
            ></button
          >
          {#each view.more as m, k (k)}
            <span class="more"
              ><button
                type="button"
                class="metric-btn"
                aria-haspopup="listbox"
                aria-expanded={picking && pickingFor === k + 1}
                onclick={() => pick(k + 1)}
                ><span class="mono">{m}</span><span class="caret" aria-hidden="true">▾</span
                ></button
              ><button
                type="button"
                class="drop"
                aria-label="Remove metric {m}"
                onclick={() => go({ more: view.more.filter((_, j) => j !== k) })}>×</button
              ></span
            >
          {/each}
          <button type="button" class="chip add" onclick={() => pick('new')}>+ Metric</button>
          {#if picking}
            <div class="pop">
              <!-- svelte-ignore a11y_autofocus -->
              <input class="search" placeholder="Search metrics" bind:value={search} autofocus />
              <div role="listbox" aria-label="Metric">
                {#each groups as [group, items] (group)}
                  <div class="pop-group">{group}</div>
                  {#each items as { m, what } (m.name)}
                    <button
                      type="button"
                      role="option"
                      aria-selected={m.name === view.metric}
                      class="pop-item"
                      onclick={() => choose(m)}
                      ><span class="mono">{m.name}</span><span class="what">{what}</span></button
                    >
                  {/each}
                {/each}
              </div>
            </div>
          {/if}
        </div>

        <div class="chips">
          {#each view.filter as [label, value], i (label + value)}
            <span class="chip"
              ><span class="dim">{labelName(label).toLowerCase()}</span>
              <span class="mono">{value}</span><button
                type="button"
                aria-label="Remove filter {labelName(label).toLowerCase()} {value}"
                onclick={() => go({ filter: view.filter.filter((_, j) => j !== i) })}>×</button
              ></span
            >
          {/each}
          {#if adding && metric}
            <span class="chip adding">
              <select aria-label="Filter by" bind:value={addLabel}>
                {#each labels as l (l)}<option value={l}>{labelName(l)}</option>{/each}
              </select>
              <select aria-label="Filter value" onchange={(e) => addFilter(e.currentTarget.value)}>
                <option value="">Value</option>
                {#each metric.labels[addLabel] ?? [] as v (v)}<option value={v}>{v}</option>{/each}
              </select>
            </span>
          {:else if metric}
            <button
              type="button"
              class="chip add"
              onclick={() => {
                adding = true;
                addLabel = labels[0] ?? GROUP;
              }}>+ Filter</button
            >
          {/if}
        </div>

        <label class="split"
          ><span class="dim">Split by</span>
          <select value={view.by} onchange={(e) => go({ by: e.currentTarget.value })}>
            <option value="">None</option>
            {#each labels as l (l)}<option value={l}>{labelName(l)}</option>{/each}
          </select></label
        >
        {#if view.by && all.length > 1}
          <label class="split"
            ><span class="dim">Top</span>
            <input
              class="top"
              type="number"
              min="1"
              step="1"
              value={view.top}
              onchange={(e) => {
                const top = e.currentTarget.valueAsNumber;
                if (Number.isInteger(top) && top > 0) go({ top });
              }}
            /></label
          >
        {/if}

        <div class="controls">
          {#if (metric?.counter ?? true) && !view.more.length}
            <Segmented
              label="Draw"
              options={[
                { value: 'perSecond', label: 'Per second' },
                { value: 'totals', label: 'Totals' },
              ]}
              bind:value={() => view.draw, (v) => go({ draw: v })}
            />
          {/if}
          <Segmented
            label="Chart window"
            options={FLOW_WINDOWS.map((w) => ({ value: String(w.minutes), label: w.label }))}
            bind:value={() => String(view.windowMinutes), (v) => go({ windowMinutes: Number(v) })}
          />
        </div>
      </div>

      {@render answered()}
      {#if answer !== null && all.length > 0}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th aria-sort={sort.aria('name')}
                  ><SortButton {sort} key="name">{view.by ? labelName(view.by) : ''}</SortButton
                  ></th
                >
                {#if answer.counter}
                  <th class="num" aria-sort={sort.aria('total')}
                    ><SortButton {sort} key="total">Total</SortButton></th
                  >
                {:else}
                  <th class="num" aria-sort={sort.aria('now')}
                    ><SortButton {sort} key="now">Now</SortButton></th
                  >
                {/if}
                <th class="num" aria-sort={sort.aria('average')}
                  ><SortButton {sort} key="average">Average</SortButton></th
                >
                <th class="num" aria-sort={sort.aria('highest')}
                  ><SortButton {sort} key="highest">Highest</SortButton></th
                >
              </tr>
            </thead>
            <tbody>
              {#each sorted as r (r.name)}
                <tr>
                  <td
                    ><span class="swatch {r.tone}"></span>
                    <span class="mono">{r.name || 'All'}</span></td
                  >
                  {#if answer.counter}
                    <td class="num">{amountIn(r.u, r.total)}</td>
                    <td class="num">{amountIn(r.u, r.average)}/s</td>
                    <td class="num">{amountIn(r.u, r.highest)}/s</td>
                  {:else}
                    <td class="num">{amount(r.now)}</td>
                    <td class="num">{amount(r.average)}</td>
                    <td class="num">{amount(r.highest)}</td>
                  {/if}
                </tr>
              {/each}
            </tbody>
            {#if answer.counter && rows.length > 1 && !view.more.length}
              <tfoot
                ><tr><td>Total</td><td class="num">{amount(sum)}</td><td></td><td></td></tr></tfoot
              >
            {/if}
          </table>
        </div>
      {/if}
    </section>
  </SectionShell>
{/if}

<style>
  .inline {
    margin: 0.4em 0;
    padding: 0.6rem 0.8rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  .inline figcaption {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.3rem 0.6rem;
    margin-bottom: 0.5rem;
    font-size: var(--text-note);
  }

  .panel {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 1rem;
  }

  .ask {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.6rem 0.8rem;
    margin-bottom: 0.8rem;
  }

  .controls {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-left: auto;
  }

  .picker {
    position: relative;
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    align-items: center;
  }

  .more {
    display: inline-flex;
    align-items: center;
  }

  .drop {
    border: 0;
    background: none;
    color: var(--dim);
    cursor: pointer;
    font-size: 1rem;
  }

  .metric-btn {
    display: inline-flex;
    gap: 0.5rem;
    align-items: center;
    padding: 0.35rem 0.7rem;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--bg);
    color: inherit;
    font-weight: 600;
    cursor: pointer;
  }

  .caret {
    color: var(--dim);
  }

  .pop {
    position: absolute;
    z-index: 5;
    top: calc(100% + 4px);
    left: 0;
    width: min(460px, 90vw);
    max-height: 360px;
    overflow: auto;
    padding: 0.4rem;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--bg);
    box-shadow: 0 6px 18px rgb(0 0 0 / 0.15);
  }

  .search {
    width: 100%;
    margin-bottom: 0.3rem;
  }

  .pop-group {
    padding: 0.4rem 0.4rem 0.2rem;
    color: var(--dim);
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .pop-item {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    width: 100%;
    padding: 0.3rem 0.5rem;
    border: 0;
    border-radius: 6px;
    background: none;
    color: inherit;
    text-align: left;
    cursor: pointer;
  }

  .pop-item[aria-selected='true'],
  .pop-item:hover {
    background: var(--panel);
  }

  .what {
    color: var(--dim);
    font-size: var(--text-micro);
  }

  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }

  .chip {
    display: inline-flex;
    gap: 0.35rem;
    align-items: center;
    padding: 0.2rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--bg);
    color: inherit;
    font-size: var(--text-body);
  }

  .chip button {
    border: 0;
    background: none;
    color: var(--dim);
    cursor: pointer;
  }

  .chip.add {
    color: var(--dim);
    cursor: pointer;
  }

  .split {
    display: inline-flex;
    gap: 0.4rem;
    align-items: center;
  }

  .top {
    width: 4.5rem;
  }

  .dim,
  .empty {
    color: var(--dim);
  }

  .over {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem 1.5rem;
    align-items: center;
    margin: 0 0 0.4rem;
  }

  .switches {
    display: flex;
    gap: 1.2rem;
    margin-left: auto;
    color: var(--dim);
    font-size: var(--text-label);
  }

  .switches label {
    display: inline-flex;
    gap: 0.4rem;
    align-items: center;
    cursor: pointer;
  }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem 1rem;
    margin: 0;
    padding: 0;
    list-style: none;
    font-size: var(--text-body);
  }

  .swatch {
    display: inline-block;
    width: 10px;
    height: 10px;
    margin-right: 0.35rem;
    border-radius: 2px;
    background: var(--c);
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

  tfoot td {
    font-weight: 700;
    border-top: 1px solid var(--border);
    border-bottom: 0;
  }
</style>
