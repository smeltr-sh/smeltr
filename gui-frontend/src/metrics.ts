// Label-aware rollups over the metric samples on a heartbeat.
//
// Every counter arrives as a per-drain delta (`Metrics::drain`), so one
// poll's value is the activity since the last sample and a running total
// is addition. `smeltr_rows_errored_total` carries `pipeline`, `function` and
// `kind`; `smeltr_rows_polled_total` carries `source`; `smeltr_rows_sent_total`
// carries `destination`.

import type { MetricSampleDto, ThreadHealth } from './api';

/** Stands in for a label the sample does not carry. */
export const NO_LABEL = '--';

/** `core::error::INGEST_FUNCTION`: marked at ingest, before any step. */
const INGEST_FUNCTION = '4294967295';

/** Label values are joined on a character no label can contain. */
const SEP = '\u0000';

export function labelOf(sample: MetricSampleDto, name: string): string {
  const hit = sample.labels.find(([k]) => k === name);
  return hit ? hit[1] : NO_LABEL;
}

/** A count that only goes up, whether whole or, like seconds spent, not. */
export function isCounter(sample: MetricSampleDto): boolean {
  return sample.value.kind === 'counter' || sample.value.kind === 'float_counter';
}

/** One metric's total across every thread, labels ignored. Feeds the
 * whole-fleet sparklines. */
export function sumMetric(threads: ThreadHealth[], metric: string): number {
  let total = 0;
  for (const t of threads) {
    for (const m of t.metrics) {
      if (m.metric === metric && isCounter(m)) total += m.value.value;
    }
  }
  return total;
}

export type BreakdownRow = { values: string[]; total: number };

/** Running totals keyed by the joined label values. */
export type BreakdownTotals = Record<string, BreakdownRow>;

/** Fold one poll's counters for `metric` into `totals`, grouped by the
 * values of `labelNames`. Returns a new object: the caller holds this in
 * reactive state. */
export function addPoll(
  totals: BreakdownTotals,
  threads: ThreadHealth[],
  metric: string,
  labelNames: string[],
): BreakdownTotals {
  const next: BreakdownTotals = { ...totals };
  for (const t of threads) {
    for (const m of t.metrics) {
      if (m.metric !== metric || !isCounter(m)) continue;
      const values = labelNames.map((n) => labelOf(m, n));
      const key = values.join(SEP);
      next[key] = { values, total: (next[key]?.total ?? 0) + m.value.value };
    }
  }
  return next;
}

/** Biggest first; ties broken on the labels so rows hold still between
 * polls. */
export function toRows(totals: BreakdownTotals): BreakdownRow[] {
  return Object.values(totals).sort(
    (a, b) => b.total - a.total || a.values.join(SEP).localeCompare(b.values.join(SEP)),
  );
}

/** A stable key for a row in a keyed `{#each}`. */
export function rowKey(row: BreakdownRow): string {
  return row.values.join(SEP);
}

/** The `function` label is a counter the config loader assigns across the
 * expanded step tree, so only the ingest sentinel has a name here.
 * Resolving the rest wants a name from the backend, not a second copy of
 * that numbering. */
export function formatFunction(id: string): string {
  return id === INGEST_FUNCTION ? 'ingest' : id;
}

/** Identifies a sample within a thread. One metric appears once per label
 * combination, so the name alone does not. */
export function sampleKey(sample: MetricSampleDto): string {
  return [sample.metric, ...sample.labels.map(([k, v]) => `${k}=${v}`)].join(SEP);
}

/** `name{label=value,...}=N`, or `name=N` with no labels. */
export function formatSample(sample: MetricSampleDto): string {
  const labels = sample.labels.map(([k, v]) => `${k}=${v}`).join(',');
  return `${sample.metric}${labels ? `{${labels}}` : ''}=${sample.value.value}`;
}
