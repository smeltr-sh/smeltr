// The metric samples on a heartbeat, as the Status page names and lists them.

import type { MetricSampleDto } from './api';

/** The `function` label a worker through 1.3 sends for an exception from ingest. A later worker sends `ingest`.
 * Drop this in 1.5. */
const INGEST_LABEL_THROUGH_1_3 = '4294967295';

/** Label values are joined on a character no label can contain. */
const SEP = '\u0000';

/** The `function` label is a counter the config loader assigns across the
 * expanded step tree, so only an exception from ingest has a name here.
 * Naming the rest needs the backend to send names, not a second copy of
 * that numbering in the GUI. */
export function formatFunction(id: string): string {
  return id === INGEST_LABEL_THROUGH_1_3 ? 'ingest' : id;
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
