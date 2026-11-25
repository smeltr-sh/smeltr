// The metric samples on a heartbeat, as the Status page names and lists them.

import type { MetricSampleDto } from './api';

/** `core::error::INGEST_LABEL`: the `function` label of an exception from ingest, which no step raised. */
const INGEST_LABEL = '4294967295';

/** Label values are joined on a character no label can contain. */
const SEP = '\u0000';

/** The `function` label is a counter the config loader assigns across the
 * expanded step tree, so only the ingest label has a name here.
 * Resolving the rest wants a name from the backend, not a second copy of
 * that numbering. */
export function formatFunction(id: string): string {
  return id === INGEST_LABEL ? 'ingest' : id;
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
