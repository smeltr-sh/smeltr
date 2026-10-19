// A Parser step's field proposal: what the sample's preview read, beside what the step produced in production
// (`GET /api/streams`). Both are the parser's own counts, so a field reads the same from either.

import type { ParserFieldProposal, ParserProposal, ParserShapeDto } from './api';
import { proposedTypeSpec, type FieldSpec, type Step } from './pipeline';
import { count } from './time';

/** What one Parser step produced in production, summed over every group, source and route that ran it. */
export interface Production {
  rows: number;
  lastSeenMs: number;
  fields: ParserFieldProposal[];
}

/** One field of a proposal, from the sample, from production, or from both. Its type is the sample's where the
 * sample has it, since the sample is read by the pipeline on this branch, and its counts are both summed. */
export interface ProposalRow {
  name: string;
  suggested: string;
  present: number;
  times: number;
  sample?: ParserFieldProposal;
  production?: ParserFieldProposal;
}

const KINDS = [
  ['bool', 'bools'],
  ['i64', 'ints'],
  ['f64', 'floats'],
  ['str', 'strings'],
  ['array', 'arrays'],
  ['struct', 'objects'],
] as const;

/** What `shapes` say step `step` of pipeline `pipeline` produced, or `null` where production never ran it. A field a
 * shape lacks was absent from that shape's rows, so every field counts against every row. */
export function productionOf(
  shapes: ParserShapeDto[],
  pipeline: string,
  step: string,
): Production | null {
  const ran = shapes.filter((s) => s.pipeline === pipeline && s.step === step);
  if (ran.length === 0) return null;
  const rows = ran.reduce((n, s) => n + s.rows, 0);
  const byName = new Map<string, ParserFieldProposal & { lastSeenMs: number; counts: number[] }>();
  for (const shape of ran) {
    for (const f of shape.fields) {
      const held = byName.get(f.name);
      if (!held) {
        byName.set(f.name, {
          name: f.name,
          declared: f.declared,
          suggested: f.suggested,
          rows,
          present: f.present,
          kinds: [],
          failed: f.failed,
          times: f.times,
          lastSeenMs: f.lastSeenMs,
          counts: KINDS.map(([, k]) => f[k]),
        });
        continue;
      }
      if (f.lastSeenMs >= held.lastSeenMs) {
        held.declared = f.declared;
        held.suggested = f.suggested;
        held.lastSeenMs = f.lastSeenMs;
      }
      held.present += f.present;
      held.failed = (held.failed ?? 0) + f.failed;
      held.times = (held.times ?? 0) + f.times;
      KINDS.forEach(([, k], i) => (held.counts[i] += f[k]));
    }
  }
  const fields = [...byName.values()].map(({ counts, lastSeenMs: _, ...f }) => ({
    ...f,
    kinds: KINDS.map(([kind], i) => [kind, counts[i]] as [string, number])
      .filter(([, n]) => n > 0)
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])),
  }));
  return { rows, lastSeenMs: Math.max(...ran.map((s) => s.lastSeenMs)), fields };
}

/** The sample's fields in the order it read them, then the fields only production saw. */
export function proposalRows(
  sample: ParserFieldProposal[],
  production: Production | null,
): ProposalRow[] {
  const seen = new Map((production?.fields ?? []).map((f) => [f.name, f]));
  const row = (
    s: ParserFieldProposal | undefined,
    p: ParserFieldProposal | undefined,
  ): ProposalRow => ({
    name: (s ?? p)!.name,
    suggested: (s ?? p)!.suggested,
    present: (s?.present ?? 0) + (p?.present ?? 0),
    times: (s?.times ?? 0) + (p?.times ?? 0),
    sample: s,
    production: p,
  });
  const names = new Set(sample.map((f) => f.name));
  return [
    ...sample.map((s) => row(s, seen.get(s.name))),
    ...(production?.fields ?? []).filter((p) => !names.has(p.name)).map((p) => row(undefined, p)),
  ];
}

/** A step's field proposal as shown on screen: in flight, failed, or read. */
export interface FieldProposal {
  loading: boolean;
  error: string;
  result: ParserProposal | null;
  /** What the step produced in production, or `null` where it has not run there. */
  production: Production | null;
  /** Each field the proposal read, with the declaration Add would make of it. */
  suggestions: Suggestion[];
}

/** The proposal open on one parser step, and what its card says it was read from. */
export interface OpenProposal {
  step: Step;
  proposal: FieldProposal;
  sampleName: string;
  onClose: () => void;
}

/** One proposed field, and its declaration as the author edits it before adding it: `null` where this builder
 * cannot express its shape. */
export interface Suggestion {
  row: ProposalRow;
  field: FieldSpec | null;
}

export function suggestionsOf(rows: ProposalRow[]): Suggestion[] {
  return rows.map((row) => {
    const ty = proposedTypeSpec(row);
    return { row, field: ty ? { name: row.name, ty } : null };
  });
}

/** What is worth saying of one field as one source saw it: where rows lack it, disagree about it, or failed it. */
export function fieldNotes(f: ParserFieldProposal): string[] {
  const notes: string[] = [];
  if (f.present !== f.rows)
    notes.push(`absent from ${f.rows - f.present} of ${count(f.rows, 'row')}`);
  if (f.kinds.length > 1)
    notes.push(`rows disagree — ${f.kinds.map(([k, n]) => `${n} ${k}`).join(', ')}`);
  if (f.failed) notes.push(`${count(f.failed, 'row')} did not fit the declared type`);
  return notes;
}
