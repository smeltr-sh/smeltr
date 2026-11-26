import type { Difference, FieldDifference } from './api';
import { sentText } from './objectChanges';

/** How one row on screen differs from what the open test locks. */
export type RowDifference = {
  /** Each value the test locks that the row holds otherwise, by path: an array's by element. */
  fields: Map<string, FieldDifference>;
  /** What differs of a whole document on the row, a sentence each. */
  notes: string[];
};

/** The leader's `differences`, on the rows on screen that show them, by key. `documents` holds each test row's documents in
 * the order a test lists them, by the key of the row that shows each, or none where it is not on screen. A document
 * the test locks and the run did not make is marked on the last on-screen row of its test row's documents. */
export function rowDifferences(
  differences: Difference[],
  documents: Map<number, (number | undefined)[]>,
): Map<number, RowDifference> {
  const out = new Map<number, RowDifference>();
  for (const d of differences) {
    const keys = documents.get(d.row) ?? [];
    const key = d.document < keys.length ? keys[d.document] : keys.findLast((k) => k !== undefined);
    if (key === undefined) continue;
    const into: RowDifference = out.get(key) ?? { fields: new Map(), notes: [] };
    out.set(key, into);
    const note = differenceNote(d);
    if (note) into.notes.push(note);
    else for (const f of d.fields ?? []) into.fields.set(f.path, f);
  }
  return out;
}

/** What differs of a whole document, as a sentence: null where both sides made a document and sent it to the same
 * place, so its fields say what differs. */
export function differenceNote({ expected, actual }: Difference): string | null {
  if (expected?.values && actual?.values && sentText(expected) === sentText(actual)) return null;
  return expected === undefined
    ? 'The test locks no more documents on this row; this run made another.'
    : actual === undefined
      ? 'The test locks another document on this row; this run made none.'
      : !expected.values && actual.values
        ? 'The test locked this row as dropped; this run kept it.'
        : expected.values && !actual.values
          ? 'The test locked a document here; this run dropped it.'
          : 'This run sent the document elsewhere.';
}
