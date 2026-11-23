import type { Difference, TestMark } from './api';
import { sentText } from './objectChanges';

/** How one row on screen differs from what the open test locks. */
export type RowDifference = {
  /** Each field the test locks that the row holds otherwise, by path: the locked value, or absent where the test
   * locks the field as missing. */
  fields: Map<string, { locked?: unknown }>;
  /** What differs of a whole document on the row, a sentence each. */
  notes: string[];
};

/** A row on screen, with the number the test gives it. */
export type TestedRow = { key: number; at: number };

/** The leader's `marks`, on the rows on screen that show them, by key. A test row's documents are the rows on screen
 * with that number, in the order the run made them. A document the test locks and the run did not make is marked on
 * the last of them. */
export function rowDifferences(marks: TestMark[], rows: TestedRow[]): Map<number, RowDifference> {
  const shown = new Map<number, number[]>();
  for (const { key, at } of rows) shown.set(at, [...(shown.get(at) ?? []), key]);
  const out = new Map<number, RowDifference>();
  for (const mark of marks) {
    const keys = shown.get(mark.row);
    if (!keys) continue;
    const key = keys[Math.min(mark.document, keys.length - 1)];
    const into: RowDifference = out.get(key) ?? { fields: new Map(), notes: [] };
    out.set(key, into);
    const note = differenceNote(mark);
    if (note) into.notes.push(note);
    else for (const path of mark.fields) into.fields.set(path, lockedOf(mark, path));
  }
  return out;
}

function lockedOf(mark: TestMark, path: string): { locked?: unknown } {
  const values = mark.expected?.values ?? {};
  return Object.hasOwn(values, path) ? { locked: values[path] } : {};
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
