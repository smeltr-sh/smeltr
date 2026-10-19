// The published function reference. The data is `reference.json`, kept honest
// by `tests/website-reference-tests.rs`: it checks the catalog against
// `ops::stdfuncs` and evaluates every example against the engine. This module
// only gives the shape a name -- TypeScript infers a JSON import structurally,
// so an entry without `result` would otherwise narrow the type of the ones
// that have it.
import data from './reference.json';
import signatures from './signatures.json';

export interface Example {
  expr: string;
  /** The JSON the expression produces. Absent when it reads the clock. */
  result?: string;
  /** `name = expr` bindings evaluated before `expr`, shown above it. */
  given?: string[];
  /** What this one example shows, rendered beside it as a comment. */
  note?: string;
  /** The exception kinds it throws. Absent when it throws none. */
  throws?: string[];
  /** The label over the run this example belongs to. See `Grouped`. */
  group?: string;
}

/**
 * A list long enough to be scanned rather than read is drawn in labeled
 * runs: the page prints a label once, above the run it starts. A label's
 * items are contiguous, checked by
 * `every_entry_carries_the_prose_the_page_renders`, so no label heads two
 * runs. A list short enough to read straight through carries no labels, and
 * a trailing item that belongs to no run carries none.
 */
export interface Grouped {
  group?: string;
}

/** One row of the `parseTime` format table: the specifier, what it matches, and a parse that shows it. */
export interface SpecifierRow {
  specifier: string;
  matches: string;
  examples: Example[];
  /** The label over the run of rows this one belongs to. See `Grouped`. */
  group?: string;
}

/** One accepted shape of a call: the parameter types and what it returns. Written by `just signatures`. */
export interface TypedRow {
  params: string;
  result: string;
}

/** Every function's typed rows, by name. The engine's own table, so the page cannot drift from it. */
export const SIGNATURES: Record<string, TypedRow[]> = signatures;

export interface FunctionEntry {
  name: string;
  signature: string;
  summary: string;
  /** Lead-in for `specifiers`, present with it. */
  specifierBlurb?: string;
  specifiers?: SpecifierRow[];
  notes?: string[];
  examples: Example[];
  /** The label over the run of functions this one belongs to. See `Grouped`. */
  group?: string;
}

/** One type category: the operators that produce it, then the functions over it. */
export interface Group {
  id: string;
  title: string;
  blurb: string;
  rows?: OperatorRow[];
  notes?: string[];
  rejected?: Rejected[];
  functions: FunctionEntry[];
}

/** One line of an operator table. A symbol takes one row per operand type it serves. */
export interface OperatorRow {
  symbol: string;
  gloss: string;
  /** One example, or two where the second case is what teaches the rule. */
  examples: Example[];
}

/** An expression the page shows as a compile error, with why. */
export interface Rejected {
  expr: string;
  why: string;
}

export const GROUPS: Group[] = data.groups;

/** One exception kind: its name, its category, and when it happens. */
export interface ExceptionKind {
  kind: string;
  category: string;
  when: string;
}

export const EXCEPTIONS: ExceptionKind[] = data.exceptions;

/** A field Smeltr gives a record, or reads from one: its name, its type, who writes it, and what it holds. */
export interface RecordField {
  name: string;
  type: string;
  setBy: 'source' | 'pipeline' | 'Smeltr';
  what: string;
}

export const RECORD_FIELDS: RecordField[] = data.recordFields as RecordField[];

/** One type of the language: its name as a declaration spells it, what it holds, and a value of it. */
export interface TypeRow {
  name: string;
  holds: string;
  /** The type the example's test declares, where `name` is generic: `array<i64>` for `array<T>`. */
  declare?: string;
  /** Absent for a type no literal or function gives. */
  example?: Example;
}

export const TYPES: TypeRow[] = data.types;

/** The examples a How it works topic shows, by the topic's id. */
export interface Topic {
  id: string;
  examples: Example[];
  rejected: Rejected[];
}

const TOPICS: Topic[] = data.topics.map((t) => ({ rejected: [], ...t }));

export function topic(id: string): Topic {
  const found = TOPICS.find((t) => t.id === id);
  if (!found) throw new Error(`reference.json has no topic ${id}`);
  return found;
}
