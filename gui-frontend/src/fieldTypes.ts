// What an expression field knows of the record it reads, and what the chain typed before a `.` is: an object, whose
// fields come next, or a value of a type, which the functions that take it come next for.

import { getContext, setContext } from 'svelte';
import { tokenize, type Token } from '../../website/src/lib/highlight/highlight';
import { RECORD_FIELDS, SIGNATURES } from '../../website/src/lib/reference';
import { keySpans } from './names';

/** Each field of a record by its name, as names spell it (`url.path`, `labels["a.b"]`), with its type as the engine
 * spells it (`str`, `array<i64>`). An open object's other fields are `name.*`, a variant. */
export type FieldTypes = readonly (readonly [string, string])[];

/** The fields every source sets on its record (`_raw`, `__source.*`). */
export const SOURCE_FIELDS: FieldTypes = RECORD_FIELDS.filter((f) => f.setBy === 'source').map(
  (f) => [f.name, f.type] as const,
);

/** The lists as one, a later list's type winning for a field two lists name. */
export function mergeFields(...lists: (FieldTypes | null | undefined)[]): FieldTypes {
  const byName = new Map<string, string>();
  for (const list of lists) for (const [name, type] of list ?? []) byName.set(name, type);
  return [...byName];
}

const KEY = Symbol('field types');

/** Called while a component initialises: the expression fields below it read the record `get` returns. */
export function provideFieldTypes(get: () => FieldTypes | undefined): void {
  setContext(KEY, get);
}

/** The record the expression fields here read, if a component above provides it. */
export function contextFieldTypes(): (() => FieldTypes | undefined) | undefined {
  return getContext<(() => FieldTypes | undefined) | undefined>(KEY);
}

/** What the chain before a `.` is. An `object`'s fields are listed by name and type, an object field's type
 * `object`. A `value` has a type the engine spells. `unknown` is anything this cannot tell. */
export type Member =
  | { kind: 'object'; fields: { name: string; type: string }[] }
  | { kind: 'value'; type: string }
  | { kind: 'unknown' };

type Node = { type?: string; open: boolean; children: Map<string, Node> };

function tree(fields: FieldTypes): Node {
  const root: Node = { open: false, children: new Map() };
  for (const [name, type] of fields) {
    let node = root;
    for (const { key } of keySpans(name)) {
      if (key === '*') {
        node.open = true;
        break;
      }
      let child = node.children.get(key);
      if (!child) node.children.set(key, (child = { open: false, children: new Map() }));
      node = child;
    }
    if (!name.endsWith('.*')) node.type = type;
  }
  return root;
}

/** One step of a chain: a field by its key, an array's element, or a call of a function on what comes before. */
type Step = { field: string } | { index: true } | { call: string };

/** The tokens of `src` with the plain ones that are whitespace dropped, each with where it starts. */
function significant(src: string): (Token & { at: number })[] {
  const out: (Token & { at: number })[] = [];
  let at = 0;
  for (const t of tokenize(src)) {
    if (t.kind !== 'plain' || t.text.trim() !== '') out.push({ ...t, at });
    at += t.text.length;
  }
  return out;
}

const isWord = (t: Token | undefined) =>
  !!t && /^[A-Za-z_$][\w$]*$/.test(t.text) && t.kind !== 'string';

/** The chain `src` ends with, read back from its end: where it starts (a field of the record, or a literal's type)
 * and its steps in order. Null where `src` does not end in a chain a `.` can follow. */
function chain(
  src: string,
): { root: 'record' | 'unknown' | { type: string }; steps: Step[] } | null {
  const toks = significant(src);
  // Only what touches the dot is the chain: `a .b` is not a member.
  const last = toks[toks.length - 1];
  if (last && last.at + last.text.length !== src.length) return { root: 'record', steps: [] };
  const steps: Step[] = [];
  let i = toks.length - 1;
  const matching = (close: string, open: string): number => {
    let depth = 0;
    for (let j = i; j >= 0; j--) {
      if (toks[j].kind !== 'plain') continue;
      if (toks[j].text === close) depth++;
      else if (toks[j].text === open && --depth === 0) return j;
    }
    return -1;
  };
  for (;;) {
    const t = toks[i];
    if (!t || (t.kind === 'plain' && !isWord(t) && t.text !== ')' && t.text !== ']')) {
      // Nothing before the dot, or an operator: the record's own fields.
      return { root: 'record', steps: steps.reverse() };
    }
    if (t.kind === 'string') return { root: { type: 'str' }, steps: steps.reverse() };
    if (t.kind === 'number') {
      // `1.` is a number being typed, not a member.
      if (!t.text.includes('.') && !/e/i.test(t.text)) return null;
      return { root: { type: 'f64' }, steps: steps.reverse() };
    }
    if (t.kind === 'literal') {
      const type = t.text === 'null' ? 'null' : 'bool';
      return { root: { type }, steps: steps.reverse() };
    }
    if (t.text === ')') {
      const open = matching(')', '(');
      const name = toks[open - 1];
      if (open < 0) return null;
      // A group, or a function called on its own: what it gives is not read here.
      if (!isWord(name) || toks[open - 2]?.text !== '.') return { root: 'unknown', steps: [] };
      steps.push({ call: name.text });
      i = open - 3;
      continue;
    }
    if (t.text === ']') {
      const open = matching(']', '[');
      if (open < 0) return null;
      const inner = toks.slice(open + 1, i);
      steps.push(
        inner.length === 1 && inner[0].kind === 'string'
          ? { field: JSON.parse(inner[0].text) as string }
          : { index: true },
      );
      i = open - 1;
      if (toks[i]?.text === '.') i--;
      continue;
    }
    if (isWord(t)) {
      steps.push({ field: t.text });
      if (toks[i - 1]?.text !== '.') return { root: 'record', steps: steps.reverse() };
      i -= 2;
      continue;
    }
    return null;
  }
}

/** What the chain before the `.` at `dot` in `src` is, read against `fields`. Null where that `.` does not follow a
 * value: it is in a string or a comment, or it is a number's decimal point. */
export function memberBefore(
  src: string,
  dot: number,
  fields: FieldTypes | undefined,
): Member | null {
  const toks = tokenize(src.slice(0, dot + 1));
  const at = toks[toks.length - 1];
  if (!at || at.kind !== 'plain' || at.text !== '.') return null;
  const read = chain(src.slice(0, dot));
  if (!read) return null;
  if (read.root === 'unknown') return { kind: 'unknown' };
  let node: Node | null = read.root === 'record' ? (fields ? tree(fields) : null) : null;
  let type: string | null = read.root === 'record' ? null : read.root.type;
  for (const step of read.steps) {
    if ('call' in step) {
      type = type === null ? null : resultType(step.call, type);
      node = null;
    } else if ('index' in step) {
      type = type?.startsWith('array<') ? type.slice('array<'.length, -1) : null;
      node = null;
    } else {
      const child = node?.children.get(step.field);
      node = child ?? null;
      type = child?.type ?? null;
    }
    if (node === null && type === null) return { kind: 'unknown' };
  }
  if (node && node.children.size > 0 && (!node.type || node.type.startsWith('{'))) {
    return {
      kind: 'object',
      fields: [...node.children].map(([name, n]) => ({
        name,
        type: n.type && !n.type.startsWith('{') ? n.type : 'object',
      })),
    };
  }
  // An object inside an array is spelled whole (`{host: str}`), which this does not read.
  if (type && type !== 'variant' && type !== 'null' && !type.startsWith('{')) {
    return { kind: 'value', type };
  }
  return { kind: 'unknown' };
}

const SPELLED = /^(str|i64|f64|bool|bytes|datetime|duration|array<.+>)$/;

/** What `name` called on a `receiver` gives, as the engine spells it, or null where its reference does not say. */
function resultType(name: string, receiver: string): string | null {
  const row = (SIGNATURES[name] ?? []).find((r) =>
    firstTakes(r.params).some((t) => fits(t, receiver)),
  );
  if (!row) return null;
  if (SPELLED.test(row.result)) return row.result;
  if (row.result === "the value's type" || row.result === "the array's type") return receiver;
  if (row.result === 'the element type' && receiver.startsWith('array<')) {
    return receiver.slice('array<'.length, -1);
  }
  return null;
}

/** The types a signature's first parameter takes: `(i64/f64, str)` takes `i64` and `f64`. */
function firstTakes(params: string): string[] {
  const first = params.replace(/^\(|\)$/g, '').split(', ')[0];
  return first ? first.split('/') : [];
}

function fits(takes: string, type: string): boolean {
  if (takes === 'any' || takes === type) return true;
  return (takes === 'array' || takes === 'array of scalars') && type.startsWith('array<');
}

/** Whether `name` takes a `type` before its dot, by the engine's signatures, with what it does take. `fits` is null
 * where the signatures do not name the function. */
export function takes(name: string, type: string): { fits: boolean | null; takes: string } {
  const rows = SIGNATURES[name];
  if (!rows) return { fits: null, takes: '' };
  const all = [...new Set(rows.flatMap((r) => firstTakes(r.params)))];
  return { fits: all.some((t) => fits(t, type)), takes: all.join('/') };
}

/** A source's record: the fields every source sets, and those its config declares (an SQL query's columns, an Avro
 * payload's fields). */
export function sourceRecord(source: object | null | undefined): FieldTypes {
  const declared =
    source && 'fields' in source ? (source.fields as { name: string; type: string }[]) : [];
  return mergeFields(
    SOURCE_FIELDS,
    (declared ?? []).map((f) => [f.name, f.type] as const),
  );
}
