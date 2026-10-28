// The pipeline builder's in-memory model and its codec against
// `config_file::dto`'s `StepDto` JSON (src/config_file/dto.rs). A subset of
// what that shape can express (no Struct-typed declared fields beyond one
// level, no nested arrays); everything it produces is the wire format the
// loader accepts, tag for tag.

import type { OnboardProposal } from './api';

/** A field's declared type. `object` is a `Struct` and `arrayOfObjects` is
 * `Array(Struct)`, each with fields of named types: the one level of nesting
 * the builder models; anything deeper is refused on load rather than
 * flattened. */
export type DataTypeSpec =
  | { kind: 'named'; name: NamedType }
  /** A CSV column left out of the record: it holds its position and produces no field, so it has no type. */
  | { kind: 'skip' }
  /** A declared datetime: `format` is the `parseTime` format its text arrives in, or empty for auto-detect. */
  | { kind: 'datetime'; format: string }
  | { kind: 'array'; of: NamedType }
  /** An open object's unclaimed fields are each a variant a step can read; a closed object's pass through, and no step reads them. */
  | { kind: 'object'; fields: ObjectField[]; open: boolean }
  /** Each element is a closed object. */
  | { kind: 'arrayOfObjects'; fields: ObjectField[] };

export interface ObjectField {
  name: string;
  ty: NamedType;
}

/** What a parser can pin a field to. `datetime` is not here: it is its own
 * shape, read by `parseTime` after the parse. `duration` and `bytes` are not
 * here either -- no parser can produce them, so offering one only ever
 * produced a column of nulls. */
export const NAMED_TYPES = ['bool', 'i64', 'f64', 'str', 'variant'] as const;
export type NamedType = (typeof NAMED_TYPES)[number];

/** One choice in a field's type picker: a declared type as the picker names it, with an array's element type and
 * nothing else of an object. */
export type TypeKey =
  NamedType | 'datetime' | `array of ${NamedType}` | 'object' | 'array of objects' | 'skip';

/** The named types in the order a picker lists them, the common ones first. */
const PICKED_NAMED: NamedType[] = ['str', 'i64', 'f64', 'bool', 'variant'];

/** A declared field's picker, one group per run between separators. `skip` is CSV's alone. */
export function declaredTypeGroups(csv: boolean): TypeKey[][] {
  const groups: TypeKey[][] = [
    [...PICKED_NAMED, 'datetime'],
    PICKED_NAMED.map((t) => `array of ${t}` as const),
    ['object', 'array of objects'],
  ];
  return csv ? [...groups, ['skip']] : groups;
}

/** An object's own field takes a named type only. */
export const OBJECT_FIELD_TYPE_GROUPS: TypeKey[][] = [PICKED_NAMED];

/** A lookup output holds one value. `skip` is for a known table's column grid, where it leaves the column out. */
export function lookupTypeGroups(grid: boolean): TypeKey[][] {
  const one: TypeKey[] = ['str', 'i64', 'f64', 'bool', 'datetime'];
  return grid ? [one, ['skip']] : [one];
}

export function typeKey(ty: DataTypeSpec): TypeKey {
  switch (ty.kind) {
    case 'named':
      return ty.name;
    case 'array':
      return `array of ${ty.of}`;
    case 'arrayOfObjects':
      return 'array of objects';
    default:
      return ty.kind;
  }
}

/** `ty` as `key` names it. An object keeps the fields it already has when switched to an array of them and back. */
export function withTypeKey(ty: DataTypeSpec, key: TypeKey): DataTypeSpec {
  if (key === typeKey(ty)) return ty;
  const fields = ty.kind === 'object' || ty.kind === 'arrayOfObjects' ? ty.fields : [];
  if (key === 'skip') return { kind: 'skip' };
  // Auto-detect is where the author lands.
  if (key === 'datetime') return { kind: 'datetime', format: '' };
  if (key === 'object') return { kind: 'object', fields, open: false };
  if (key === 'array of objects') return { kind: 'arrayOfObjects', fields };
  if (key.startsWith('array of '))
    return { kind: 'array', of: key.slice('array of '.length) as NamedType };
  return { kind: 'named', name: key as NamedType };
}

/** What the parser counted of a field it proposes: the type its values suggest, how many rows held it, and how many
 * of those read as a time (`timeparse::reads_as_time`). */
export interface ProposedField {
  name: string;
  suggested: string;
  present: number;
  times?: number;
}

/** A proposed field as a declared-field type this builder can express: a `datetime` when every value it held read as
 * a time, else what `suggested` names, or `null` for a shape it cannot: a bare `struct`, or an array of anything but
 * one named type. */
export function proposedTypeSpec(f: ProposedField): DataTypeSpec | null {
  const { suggested, present, times = 0 } = f;
  if (times > 0 && times === present && ['i64', 'f64', 'str'].includes(suggested)) {
    return { kind: 'datetime', format: '' };
  }
  if ((NAMED_TYPES as readonly string[]).includes(suggested)) {
    return { kind: 'named', name: suggested as NamedType };
  }
  const arrayOf = /^array\(([a-z0-9_]+)\)$/.exec(suggested)?.[1];
  return arrayOf && (NAMED_TYPES as readonly string[]).includes(arrayOf)
    ? { kind: 'array', of: arrayOf as NamedType }
    : null;
}

/** Declare each of `fields` that `declared` does not declare yet, as `proposedTypeSpec` types it. A field this
 * builder cannot type stays undeclared. */
export function declareProposed(declared: FieldSpec[], fields: ProposedField[]) {
  for (const f of fields) {
    const ty = proposedTypeSpec(f);
    if (ty && !isDeclared(declared, f.name)) declared.push({ name: f.name, ty });
  }
}

export function isDeclared(declared: FieldSpec[], name: string): boolean {
  return declared.some((d) => d.name === name);
}

export interface FieldSpec {
  name: string;
  ty: DataTypeSpec;
}

/** `quote` is a single character, or `''` for none. `nextKey` is whether an
 * unquoted value runs to the separator before the next key; absent means on.
 * `splitDots` is whether a key the parser reads is split at its dots into a
 * path; absent means the format's own default (`SPLIT_DOTS_DEFAULT`). */
export type ParserKind = { splitDots?: boolean } & (
  | { type: 'json' }
  /** The columns are the step's own `declared` list, in order. */
  | { type: 'csv'; delimiter: string }
  | { type: 'keyValue'; pairSep: string; kvSep: string; quote: string; nextKey?: boolean }
  | { type: 'grok'; patterns: string[]; customPatterns: { name: string; pattern: string }[] }
);

/** The name a reader sees for each parser format, in the order the format menu lists them. One parser reads regular
 * expressions and Grok patterns alike, so its name says both. */
export const PARSER_FORMAT_NAMES: Record<ParserKind['type'], string> = {
  json: 'JSON',
  csv: 'CSV',
  keyValue: 'Key/value',
  grok: 'Regex/Grok',
};

/** Each format a sample can be proposed as (`proposeOnboard`), as a reader names it. CEF has no parser of its own, and a
 * line format is read by a Regex/Grok parser on its own whole-line pattern. */
export const PROPOSED_FORMAT_NAMES: Record<NonNullable<OnboardProposal['format']>, string> = {
  ...PARSER_FORMAT_NAMES,
  cef: 'CEF',
  syslog: 'classic syslog',
  syslog5424: 'RFC 5424 syslog',
  commonLog: 'common access log',
  combinedLog: 'combined access log',
};

/** Dots are Key/value's only way to nest, and a grok capture name is a path
 * by habit; a JSON or CSV key is one key. */
export const SPLIT_DOTS_DEFAULT: Record<ParserKind['type'], boolean> = {
  json: false,
  csv: false,
  keyValue: true,
  grok: true,
};

/** Whether this parser splits a key at its dots. */
export function splitsDots(kind: ParserKind): boolean {
  return kind.splitDots ?? SPLIT_DOTS_DEFAULT[kind.type];
}

/** Sets `splitDots`, keeping it absent at the format's default, so a saved
 * pipeline that never touched it does not change. */
export function setSplitDots(kind: ParserKind, on: boolean): void {
  if (on === SPLIT_DOTS_DEFAULT[kind.type]) delete kind.splitDots;
  else kind.splitDots = on;
}

export const MATCH_KINDS = ['exact', 'cidr', 'regex'] as const;
export type MatchKind = (typeof MATCH_KINDS)[number];

export const MULTIPLICITIES = ['best', 'first', 'all'] as const;
export type Multiplicity = (typeof MULTIPLICITIES)[number];

/** Display labels; the wire values are `plan::config::Multiplicity`'s. */
export const MULTIPLICITY_LABELS: Record<Multiplicity, string> = {
  best: 'Most exact (try every candidate, keep the best)',
  first: 'First match (fastest, table order)',
  all: 'All matches',
};

/** One matched table column (or, for MMDB, a dotted record path) mapped to
 * an output field, with its own miss default. An empty `default` is null
 * on a miss, never the empty string. */
/** A lookup output's type: one value, so a named type or a datetime. `str` is the default and is not written. */
export type LookupOutputType = Extract<DataTypeSpec, { kind: 'named' } | { kind: 'datetime' }>;

export const STR_OUTPUT: LookupOutputType = { kind: 'named', name: 'str' };

export interface LookupOutput {
  source: string;
  field: string;
  default: string;
  ty: LookupOutputType;
}

interface FunctionCommon {
  id: string;
  filter: string;
  disabled: boolean;
}

export type Step =
  | { type: 'if'; id: string; condition: string; then: Step[]; else: Step[] }
  | { type: 'comment'; id: string; text: string }
  | (FunctionCommon & { type: 'eval'; assignments: { target: string; expr: string }[] })
  | (FunctionCommon & { type: 'code'; body: string })
  | (FunctionCommon & { type: 'drop'; condition: string })
  | (FunctionCommon & { type: 'rename'; pairs: { from: string; to: string }[] })
  | (FunctionCommon & {
      type: 'parser';
      input: string;
      /** The object the parse writes into; `$`, the record. */
      into: string;
      kind: ParserKind;
      declared: FieldSpec[];
    })
  | (FunctionCommon & {
      type: 'lookup';
      input: string;
      /** A `LookupTableDto.id`. */
      table: string;
      matchKind: MatchKind;
      multiplicity: Multiplicity;
      keyColumn: string;
      outputs: LookupOutput[];
    })
  | (FunctionCommon & { type: 'unroll'; field: string; keepParentFields: boolean })
  | (FunctionCommon & {
      type: 'call';
      /** A `PipelineDto.id` whose steps are inlined here. */
      pipeline: string;
    });

export const STEP_TYPES = [
  'parser',
  'eval',
  'code',
  'drop',
  'rename',
  'lookup',
  'unroll',
  'call',
  'if',
  'comment',
] as const;
export type StepType = (typeof STEP_TYPES)[number];

// Seeded from the clock so a fresh id never collides with a hand-written
// one already in the pipeline (`eval-1`), which the loader rejects as a
// duplicate.
let counter = Date.now();
function nextId(prefix: string): string {
  counter += 1;
  return `${prefix}-${counter}`;
}

/** The field a new parse step reads: `_raw` when the sample's record has it
 * or there is no sample, else the record's first text field. */
export function parseInputFor(record: Record<string, unknown> | undefined): string {
  if (!record || '_raw' in record) return '_raw';
  return Object.keys(record).find((k) => typeof record[k] === 'string') ?? '_raw';
}

/** A new step of the given type, pre-filled with a runnable default.
 * `parseInput` is the field a parse step reads (`parseInputFor`). */
export function newStep(type: StepType, parseInput = '_raw'): Step {
  switch (type) {
    case 'if':
      return { type, id: nextId('if'), condition: 'true', then: [], else: [] };
    case 'comment':
      return { type, id: nextId('comment'), text: '' };
    case 'eval':
      return {
        type,
        id: nextId('eval'),
        filter: '',
        disabled: false,
        assignments: [{ target: '', expr: '' }],
      };
    case 'code':
      return { type, id: nextId('code'), filter: '', disabled: false, body: '' };
    case 'drop':
      return { type, id: nextId('drop'), filter: '', disabled: false, condition: 'false' };
    case 'rename':
      return {
        type,
        id: nextId('rename'),
        filter: '',
        disabled: false,
        pairs: [{ from: '', to: '' }],
      };
    case 'parser':
      return {
        type,
        id: nextId('parse'),
        filter: '',
        disabled: false,
        input: parseInput,
        into: '$',
        kind: { type: 'json' },
        declared: [],
      };
    case 'lookup':
      return {
        type,
        id: nextId('lookup'),
        filter: '',
        disabled: false,
        input: '',
        table: '',
        matchKind: 'exact',
        multiplicity: 'best',
        keyColumn: '',
        outputs: [],
      };
    case 'unroll':
      return {
        type,
        id: nextId('unroll'),
        filter: '',
        disabled: false,
        field: '',
        keepParentFields: false,
      };
    case 'call':
      return { type, id: nextId('call'), filter: '', disabled: false, pipeline: '' };
  }
}

function dataTypeDto(ty: DataTypeSpec): unknown {
  if (ty.kind === 'named') return ty.name;
  if (ty.kind === 'skip') return 'skip';
  if (ty.kind === 'datetime')
    return ty.format ? { type: 'datetime', format: ty.format } : 'datetime';
  if (ty.kind === 'array') return { type: 'array', of: ty.of };
  const struct = { type: 'struct', fields: ty.fields.map((f) => ({ name: f.name, type: f.ty })) };
  if (ty.kind === 'arrayOfObjects') return { type: 'array', of: struct };
  // A closed object is written without the key, as the loader reads its absence.
  return ty.open ? { ...struct, open: true } : struct;
}

/** `quote` is `Option<u8>` on the wire and the loader requires exactly one
 * character when present, so an empty quote is omitted, not sent as `""`.
 * `nextKey` and `splitDots` are omitted at their defaults. */
function parserKindDto(kind: ParserKind): unknown {
  const { splitDots, ...base } = kind;
  const out: Record<string, unknown> = { ...base };
  if (base.type === 'keyValue' && base.quote === '') delete out.quote;
  if (base.type === 'keyValue' && base.nextKey !== false) delete out.nextKey;
  if (splitDots !== undefined && splitDots !== SPLIT_DOTS_DEFAULT[kind.type]) {
    out.splitDots = splitDots;
  }
  return out;
}

function functionFields(step: FunctionCommon): {
  id: string;
  filter: string | null;
  disabled: boolean;
} {
  return {
    id: step.id,
    filter: step.filter.trim() === '' ? null : step.filter,
    disabled: step.disabled,
  };
}

/** One `Step` (recursively, for `if`) to the `StepDto` JSON shape. */
export function stepToDto(step: Step): unknown {
  switch (step.type) {
    case 'if':
      return {
        type: 'if',
        id: step.id,
        condition: step.condition,
        then: step.then.map(stepToDto),
        else: step.else.map(stepToDto),
      };
    case 'comment':
      return { type: 'comment', id: step.id, text: step.text };
    case 'eval':
      return { type: 'eval', ...functionFields(step), assignments: step.assignments };
    case 'code':
      return { type: 'code', ...functionFields(step), body: step.body };
    case 'drop':
      return { type: 'drop', ...functionFields(step), condition: step.condition };
    case 'rename':
      return { type: 'rename', ...functionFields(step), pairs: step.pairs };
    case 'parser':
      return {
        type: 'parser',
        ...functionFields(step),
        input: step.input,
        into: step.into.trim(),
        kind: parserKindDto(step.kind),
        declared: step.declared.map((f) => ({ name: f.name, type: dataTypeDto(f.ty) })),
      };
    case 'lookup':
      return {
        type: 'lookup',
        ...functionFields(step),
        input: step.input,
        table: step.table,
        matchKind: step.matchKind,
        multiplicity: step.multiplicity,
        keyColumn: step.keyColumn,
        outputs: step.outputs.map((o) => ({
          source: o.source,
          field: o.field,
          default: o.default,
          ...(o.ty.kind === 'named' && o.ty.name === 'str' ? {} : { type: dataTypeDto(o.ty) }),
        })),
      };
    case 'unroll':
      return {
        type: 'unroll',
        ...functionFields(step),
        field: step.field,
        keepParentFields: step.keepParentFields,
      };
    case 'call':
      return { type: 'call', ...functionFields(step), pipeline: step.pipeline };
  }
}

/** Every lookup table id any `lookup` step, at any depth, refers to. */
function collectLookupTables(steps: Step[]): string[] {
  const found = new Set<string>();
  for (const s of flatSteps(steps))
    if (s.type === 'lookup' && s.table.trim() !== '') found.add(s.table.trim());
  return [...found];
}

/** Lookup table ids referenced but not in `known`. The picker only offers
 * real ids, but a step loaded from disk can hold one that no longer
 * resolves; this catches it before Save. */
export function unknownLookupTables(steps: Step[], known: string[]): string[] {
  const knownSet = new Set(known);
  return collectLookupTables(steps).filter((t) => !knownSet.has(t));
}

function rawStepObjects(steps: unknown[]): Record<string, unknown>[] {
  return steps.filter((s): s is Record<string, unknown> => typeof s === 'object' && s !== null);
}

/** `call` targets in raw `StepDto[]` JSON. Walks the wire shape rather
 * than `Step[]` because a called pipeline may use a shape the builder
 * cannot represent, and scanning it must not throw. */
function collectRawCalledPipelines(steps: unknown[], found: Set<string>) {
  for (const s of rawStepObjects(steps)) {
    if (s.type === 'call' && typeof s.pipeline === 'string' && s.pipeline.trim() !== '') {
      found.add(s.pipeline.trim());
    } else if (s.type === 'if') {
      if (Array.isArray(s.then)) collectRawCalledPipelines(s.then, found);
      if (Array.isArray(s.else)) collectRawCalledPipelines(s.else, found);
    }
  }
}

/** `api.ts`'s `PipelineDto`, restated so this module does not depend on
 * the REST client. */
interface RawPipelineDto {
  id: string;
  name: string;
  steps: unknown[];
  schemas?: string[];
}

/**
 * The `config_file` JSON document for previewing one pipeline: that
 * pipeline, every other pipeline it `call`s (transitively, included as
 * fetched so a shape the builder cannot represent still previews), and
 * every lookup table. No sources, routes or destinations: a preview
 * compiles and runs a pipeline, and delivery is route config.
 *
 * An unresolved `call` target is left for the backend to name.
 */
export function buildFileConfig(
  pipelineId: string,
  pipelineName: string,
  steps: Step[],
  otherPipelines: RawPipelineDto[],
  lookupTables: unknown[],
  grokPatterns: unknown[] = [],
  schemas: unknown[] = [],
  produces: string[] = [],
): unknown {
  const callerSteps = steps.map(stepToDto);
  const byId = new Map(otherPipelines.map((p) => [p.id, p]));

  const includedPipelines: RawPipelineDto[] = [
    {
      id: pipelineId,
      name: pipelineName,
      steps: callerSteps,
      ...(produces.length > 0 ? { schemas: produces } : {}),
    },
  ];
  const visited = new Set([pipelineId]);
  const toVisit: string[] = [];

  const scan = (dtoSteps: unknown[]) => {
    const called = new Set<string>();
    collectRawCalledPipelines(dtoSteps, called);
    for (const calledId of called) if (!visited.has(calledId)) toVisit.push(calledId);
  };
  scan(callerSteps);

  while (toVisit.length > 0) {
    const nextId = toVisit.pop();
    if (nextId === undefined || visited.has(nextId)) continue;
    visited.add(nextId);
    const callee = byId.get(nextId);
    if (!callee) continue;
    includedPipelines.push(callee);
    scan(callee.steps);
  }

  return {
    version: 1,
    destinations: [],
    lookupTables,
    grokPatterns,
    schemas,
    pipelines: includedPipelines,
  };
}

/**
 * The step tree as it stood right before (`includeTarget: false`) or right
 * after (`true`) `target` ran; a per-step diff runs both through
 * `/api/preview` like a normal Run. `target` is the step the caller holds,
 * matched by identity, so two steps that share an id cannot be confused.
 *
 * Everything from `target`'s position on is dropped, including the other
 * branch of any ancestor `if`. An `if` target goes to the after side whole,
 * both branches with it, so the diff shows what its block did.
 *
 * `null` when `target` is not in `steps`: a stale selection, not an error.
 */
export function truncateSteps(steps: Step[], target: Step, includeTarget: boolean): Step[] | null {
  const result: Step[] = [];
  for (const step of steps) {
    if (step === target) {
      if (includeTarget) result.push(step);
      return result;
    }
    if (step.type === 'if') {
      const inThen = truncateSteps(step.then, target, includeTarget);
      if (inThen !== null) {
        result.push({ ...step, then: inThen, else: [] });
        return result;
      }
      const inElse = truncateSteps(step.else, target, includeTarget);
      if (inElse !== null) {
        result.push({ ...step, then: [], else: inElse });
        return result;
      }
    }
    result.push(step);
  }
  return null;
}

/** Every step in the order an author reads them: an `if` before its branches, `then` before `else`. */
export function flatSteps(steps: Step[]): Step[] {
  return steps.flatMap((s) =>
    s.type === 'if' ? [s, ...flatSteps(s.then), ...flatSteps(s.else)] : [s],
  );
}

/** The steps `truncateSteps` keeps, as `steps` holds them: those that have run
 * at that point. An ancestor `if` of `target` is among them. */
export function stepsRun(steps: Step[], target: Step, includeTarget: boolean): Set<Step> | null {
  const cut = truncateSteps(steps, target, includeTarget);
  if (cut === null) return null;
  const run = new Set<Step>();
  // Each list `truncateSteps` keeps is a prefix of the one it came from.
  const pair = (whole: Step[], kept: Step[]) =>
    kept.forEach((k, i) => {
      const s = whole[i];
      run.add(s);
      if (s.type === 'if' && k.type === 'if') {
        pair(s.then, k.then);
        pair(s.else, k.else);
      }
    });
  pair(steps, cut);
  return run;
}

/** The object a parser's `into` names, as its header and its Advanced show it: none for `$`, where every parser
 * writes unless told otherwise, nor for a blank box, which names nothing. */
export function shownInto(into: string): string | null {
  const name = into.trim();
  return name === '' || name === '$' ? null : name;
}

/** A step as the records name it: its number in the step list, its type and
 * what it does. */
export function stepName(step: Step, number: number): string {
  return `Step ${number} · ${STEP_LABELS[step.type]} · ${stepSummary(step)}`;
}

export function stepSummary(step: Step): string {
  switch (step.type) {
    case 'if':
      return `if ${step.condition || '…'}`;
    case 'comment':
      return step.text || '(empty comment)';
    case 'eval':
      return step.assignments.length
        ? step.assignments.map((a) => `${a.target || '…'} = ${a.expr || '…'}`).join(', ')
        : '(no assignments)';
    case 'code':
      return `${step.body.split('\n').filter((l) => l.trim()).length} line(s) of code`;
    case 'drop':
      return `drop where ${step.condition || '…'}`;
    case 'rename':
      return step.pairs.length
        ? step.pairs.map((p) => `${p.from || '…'} → ${p.to || '…'}`).join(', ')
        : '(no pairs)';
    case 'parser':
      return `${step.input || '_raw'} as ${PARSER_FORMAT_NAMES[step.kind.type]}${shownInto(step.into) ? ` into ${shownInto(step.into)}` : ''}${step.declared.length ? ` (${step.declared.length} declared)` : ''}`;
    case 'lookup':
      return `${step.input || '…'} → ${step.outputs.map((o) => o.field || '…').join(', ') || '(no outputs)'}`;
    case 'unroll':
      return `${step.field || '(no field)'}${step.keepParentFields ? ' (keeps parent fields)' : ''}`;
    case 'call':
      return step.pipeline || '(no pipeline chosen)';
  }
}

/** The same three states a record field or a grid row already carries
 * (`recordDiff.ts`), reused here at step granularity. */
export interface StepDiffRow {
  kind: 'added' | 'removed' | 'changed';
  summary: string;
}

/** Every top-level step that differs between `base` and `compare`, matched
 * by id. */
export function diffSteps(base: Step[], compare: Step[]): StepDiffRow[] {
  const byId = (steps: Step[]) => new Map(steps.map((s) => [s.id, s]));
  const baseById = byId(base);
  const compareById = byId(compare);
  const rows: StepDiffRow[] = [];
  for (const [id, step] of compareById) {
    const was = baseById.get(id);
    if (!was) rows.push({ kind: 'added', summary: stepSummary(step) });
    else if (JSON.stringify(stepToDto(step)) !== JSON.stringify(stepToDto(was)))
      rows.push({ kind: 'changed', summary: stepSummary(step) });
  }
  for (const [id, step] of baseById) {
    if (!compareById.has(id)) rows.push({ kind: 'removed', summary: stepSummary(step) });
  }
  return rows;
}

// -- JSON -> model ----------------------------------------------------------

class PipelineJsonError extends Error {}

function str(obj: Record<string, unknown>, key: string, where: string): string {
  const v = obj[key];
  if (typeof v !== 'string')
    throw new PipelineJsonError(`${where}: expected "${key}" to be a string`);
  return v;
}
function optStr(obj: Record<string, unknown>, key: string): string {
  const v = obj[key];
  return typeof v === 'string' ? v : '';
}
function bool(obj: Record<string, unknown>, key: string): boolean {
  const v = obj[key];
  return typeof v === 'boolean' ? v : false;
}
function arr(obj: Record<string, unknown>, key: string, where: string): unknown[] {
  const v = obj[key];
  if (!Array.isArray(v)) throw new PipelineJsonError(`${where}: expected "${key}" to be an array`);
  return v;
}
/** An array the wire format defaults to empty when absent. */
function optArr(obj: Record<string, unknown>, key: string): unknown[] {
  const v = obj[key];
  return Array.isArray(v) ? v : [];
}
function optStrArr(obj: Record<string, unknown>, key: string): string[] {
  const v = obj[key];
  return Array.isArray(v) ? v.filter((s): s is string => typeof s === 'string') : [];
}
function rec(v: unknown, where: string): Record<string, unknown> {
  if (typeof v !== 'object' || v === null || Array.isArray(v))
    throw new PipelineJsonError(`${where}: expected an object`);
  return v as Record<string, unknown>;
}
function oneOf<T extends string>(v: unknown, options: readonly T[], where: string): T {
  if (typeof v === 'string' && (options as readonly string[]).includes(v)) return v as T;
  throw new PipelineJsonError(
    `${where}: expected one of ${options.join(', ')}, found ${JSON.stringify(v)}`,
  );
}

function dtoToDataType(dto: unknown, where: string): DataTypeSpec {
  if (typeof dto === 'string') {
    if (dto === 'skip') return { kind: 'skip' };
    if (dto === 'datetime') return { kind: 'datetime', format: '' };
    if (!(NAMED_TYPES as readonly string[]).includes(dto)) {
      throw new PipelineJsonError(
        `${where}: unsupported type "${dto}" (the visual builder only supports ${NAMED_TYPES.join(', ')})`,
      );
    }
    return { kind: 'named', name: dto as NamedType };
  }
  const o = rec(dto, where);
  if (o.type === 'datetime') {
    if (typeof o.format !== 'string' || o.format === '') {
      throw new PipelineJsonError(
        `${where}: a datetime object carries the parseTime format its text arrives in; a bare "datetime" detects the shape`,
      );
    }
    return { kind: 'datetime', format: o.format };
  }
  if (
    o.type === 'array' &&
    typeof o.of === 'string' &&
    (NAMED_TYPES as readonly string[]).includes(o.of)
  ) {
    return { kind: 'array', of: o.of as NamedType };
  }
  if (o.type === 'array' && o.of && typeof o.of === 'object') {
    const inner = o.of as Record<string, unknown>;
    const fields =
      inner.type === 'struct' && structOpen(inner) === false ? namedFields(inner.fields) : null;
    if (fields) return { kind: 'arrayOfObjects', fields };
  }
  if (o.type === 'struct') {
    const fields = namedFields(o.fields);
    const open = structOpen(o);
    if (fields && open !== null) return { kind: 'object', fields, open };
  }
  throw new PipelineJsonError(
    `${where}: the visual builder can represent a named type, an array of one named type, an object whose fields all have named types, open or closed, or an array of closed objects whose fields all have named types — not an array of arrays, or an array or object inside an object`,
  );
}

/** Whether a struct is open, reading an absent `open` as closed as the loader
 * does; `null` when `open` is not a boolean. */
function structOpen(o: Record<string, unknown>): boolean | null {
  if (o.open === undefined) return false;
  return typeof o.open === 'boolean' ? o.open : null;
}

/** A struct's field list, or `null` when a field's type is not named and the
 * builder cannot show it. */
function namedFields(raw: unknown): ObjectField[] | null {
  if (!Array.isArray(raw)) return null;
  const fields: ObjectField[] = [];
  for (const r of raw) {
    const f = r as Record<string, unknown>;
    if (typeof f.name !== 'string' || typeof f.type !== 'string') return null;
    if (!(NAMED_TYPES as readonly string[]).includes(f.type)) return null;
    fields.push({ name: f.name, ty: f.type as NamedType });
  }
  return fields;
}

function dtoToParserKind(dto: unknown, where: string): ParserKind {
  const o = rec(dto, where);
  const kind = dtoToParserFormat(o, where);
  if (typeof o.splitDots === 'boolean') setSplitDots(kind, o.splitDots);
  return kind;
}

function dtoToParserFormat(o: Record<string, unknown>, where: string): ParserKind {
  const t = o.type;
  if (t === 'json') return { type: 'json' };
  if (t === 'csv') {
    return { type: 'csv', delimiter: str(o, 'delimiter', where) };
  }
  if (t === 'keyValue') {
    return {
      type: 'keyValue',
      pairSep: str(o, 'pairSep', where),
      kvSep: str(o, 'kvSep', where),
      quote: optStr(o, 'quote'),
      ...(o.nextKey === false && { nextKey: false }),
    };
  }
  if (t === 'grok') {
    return {
      type: 'grok',
      patterns: optStrArr(o, 'patterns'),
      customPatterns: Array.isArray(o.customPatterns)
        ? (o.customPatterns as { name: string; pattern: string }[])
        : [],
    };
  }
  throw new PipelineJsonError(`${where}: unknown parser kind "${String(t)}"`);
}

/** One `StepDto` (recursively, for `if`) to a `Step`. Throws
 * `PipelineJsonError` for anything the builder cannot represent; never
 * drops or guesses. */
function dtoToStep(dto: unknown, where: string): Step {
  const o = rec(dto, where);
  const t = o.type;
  const common = (id: string) => ({
    id,
    filter: optStr(o, 'filter'),
    disabled: bool(o, 'disabled'),
  });

  switch (t) {
    case 'if':
      return {
        type: 'if',
        id: str(o, 'id', where),
        condition: str(o, 'condition', where),
        then: optArr(o, 'then').map((s, i) => dtoToStep(s, `${where}.then[${i}]`)),
        else: optArr(o, 'else').map((s, i) => dtoToStep(s, `${where}.else[${i}]`)),
      };
    case 'comment':
      return { type: 'comment', id: str(o, 'id', where), text: optStr(o, 'text') };
    case 'eval': {
      const id = str(o, 'id', where);
      return {
        type: 'eval',
        ...common(id),
        assignments: arr(o, 'assignments', where).map((a) => {
          const ao = rec(a, `${where}.assignments`);
          return { target: str(ao, 'target', where), expr: str(ao, 'expr', where) };
        }),
      };
    }
    case 'code': {
      const id = str(o, 'id', where);
      return { type: 'code', ...common(id), body: optStr(o, 'body') };
    }
    case 'drop': {
      const id = str(o, 'id', where);
      return { type: 'drop', ...common(id), condition: str(o, 'condition', where) };
    }
    case 'rename': {
      const id = str(o, 'id', where);
      return {
        type: 'rename',
        ...common(id),
        pairs: arr(o, 'pairs', where).map((p) => {
          const po = rec(p, `${where}.pairs`);
          return { from: str(po, 'from', where), to: str(po, 'to', where) };
        }),
      };
    }
    case 'parser': {
      const id = str(o, 'id', where);
      return {
        type: 'parser',
        ...common(id),
        input: str(o, 'input', where),
        into: typeof o.into === 'string' ? o.into : '$',
        kind: dtoToParserKind(o.kind, `${where}.kind`),
        declared: optArr(o, 'declared').map((f) => {
          const fo = rec(f, `${where}.declared`);
          return {
            name: str(fo, 'name', where),
            ty: dtoToDataType(fo.type, `${where}.declared.${String(fo.name)}`),
          };
        }),
      };
    }
    case 'lookup': {
      const id = str(o, 'id', where);
      const outputs = arr(o, 'outputs', where).map((out) => {
        const oo = rec(out, `${where}.outputs`);
        const ty =
          oo.type === undefined ? STR_OUTPUT : dtoToDataType(oo.type, `${where}.outputs.type`);
        if (ty.kind !== 'named' && ty.kind !== 'datetime') {
          throw new PipelineJsonError(
            `${where}.outputs.type: a lookup output is one value, not an array or an object`,
          );
        }
        return {
          source: str(oo, 'source', where),
          field: str(oo, 'field', where),
          default: optStr(oo, 'default'),
          ty,
        };
      });
      return {
        type: 'lookup',
        ...common(id),
        input: str(o, 'input', where),
        table: str(o, 'table', where),
        matchKind: oneOf(o.matchKind, MATCH_KINDS, `${where}.matchKind`),
        multiplicity:
          typeof o.multiplicity === 'string'
            ? oneOf(o.multiplicity, MULTIPLICITIES, `${where}.multiplicity`)
            : 'best',
        keyColumn: optStr(o, 'keyColumn'),
        outputs,
      };
    }
    case 'unroll': {
      const id = str(o, 'id', where);
      return {
        type: 'unroll',
        ...common(id),
        field: str(o, 'field', where),
        keepParentFields: bool(o, 'keepParentFields'),
      };
    }
    case 'call': {
      const id = str(o, 'id', where);
      return { type: 'call', ...common(id), pipeline: str(o, 'pipeline', where) };
    }
    default:
      throw new PipelineJsonError(`${where}: unknown step type "${String(t)}"`);
  }
}

/** `StepDto[]` as parsed JSON (`PipelineDto.steps`) to `Step[]`. */
export function stepsFromDto(dtoSteps: unknown[]): Step[] {
  return dtoSteps.map((s, i) => dtoToStep(s, `steps[${i}]`));
}

/**
 * A hand-edited `PipelineDto` document (`{"id", "name", "steps"}`) to the
 * builder's model. `id` is ignored: the backend
 * resynthesizes it from `name` on save. Throws `PipelineJsonError` naming
 * what the builder could not represent.
 */
export function parsePipelineDtoJson(text: string): {
  pipelineName: string;
  steps: Step[];
  produces: string[];
} {
  let doc: unknown;
  try {
    doc = JSON.parse(text);
  } catch (e) {
    throw new PipelineJsonError(`invalid JSON: ${e instanceof Error ? e.message : String(e)}`);
  }
  const root = rec(doc, '(root)');
  return {
    pipelineName: str(root, 'name', '(root)'),
    steps: stepsFromDto(arr(root, 'steps', '(root)')),
    produces: 'schemas' in root ? arr(root, 'schemas', '(root)').map(String) : [],
  };
}

// -- compile-error attribution ---------------------------------------------

/** One entry of an `errors` array, from a preview or a refused save. `step`
 * is the id of the step that produced it, or `null` for an error raised
 * before any step was compiled. `function` is the compiler's own index,
 * kept because the compiler reports it, and not used to place anything.
 * `assignmentIndex` names the one eval assignment this came from, by its
 * position in the step's own assignment list -- `null` for every other kind
 * of error, including one raised elsewhere in an eval step (its filter,
 * say). By position rather than by the assignment's own target field: an
 * eval step clobbers-always, so two assignments can share a target, and a
 * name would then place an error naming it on every one of them. */
export interface PreviewError {
  step: string | null;
  function: number | null;
  assignmentIndex: number | null;
  /** Which of the assignment's two boxes, `target` or `expr`, where the
   * bundle's validation names one. A compile error names none: it is the
   * expression's. */
  field?: string | null;
  message: string;
}

/** Every step, by its own id, at any depth. */
function stepsById(steps: Step[]): Map<string, Step> {
  return new Map(flatSteps(steps).map((s) => [s.id, s]));
}

/** Errors grouped by the step they belong to, found by id, keeping each
 * error whole rather than only its message -- a step editor uses
 * `assignmentIndex` to put an eval assignment's own error on that row.
 *
 * Not by position: `config_file::load` inlines a `call`'s callee before it
 * numbers anything, so a call that expands to three steps takes three
 * indices there and one here, and everything after it would be placed one
 * or more steps off. An id names the same step on both sides, and an error
 * from an inlined callee names a step this pipeline does not have -- so it
 * is left unplaced rather than pinned on the wrong one. */
export function errorsByStep(steps: Step[], errors: PreviewError[]): Map<Step, PreviewError[]> {
  const byId = stepsById(steps);
  const result = new Map<Step, PreviewError[]>();
  for (const e of errors) {
    const step = e.step === null ? undefined : byId.get(e.step);
    if (!step) continue;
    const list = result.get(step);
    if (list) list.push(e);
    else result.set(step, [e]);
  }
  return result;
}

/** The steps `byStep` names, in `flatSteps`'s order rather than the order
 * errors happened to arrive in, so "the first one" means the first an author
 * would scroll past, not whichever the API answered first. */
export function orderedStepErrors(
  steps: Step[],
  byStep: Map<Step, PreviewError[]>,
): { step: Step; messages: string[] }[] {
  return flatSteps(steps).flatMap((step) => {
    const errs = byStep.get(step);
    return errs ? [{ step, messages: errs.map((e) => e.message) }] : [];
  });
}

export const STEP_LABELS: Record<StepType, string> = {
  parser: 'Parser',
  eval: 'Eval',
  code: 'Code',
  drop: 'Drop',
  rename: 'Rename',
  lookup: 'Lookup',
  unroll: 'Unroll',
  call: 'Call',
  if: 'If',
  comment: 'Comment',
};

/** What each step does, as the website's step reference says it. */
export const STEP_SUMMARIES: Record<StepType, string> = {
  parser: 'Reads a text field as JSON, CSV, Key/value or Regex/Grok into fields.',
  eval: 'Sets fields to the values of expressions.',
  code: 'Runs a program on each record.',
  drop: 'Removes each record where a condition is true.',
  rename: 'Moves fields to new names.',
  lookup: 'Adds fields from the rows of a lookup table that match a value.',
  unroll: 'Turns each item of an array field into its own record.',
  call: "Runs another pipeline's steps in place.",
  if: 'Sends each record through one of two lists of steps.',
  comment: 'Holds a note. Records pass it unchanged.',
};
