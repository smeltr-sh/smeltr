// The kinds and categories an `exceptions` setting answers by, mirrored by
// hand from `src/core/error.rs` (`Category::ALL`, `ExceptionKind::KINDS`). A
// test there reads the `CATEGORIES` line and the `KINDS` lines below, so keep
// each one line, in the Rust order.

import type { DestinationDto } from './api';

// prettier-ignore
export const CATEGORIES = ['parse', 'math', 'limits', 'encoding', 'delivery'] as const;

export type Category = (typeof CATEGORIES)[number];

export const KINDS: readonly (readonly [string, Category])[] = [
  ['Unparsable', 'parse'],
  ['UnparsableField', 'parse'],
  ['CoercionRefused', 'parse'],
  ['Overflow', 'math'],
  ['DivideByZero', 'math'],
  ['InvalidUtf8', 'encoding'],
  ['GuessedEncoding', 'encoding'],
  ['LoopBudgetExceeded', 'limits'],
  ['KeyContradiction', 'parse'],
  ['IndexOutOfRange', 'limits'],
  ['NaN', 'math'],
  ['Unmatched', 'delivery'],
  ['Unprocessable', 'delivery'],
  ['Refused', 'delivery'],
];

/** What each kind marks, for its `?`, from its doc in `src/core/error.rs`. */
export const KIND_HELP: Record<string, string> = {
  Unparsable:
    'The parser got nothing from the row: malformed JSON, an unterminated CSV quote, a Grok pattern that matched nothing.',
  UnparsableField:
    'A declared field could not be read as its declared type, and is null. A field declared n: i64 receiving "abc".',
  CoercionRefused: 'A value could not be converted to the type a declaration gave it, and is null.',
  Overflow: 'Integer arithmetic ran past the range of i64. The result is null.',
  DivideByZero: 'A division by zero. The result is null, not Infinity.',
  InvalidUtf8:
    'Bytes that did not decode under the source’s encoding were replaced with U+FFFD. The rest of the text is kept.',
  GuessedEncoding:
    'The source’s auto encoding recognized neither UTF-8 nor UTF-16 and guessed. The text may well be right; a source that lands here often should declare its encoding.',
  LoopBudgetExceeded:
    'A while loop ran out of time with the row’s condition still true. Its accumulators are not what was asked for.',
  KeyContradiction:
    'Two keys of one record contradict after flattening, and one was kept: the same key twice, or a value at a name beside a path under that name.',
  IndexOutOfRange:
    'An array read or write, or a string read, named a position the value does not have. The read is null; the write does nothing.',
  NaN: 'A parse met NaN, Infinity or a number past the range of f64. That key has no value.',
  Unmatched: 'The record did not match a route and delivery condition to reach a destination.',
  Unprocessable:
    'No pipeline could finish the record: one crashed on its batch, or the record does not fit the memory budget alone.',
  Refused: 'A destination refused the record.',
};

/** What a row marked with an exception becomes. A deliver answer names destinations; each writes the row in its own
 * `format`. */
export type Answer = 'ignore' | 'tag' | 'drop' | 'block' | { deliver: string[] };

export type AnswerName = 'ignore' | 'tag' | 'drop' | 'deliver' | 'block';

/** Weakest first: a row marked several ways takes the strongest. */
export const ANSWERS: readonly AnswerName[] = ['ignore', 'tag', 'drop', 'deliver', 'block'];

export const nameOf = (a: Answer): AnswerName => (typeof a === 'string' ? a : 'deliver');

/** An answer in a few words: its name, or the destinations it delivers to. */
export const answerText = (a: Answer): string =>
  typeof a === 'string' ? a : `deliver to ${a.deliver.join(', ')}`;

/** An `exceptions` layer as the config writes it: `categories` answers each category it names, then `kinds` each
 * kind it names, over the layer beneath. `tagField` names the field a marked row's exceptions go in. */
export interface ExceptionsDto {
  categories?: Record<string, Answer>;
  kinds?: Record<string, Answer>;
  tagField?: string;
}

/** Each kind's answer under no setting at all: an encoding exception kept its value, so its row goes on as it is. A
 * delivery exception blocks. Every other is tagged. */
export const BUILT_IN: Record<string, Answer> = Object.fromEntries(
  KINDS.map(([kind, category]) => [
    kind,
    category === 'encoding' ? 'ignore' : category === 'delivery' ? 'block' : 'tag',
  ]),
);

/** `layer` over `beneath`, kind by kind, as the engine lays a route's layer over its group's. */
export function lay(
  beneath: Record<string, Answer>,
  layer: ExceptionsDto | undefined,
): Record<string, Answer> {
  const out = { ...beneath };
  for (const [kind, category] of KINDS) {
    const answer = layer?.kinds?.[kind] ?? layer?.categories?.[category];
    if (answer !== undefined) out[kind] = answer;
  }
  return out;
}

const KIND_NAMES = KINDS.map(([kind]) => kind);
export const isCategory = (name: string) => (CATEGORIES as readonly string[]).includes(name);

/** The kinds `name` answers for: a category's own, or the kind it is. */
export function kindsOf(name: string): string[] {
  return isCategory(name) ? KINDS.filter(([, c]) => c === name).map(([kind]) => kind) : [name];
}

const DELIVERY = kindsOf('delivery');

/** Whether `name`'s kinds can take `answer`: a delivery kind has no row to go on with, so it is never ignored or
 * tagged. The load refuses it. */
export const possible = (name: string, answer: AnswerName): boolean =>
  (answer !== 'ignore' && answer !== 'tag') || !kindsOf(name).every((k) => DELIVERY.includes(k));

/** The answer `dto` itself gives `name`, not one it inherits. */
export function ownAnswer(dto: ExceptionsDto | undefined, name: string): Answer | undefined {
  return isCategory(name) ? dto?.categories?.[name] : dto?.kinds?.[name];
}

/** `dto` with `name` answered by `answer`. A category's answer clears its kinds, so all of them take it. `undefined`
 * clears `name` alone. Names in `CATEGORIES` and `KINDS` order and no empty map, so one setting always writes the same
 * JSON: the pages compare it that way to know what is unsaved. */
export function set(
  dto: ExceptionsDto | undefined,
  name: string,
  answer: Answer | undefined,
): ExceptionsDto | undefined {
  const given = { ...dto?.categories, ...dto?.kinds } as Record<string, Answer | undefined>;
  if (answer !== undefined) for (const kind of kindsOf(name)) given[kind] = undefined;
  given[name] = answer;
  const inOrder = (names: readonly string[]) =>
    Object.fromEntries(names.flatMap((n) => (given[n] === undefined ? [] : [[n, given[n]]])));
  // In byte order, as the server writes them, so a setting read back compares equal to one edited back.
  const out: ExceptionsDto = {
    categories: inOrder([...CATEGORIES].sort()),
    kinds: inOrder([...KIND_NAMES].sort()),
  };
  if (dto?.tagField) out.tagField = dto.tagField;
  for (const key of ['categories', 'kinds'] as const)
    if (Object.keys(out[key] ?? {}).length === 0) delete out[key];
  return Object.keys(out).length === 0 ? undefined : out;
}

/** `dto` naming `field` as its tag field; an empty one names none. */
export function withTagField(dto: ExceptionsDto | undefined, field: string) {
  const { tagField: _, ...rest } = dto ?? {};
  const out = field.trim() === '' ? rest : { ...rest, tagField: field };
  return Object.keys(out).length === 0 ? undefined : out;
}

/** How many answers and names `dto` sets over the layer beneath it. */
export const overrideCount = (dto: ExceptionsDto | undefined): number =>
  Object.keys({ ...dto?.categories, ...dto?.kinds }).length + (dto?.tagField ? 1 : 0);

/** The group's layer with every category answered: one it leaves unset reads as its built-in answer. */
export const withCategories = (dto: ExceptionsDto | undefined): ExceptionsDto => ({
  ...dto,
  categories: Object.fromEntries(
    [...CATEGORIES].sort().map((c) => [c, dto?.categories?.[c] ?? BUILT_IN[kindsOf(c)[0]]]),
  ),
});

/** Where each destination sends what it refuses: its own layer over the group's. */
function refusedTo(destinations: DestinationDto[], group: ExceptionsDto | undefined) {
  const byGroup = lay(BUILT_IN, group);
  return new Map(
    destinations.map((d) => {
      const a = lay(byGroup, d.exceptions).Refused;
      return [d.id, typeof a === 'object' ? a : undefined];
    }),
  );
}

type Refusals = ReturnType<typeof refusedTo>;

/** Each destination `id` may hand a record to, `id` first: down its refusals, each once. */
function chain(id: string, next: Refusals): string[] {
  const seen = [id];
  for (let i = 0; i < seen.length; i++)
    for (const t of next.get(seen[i])?.deliver ?? []) if (!seen.includes(t)) seen.push(t);
  return seen;
}

/** What the load would refuse in these answers, each in a sentence: a delivery to nowhere, and refusals that lead back
 * to where they started. */
export function problems(
  group: ExceptionsDto | undefined,
  routes: { id: string; exceptions?: ExceptionsDto }[],
  destinations: DestinationDto[],
): string[] {
  const ids = new Set(destinations.map((d) => d.id));
  const out: string[] = [];
  const layers: [string, ExceptionsDto | undefined][] = [
    ['the group', group],
    ...routes.map((r): [string, ExceptionsDto | undefined] => [`route ${r.id}`, r.exceptions]),
    ...destinations.map((d): [string, ExceptionsDto | undefined] => [
      `destination ${d.id}`,
      d.exceptions,
    ]),
  ];
  for (const [who, layer] of layers)
    for (const [row, a] of Object.entries({ ...layer?.categories, ...layer?.kinds })) {
      if (typeof a !== 'object') continue;
      // A category every kind of which answers for itself sends nothing; the load checks each kind's own answer.
      if (isCategory(row) && kindsOf(row).every((k) => layer?.kinds?.[k] !== undefined)) continue;
      if (a.deliver.length === 0) out.push(`${who} delivers ${row} to no destination`);
      for (const t of a.deliver)
        if (!ids.has(t)) out.push(`${who} delivers ${row} to ${t}, which does not exist`);
    }
  const next = refusedTo(destinations, group);
  for (const d of destinations)
    if (chain(d.id, next).some((id) => next.get(id)?.deliver.includes(d.id)))
      out.push(`refusals at ${d.id} lead back to it`);
  return out;
}
