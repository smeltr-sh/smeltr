// An SQL source's form: what Run's columns suggest, and what each mode reads. The query is the
// author's; smeltr only chooses how new rows are found and which columns become fields.

import type { SqlColumn, SqlField, SqlMode, SqlStatement } from './api';

export type ModeType = SqlMode['type'];

/** The four modes, cheapest first: each one down costs the database and the leader more. `level` is that cost,
 * 1 to 4. */
export const MODES: { type: ModeType; name: string; what: string; cost: string; level: number }[] =
  [
    {
      type: 'incrementing',
      name: 'By an incrementing column',
      what: 'Each row once, when it appears: an id or an insert time.',
      cost: 'Each poll reads only the new rows, through an index.',
      level: 1,
    },
    {
      type: 'updatedAt',
      name: 'By updated at',
      what: 'Each row when it appears and each time its updated at moves. A deleted row is not seen.',
      cost: 'Each poll reads only the changed rows, through an index.',
      level: 2,
    },
    {
      type: 'comparing',
      name: 'By comparing values',
      what: 'Each row when it appears or its values differ from the last poll.',
      cost: 'Each poll reads the whole result, and the leader keeps a hash per row.',
      level: 3,
    },
    {
      type: 'everyPoll',
      name: 'Every poll',
      what: 'The whole result, each poll.',
      cost: 'Each poll reads and sends the whole result.',
      level: 4,
    },
  ];

/** How long a time position waits for late commits when it names no wait (`io::sql_source`). */
export const DEFAULT_SETTLE_SECS = 30;

const isTime = (c: SqlColumn) => c.suggested === 'datetime';

/** Whether a column can order the result: an integer or a time. */
export const canOrder = (c: SqlColumn) => c.suggested === 'i64' || isTime(c);

const named = (c: SqlColumn, words: string[]) =>
  words.some((w) => c.name.toLowerCase().includes(w));

/** `columns` with `unique` kept only where it holds of the result. A column unique on its table repeats once the query
 * joins that table to another, so a result read from more than one table has no unique column. */
export function uniqueInResult(columns: SqlColumn[]): SqlColumn[] {
  const tables = new Set(columns.flatMap((c) => (c.table ? [c.table] : [])));
  return tables.size > 1 ? columns.map((c) => ({ ...c, unique: false })) : columns;
}

/** The column that tells apart rows sharing `position`'s value: none when it is unique alone,
 * else the first unique column, an integer before any other. */
export function tieBreak(columns: SqlColumn[], position: string): string | undefined {
  const at = columns.find((c) => c.name === position);
  if (!at || at.unique) return undefined;
  const unique = columns.filter((c) => c.unique && c.name !== position);
  return (unique.find((c) => c.suggested === 'i64') ?? unique[0])?.name;
}

/** The position for a column: it, and the column that tells rows apart where it repeats. */
export function positionFor(columns: SqlColumn[], column: string): string[] {
  const tie = tieBreak(columns, column);
  return tie ? [column, tie] : [column];
}

/** The mode a result points to. A time named for updates reads updates; a time named for inserts,
 * or a unique integer, reads each row once; a unique key alone compares; anything else reads every
 * poll. The author confirms it: smeltr cannot know that an id only grows. */
export function suggestMode(columns: SqlColumn[]): SqlMode {
  const updated = columns.find((c) => isTime(c) && named(c, ['updated', 'modified', 'changed']));
  if (updated) return { type: 'updatedAt', position: positionFor(columns, updated.name) };
  const inserted =
    columns.find((c) => isTime(c) && named(c, ['created', 'inserted', 'occurred'])) ??
    columns.find((c) => c.unique && c.suggested === 'i64');
  if (inserted) return { type: 'incrementing', position: positionFor(columns, inserted.name) };
  const key = columns.find((c) => c.unique);
  if (key) return { type: 'comparing', key: [key.name] };
  return { type: 'everyPoll' };
}

/** `mode` switched to `type`, keeping its column where the new mode takes one. */
export function switchMode(mode: SqlMode, type: ModeType, columns: SqlColumn[]): SqlMode {
  const first =
    mode.type === 'incrementing' || mode.type === 'updatedAt'
      ? mode.position[0]
      : mode.type === 'comparing'
        ? mode.key[0]
        : undefined;
  if (type === 'incrementing' || type === 'updatedAt') {
    const column = first ?? columns.find(canOrder)?.name;
    return { type, position: column ? positionFor(columns, column) : [] };
  }
  if (type === 'comparing') {
    const column = first ?? columns.find((c) => c.unique)?.name;
    return { type, key: column ? [column] : [] };
  }
  return { type };
}

/** The fields and mode after Run found `columns`. A source whose query still returns every field it declares keeps
 * them and its mode; a column new to the result stays off until its author turns it on. One that declares none, or a
 * field the query no longer returns, takes every column, keeping the type of each field still there, and the mode the
 * columns point to: its query has changed shape. */
export function afterRun(
  fields: SqlField[],
  mode: SqlMode,
  columns: SqlColumn[],
): { fields: SqlField[]; mode: SqlMode } {
  if (fields.length > 0 && fields.every((f) => columns.some((c) => c.name === f.name)))
    return { fields, mode };
  return {
    fields: columns.map((c) => ({
      name: c.name,
      type: fields.find((f) => f.name === c.name)?.type ?? c.suggested,
    })),
    mode: suggestMode(columns),
  };
}

/** What each column does in `mode`, as the columns table marks it. */
export function roleOf(mode: SqlMode, column: string): string | undefined {
  if (mode.type === 'incrementing' || mode.type === 'updatedAt') {
    const at = mode.position.indexOf(column);
    return at === 0 ? 'position' : at > 0 ? 'tie-break' : undefined;
  }
  if (mode.type === 'comparing' && mode.key.includes(column)) return 'key';
  return undefined;
}

/** What fills each placeholder of `statement`: the column's value in `last`, the last row sent, or the value the
 * source starts from. */
export function bindNotes(
  statement: SqlStatement,
  last?: Record<string, unknown>,
  start?: string,
  kind: SqlKind = 'postgres',
): string[] {
  return statement.binds.map((bind, i) => {
    // MySQL's placeholders are each a bare `?`, so a note counts them.
    const at = kind === 'mysql' ? `?${i + 1}` : `$${i + 1}`;
    if (bind === 'start') return `${at} = ${start ?? 'where the source starts'} (start from)`;
    if (last && bind.last in last) return `${at} = ${shown(last[bind.last])} (${bind.last})`;
    return `${at} = ${bind.last} in the last row sent`;
  });
}

const shown = (value: unknown) =>
  value === null || value === undefined
    ? 'null'
    : typeof value === 'string'
      ? value
      : JSON.stringify(value);

const SQL_KEYWORDS = new Set(
  (
    'select from where and or not as with join left right inner outer full cross on using group by ' +
    'order having limit offset union all distinct case when then else end in is null between like ' +
    'ilike exists interval asc desc true false lateral over partition filter cast top'
  ).split(' '),
);

const escape = (text: string) =>
  text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const SQL_TOKEN =
  /(--[^\n]*|\/\*[\s\S]*?(?:\*\/|$))|('(?:[^']|'')*'?)|("(?:[^"]|"")*"?)|(\$\d+)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_$]*)/g;

/** MySQL's: a `#` comment too, strings that escape with a backslash, either quote a string, a name quoted in
 * backticks, and `?` placeholders. */
const MYSQL_TOKEN =
  /((?:--(?=\s|$)|#)[^\n]*|\/\*[\s\S]*?(?:\*\/|$))|('(?:[^'\\]|''|\\.)*'?|"(?:[^"\\]|""|\\.)*"?)|(`(?:[^`]|``)*`?)|(\?)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_$]*)/g;

/** The databases an SQL source reads, by source type. */
export type SqlKind = 'postgres' | 'mysql';

/** SQL colored for the page, in the highlighter's token classes: keywords, strings, numbers, comments and
 * placeholders, as `kind` reads them. A quoted name stays plain. The result is HTML with the text escaped. */
export function highlightSql(text: string, kind: SqlKind = 'postgres'): string {
  let out = '';
  let from = 0;
  for (const m of text.matchAll(kind === 'mysql' ? MYSQL_TOKEN : SQL_TOKEN)) {
    const at = m.index ?? 0;
    out += escape(text.slice(from, at));
    const [token, comment, string, , placeholder, number, word] = m;
    const kind = comment
      ? 'comment'
      : string
        ? 'string'
        : placeholder
          ? 'literal'
          : number
            ? 'number'
            : word && SQL_KEYWORDS.has(word.toLowerCase())
              ? 'keyword'
              : undefined;
    out += kind ? `<span class="tok-${kind}">${escape(token)}</span>` : escape(token);
    from = at + token.length;
  }
  return out + escape(text.slice(from));
}
