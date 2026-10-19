import { describe, expect, it } from 'vitest';
import type { SqlColumn } from './api';
import {
  afterRun,
  bindNotes,
  highlightSql,
  positionFor,
  roleOf,
  suggestMode,
  switchMode,
  tieBreak,
  uniqueInResult,
} from './sql';

const col = (name: string, suggested: string, unique = false): SqlColumn => ({
  name,
  databaseType: suggested,
  suggested,
  unique,
});

const events = [
  col('event_id', 'i64', true),
  col('occurred_at', 'datetime'),
  col('user_name', 'str'),
];

describe('the suggested mode', () => {
  it('reads an insert time once, told apart by the unique id', () => {
    expect(suggestMode(events)).toEqual({
      type: 'incrementing',
      position: ['occurred_at', 'event_id'],
    });
  });

  it('reads updates by a time named for them', () => {
    expect(suggestMode([col('order_id', 'i64', true), col('updated_at', 'datetime')])).toEqual({
      type: 'updatedAt',
      position: ['updated_at', 'order_id'],
    });
  });

  it('takes a unique integer alone when there is no time', () => {
    expect(suggestMode([col('audit_id', 'i64', true), col('action', 'str')])).toEqual({
      type: 'incrementing',
      position: ['audit_id'],
    });
  });

  it('compares a table with a unique key and no column that grows', () => {
    expect(suggestMode([col('sku', 'str', true), col('price', 'str')])).toEqual({
      type: 'comparing',
      key: ['sku'],
    });
  });

  it('reads every poll when nothing is unique', () => {
    expect(suggestMode([col('queue', 'str'), col('waiting', 'i64')])).toEqual({
      type: 'everyPoll',
    });
  });
});

describe('a position', () => {
  it('needs no tie-break for a unique column', () => {
    expect(tieBreak(events, 'event_id')).toBeUndefined();
    expect(positionFor(events, 'event_id')).toEqual(['event_id']);
  });

  it('is told apart by a unique column where it repeats', () => {
    expect(tieBreak(events, 'occurred_at')).toBe('event_id');
  });
});

describe('a joined result', () => {
  const joined = [
    { ...col('order_id', 'i64', true), table: 'app.orders' },
    { ...col('line_id', 'i64', true), table: 'app.lines' },
    { ...col('placed_at', 'datetime'), table: 'app.orders' },
  ];

  it('has no unique column, since a unique one repeats across the join', () => {
    expect(uniqueInResult(joined).some((c) => c.unique)).toBe(false);
    expect(suggestMode(uniqueInResult(joined))).toEqual({ type: 'everyPoll' });
  });

  it('keeps uniqueness when every column is from one table', () => {
    const one = joined.filter((c) => c.table === 'app.orders');
    expect(uniqueInResult(one)).toBe(one);
  });
});

describe('switching mode', () => {
  it('keeps the column a position and a key both take', () => {
    const mode = switchMode({ type: 'everyPoll' }, 'comparing', events);
    expect(mode).toEqual({ type: 'comparing', key: ['event_id'] });
    expect(switchMode(mode, 'incrementing', events)).toEqual({
      type: 'incrementing',
      position: ['event_id'],
    });
  });

  it('marks each column with what it does', () => {
    const mode = { type: 'incrementing' as const, position: ['occurred_at', 'event_id'] };
    expect(roleOf(mode, 'occurred_at')).toBe('position');
    expect(roleOf(mode, 'event_id')).toBe('tie-break');
    expect(roleOf(mode, 'user_name')).toBeUndefined();
  });
});

describe('after Run', () => {
  const declared = [
    { name: 'event_id', type: 'i64' },
    { name: 'occurred_at', type: 'str' },
  ];
  const mode = { type: 'incrementing' as const, position: ['event_id'] };

  it('keeps the fields and mode of a query that still returns them, and leaves a new column off', () => {
    expect(afterRun(declared, mode, events)).toEqual({ fields: declared, mode });
  });

  it('starts over for a query that no longer returns a field, keeping the types of those still there', () => {
    const changed = [col('occurred_at', 'datetime'), col('device', 'str', true)];
    expect(afterRun(declared, mode, changed)).toEqual({
      fields: [
        { name: 'occurred_at', type: 'str' },
        { name: 'device', type: 'str' },
      ],
      mode: { type: 'incrementing', position: ['occurred_at', 'device'] },
    });
  });

  it('declares every column as its suggested type for a source that declares none', () => {
    expect(afterRun([], { type: 'everyPoll' }, events).fields).toEqual([
      { name: 'event_id', type: 'i64' },
      { name: 'occurred_at', type: 'datetime' },
      { name: 'user_name', type: 'str' },
    ]);
  });
});

describe('a placeholder', () => {
  const statement = { sql: '', binds: [{ last: 'occurred_at' }, 'start' as const] };

  it('is named by what fills it', () => {
    expect(bindNotes(statement)).toEqual([
      '$1 = occurred_at in the last row sent',
      '$2 = where the source starts (start from)',
    ]);
  });

  it('shows the value from the last row sent and where the source starts', () => {
    expect(bindNotes(statement, { occurred_at: '2026-10-10T02:31:08Z' }, 'now')).toEqual([
      '$1 = 2026-10-10T02:31:08Z (occurred_at)',
      '$2 = now (start from)',
    ]);
  });

  it('is counted in MySQL, whose placeholders are each a bare ?', () => {
    expect(bindNotes(statement, undefined, 'now', 'mysql')).toEqual([
      '?1 = occurred_at in the last row sent',
      '?2 = now (start from)',
    ]);
  });
});

describe('SQL coloring', () => {
  it('colors keywords in any case, and leaves names plain', () => {
    expect(highlightSql('select id FROM t')).toBe(
      '<span class="tok-keyword">select</span> id <span class="tok-keyword">FROM</span> t',
    );
  });

  it('keeps a keyword inside a string or a quoted name a string or a name', () => {
    expect(highlightSql(`'from' "select"`)).toBe(`<span class="tok-string">'from'</span> "select"`);
  });

  it('escapes the text and marks placeholders, numbers and comments', () => {
    expect(highlightSql('a < $1 -- <b>\nLIMIT 20')).toBe(
      'a &lt; <span class="tok-literal">$1</span> <span class="tok-comment">-- &lt;b&gt;</span>\n' +
        '<span class="tok-keyword">LIMIT</span> <span class="tok-number">20</span>',
    );
  });

  it("reads MySQL's backtick names, # comments, escaped strings and ? placeholders", () => {
    expect(highlightSql("`from` > ? # <b>\n'it\\'s' \"select\"", 'mysql')).toBe(
      '`from` &gt; <span class="tok-literal">?</span> <span class="tok-comment"># &lt;b&gt;</span>\n' +
        `<span class="tok-string">'it\\'s'</span> <span class="tok-string">"select"</span>`,
    );
  });
});
