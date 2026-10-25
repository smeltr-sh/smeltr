import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  recordsFromText,
  recordLine,
  needsBranch,
  listTests,
  runTests,
  mintEnrollmentCode,
  uploadLookupFile,
  getConfig,
  putSources,
  overwriteBranch,
  bringCurrent,
  MAIN_BRANCH,
  setBranch,
  expandBatch,
} from './api';

// A fresh browser opens on main, where a write first asks for a branch; the writes here are an analyst's on one.
beforeEach(() => setBranch('work'));

describe('recordsFromText', () => {
  it('takes a minified JSON object as one record', () => {
    expect(recordsFromText('{"a":1,"b":2}')).toEqual([{ _raw: '{"a":1,"b":2}' }]);
  });

  it('takes a pretty-printed JSON object as one record, not one per line', () => {
    const text = '{\n  "a": 1,\n  "b": 2\n}';
    expect(recordsFromText(text)).toEqual([{ _raw: '{"a":1,"b":2}' }]);
  });

  it('takes an object with a `_raw` field as a whole record', () => {
    const text = '{"_raw":"a","__capture":{"group":"g"}}\nplain';
    expect(recordsFromText(text)).toEqual([
      { _raw: 'a', __capture: { group: 'g' } },
      { _raw: 'plain' },
    ]);
    expect(recordsFromText('[{"_raw":"a"},{"b":2}]')).toEqual([{ _raw: 'a' }, { _raw: '{"b":2}' }]);
  });

  it('takes a JSON array as one record per element', () => {
    const text = '[{"a":1},{"b":2},{"c":3}]';
    expect(recordsFromText(text)).toEqual([
      { _raw: '{"a":1}' },
      { _raw: '{"b":2}' },
      { _raw: '{"c":3}' },
    ]);
  });

  it('takes NDJSON/JSONL as one record per line', () => {
    const text = '{"a":1}\n{"b":2}\n{"c":3}';
    expect(recordsFromText(text)).toEqual([
      { _raw: '{"a":1}' },
      { _raw: '{"b":2}' },
      { _raw: '{"c":3}' },
    ]);
  });

  it('takes a non-JSON log file as one record per non-empty line', () => {
    const text = '2024-01-01 first line\n\n2024-01-02 second line\n';
    expect(recordsFromText(text)).toEqual([
      { _raw: '2024-01-01 first line' },
      { _raw: '2024-01-02 second line' },
    ]);
  });
});

describe('expandBatch', () => {
  it('puts a constant column back on every row', () => {
    const batch = expandBatch({
      columns: [
        { name: '_raw', type: 'string' },
        { name: '__source.type', type: 'string', value: 'kafka' },
        { name: '__source.offset', type: 'int' },
      ],
      rows: [
        { values: ['a', 1], worker: 'w-a' },
        { values: ['b', 2], worker: 'w-b' },
      ],
    });
    expect(batch.columns).toEqual([
      { name: '_raw', type: 'string' },
      { name: '__source.type', type: 'string' },
      { name: '__source.offset', type: 'int' },
    ]);
    expect(batch.rows).toEqual([
      { values: ['a', 'kafka', 1], failed: [], worker: 'w-a' },
      { values: ['b', 'kafka', 2], failed: [], worker: 'w-b' },
    ]);
  });

  it('keeps a constant null, which is a value', () => {
    const batch = expandBatch({
      columns: [
        { name: 'a', type: 'string' },
        { name: '__destination.placement', type: 'string', value: null },
      ],
      rows: [{ values: ['x'] }],
    });
    expect(batch.rows).toEqual([{ values: ['x', null], failed: [] }]);
  });

  it('moves a failed index past the constants before it', () => {
    const batch = expandBatch({
      columns: [
        { name: 'route', type: 'string', value: 'r1' },
        { name: 'n', type: 'int' },
        { name: 'host', type: 'string', value: 'h' },
        { name: 'm', type: 'int' },
      ],
      rows: [{ values: ['x1', 'y2'], failed: [0, 1] }],
    });
    expect(batch.rows[0]).toEqual({ values: ['r1', 'x1', 'h', 'y2'], failed: [1, 3] });
  });

  it('leaves a batch with no constants as it came', () => {
    const batch = expandBatch({
      columns: [
        { name: 'a', type: 'string' },
        { name: 'b', type: 'int' },
      ],
      rows: [{ values: ['x', 'oops'], failed: [1], worker: 'w-a' }],
    });
    expect(batch.rows).toEqual([{ values: ['x', 'oops'], failed: [1], worker: 'w-a' }]);
  });
});

describe('recordLine', () => {
  it('shows a byte record as its text', () => {
    expect(recordLine({ _raw: 'a line' })).toBe('a line');
  });

  it('shows any other record as compact JSON', () => {
    expect(recordLine({ id: 1, name: 'x' })).toBe('{"id":1,"name":"x"}');
    expect(recordLine({ _raw: 'a', id: 1 })).toBe('{"_raw":"a","id":1}');
    expect(recordLine({ _raw: 7 })).toBe('{"_raw":7}');
  });
});

describe('needsBranch', () => {
  it('stops a write aimed at main', () => {
    expect(needsBranch(MAIN_BRANCH, 'PUT')).toBe(true);
    expect(needsBranch(MAIN_BRANCH, 'DELETE')).toBe(true);
    expect(needsBranch(MAIN_BRANCH, 'POST')).toBe(true);
  });

  /** Reading main is how the fleet's own configuration is looked at. */
  it('lets a read on main through', () => {
    expect(needsBranch(MAIN_BRANCH, 'GET')).toBe(false);
    expect(needsBranch(MAIN_BRANCH, undefined)).toBe(false);
  });

  it('has nothing to ask about a write already on a branch', () => {
    expect(needsBranch('feature-x', 'PUT')).toBe(false);
    expect(needsBranch('work', 'DELETE')).toBe(false);
  });
});

// The query string a review's test calls actually send. `test_api::TestScopeQuery` reads `ownScopes`
// as a `bool`, and serde takes only `true` or `false` there -- `1` deserializes to an HTTP 400, which
// reaches the reviewer as no test results at all. Neither side's own tests can see the other's, so the
// literal is pinned here and in `test_api::the_query_a_review_sends_parses`.
describe('a review names the branch under review and asks for its own scopes', () => {
  const calls: string[] = [];

  afterEach(() => {
    calls.length = 0;
    vi.unstubAllGlobals();
  });

  function captureFetch() {
    vi.stubGlobal('fetch', (url: string) => {
      calls.push(url);
      return Promise.resolve(new Response('{"ok":true,"errors":[],"tests":[]}'));
    });
  }

  it('spells ownScopes true when listing', async () => {
    captureFetch();
    await listTests('al/vendor-returns');
    expect(calls).toEqual(['/api/tests?branch=al%2Fvendor-returns&ownScopes=true']);
  });

  it('spells ownScopes true when running', async () => {
    captureFetch();
    await runTests({ touched: true }, 'al/vendor-returns');
    expect(calls).toEqual(['/api/tests/run?branch=al%2Fvendor-returns&ownScopes=true']);
  });
});

describe('what a body is sent as', () => {
  let sent: RequestInit | undefined;

  afterEach(() => {
    sent = undefined;
    vi.unstubAllGlobals();
  });

  function captureFetch() {
    vi.stubGlobal('fetch', (_url: string, init: RequestInit) => {
      sent = init;
      return Promise.resolve(new Response('{"ok":true}'));
    });
  }

  it('names JSON on a body that is JSON, so the leader does not refuse it', async () => {
    captureFetch();
    await mintEnrollmentCode({
      groupId: 'ingest',
      kind: 'permanent',
      maxUses: 1,
      lifetimeHours: 24,
    });
    expect(new Headers(sent?.headers).get('content-type')).toBe('application/json');
  });

  // FormData carries a boundary the browser generates and puts in the header
  // itself. Naming a type here strips the boundary, and the leader's multipart
  // extractor rejects every upload -- silently, since nothing else changes.
  it('leaves FormData to name itself, boundary and all', async () => {
    captureFetch();
    await uploadLookupFile(new File(['a,b\n1,2\n'], 'rates.csv', { type: 'text/csv' }));
    expect(new Headers(sent?.headers).has('content-type')).toBe(false);
    expect(sent?.body).toBeInstanceOf(FormData);
  });
});

// The leader pushes over the host's branch only while the host is still where the person was shown,
// so the tip travels with the call -- and `null`, not an absent key, when the host had no such branch.
describe('overwriting the host', () => {
  const calls: { url: string; body: unknown }[] = [];

  afterEach(() => {
    calls.length = 0;
    vi.unstubAllGlobals();
  });

  function captureFetch() {
    vi.stubGlobal('fetch', (url: string, init: RequestInit) => {
      calls.push({ url, body: JSON.parse(String(init.body)) });
      return Promise.resolve(new Response('{"ok":true,"error":null}'));
    });
  }

  it('names the branch and the host tip it was shown', async () => {
    captureFetch();
    await overwriteBranch('al/vendor-returns', 'def5678', 'abc1234');
    expect(calls).toEqual([
      {
        url: '/api/branches/al%2Fvendor-returns/overwrite',
        body: { tip: 'def5678', hostTip: 'abc1234' },
      },
    ]);
  });

  it('sends null when the host has no such branch', async () => {
    captureFetch();
    await overwriteBranch('work', 'def5678', null);
    expect(calls).toEqual([
      { url: '/api/branches/work/overwrite', body: { tip: 'def5678', hostTip: null } },
    ]);
  });
});

// The base commit is what lets the leader refuse a save onto a branch that moved
// (user-stories.md, W9). It is the commit of the last adopting read, which is the
// write's own read only for as long as the editor that made the edit is still on
// screen. An edit that outlives its read has to carry its own, or the read that
// would notice the move is the read that adopts the new head and there is nothing
// left to refuse.
describe('the commit a write is judged against', () => {
  let sent: RequestInit | undefined;

  afterEach(() => {
    sent = undefined;
    vi.unstubAllGlobals();
  });

  /** Answers every call with `commit`, so a read moves the shared base to it. */
  function serve(commit: string) {
    vi.stubGlobal('fetch', (_url: string, init: RequestInit) => {
      sent = init;
      return Promise.resolve(new Response(JSON.stringify({ ok: true, errors: [], commit })));
    });
  }

  function baseSent(): string | null {
    return new Headers(sent?.headers).get('x-smeltr-base-commit');
  }

  it('is the last read, for a write whose own read it was', async () => {
    serve('c1');
    await getConfig();
    await putSources([]);
    expect(baseSent()).toBe('c1');
  });

  it('is the one an edit was made against, when that edit outlived its read', async () => {
    serve('c1');
    await getConfig();
    serve('c2');
    await getConfig();
    await putSources([], 'c1');
    expect(baseSent()).toBe('c1');
  });

  it('is absent when the caller says the edit is against no commit', async () => {
    serve('c1');
    await getConfig();
    await putSources([], null);
    expect(baseSent()).toBe(null);
  });

  /** `undefined` is "I did not say", not "against nothing". A page that never
   * held its edit passes nothing and still gets the shared base. */
  it('falls back to the last read when the caller passes nothing', async () => {
    serve('c1');
    await getConfig();
    await putSources([], undefined);
    expect(baseSent()).toBe('c1');
  });
});

// A choice is only good against the two tips the conflicts were found at, so
// the answer carries them back with a side for each conflict, in its order.
describe('bringCurrent', () => {
  afterEach(() => vi.unstubAllGlobals());

  function sentBody(): Promise<unknown> {
    return new Promise((resolve) => {
      vi.stubGlobal('fetch', (_url: string, init: RequestInit) => {
        resolve(JSON.parse(String(init.body)));
        return Promise.resolve(new Response(JSON.stringify({ ok: true, errors: [] })));
      });
    });
  }

  it('asks with no choices the first time', async () => {
    const body = sentBody();
    await bringCurrent('work');
    expect(await body).toEqual({});
  });

  it('answers each conflict with its side, against the tips it was shown', async () => {
    const body = sentBody();
    const conflicts = {
      main: 'm1',
      work: 'w1',
      conflicts: [
        { path: 'groups/west/sources.json', id: 'a', base: null, main: '{}', work: '{}' },
        { path: 'notes.txt', base: 'b', main: 'm', work: 'w' },
      ],
    };
    await bringCurrent('work', { conflicts, sides: ['work', 'main'] });
    expect(await body).toEqual({
      choices: {
        main: 'm1',
        work: 'w1',
        picks: [
          { path: 'groups/west/sources.json', id: 'a', side: 'work' },
          { path: 'notes.txt', side: 'main' },
        ],
      },
    });
  });
});
