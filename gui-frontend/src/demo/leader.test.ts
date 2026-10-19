import { afterEach, describe, expect, it, vi } from 'vitest';
import { DemoLeader, callersOf, keyOf, putBack, shifted, type Recording } from './leader';

const RECORDED = 1_790_000_000_000;

describe('keyOf', () => {
  it('sorts the query, so one question asked two ways is one key', () => {
    expect(keyOf(new URL('http://x/api/config?scope=apps&branch=main'))).toBe(
      '/api/config?branch=main&scope=apps',
    );
  });
});

describe('shifted', () => {
  it('moves milliseconds by size, and seconds and microseconds by their key', () => {
    const day = 86_400_000;
    expect(
      shifted(
        {
          lastSeenMillis: RECORDED,
          lastSave: RECORDED / 1000,
          lockedAt: RECORDED * 1000,
          rate: 29.3,
          offset: RECORDED / 1000,
        },
        day,
      ),
    ).toEqual({
      lastSeenMillis: RECORDED + day,
      lastSave: RECORDED / 1000 + 86_400,
      lockedAt: (RECORDED + day) * 1000,
      rate: 29.3,
      offset: RECORDED / 1000,
    });
  });

  it("moves an answer's buckets by whole buckets of the width it names", () => {
    const flow = {
      bucketMillis: 10_000,
      nowMillis: RECORDED,
      rowsIn: [{ points: [{ bucketMillis: RECORDED, value: 3 }] }],
    };
    expect(shifted(flow, 12_345)).toEqual({
      bucketMillis: 10_000,
      nowMillis: RECORDED + 10_000,
      rowsIn: [{ points: [{ bucketMillis: RECORDED + 10_000, value: 3 }] }],
    });
  });
});

describe('callersOf', () => {
  it("reads calls inside an if step's branches", () => {
    expect(
      callersOf([
        { id: 'a', steps: [{ type: 'call', pipeline: 'common' }] },
        {
          id: 'b',
          steps: [{ type: 'if', then: [], else: [{ type: 'call', pipeline: 'common' }] }],
        },
      ]),
    ).toEqual({ common: ['a', 'b'] });
  });
});

describe('putBack', () => {
  it('puts an object back after the one it followed, or removes one main never had', () => {
    const was = [{ id: 'a' }, { id: 'b' }, { id: 'c' }];
    expect(putBack(was, [{ id: 'c' }, { id: 'a' }], 'b')?.objects).toEqual([
      { id: 'c' },
      { id: 'a' },
      { id: 'b' },
    ]);
    expect(putBack(was, [{ id: 'a' }, { id: 'new' }], 'new')?.objects).toEqual([{ id: 'a' }]);
    expect(putBack(was, was, 'a')?.same).toBe(true);
    expect(putBack(was, was, 'nowhere')).toBeUndefined();
  });
});

const COMMIT = 'f'.repeat(40);
const config = (body: object) => ({
  status: 200,
  body: { ok: true, errors: [], ...body, commit: COMMIT },
});

/** A recording of one group with one source, whose capture took four records from two workers. */
function recording(): Recording {
  const row = (worker: string, raw: string) => ({ values: [raw], worker });
  return {
    recordedAt: RECORDED,
    answers: {
      '/api/config?branch=main&scope=shared': config({ config: { version: 1 } }),
      '/api/config?branch=main&scope=network': config({
        config: { version: 1, sources: [{ id: 'fw', type: 'kafka', topics: ['t'] }] },
      }),
      '/api/worker-groups?branch=main': config({ groups: [{ id: 'network' }] }),
    },
    moving: {},
    movingAt: [RECORDED],
    captures: {
      'network/source/fw': {
        batches: [
          {
            columns: [{ name: '_raw', type: 'str' }],
            rows: [row('w1', 'a'), row('w2', 'b'), row('w1', 'c'), row('w2', 'd')],
          },
        ],
        sample: {
          columns: [{ name: '_raw', type: 'str' }],
          records: [{ _raw: 'a' }, { _raw: 'b' }, { _raw: 'c' }, { _raw: 'd' }],
        },
      },
    },
  };
}

describe('DemoLeader', () => {
  const at = (path: string) => new URL(path, 'http://demo');

  afterEach(() => vi.unstubAllGlobals());

  it('forks a branch from main on its first write, and leaves main as it was', async () => {
    const asked: string[] = [];
    vi.stubGlobal('fetch', async (url: string) => {
      asked.push(url);
      return new Response(JSON.stringify({ ok: true, errors: [] }));
    });
    const leader = new DemoLeader(recording(), 'http://service', null, () => RECORDED);
    const sources = [{ id: 'syslog', type: 'syslogReceiver', port: 5514 }];
    const saved = await leader.answer(
      'PUT',
      at('/api/sources?branch=mine&scope=network'),
      JSON.stringify(sources),
    );
    expect(asked).toEqual(['http://service/validate']);
    const commit = (saved.body as { commit: string }).commit;
    expect(commit).not.toBe(COMMIT);

    const mine = await leader.answer('GET', at('/api/config?branch=mine&scope=network'), null);
    expect(mine.body).toMatchObject({ commit, config: { sources } });
    const main = await leader.answer('GET', at('/api/config?branch=main&scope=network'), null);
    expect(main.body).toMatchObject({ commit: COMMIT, config: { sources: [{ id: 'fw' }] } });
    const branches = await leader.answer('GET', at('/api/branches'), null);
    expect((branches.body as { branches: unknown[] }).branches).toMatchObject([
      { name: 'mine', head: commit, commits: 1, touches: ['groups/network/sources.json'] },
    ]);
  });

  it('ends a capture once it holds what it asked for', async () => {
    let now = RECORDED;
    const leader = new DemoLeader(recording(), 'http://service', null, () => now);
    const started = (await leader.answer(
      'POST',
      at('/api/captures'),
      JSON.stringify({
        group: 'network',
        source: { id: 'fw', type: 'kafka', topics: ['t'] },
        maxMatches: 3,
      }),
    )) as { body: { session: string; maxMatches: number } };
    expect(started.body.maxMatches).toBe(3);
    const session = started.body.session;

    // Records arrive over the pool's span; the capture ends once it holds what it asked for.
    const status = async () =>
      (
        (await leader.answer('GET', at(`/api/captures/${session}`), null)).body as {
          capture: { done: boolean; batches: { rows: unknown[] }[]; ended: [string, unknown][] };
        }
      ).capture;
    expect((await status()).done).toBe(false);
    now += 60_000;
    const done = await status();
    expect(done.done).toBe(true);
    expect(done.batches[0].rows).toHaveLength(3);
    expect(done.ended).toEqual([
      ['w1', 'matches'],
      ['w2', 'matches'],
    ]);
  });

  it('answers what the recording read on another branch for one it never saw', async () => {
    const r = recording();
    r.answers['/api/tests?branch=work&scope=network'] = config({ tests: [{ id: 't' }] });
    const leader = new DemoLeader(r, 'http://service', null, () => RECORDED);
    const tests = await leader.answer('GET', at('/api/tests?branch=mine&scope=network'), null);
    expect((tests.body as { tests: unknown[] }).tests).toEqual([{ id: 't' }]);
    const none = await leader.answer('GET', at('/api/tests?branch=mine&scope=apps'), null);
    expect((none.body as { tests: unknown[] }).tests).toEqual([]);
  });

  it("asks the service for an object's connections over the groups that run it", async () => {
    const asked: Record<string, unknown>[] = [];
    vi.stubGlobal('fetch', async (_url: string, init: RequestInit) => {
      asked.push(JSON.parse(init.body as string));
      return new Response(JSON.stringify({ ok: true, errors: [] }));
    });
    const leader = new DemoLeader(recording(), 'http://service', null, () => RECORDED);
    await leader.answer(
      'GET',
      at('/api/connections?branch=main&scope=network&kind=source&id=fw'),
      null,
    );
    await leader.answer(
      'GET',
      at('/api/connections?branch=main&scope=shared&kind=pipeline&id=p'),
      null,
    );
    expect(asked[0]).toMatchObject({ kind: 'source', id: 'fw', groups: [{ id: 'network' }] });
    expect(asked[1]).toMatchObject({ kind: 'pipeline', groups: [{ id: 'network' }] });
  });

  it("runs a scope's tests in the service with its bundle, samples and tests", async () => {
    const asked: { url: string; body: Record<string, unknown> }[] = [];
    vi.stubGlobal('fetch', async (url: string, init: RequestInit) => {
      asked.push({ url, body: JSON.parse(init.body as string) });
      return new Response(
        JSON.stringify({
          ok: true,
          errors: [],
          tests: [{ id: 't' }],
          results: [{ id: 't', scope: 'network' }],
        }),
      );
    });
    const r = recording();
    const test = {
      id: 't',
      name: 't',
      pipeline: 'p',
      sample: 's',
      lockedAt: 0,
      rowsIn: 1,
      expected: [],
    };
    r.answers['/api/tests/t?branch=main&scope=network'] = config({ test });
    const leader = new DemoLeader(r, 'http://service', null, () => RECORDED);
    const ran = await leader.answer(
      'POST',
      at('/api/tests/run?branch=main&scope=network'),
      JSON.stringify({ ids: [] }),
    );
    expect(ran.body).toMatchObject({ ok: true, results: [{ id: 't', scope: 'network' }] });
    expect(asked[0].url).toBe('http://service/tests');
    expect(asked[0].body).toMatchObject({ groupId: 'network', tests: [test], run: ['t'] });
    const listed = await leader.answer('GET', at('/api/tests?branch=main&scope=network'), null);
    expect(listed.body).toMatchObject({ tests: [{ scope: 'network', id: 't' }] });
  });
  it("draws the map's measurements on a branch it never watched from main's, under the branch's commit", async () => {
    const r = recording();
    r.moving['/api/map/measurements?branch=main'] = [{ commit: COMMIT, nodes: [{ id: 'fw' }] }];
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const leader = new DemoLeader(r, 'http://service', null, () => RECORDED);
    await leader.answer('PUT', at('/api/sources?branch=mine&scope=network'), '[]');
    const m = await leader.answer('GET', at('/api/map/measurements?branch=mine'), null);
    const branches = await leader.answer('GET', at('/api/branches'), null);
    const head = (branches.body as { branches: { head: string }[] }).branches[0].head;
    expect(m.body).toEqual({ commit: head, nodes: [{ id: 'fw' }] });
  });

  it("answers a visitor's branch read at its own commit or at main's", async () => {
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const leader = new DemoLeader(recording(), 'http://service', null, () => RECORDED);
    const sources = [{ id: 'syslog', type: 'syslogReceiver', port: 5514 }];
    const saved = await leader.answer(
      'PUT',
      at('/api/sources?branch=mine&scope=network'),
      JSON.stringify(sources),
    );
    const commit = (saved.body as { commit: string }).commit;
    const now = await leader.answer(
      'GET',
      at(`/api/config?branch=mine&scope=network&at=${commit}`),
      null,
    );
    expect(now.body).toMatchObject({ config: { sources } });
    const base = await leader.answer(
      'GET',
      at(`/api/config?branch=mine&scope=network&at=${COMMIT}`),
      null,
    );
    expect(base.body).toMatchObject({ config: { sources: [{ id: 'fw' }] } });
  });

  it('answers a pipeline deleted on a branch as gone, not as main has it', async () => {
    const r = recording();
    r.answers['/api/config?branch=main&scope=network'] = config({
      config: { version: 1, pipelines: [{ id: 'p', steps: [] }] },
    });
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const leader = new DemoLeader(r, 'http://service', null, () => RECORDED);
    await leader.answer('DELETE', at('/api/pipelines/p?branch=mine&scope=network'), null);
    const gone = await leader.answer('GET', at('/api/pipelines/p?branch=mine&scope=network'), null);
    expect(gone.status).toBe(404);
    const kept = await leader.answer('GET', at('/api/pipelines/p?branch=main&scope=network'), null);
    expect(kept.status).toBe(200);
  });

  it("says what the service said when it refuses, or that it can't be reached", async () => {
    const leader = new DemoLeader(recording(), 'http://service', null, () => RECORDED);
    const propose = () =>
      leader.answer('POST', at('/api/onboard/propose'), JSON.stringify({ records: [] }));
    vi.stubGlobal(
      'fetch',
      async () =>
        new Response(JSON.stringify({ ok: false, errors: [{ message: 'too big' }] }), {
          status: 413,
        }),
    );
    expect(await propose()).toMatchObject({ status: 413, body: { error: 'too big' } });
    vi.stubGlobal('fetch', async () => {
      throw new TypeError('Failed to fetch');
    });
    expect(await propose()).toMatchObject({
      status: 502,
      body: { error: 'TypeError: Failed to fetch' },
    });
  });
  it('refuses a shared save that breaks a group running it, naming the group', async () => {
    vi.stubGlobal('fetch', async (_: string, init: RequestInit) => {
      const body = JSON.parse(init.body as string);
      const errors = body.groupId ? [{ message: 'unknown source "fw"' }] : [];
      return new Response(JSON.stringify({ ok: !errors.length, errors }));
    });
    const leader = new DemoLeader(recording(), 'http://service', null, () => RECORDED);
    const saved = await leader.answer('PUT', at('/api/sources?branch=mine&scope=shared'), '[]');
    expect(saved.body).toMatchObject({
      ok: false,
      errors: [{ message: 'worker group "network": unknown source "fw"' }],
    });
  });

  it('saves a capture at a destination as a sample of what it was sent', async () => {
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const r = recording();
    r.captures['network/destination/es'] = {
      ...r.captures['network/source/fw'],
      sample: {
        columns: [{ name: 'event', type: 'str' }],
        records: [{ event: 'a' }, { event: 'b' }],
      },
    };
    r.captures['network/destination/es'].batches = [
      {
        columns: [{ name: 'event', type: 'str' }],
        rows: [
          { values: ['a'], worker: 'w1' },
          { values: ['b'], worker: 'w1' },
        ],
      },
    ];
    let now = RECORDED;
    const leader = new DemoLeader(r, 'http://service', null, () => now);
    const started = (await leader.answer(
      'POST',
      at('/api/captures'),
      JSON.stringify({ group: 'network', destination: 'es', maxMatches: 10 }),
    )) as { body: { session: string } };
    now += 60_000;
    await leader.answer('GET', at(`/api/captures/${started.body.session}`), null);
    const saved = await leader.answer(
      'POST',
      at(`/api/captures/${started.body.session}/save?branch=mine&scope=network`),
      JSON.stringify({ id: 'sent', name: 'sent' }),
    );
    expect(saved.body).toMatchObject({ ok: true, records: 2 });
    const sample = await leader.answer(
      'GET',
      at('/api/samples/sent?branch=mine&scope=network'),
      null,
    );
    expect(sample.body).toMatchObject({ sample: { records: [{ event: 'a' }, { event: 'b' }] } });
  });
  it('starts a branch at main, lists it before its first save, and deletes it', async () => {
    const leader = new DemoLeader(recording(), 'http://service', null, () => RECORDED);
    const started = await leader.answer(
      'POST',
      at('/api/branches'),
      JSON.stringify({ name: 'next' }),
    );
    expect(started.body).toMatchObject({ ok: true, head: COMMIT });
    const again = await leader.answer(
      'POST',
      at('/api/branches'),
      JSON.stringify({ name: 'next' }),
    );
    expect(again.body).toMatchObject({
      ok: false,
      errors: [{ message: 'branch "next" is already there' }],
    });
    const unsafe = await leader.answer('POST', at('/api/branches'), JSON.stringify({ name: '-x' }));
    expect(unsafe.body).toMatchObject({ ok: false });
    const listed = await leader.answer('GET', at('/api/branches'), null);
    expect((listed.body as { branches: unknown[] }).branches).toMatchObject([
      { name: 'next', commits: 0 },
    ]);

    await leader.answer('DELETE', at('/api/branches/next'), null);
    const after = await leader.answer('GET', at('/api/branches'), null);
    expect((after.body as { branches: unknown[] }).branches).toEqual([]);
    const main = await leader.answer('DELETE', at('/api/branches/main'), null);
    expect(main.body).toMatchObject({ ok: false });
  });

  it("undoes one object back to main's, as one save, and refuses one already as main has it", async () => {
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const leader = new DemoLeader(recording(), 'http://service', null, () => RECORDED);
    const edited = [{ id: 'fw', type: 'kafka', topics: ['other'] }];
    await leader.answer(
      'PUT',
      at('/api/sources?branch=mine&scope=network'),
      JSON.stringify(edited),
    );
    const undo = () =>
      leader.answer(
        'POST',
        at('/api/undo?branch=mine&scope=network'),
        JSON.stringify({ section: 'sources', id: 'fw' }),
      );
    expect((await undo()).body).toMatchObject({ ok: true });
    const mine = await leader.answer('GET', at('/api/config?branch=mine&scope=network'), null);
    expect(mine.body).toMatchObject({ config: { sources: [{ id: 'fw', topics: ['t'] }] } });
    expect((await undo()).body).toMatchObject({
      ok: false,
      errors: [{ message: expect.stringContaining('there is nothing to undo') }],
    });
  });
});
