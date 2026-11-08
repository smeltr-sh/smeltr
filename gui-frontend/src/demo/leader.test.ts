import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  DEMO_REFUSAL,
  DemoLeader,
  callersOf,
  keyOf,
  putBack,
  shifted,
  type Pool,
  type Recording,
} from './leader';

const RECORDED = 1_790_000_000_000;

describe('keyOf', () => {
  it('sorts the query, so one question asked two ways is one key', () => {
    expect(keyOf(new URL('http://x/api/areas/apps/stats?kind=source&branch=main'))).toBe(
      '/api/areas/apps/stats?branch=main&kind=source',
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

/** A recording and its parts: each capture, and any answer `parts` holds. */
type Recorded = Recording & { captures: Record<string, Pool>; parts: Record<string, unknown> };

/** A leader over `r` at `now`, reading its parts from `r`. */
function demoLeader(r: Recorded, now: () => number): DemoLeader {
  return new DemoLeader(
    r,
    'http://service',
    null,
    now,
    async (key) => r.captures[key] ?? r.parts[key],
  );
}

/** A recording of one group with one source, whose capture took four records from two workers. */
function recording(): Recorded {
  const row = (worker: string, raw: string) => ({ values: [raw], worker });
  return {
    recordedAt: RECORDED,
    answers: {
      '/api/areas/shared?branch=main': config({ config: { version: 1 } }),
      '/api/areas/network?branch=main': config({
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
    parts: {},
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
    const leader = demoLeader(recording(), () => RECORDED);
    const sources = [{ id: 'syslog', type: 'syslogReceiver', port: 5514 }];
    const saved = await leader.answer(
      'PUT',
      at('/api/areas/network/sources?branch=mine'),
      JSON.stringify(sources),
    );
    expect(asked).toEqual(['http://service/validate']);
    const commit = (saved.body as { commit: string }).commit;
    expect(commit).not.toBe(COMMIT);

    const mine = await leader.answer('GET', at('/api/areas/network?branch=mine'), null);
    expect(mine.body).toMatchObject({ commit, config: { sources } });
    const main = await leader.answer('GET', at('/api/areas/network?branch=main'), null);
    expect(main.body).toMatchObject({ commit: COMMIT, config: { sources: [{ id: 'fw' }] } });
    const branches = await leader.answer('GET', at('/api/branches'), null);
    expect((branches.body as { branches: unknown[] }).branches).toMatchObject([
      { name: 'mine', head: commit, commits: 1, touches: ['groups/network/sources.json'] },
    ]);
  });

  it('ends a capture once it holds what it asked for', async () => {
    let now = RECORDED;
    const leader = demoLeader(recording(), () => now);
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
          capture: { done: boolean; records: number; ended: [string, unknown][] };
        }
      ).capture;
    const records = async () =>
      (
        (await leader.answer('GET', at(`/api/captures/${session}/records`), null)).body as {
          batches: { rows: unknown[] }[];
        }
      ).batches;
    expect((await status()).done).toBe(false);
    now += 60_000;
    const done = await status();
    expect(done.done).toBe(true);
    expect(done.records).toBe(3);
    expect((await records())[0].rows).toHaveLength(3);
    expect(done.ended).toEqual([
      ['w1', 'matches'],
      ['w2', 'matches'],
    ]);
  });

  it('answers what the recording read on another branch for one it never saw', async () => {
    const r = recording();
    r.answers['/api/areas/network/tests?branch=work'] = config({ tests: [{ id: 't' }] });
    const leader = demoLeader(r, () => RECORDED);
    const tests = await leader.answer('GET', at('/api/areas/network/tests?branch=mine'), null);
    expect((tests.body as { tests: unknown[] }).tests).toEqual([{ id: 't' }]);
    const none = await leader.answer('GET', at('/api/areas/apps/tests?branch=mine'), null);
    expect((none.body as { tests: unknown[] }).tests).toEqual([]);
  });

  it('asks the service for the schema releases it carries', async () => {
    const asked: string[] = [];
    vi.stubGlobal('fetch', async (url: string) => {
      asked.push(url);
      return new Response(JSON.stringify({ ok: true, errors: [] }));
    });
    const leader = demoLeader(recording(), () => RECORDED);
    await leader.answer('GET', at('/api/preview/schema-standards'), null);
    expect(asked).toEqual(['http://service/schemas/standards']);
  });

  it("asks the service for an object's connections over the groups that run it", async () => {
    const asked: Record<string, unknown>[] = [];
    vi.stubGlobal('fetch', async (_url: string, init: RequestInit) => {
      asked.push(JSON.parse(init.body as string));
      return new Response(JSON.stringify({ ok: true, errors: [] }));
    });
    const leader = demoLeader(recording(), () => RECORDED);
    await leader.answer('GET', at('/api/areas/network/sources/fw/connections?branch=main'), null);
    await leader.answer('GET', at('/api/areas/shared/pipelines/p/connections?branch=main'), null);
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
          results: [{ id: 't', area: 'network' }],
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
    r.answers['/api/areas/network/tests/t?branch=main'] = config({ test });
    const leader = demoLeader(r, () => RECORDED);
    const ran = await leader.answer(
      'POST',
      at('/api/areas/network/test-runs?branch=main'),
      JSON.stringify({ ids: [] }),
    );
    expect(ran.body).toMatchObject({ ok: true, results: [{ id: 't', area: 'network' }] });
    expect(asked[0].url).toBe('http://service/tests');
    expect(asked[0].body).toMatchObject({ groupId: 'network', tests: [test], run: ['t'] });
    const listed = await leader.answer('GET', at('/api/areas/network/tests?branch=main'), null);
    expect(listed.body).toMatchObject({ tests: [{ area: 'network', id: 't' }] });
  });
  it('answers a read the recording keeps in a part, reading the part once', async () => {
    const r = recording();
    const key = '/api/flow?windowMinutes=10080';
    r.parts[key] = { status: 200, body: { ok: true, bucketMillis: 300000 } };
    let reads = 0;
    const leader = new DemoLeader(
      r,
      'http://service',
      null,
      () => RECORDED,
      async (k) => {
        reads++;
        return r.parts[k];
      },
    );
    for (let i = 0; i < 2; i++) {
      const flow = await leader.answer('GET', at(key), null);
      expect(flow).toEqual({ status: 200, body: { ok: true, bucketMillis: 300000 } });
    }
    expect(reads).toBe(1);
  });
  it("draws the map's measurements on a branch it never watched from main's, under the branch's commit", async () => {
    const r = recording();
    r.moving['/api/map/measurements?branch=main'] = [{ commit: COMMIT, nodes: [{ id: 'fw' }] }];
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const leader = demoLeader(r, () => RECORDED);
    await leader.answer('PUT', at('/api/areas/network/sources?branch=mine'), '[]');
    const m = await leader.answer('GET', at('/api/map/measurements?branch=mine'), null);
    const branches = await leader.answer('GET', at('/api/branches'), null);
    const head = (branches.body as { branches: { head: string }[] }).branches[0].head;
    expect(m.body).toEqual({ commit: head, nodes: [{ id: 'fw' }] });
  });
  it("answers a node's stats on a branch it never watched with main's, moving", async () => {
    const r = recording();
    const key = '/api/areas/network/stats?branch=main&id=fw&kind=source&windowMinutes=10';
    r.moving[key] = [{ rate: 1 }];
    const leader = demoLeader(r, () => RECORDED);
    const stats = await leader.answer('GET', at(key.replace('branch=main', 'branch=work')), null);
    expect(stats.body).toEqual({ rate: 1 });
  });

  it("answers a visitor's branch read at its own commit or at main's", async () => {
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const leader = demoLeader(recording(), () => RECORDED);
    const sources = [{ id: 'syslog', type: 'syslogReceiver', port: 5514 }];
    const saved = await leader.answer(
      'PUT',
      at('/api/areas/network/sources?branch=mine'),
      JSON.stringify(sources),
    );
    const commit = (saved.body as { commit: string }).commit;
    const now = await leader.answer('GET', at(`/api/areas/network?branch=mine&at=${commit}`), null);
    expect(now.body).toMatchObject({ config: { sources } });
    const base = await leader.answer(
      'GET',
      at(`/api/areas/network?branch=mine&at=${COMMIT}`),
      null,
    );
    expect(base.body).toMatchObject({ config: { sources: [{ id: 'fw' }] } });
  });

  it('answers a pipeline deleted on a branch as gone, not as main has it', async () => {
    const r = recording();
    r.answers['/api/areas/network?branch=main'] = config({
      config: { version: 1, pipelines: [{ id: 'p', steps: [] }] },
    });
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const leader = demoLeader(r, () => RECORDED);
    await leader.answer('DELETE', at('/api/areas/network/pipelines/p?branch=mine'), null);
    const gone = await leader.answer('GET', at('/api/areas/network/pipelines/p?branch=mine'), null);
    expect(gone.status).toBe(404);
    const kept = await leader.answer('GET', at('/api/areas/network/pipelines/p?branch=main'), null);
    expect(kept.status).toBe(200);
  });

  it('holds its asks while the service wakes, and sends them once it answers', async () => {
    vi.useFakeTimers();
    const asked: string[] = [];
    let up = false;
    vi.stubGlobal('fetch', async (url: string) => {
      asked.push(url);
      if (url.endsWith('/wake')) return new Response(null, { status: up ? 204 : 503 });
      return new Response(JSON.stringify({ ok: true, errors: [] }));
    });
    const leader = demoLeader(recording(), Date.now);
    const heard: boolean[] = [];
    leader.wake((waking) => heard.push(waking));
    const proposed = leader.answer('POST', at('/api/preview/onboard'), '{}');
    await vi.advanceTimersByTimeAsync(3_000);
    expect(asked).toEqual(['http://service/wake', 'http://service/wake']);
    up = true;
    await vi.advanceTimersByTimeAsync(3_000);
    expect((await proposed).status).toBe(200);
    expect(asked.at(-1)).toBe('http://service/onboard/propose');
    expect(heard).toEqual([true, false]);
    vi.useRealTimers();
  });

  it("says what the service said when it refuses, or that it can't be reached", async () => {
    const leader = demoLeader(recording(), () => RECORDED);
    const propose = () =>
      leader.answer('POST', at('/api/preview/onboard'), JSON.stringify({ records: [] }));
    vi.stubGlobal(
      'fetch',
      async () =>
        new Response(JSON.stringify({ ok: false, errors: [{ message: 'too big' }] }), {
          status: 413,
        }),
    );
    expect(await propose()).toMatchObject({
      status: 413,
      body: { ok: false, errors: [{ message: 'too big' }] },
    });
    vi.stubGlobal('fetch', async () => {
      throw new TypeError('Failed to fetch');
    });
    expect(await propose()).toMatchObject({
      status: 502,
      body: { ok: false, errors: [{ message: 'TypeError: Failed to fetch' }] },
    });
  });
  it('refuses a shared save that breaks a group running it, naming the group', async () => {
    vi.stubGlobal('fetch', async (_: string, init: RequestInit) => {
      const body = JSON.parse(init.body as string);
      const errors = body.groupId ? [{ message: 'unknown source "fw"' }] : [];
      return new Response(JSON.stringify({ ok: !errors.length, errors }));
    });
    const leader = demoLeader(recording(), () => RECORDED);
    const saved = await leader.answer('PUT', at('/api/areas/shared/sources?branch=mine'), '[]');
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
    const leader = demoLeader(r, () => now);
    const started = (await leader.answer(
      'POST',
      at('/api/captures'),
      JSON.stringify({ group: 'network', destination: 'es', maxMatches: 10 }),
    )) as { body: { session: string } };
    now += 60_000;
    await leader.answer('GET', at(`/api/captures/${started.body.session}`), null);
    const saved = await leader.answer(
      'POST',
      at('/api/areas/network/samples?branch=mine'),
      JSON.stringify({ capture: started.body.session, id: 'sent', name: 'sent' }),
    );
    expect(saved.body).toMatchObject({ ok: true, records: 2 });
    const sample = await leader.answer(
      'GET',
      at('/api/areas/network/samples/sent?branch=mine'),
      null,
    );
    expect(sample.body).toMatchObject({ sample: { records: [{ event: 'a' }, { event: 'b' }] } });
  });
  it('starts a branch at main, lists it before its first save, and deletes it', async () => {
    const leader = demoLeader(recording(), () => RECORDED);
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
    const leader = demoLeader(recording(), () => RECORDED);
    const edited = [{ id: 'fw', type: 'kafka', topics: ['other'] }];
    await leader.answer(
      'PUT',
      at('/api/areas/network/sources?branch=mine'),
      JSON.stringify(edited),
    );
    const undo = () =>
      leader.answer(
        'POST',
        at('/api/areas/network/undo?branch=mine'),
        JSON.stringify({ section: 'sources', id: 'fw' }),
      );
    expect((await undo()).body).toMatchObject({ ok: true });
    const mine = await leader.answer('GET', at('/api/areas/network?branch=mine'), null);
    expect(mine.body).toMatchObject({ config: { sources: [{ id: 'fw', topics: ['t'] }] } });
    expect((await undo()).body).toMatchObject({
      ok: false,
      errors: [{ message: expect.stringContaining('there is nothing to undo') }],
    });
  });
  it("plays a change's review as the leader does, saves and all, and stops short of deploying it", async () => {
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const r = recording();
    r.answers['/api/auth/session'] = {
      status: 200,
      body: { ok: true, errors: [], displayName: 'Demo User', userId: 'u_demo' },
    };
    const leader = demoLeader(r, () => RECORDED);
    const post = (path: string, body: object) =>
      leader.answer('POST', at(`/api/branches/mine/${path}`), JSON.stringify(body));
    const message = (a: { body: unknown }) =>
      (a.body as { errors: { message: string }[] }).errors[0]?.message;

    await leader.answer('POST', at('/api/branches'), JSON.stringify({ name: 'mine' }));
    expect(message(await post('open', { title: 'Fix' }))).toBe(
      '"mine" changes nothing yet -- save something on it first',
    );
    const sources = [{ id: 'fw', type: 'kafka', topics: ['other'] }];
    await leader.answer(
      'PUT',
      at('/api/areas/network/sources?branch=mine'),
      JSON.stringify(sources),
    );
    expect(message(await post('open', { title: ' ' }))).toBe(
      'a change needs a title: it is what the deploy commit is called',
    );
    expect((await post('open', { title: 'Fix', description: 'Why' })).body).toMatchObject({
      ok: true,
    });
    expect(message(await post('open', { title: 'Fix' }))).toBe('"mine" is already open for review');
    const listed = await leader.answer('GET', at('/api/branches'), null);
    expect((listed.body as { branches: unknown[] }).branches).toMatchObject([
      {
        name: 'mine',
        change: { state: 'open', title: 'Fix', author: 'Demo User', authorId: 'u_demo' },
      },
    ]);

    const renamed = await leader.answer(
      'PATCH',
      at('/api/branches/mine'),
      JSON.stringify({ title: 'Fix the feed', description: 'Why' }),
    );
    expect(renamed.body).toMatchObject({ ok: true });
    const anchor = { kind: 'object', area: 'network', section: 'sources', objectId: 'fw' };
    await post('comments', { body: 'Is this topic right?', anchor });
    const timeline = async () =>
      (
        (await leader.answer('GET', at('/api/branches/mine/timeline'), null)).body as {
          events: { kind: string; id?: string; anchor?: object; at?: string }[];
        }
      ).events;
    const root = (await timeline()).at(-1)!;
    expect(root).toMatchObject({ kind: 'comment', anchor, at: expect.any(String) });
    await post('comments', { body: 'It is.', replyTo: root.id });
    expect((await timeline()).at(-1)).toMatchObject({ kind: 'comment', replyTo: root.id, anchor });
    const thread = (resolved: boolean) =>
      leader.answer(
        'POST',
        at(`/api/branches/mine/threads/${root.id}`),
        JSON.stringify({ resolved }),
      );
    expect((await thread(true)).body).toMatchObject({ ok: true });
    expect(message(await thread(true))).toBe('this thread is already resolved');

    // Whoever opened it may approve it: one administrator works alone.
    expect((await post('reviews', { verdict: 'approve', body: '' })).body).toMatchObject({
      ok: true,
    });
    expect(message(await post('deploy', {}))).toBe(DEMO_REFUSAL);

    await post('close', { reason: '' });
    expect(message(await post('close', {}))).toBe('"mine" is not open for review');
    await post('open', { title: 'Fix the feed' });
    expect((await timeline()).map((e) => e.kind)).toEqual([
      'saved',
      'opened',
      'renamed',
      'comment',
      'comment',
      'resolved',
      'approved',
      'closed',
      'reopened',
    ]);
  });

  it('lets the visitor review a change the fleet was recorded with, from what the recording read of it', async () => {
    const r = recording();
    r.answers['/api/auth/session'] = {
      status: 200,
      body: { displayName: 'Demo User', userId: 'u_demo' },
    };
    const change = {
      state: 'open',
      title: 'Theirs',
      description: '',
      opened: RECORDED / 1000,
      updated: RECORDED / 1000,
      reason: '',
      author: 'Priya',
      authorId: 'u_priya',
    };
    r.answers['/api/branches'] = {
      status: 200,
      body: {
        branches: [
          {
            name: 'theirs',
            head: COMMIT,
            base: COMMIT,
            commits: 1,
            lastSave: RECORDED / 1000,
            touches: [],
            change,
          },
        ],
      },
    };
    r.answers['/api/branches/theirs/timeline'] = {
      status: 200,
      body: {
        events: [
          {
            kind: 'saved',
            time: RECORDED / 1000,
            author: 'Priya',
            text: 'edit routes',
            commit: COMMIT,
          },
        ],
      },
    };
    const leader = demoLeader(r, () => RECORDED);
    const approved = await leader.answer(
      'POST',
      at('/api/branches/theirs/reviews'),
      JSON.stringify({ verdict: 'approve', body: 'Looks right' }),
    );
    expect(approved.body).toMatchObject({ ok: true });
    const events = (await leader.answer('GET', at('/api/branches/theirs/timeline'), null)).body as {
      events: object[];
    };
    expect(events.events).toMatchObject([
      { kind: 'saved', text: 'edit routes' },
      { kind: 'approved', author: 'Demo User', text: 'Looks right' },
    ]);
  });

  it("asks the service for a branch's diff against main, every scope with its samples and tests", async () => {
    const asked: { url: string; body: { base: object; branch: object; path?: string } }[] = [];
    vi.stubGlobal('fetch', async (url: string, init: RequestInit) => {
      asked.push({ url, body: JSON.parse(String(init.body)) });
      return new Response(JSON.stringify({ ok: true, errors: [], files: [], diff: '' }));
    });
    const leader = demoLeader(recording(), () => RECORDED);
    const sources = [{ id: 'fw', type: 'kafka', topics: ['other'] }];
    await leader.answer(
      'PUT',
      at('/api/areas/network/sources?branch=mine'),
      JSON.stringify(sources),
    );
    asked.length = 0;
    await leader.answer(
      'GET',
      at('/api/branches/mine/diff?path=groups/network/sources.json'),
      null,
    );
    expect(asked).toMatchObject([
      {
        url: 'http://service/diff',
        body: {
          base: { network: { config: { sources: [{ topics: ['t'] }] }, samples: [], tests: [] } },
          branch: { network: { config: { sources } } },
          path: 'groups/network/sources.json',
        },
      },
    ]);

    // What landed between main and the branch's head: every file, and the text of the one named.
    asked.length = 0;
    const head = (
      (await leader.answer('GET', at('/api/branches'), null)).body as {
        branches: { head: string }[];
      }
    ).branches[0].head;
    const q = new URLSearchParams({ from: COMMIT, to: head, path: 'groups/network/sources.json' });
    await leader.answer('GET', at(`/api/main/diff?${q}`), null);
    expect(asked.map((a) => a.body.path)).toEqual([undefined, 'groups/network/sources.json']);
  });

  it('diffs one save of several, as a review shows a save on its own', async () => {
    const asked: { base: object; branch: object }[] = [];
    vi.stubGlobal('fetch', async (_: string, init: RequestInit) => {
      asked.push(JSON.parse(String(init.body)));
      return new Response(JSON.stringify({ ok: true, errors: [], files: [], diff: '' }));
    });
    const leader = demoLeader(recording(), () => RECORDED);
    const put = (topic: string) =>
      leader.answer(
        'PUT',
        at('/api/areas/network/sources?branch=mine'),
        JSON.stringify([{ id: 'fw', type: 'kafka', topics: [topic] }]),
      );
    await put('first');
    await put('second');
    await leader.answer(
      'POST',
      at('/api/branches/mine/open'),
      JSON.stringify({ title: 'Two saves' }),
    );
    const timeline = (await leader.answer('GET', at('/api/branches/mine/timeline'), null)).body as {
      events: { kind: string; commit: string; parent: string }[];
    };
    const second = timeline.events.filter((e) => e.kind === 'saved')[1];
    asked.length = 0;
    const q = new URLSearchParams({ from: second.parent, to: second.commit });
    const diff = await leader.answer('GET', at(`/api/main/diff?${q}`), null);
    expect(diff.body).toMatchObject({ ok: true });
    expect(asked).toMatchObject([
      {
        base: { network: { config: { sources: [{ topics: ['first'] }] } } },
        branch: { network: { config: { sources: [{ topics: ['second'] }] } } },
      },
    ]);
  });

  it('keeps of each earlier save only the scope it changed, and rebuilds the rest', async () => {
    const asked: { base: Record<string, unknown>; branch: Record<string, unknown> }[] = [];
    vi.stubGlobal('fetch', async (_: string, init: RequestInit) => {
      asked.push(JSON.parse(String(init.body)));
      return new Response(JSON.stringify({ ok: true, errors: [], files: [], diff: '' }));
    });
    const kept = new Map<string, string>();
    const store = {
      getItem: (k: string) => kept.get(k) ?? null,
      setItem: (k: string, v: string) => void kept.set(k, v),
    } as Storage;
    const r = recording();
    const leader = new DemoLeader(r, 'http://service', store, () => RECORDED);
    const put = (topic: string) =>
      leader.answer(
        'PUT',
        at('/api/areas/network/sources?branch=mine'),
        JSON.stringify([{ id: 'fw', type: 'kafka', topics: [topic] }]),
      );
    await put('first');
    await leader.answer(
      'PUT',
      at('/api/areas/shared/sources?branch=mine'),
      JSON.stringify([{ id: 'all', type: 'kafka', topics: ['all'] }]),
    );
    await put('third');
    const saved = JSON.parse([...kept.values()].at(-1)!) as {
      branches: { mine: { past: { scope: string; bundle?: unknown }[] } };
    };
    expect(saved.branches.mine.past.map((e) => e.scope)).toEqual(['network', 'shared', 'network']);
    expect(Object.keys(saved.branches.mine.past[1])).not.toContain('bundles');

    await leader.answer('POST', at('/api/branches/mine/open'), JSON.stringify({ title: 'Three' }));
    const timeline = (await leader.answer('GET', at('/api/branches/mine/timeline'), null)).body as {
      events: { kind: string; commit: string; parent: string }[];
    };
    const second = timeline.events.filter((e) => e.kind === 'saved')[1];
    asked.length = 0;
    const q = new URLSearchParams({ from: second.parent, to: second.commit });
    await leader.answer('GET', at(`/api/main/diff?${q}`), null);
    // The shared save, between the network save before it and the one after.
    expect(asked).toMatchObject([
      {
        base: { network: { config: { sources: [{ topics: ['first'] }] } } },
        branch: {
          shared: { config: { sources: [{ id: 'all' }] } },
          network: { config: { sources: [{ topics: ['first'] }] } },
        },
      },
    ]);
    expect(
      (asked[0].base.shared as { config: { sources?: unknown } }).config.sources,
    ).toBeUndefined();
  });
  it('answers a sample read at a commit the tab holds, as a review reads both ends of a change', async () => {
    const r = recording();
    const sample = { id: 's1', name: 'one', records: [{ _raw: 'a' }] };
    r.answers['/api/areas/network/samples/s1?branch=main'] = config({ sample });
    const leader = demoLeader(r, () => RECORDED);
    const read = await leader.answer(
      'GET',
      at(`/api/areas/network/samples/s1?branch=mine&at=${COMMIT}`),
      null,
    );
    expect(read.body).toMatchObject({ ok: true, sample, commit: COMMIT });

    // A test is read by the file it is stored in, whose name is its id slugged.
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify({ ok: true, errors: [] })));
    const test = { name: 'Proto as text', sample: 's1', lockedAt: 0, rowsIn: 1, expected: [] };
    await leader.answer(
      'PUT',
      at(`/api/areas/network/tests/${encodeURIComponent('Proto as text')}?branch=mine`),
      JSON.stringify(test),
    );
    const head = (
      (await leader.answer('GET', at('/api/branches'), null)).body as {
        branches: { head: string }[];
      }
    ).branches[0].head;
    const locked = await leader.answer(
      'GET',
      at(`/api/areas/network/tests/proto-as-text?branch=mine&at=${head}`),
      null,
    );
    expect(locked.body).toMatchObject({ ok: true, test: { id: 'Proto as text' } });
  });
});
