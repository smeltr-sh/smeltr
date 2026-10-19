import { describe, expect, it } from 'vitest';
import recorded from './recorded.json';
import { TASKS, sawStatus, type World } from './guide';

const answers = (recorded as { answers: Record<string, { body: unknown }> }).answers;
const network = (branch: string) =>
  (answers[`/api/config?branch=${branch}&scope=network`].body as { config: World['config'] })
    .config;

const fresh = (over: Partial<World> = {}): World => ({
  sawStatus: false,
  branch: 'main',
  config: network('main'),
  samples: [],
  tests: ['a'],
  mainTests: ['a'],
  mainPipelines: (network('main')?.pipelines ?? []).map((p) => p.id),
  ...over,
});

const done = (world: World) => TASKS.filter((t) => t.done(world)).map((t) => t.id);

describe('the guide', () => {
  it('ticks nothing on the recorded main', () => {
    expect(done(fresh())).toEqual([]);
  });

  it('ticks nothing but the branch on a fresh branch, whose bundle is main', () => {
    expect(done(fresh({ branch: 'fix' }))).toEqual(['branch']);
  });

  it('ticks pipeline on the recorded cisco-asa draft, which holds one, and capture and route as each is there', () => {
    const draft = network('cisco-asa-draft')!;
    const branch = 'cisco-asa-draft';
    expect(done(fresh({ branch, config: draft }))).toEqual(['branch', 'pipeline']);
    const samples = [{ source: 'cisco-asa' }];
    expect(done(fresh({ branch, config: draft, samples }))).toEqual([
      'branch',
      'capture',
      'pipeline',
    ]);
    const unpiped = { ...draft, routes: [...(draft.routes ?? []), { source: 'cisco-asa' }] };
    expect(done(fresh({ branch, config: unpiped, samples }))).not.toContain('route');
    const routed = {
      ...draft,
      routes: [...(draft.routes ?? []), { source: 'cisco-asa', pipeline: 'cisco-asa' }],
    };
    expect(done(fresh({ branch, config: routed, samples }))).toContain('route');
  });

  it("ticks no pipeline on main, whose pipelines are all main's", () => {
    expect(done(fresh({ branch: 'fix' }))).not.toContain('pipeline');
  });

  it('ticks proto when the FortiGate pipeline declares it as anything but i64', () => {
    const config = structuredClone(network('main'))!;
    const fortigate = config.pipelines!.find((p) => p.id === 'fortigate-traffic')!;
    const proto = fortigate
      .steps!.flatMap((s) => s.declared ?? [])
      .find((d) => d.name === 'proto')!;
    expect(proto.type).toBe('i64');
    proto.type = 'str';
    expect(done(fresh({ branch: 'fix', config }))).toContain('proto');
    expect(done(fresh({ config }))).not.toContain('proto');
  });

  it('ticks the test for a test main lacks', () => {
    expect(done(fresh({ branch: 'fix', tests: ['a', 'b'] }))).toContain('test');
  });
});

describe('sawStatus', () => {
  it('remembers that Status was opened', () => {
    const kept = new Map<string, string>();
    const store = {
      getItem: (k: string) => kept.get(k) ?? null,
      setItem: (k: string, v: string) => void kept.set(k, v),
    } as Storage;
    expect(sawStatus('#/map', store)).toBe(false);
    expect(sawStatus('#/status/issues', store)).toBe(true);
    expect(sawStatus('#/build/routes', store)).toBe(true);
  });
});
