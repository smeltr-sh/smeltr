// The leader the website demo's GUI talks to, played in the visitor's own browser.
//
// The GUI reaches its leader only through `fetch('/api/...')`, so the demo answers those calls here and the GUI runs
// unchanged. The visitor's branches, bundles, samples and captures live in this tab (`sessionStorage`, since the GUI
// reloads itself). What only the engine can answer -- a preview, a route preview, a save's validation, a grok
// pattern's captures, a line breaker's verdict, which records a capture filter keeps, a scope's tests, a branch's map
// and what a captured sample reads as -- goes to the demo's preview service (`src/bin/smeltr-demo`), with the bundle
// and sample it is about. Everything else is what the staged fleet answered when `just demo-fixtures` recorded it,
// moved to the visitor's now.

import type { ApiError, CaptureBatchDto, CaptureEndDto, CaptureStatusDto, SampleDto } from '../api';
import { CHANGED_EVENT } from './guide';
import { bareError } from '../errors.svelte';

/** What `tools/screenshots/demo.mjs` recorded. */
export interface Recording {
  /** What every recorded time is relative to. */
  recordedAt: number;
  /** The last answer to each GET, by `keyOf`. */
  answers: Record<string, { status: number; body: unknown }>;
  /** The live answers, read every five seconds, by `keyOf`. */
  moving: Record<string, unknown[]>;
  /** When each of `moving`'s reads was made: the times in a read are relative to it, not to `recordedAt`. */
  movingAt: number[];
  /** By `<group>/source/<id>` or `<group>/destination/<id>`: what main's capture of it took. A source's carries the
   * sample a save made of it too, record for row. */
  captures: Record<string, { batches: CaptureBatchDto[]; sample?: Pool }>;
}

type Pool = Required<Pick<SampleDto, 'columns' | 'records'>>;
type Bundle = Record<string, unknown> & { pipelines?: { id: string; steps?: unknown[] }[] };

const SHARED = 'shared';
/** How often the recording read the live answers. */
const MOVING_EVERY_MS = 5000;
/** How long an emulated capture takes to read its pool. */
const POOL_SPAN_MS = 8000;
const STORE = 'smeltr-demo';

/** `/api/x?b=2&a=1` as `/api/x?a=1&b=2`, the recording's key for it. */
export function keyOf(url: URL): string {
  const q = [...url.searchParams.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return url.pathname + (q.length ? '?' + new URLSearchParams(q) : '');
}

/** Times the recording holds, by key, that are not milliseconds. */
const SECONDS = new Set(['time', 'lastSave', 'opened', 'updated']);
const MICROS = new Set(['lockedAt']);

/** `value` with every time in it moved on by `deltaMs`: a millisecond count by its size, which nothing but a time
 * reaches, and seconds and microseconds by their key. An answer that names its buckets' width (`bucketMillis` below
 * a time's size) moves by a whole number of buckets, so its points stay on the grid its charts read them from. */
export function shifted(value: unknown, deltaMs: number, key?: string): unknown {
  if (Array.isArray(value)) return value.map((v) => shifted(v, deltaMs, key));
  if (value !== null && typeof value === 'object') {
    const width = (value as { bucketMillis?: unknown }).bucketMillis;
    const delta =
      typeof width === 'number' && width > 0 && width < 1.5e12
        ? Math.round(deltaMs / width) * width
        : deltaMs;
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, shifted(v, delta, k)]));
  }
  if (typeof value !== 'number' || !Number.isInteger(value)) return value;
  if (key !== undefined && SECONDS.has(key) && value > 1e9 && value < 1e10) {
    return value + Math.round(deltaMs / 1000);
  }
  if (key !== undefined && MICROS.has(key) && value > 1e15 && value < 1e16) {
    return value + deltaMs * 1000;
  }
  if (value > 1.5e12 && value < 2.5e12) return value + deltaMs;
  return value;
}

function newCommit(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(20));
  return [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

/** One branch as the visitor has it. */
interface Branch {
  commit: string;
  /** The commit of `main` it left at. */
  base: string;
  commits: number;
  /** Unix seconds. */
  lastSave: number;
  touches: string[];
  /** Started on purpose, so it is listed before its first save. */
  started?: boolean;
  /** By scope: `shared` or a group's id. */
  bundles: Record<string, Bundle>;
  samples: Record<string, Record<string, SampleDto>>;
  /** By scope, then id: each locked test as the leader stores it (`config_file::TestDto`). */
  tests: Record<string, Record<string, TestDto>>;
}

/** What a scope holds beside its bundle. */
interface Held {
  samples: Record<string, SampleDto>;
  tests: Record<string, TestDto>;
}

/** A locked test, as `config_file::TestDto` stores it. */
interface TestDto {
  id: string;
  name: string;
  pipeline?: string;
  sample: string;
  lockedAt: number;
  rowsIn: number;
  expected: unknown[];
}

interface Capture {
  group: string;
  /** Rows of the pool's batches, flattened in order, that the capture keeps. */
  kept: number[];
  pool: string | null;
  source: string | null;
  destination: string | null;
  filter: string | null;
  maxMatches: number;
  deadlineSecs: number;
  startedAt: number;
  stoppedAt: number | null;
}

interface State {
  branches: Record<string, Branch>;
  captures: Record<string, Capture>;
}

type Answer = { status: number; body: unknown };

const ok = (body: Record<string, unknown> = {}): Answer => ({
  status: 200,
  body: { ok: true, errors: [], ...body },
});
const refused = (errors: ApiError[], status = 200): Answer => ({
  status,
  body: { ok: false, errors },
});

/** How long the page holds its asks for a cold service, which Cloudflare can take minutes to boot, and how often it
 * knocks meanwhile. */
const WAKE_LIMIT_MS = 180_000;
const WAKE_RETRY_MS = 3_000;

/** Whether the service is still waking, for the demo's strip, and the event that says it changed. */
export const waking = { now: false };
export const WAKING_EVENT = 'smeltr-demo:waking';

/** The service failing to answer, as a refusal both of the GUI's readers show: one that reads `errors` and one that
 * reads `error` from a status that is not 200 (`api.ts`, `apiJson`). */
const unreachable = (message: string, status: number): Answer => ({
  status,
  body: { ok: false, errors: [bareError(message)], error: message },
});

/** The sections a bundle stores as one file each, by the path the GUI writes them at. */
const SECTIONS: Record<string, string> = {
  '/api/sources': 'sources',
  '/api/destinations': 'destinations',
  '/api/routes': 'routes',
  '/api/lookup-tables': 'lookupTables',
  '/api/grok-patterns': 'grokPatterns',
};

/** Each pipeline that calls another, by the one it calls (`config_file::callers_of`). */
export function callersOf(
  pipelines: { id: string; steps?: unknown[] }[],
): Record<string, string[]> {
  const callers: Record<string, string[]> = {};
  const walk = (steps: unknown[], caller: string) => {
    for (const step of steps as Record<string, unknown>[]) {
      if (step.type === 'call' && typeof step.pipeline === 'string') {
        (callers[step.pipeline] ??= []).push(caller);
      }
      if (step.type === 'if') {
        walk((step.then as unknown[]) ?? [], caller);
        walk((step.else as unknown[]) ?? [], caller);
      }
    }
  };
  for (const p of pipelines) walk(p.steps ?? [], p.id);
  return callers;
}

/** A section undo puts an object back in, as the editor names it, with the noun its messages call one by. */
const UNDONE: Record<string, string> = {
  sources: 'source',
  destinations: 'destination',
  routes: 'route',
  lookupTables: 'lookup table',
  grokPatterns: 'grok pattern',
  pipelines: 'pipeline',
};

/** `now` with the object `id` as `was` holds it, after the object it followed there, or removed when `was` has none,
 * as the leader's undo puts one back (`config_api::put_back`). `undefined` when neither holds it. */
export function putBack(
  was: { id: string }[],
  now: { id: string }[],
  id: string,
): { objects: { id: string }[]; same: boolean; name: string } | undefined {
  const atBase = was.findIndex((o) => o.id === id);
  const atTip = now.findIndex((o) => o.id === id);
  const shown = atBase >= 0 ? was[atBase] : atTip >= 0 ? now[atTip] : undefined;
  if (!shown) return undefined;
  const named = (shown as { name?: unknown }).name;
  const objects = now.filter((o) => o.id !== id);
  if (atBase >= 0) {
    // Order is meaning in some sections -- routes are tried in it -- so it goes back after the nearest object that
    // preceded it and is still there, or first when none is.
    const after = was
      .slice(0, atBase)
      .reverse()
      .map((o) => objects.findIndex((p) => p.id === o.id))
      .find((i) => i >= 0);
    objects.splice(after === undefined ? 0 : after + 1, 0, was[atBase]);
  }
  const same = JSON.stringify(objects) === JSON.stringify(now);
  return { objects, same, name: typeof named === 'string' ? named : id };
}

/** Why `name` is not one a branch may take, as the leader's `git::check_branch_name` refuses it, or `''`. */
function badBranchName(name: string): string {
  const fits =
    name.length > 0 &&
    name.length <= 100 &&
    !name.endsWith('.lock') &&
    name.split('/').every((part) => /^[A-Za-z0-9_][A-Za-z0-9_.-]*$/.test(part));
  return fits
    ? ''
    : `unsafe branch name ${JSON.stringify(name)}: only letters, digits, '-', '_', '.' and '/' as a separator are allowed, no part may start with '.' or '-' or be empty, and it must be 1-100 characters`;
}

export class DemoLeader {
  private state: State;
  private readonly started = Date.now();
  private readonly maps = new Map<string, Promise<Answer>>();

  /** Settles once the service answers. The asks wait on it, so a cold start holds them rather than fails them. */
  private awake: Promise<void> = Promise.resolve();

  constructor(
    private readonly recording: Recording,
    /** The preview service's origin, or a path on this one. */
    private readonly service: string,
    private readonly store: Storage | null = null,
    private readonly now: () => number = Date.now,
  ) {
    this.state = this.load() ?? this.seed();
  }

  private load(): State | null {
    try {
      const saved = this.store?.getItem(STORE);
      return saved ? (JSON.parse(saved) as State) : null;
    } catch {
      return null;
    }
  }

  private save() {
    try {
      this.store?.setItem(STORE, JSON.stringify(this.state));
    } catch {
      // A full or blocked store loses the visitor's work on a reload, not this answer.
    }
  }

  private recorded(key: string): Answer | undefined {
    const answer = this.recording.answers[key];
    return answer && { status: answer.status, body: this.moved(answer.body) };
  }

  /** What the recording answered to `url`, or to the same question on another branch it read, which is the nearest
   * the demo holds to a branch it never saw. */
  private nearest(url: URL): Answer {
    const found = this.recorded(keyOf(url));
    if (found || !url.searchParams.has('branch')) return found ?? NOT_RECORDED;
    for (const branch of ['main', 'work']) {
      const other = new URL(url);
      other.searchParams.set('branch', branch);
      const answer = this.recorded(keyOf(other));
      if (answer) return answer;
    }
    // A scope the fleet had no tests in.
    if (url.pathname === '/api/tests') return ok({ tests: [] });
    return NOT_RECORDED;
  }

  /** `body`, read at `readAt`, as if read now. */
  private moved(body: unknown, readAt = this.recording.recordedAt): unknown {
    return shifted(clone(body), this.now() - readAt);
  }

  /** Every branch the recording read a bundle on, with the samples and tests it read there; each scope a branch lacks
   * is taken from `main`. */
  private seed(): State {
    const configs = new Map<string, Map<string, { config: Bundle; commit: string }>>();
    const samples: Record<string, Branch['samples']> = {};
    const tests: Record<string, Branch['tests']> = {};
    for (const [key, { body }] of Object.entries(this.recording.answers)) {
      const url = new URL(key, 'http://demo');
      const branch = url.searchParams.get('branch');
      const scope = url.searchParams.get('scope');
      if (!branch || !scope || url.searchParams.has('at')) continue;
      const b = body as { config?: Bundle; commit?: string; sample?: SampleDto; test?: TestDto };
      if (url.pathname === '/api/config' && b.config && b.commit) {
        if (!configs.has(branch)) configs.set(branch, new Map());
        configs.get(branch)!.set(scope, { config: b.config, commit: b.commit });
      }
      if (url.pathname.startsWith('/api/samples/') && b.sample) {
        ((samples[branch] ??= {})[scope] ??= {})[b.sample.id] = b.sample;
      }
      if (url.pathname.startsWith('/api/tests/') && b.test) {
        ((tests[branch] ??= {})[scope] ??= {})[b.test.id] = b.test;
      }
    }
    const main = configs.get('main')!;
    const listed =
      (
        this.recording.answers['/api/branches']?.body as {
          branches?: {
            name: string;
            head: string;
            base: string;
            commits: number;
            lastSave: number;
            touches: string[];
          }[];
        }
      )?.branches ?? [];
    const branches: Record<string, Branch> = {};
    const names = new Set([...configs.keys(), ...listed.map((b) => b.name)]);
    for (const name of names) {
      const own = configs.get(name) ?? new Map();
      const bundles: Record<string, Bundle> = {};
      for (const [scope, { config }] of main)
        bundles[scope] = clone((own.get(scope) ?? { config }).config);
      const entry = listed.find((b) => b.name === name);
      const commit = entry?.head ?? [...own.values()][0]?.commit ?? main.get(SHARED)!.commit;
      branches[name] = {
        commit,
        base: entry?.base ?? main.get(SHARED)!.commit,
        commits: entry?.commits ?? 0,
        lastSave:
          (entry?.lastSave ?? Math.floor(this.recording.recordedAt / 1000)) +
          Math.round((this.now() - this.recording.recordedAt) / 1000),
        touches: entry?.touches ?? [],
        bundles,
        samples: clone(samples[name] ?? samples.main ?? {}),
        tests: clone(tests[name] ?? tests.main ?? {}),
      };
    }
    return { branches, captures: {} };
  }

  /** `name`, forked from `main` on its first write, as the leader forks one. */
  private branch(name: string, write = false): Branch | undefined {
    const known = this.state.branches[name];
    if (known || !write) return known;
    const main = this.state.branches.main;
    const forked: Branch = {
      ...clone(main),
      base: main.commit,
      commits: 0,
      touches: [],
    };
    this.state.branches[name] = forked;
    return forked;
  }

  private groups(): string[] {
    const body = this.recording.answers['/api/worker-groups?branch=main']?.body as {
      groups?: { id: string }[];
    };
    return (body?.groups ?? []).map((g) => g.id);
  }

  /** Boots the service now, so it is up by the time the visitor asks it anything, and holds every ask until it
   * answers or `WAKE_LIMIT_MS` passes. `announce` hears when it starts waking and when it is done. */
  wake(announce: (waking: boolean) => void = () => {}) {
    announce(true);
    this.awake = (async () => {
      const until = this.now() + WAKE_LIMIT_MS;
      while (this.now() < until) {
        try {
          // Anything but a 5xx is a service that answers: a local smeltr-demo has no /wake, and says 404.
          if ((await fetch(`${this.service}/wake`, { method: 'POST' })).status < 500) break;
        } catch {
          // Nothing answered at all; the next try may.
        }
        await new Promise((r) => setTimeout(r, WAKE_RETRY_MS));
      }
      announce(false);
    })();
  }

  private async ask(path: string, body: unknown): Promise<Answer> {
    await this.awake;
    try {
      const res = await fetch(`${this.service}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const text = await res.text();
      let answer: { errors?: ApiError[] };
      try {
        answer = JSON.parse(text);
      } catch {
        return unreachable(text || `HTTP ${res.status}`, res.ok ? 502 : res.status);
      }
      if (res.ok) return { status: res.status, body: answer };
      const said = (answer.errors ?? []).map((e) => e.message).join('; ');
      return { status: res.status, body: { ...answer, error: said || `HTTP ${res.status}` } };
    } catch (e) {
      return unreachable(String(e), 502);
    }
  }

  /** What the service needs to know of the bundle a scope runs: the group's own with `shared` beside it. */
  private bundleOf(branch: Branch, scope: string, own?: Bundle) {
    const bundle = own ?? branch.bundles[scope];
    return scope === SHARED
      ? { shared: bundle }
      : { shared: branch.bundles[SHARED], group: bundle, groupId: scope };
  }

  /** Commit `edit` to `scope` of `name` once the service says a save would take what it leaves. */
  private async write(
    name: string,
    scope: string,
    path: string,
    edit: (bundle: Bundle, held: Held) => void,
  ): Promise<Answer> {
    const branch = this.branch(name, true)!;
    const bundle = clone(branch.bundles[scope] ?? { version: 1 });
    const held: Held = {
      samples: clone(branch.samples[scope] ?? {}),
      tests: clone(branch.tests[scope] ?? {}),
    };
    edit(bundle, held);
    const verdict = await this.ask('/validate', this.bundleOf(branch, scope, bundle));
    if (!(verdict.body as { ok?: boolean }).ok) return verdict;
    if (scope === SHARED) {
      const broken = await this.brokenGroup(branch, bundle);
      if (broken) return broken;
    }
    branch.bundles[scope] = bundle;
    branch.samples[scope] = held.samples;
    branch.tests[scope] = held.tests;
    branch.commit = newCommit();
    branch.commits += 1;
    branch.lastSave = Math.floor(this.now() / 1000);
    const file = `${scope === SHARED ? SHARED : `groups/${scope}`}/${path}`;
    if (!branch.touches.includes(file)) branch.touches.push(file);
    this.save();
    return ok({ commit: branch.commit });
  }

  /** What a save of `shared` would refuse it for in a group that runs it merged with its own, each error named for
   * the group, as the leader's save names it. */
  private async brokenGroup(branch: Branch, shared: Bundle): Promise<Answer | undefined> {
    const verdicts = await Promise.all(
      this.groups().map(async (id) => ({
        id,
        verdict: await this.ask('/validate', {
          shared,
          group: branch.bundles[id] ?? { version: 1 },
          groupId: id,
        }),
      })),
    );
    for (const { id, verdict } of verdicts) {
      const v = verdict.body as { ok?: boolean; errors?: ApiError[] };
      if (v.ok) continue;
      if (verdict.status !== 200) return verdict;
      const named = (v.errors ?? []).map((e) => ({
        ...e,
        message: `worker group ${JSON.stringify(id)}: ${e.message}`,
      }));
      return refused(named);
    }
    return undefined;
  }

  /** The leader's answer to `method url`, with `body` as sent. */
  async answer(method: string, url: URL, body: string | null): Promise<Answer> {
    const path = url.pathname;
    const q = url.searchParams;
    const name = q.get('branch') ?? 'main';
    const scope = q.get('scope') ?? SHARED;
    const json = () => (body ? JSON.parse(body) : {});
    const at = q.has('at');

    // A branch read at its own commit, or at main's (the editor's "changed on this branch"), is what this tab holds;
    // the recording read neither for a branch the visitor started.
    if (method === 'GET' && at && path === '/api/config') {
      const commit = q.get('at');
      const held = [this.branch(name), this.branch('main')].find((b) => b?.commit === commit);
      const own = held?.bundles[scope] ?? (scope === SHARED ? undefined : { version: 1 });
      if (held && own) return ok({ config: own, commit });
    }

    if (method === 'GET' && !at) {
      const branch = this.branch(name);
      const own =
        branch && (branch.bundles[scope] ?? (scope === SHARED ? undefined : { version: 1 }));
      if (path === '/api/config' && branch && own) {
        return ok({ config: own, commit: branch.commit });
      }
      if (path === '/api/pipelines' && branch && own) {
        return ok({ pipelines: own.pipelines ?? [], commit: branch.commit });
      }
      const pipeline = path.match(/^\/api\/pipelines\/([^/]+)$/);
      if (pipeline && branch && own) {
        const id = decodeURIComponent(pipeline[1]);
        const found = (own.pipelines ?? []).find((p) => p.id === id);
        if (found) return ok({ pipeline: found, commit: branch.commit });
        const missing = refused([bareError(`no pipeline ${JSON.stringify(id)}`)], 404);
        return {
          ...missing,
          body: { ...(missing.body as object), pipeline: null, commit: branch.commit },
        };
      }
      if (path === '/api/pipeline-callers' && branch && own) {
        return ok({ callers: callersOf(own.pipelines ?? []) });
      }
      if (path === '/api/samples' && branch) {
        const samples = Object.values(branch.samples[scope] ?? {}).map((s) => ({
          id: s.id,
          name: s.name,
          source: s.source,
          records: s.records.length,
          captured: (s.records[0]?.__capture as { timestamp?: string } | undefined)?.timestamp,
        }));
        return ok({ samples, commit: branch.commit });
      }
      const sample = path.match(/^\/api\/samples\/([^/]+)$/);
      if (sample && branch) {
        const found = branch.samples[scope]?.[decodeURIComponent(sample[1])];
        if (found) return ok({ sample: found, commit: branch.commit });
        const missing = refused([bareError('there is no sample by that name on this branch')]);
        return {
          ...missing,
          body: { ...(missing.body as object), sample: null, commit: branch.commit },
        };
      }
      if (path === '/api/branches') return this.branches();
      if (path === '/api/tests' && branch)
        return this.tests(branch, q.has('ownScopes') ? null : scope, []);
      const test = path.match(/^\/api\/tests\/([^/]+)$/);
      if (test && branch) {
        const found = branch.tests[scope]?.[decodeURIComponent(test[1])];
        if (found) return ok({ test: found });
        return refused([bareError('there is no test by that name on this branch')]);
      }
      if (path === '/api/map' && branch) return this.map(branch);
      if (path === '/api/connections' && branch) {
        // An object `shared/` declares runs in every group; a group's own, in that group alone.
        const groups = (scope === SHARED ? this.groups() : [scope]).map((id) => ({
          id,
          config: branch.bundles[id] ?? { version: 1 },
        }));
        return this.ask('/connections', {
          shared: branch.bundles[SHARED],
          groups,
          kind: q.get('kind'),
          id: q.get('id'),
        });
      }
      const capture = path.match(/^\/api\/captures\/([^/]+)$/);
      if (capture) return this.captureStatus(decodeURIComponent(capture[1]));
      const live = this.live(url, branch);
      if (live) return live;
      return this.nearest(url);
    }

    if (method === 'PUT' && SECTIONS[path]) {
      const section = SECTIONS[path];
      const items = json();
      return this.write(name, scope, `${path.slice('/api/'.length)}.json`, (b) => {
        b[section] = items;
      });
    }
    const pipeline = path.match(/^\/api\/pipelines\/([^/]+)$/);
    if (pipeline && (method === 'PUT' || method === 'DELETE')) {
      const id = decodeURIComponent(pipeline[1]);
      const next = method === 'PUT' ? json() : null;
      return this.write(name, scope, `pipelines/${id}.json`, (b) => {
        const pipelines = (b.pipelines ?? []).filter((p) => p.id !== id);
        const was = (b.pipelines ?? []).findIndex((p) => p.id === id);
        if (next) pipelines.splice(was < 0 ? pipelines.length : was, 0, next);
        b.pipelines = pipelines;
      });
    }
    if (path === '/api/tests' && method === 'PUT') {
      const test = json() as TestDto;
      return this.write(name, scope, `tests/${test.id}.json`, (_, { tests }) => {
        tests[test.id] = test;
      });
    }
    const test = path.match(/^\/api\/tests\/([^/]+)$/);
    if (test && method === 'DELETE') {
      const id = decodeURIComponent(test[1]);
      return this.write(name, scope, `tests/${id}.json`, (_, { tests }) => {
        delete tests[id];
      });
    }
    if (path === '/api/tests/run' && method === 'POST') {
      const branch = this.branch(name) ?? this.state.branches.main;
      const { ids } = json() as { ids?: string[] };
      return this.tests(branch, q.has('ownScopes') ? null : scope, ids ?? [], true);
    }

    if (path === '/api/samples' && method === 'PUT') {
      const sample = json() as SampleDto;
      return this.write(name, scope, `samples/${sample.id}.json`, (_, { samples }) => {
        samples[sample.id] = sample;
      });
    }
    const sample = path.match(/^\/api\/samples\/([^/]+)$/);
    if (sample && method === 'DELETE') {
      const id = decodeURIComponent(sample[1]);
      return this.write(name, scope, `samples/${id}.json`, (_, { samples }) => {
        delete samples[id];
      });
    }

    if (path === '/api/branches' && method === 'POST') {
      return this.startBranch(String((json() as { name?: unknown }).name ?? ''));
    }
    const named = path.match(/^\/api\/branches\/([^/]+)$/);
    if (named && method === 'DELETE') return this.deleteBranch(decodeURIComponent(named[1]));
    if (path === '/api/undo' && method === 'POST') return this.undo(name, scope, json());

    if (method === 'POST') {
      if (path === '/api/preview') return this.preview(name, scope, json());
      if (path === '/api/route-preview') return this.routePreview(name, scope, json());
      if (path === '/api/grok-captures') return this.ask('/grok-captures', json());
      if (path === '/api/line-breaker') return this.ask('/line-breaker', json());
      if (path === '/api/format') return this.ask('/format', json());
      if (path === '/api/onboard/propose') return this.ask('/onboard/propose', json());
      if (path === '/api/captures') return this.startCapture(json());
      const capture = path.match(/^\/api\/captures\/([^/]+)\/(stop|save|discard)$/);
      if (capture) {
        const session = decodeURIComponent(capture[1]);
        if (capture[2] === 'stop') return this.stopCapture(session);
        if (capture[2] === 'discard') return this.discardCapture(session);
        return this.saveCapture(session, name, scope, json());
      }
      if (path === '/api/auth/activity') return ok();
    }

    if (method === 'GET') return this.nearest(url);
    // `results` too: a test run's caller reads it whether or not the run was refused.
    return { status: 200, body: { ok: false, errors: [bareError(DEMO_REFUSAL)], results: [] } };
  }

  /** A live answer, as the fleet moved when it was recorded, round and round. The map's measurements on a branch the
   * recording did not watch are main's: one fleet runs whichever branch it is drawn from. */
  private live(url: URL, branch: Branch | undefined): Answer | undefined {
    const onMain = new URL(url);
    if (url.pathname === '/api/map/measurements') onMain.searchParams.set('branch', 'main');
    const series = this.recording.moving[keyOf(url)] ?? this.recording.moving[keyOf(onMain)];
    if (!series?.length) return undefined;
    const at = Math.floor((this.now() - this.started) / MOVING_EVERY_MS) % series.length;
    const body = this.moved(series[at], this.recording.movingAt[at]) as Record<string, unknown>;
    // The map learns its drawing is stale from the commit its measurements carry.
    if (url.pathname === '/api/map/measurements' && branch) body.commit = branch.commit;
    return { status: 200, body };
  }

  /** `scope`'s tests on `branch`, or every scope's when `scope` is null, as the leader lists them; with `run`, what
   * each named in `ids` (every one, when it names none) makes of the branch, as the leader runs them. */
  private async tests(
    branch: Branch,
    scope: string | null,
    ids: string[],
    run = false,
  ): Promise<Answer> {
    const scopes = scope === null ? Object.keys(branch.tests) : [scope];
    const tests: unknown[] = [];
    const results: unknown[] = [];
    for (const s of scopes) {
      const held = Object.values(branch.tests[s] ?? {});
      if (!held.length) continue;
      const answer = await this.ask('/tests', {
        ...this.bundleOf(branch, s),
        tests: held,
        samples: Object.values(branch.samples[s] ?? {}),
        run: run ? (ids.length ? ids : held.map((t) => t.id)) : [],
      });
      const a = answer.body as {
        ok?: boolean;
        errors?: ApiError[];
        tests?: object[];
        results?: unknown[];
      };
      if (!a.ok) return { status: answer.status, body: { ...a, tests: [], results: [] } };
      tests.push(...(a.tests ?? []).map((t) => ({ scope: s, ...t })));
      results.push(...(a.results ?? []));
    }
    return run ? ok({ results }) : ok({ tests });
  }

  /** `name` started at `main`, as the leader starts a branch on purpose (`git::create_branch`). */
  private startBranch(name: string): Answer {
    const bad = badBranchName(name);
    if (bad) return refused([bareError(bad)]);
    if (name === 'main') {
      return refused([bareError('main is what deploys move -- it is not a branch to start')]);
    }
    if (this.state.branches[name]) {
      return refused([bareError(`branch ${JSON.stringify(name)} is already there`)]);
    }
    const started = this.branch(name, true)!;
    started.started = true;
    this.save();
    return ok({ head: started.commit });
  }

  /** `name` gone, with the work it carries, as the leader deletes a branch. */
  private deleteBranch(name: string): Answer {
    if (name === 'main')
      return refused([bareError('main is what deploys move and is not deleted')]);
    if (!this.state.branches[name]) {
      return refused([bareError(`there is no branch ${JSON.stringify(name)}`)]);
    }
    delete this.state.branches[name];
    this.save();
    return ok();
  }

  /** One object of `scope` on `name` put back the way `main` holds it, as one save, as the leader's undo does. The
   * demo's `main` never moves, so it is where every branch left it. */
  private async undo(
    name: string,
    scope: string,
    req: { section?: string; id?: string },
  ): Promise<Answer> {
    const section = req.section ?? '';
    const id = req.id ?? '';
    const noun = UNDONE[section];
    const branch = name === 'main' ? undefined : this.state.branches[name];
    if (!noun)
      return refused([bareError(`there is no section ${JSON.stringify(section)} to undo in`)]);
    if (!branch) {
      return refused([
        bareError(`${JSON.stringify(name)} has no saves, so there is nothing to undo`),
      ]);
    }
    const listOf = (b: Bundle | undefined) => (b?.[section] as { id: string }[] | undefined) ?? [];
    const was = listOf(this.state.branches.main.bundles[scope]);
    const back = putBack(was, listOf(branch.bundles[scope]), id);
    if (!back) {
      return refused([
        bareError(
          `there is no ${noun} ${JSON.stringify(id)} on ${JSON.stringify(name)} or where it left main`,
        ),
      ]);
    }
    if (back.same) {
      return refused([
        bareError(
          `${noun} ${JSON.stringify(back.name)} is as it was where ${JSON.stringify(name)} left main: there is nothing to undo`,
        ),
      ]);
    }
    const file =
      section === 'pipelines'
        ? `pipelines/${id}.json`
        : `${Object.keys(SECTIONS)
            .find((k) => SECTIONS[k] === section)!
            .slice('/api/'.length)}.json`;
    return this.write(name, scope, file, (b) => {
      b[section] = back.objects;
    });
  }

  private branches(): Answer {
    const listed =
      (this.recording.answers['/api/branches']?.body as { branches?: { name: string }[] })
        ?.branches ?? [];
    const main = this.state.branches.main;
    const branches = Object.entries(this.state.branches)
      .filter(
        ([name, b]) =>
          name !== 'main' && (b.commits > 0 || b.started || listed.some((l) => l.name === name)),
      )
      .map(([name, b]) => ({
        ...(this.moved(listed.find((l) => l.name === name) ?? {}) as object),
        name,
        head: b.commit,
        base: b.base,
        lastSave: b.lastSave,
        commits: b.commits,
        behind: b.base === main.commit ? 0 : 1,
        touches: b.touches,
      }));
    return ok({ branches });
  }

  private map(branch: Branch): Promise<Answer> {
    const known = this.maps.get(branch.commit);
    if (known) return known;
    const groups = this.groups().map((id) => ({
      id,
      config: branch.bundles[id] ?? { version: 1 },
    }));
    const commit = branch.commit;
    const drawn = this.ask('/map', { shared: branch.bundles[SHARED], groups }).then((a) => {
      if ((a.body as { ok?: boolean }).ok) (a.body as Record<string, unknown>).commit = commit;
      else this.maps.delete(commit);
      return a;
    });
    this.maps.set(commit, drawn);
    return drawn;
  }

  private sampleOn(branch: Branch | undefined, scope: string, id: string): SampleDto | undefined {
    return branch?.samples[scope]?.[id];
  }

  private async preview(
    name: string,
    scope: string,
    req: {
      config: string;
      pipeline: string | null;
      sample: string | null;
      show_internal: boolean;
      at_micros: number | null;
      filter: string | null;
      attribution?: boolean;
      after_dot: unknown;
      capture_filter: unknown;
    },
  ): Promise<Answer> {
    const branch = this.branch(name) ?? this.state.branches.main;
    const sample = req.sample === null ? undefined : this.sampleOn(branch, scope, req.sample);
    if (req.sample !== null && !sample) {
      return refused([
        bareError(`there is no sample ${JSON.stringify(req.sample)} on this branch`),
      ]);
    }
    return this.ask('/preview', {
      ...this.bundleOf(branch, scope, JSON.parse(req.config)),
      pipeline: req.pipeline,
      // What follows a dot reads the sample's columns, never its records: only the one a sample must hold crosses
      // the network.
      sample: sample && req.after_dot ? { ...sample, records: sample.records.slice(0, 1) } : sample,
      showInternal: req.show_internal,
      atMicros: req.at_micros,
      filter: req.filter,
      attribution: req.attribution ?? false,
      afterDot: req.after_dot ?? undefined,
      captureFilter: req.capture_filter ?? undefined,
    });
  }

  private async routePreview(
    name: string,
    scope: string,
    req: { config: string; sample: string; at_micros: number | null },
  ): Promise<Answer> {
    const branch = this.branch(name) ?? this.state.branches.main;
    const sample = this.sampleOn(branch, scope, req.sample);
    if (!sample)
      return refused([
        bareError(`there is no sample ${JSON.stringify(req.sample)} on this branch`),
      ]);
    return this.ask('/route-preview', {
      ...this.bundleOf(branch, scope, JSON.parse(req.config)),
      sample,
      atMicros: req.at_micros,
    });
  }

  /** The recorded capture a source's capture reads: its own, or one of main's sources reading the same topics. */
  private poolFor(group: string, source: Record<string, unknown>): string | null {
    const own = `${group}/source/${source.id}`;
    if (this.recording.captures[own]) return own;
    const topics = new Set((source.topics as string[] | undefined) ?? []);
    const main = this.state.branches.main;
    for (const g of this.groups()) {
      for (const s of (main.bundles[g]?.sources as Record<string, unknown>[] | undefined) ?? []) {
        const key = `${g}/source/${s.id}`;
        const read = (s.topics as string[] | undefined) ?? [];
        if (
          s.type === source.type &&
          read.some((t) => topics.has(t)) &&
          this.recording.captures[key]
        ) {
          return key;
        }
      }
    }
    return null;
  }

  private rowsOf(pool: string | null): number {
    if (!pool) return 0;
    return this.recording.captures[pool].batches.reduce((n, b) => n + b.rows.length, 0);
  }

  private async startCapture(req: {
    group: string;
    source?: Record<string, unknown>;
    destination?: string;
    filter?: string;
    maxMatches?: number;
    deadlineSecs?: number;
  }): Promise<Answer> {
    const maxMatches = Math.min(Math.max(req.maxMatches ?? 100, 1), 1000);
    const deadlineSecs = Math.min(Math.max(req.deadlineSecs ?? 30, 1), 300);
    let pool: string | null;
    let kept: number[];
    if (req.source) {
      pool = this.poolFor(req.group, req.source);
      kept = [...Array(this.rowsOf(pool)).keys()];
      if (pool && req.filter?.trim()) {
        // The filter runs over the source as the visitor has it, in the bundle as the branch holds it.
        const main = this.state.branches.main;
        const own = clone(main.bundles[req.group] ?? { version: 1 });
        own.sources = [
          ...((own.sources as { id: string }[] | undefined) ?? []).filter(
            (s) => s.id !== req.source!.id,
          ),
          req.source,
        ];
        const answer = await this.ask('/capture-filter', {
          ...this.bundleOf(main, req.group, own),
          source: req.source.id,
          filter: req.filter,
          sample: { id: 'pool', name: 'pool', ...this.recording.captures[pool].sample },
        });
        const a = answer.body as { ok?: boolean; errors?: ApiError[]; kept?: number[] };
        if (!a.ok) return refused(a.errors ?? []);
        kept = a.kept ?? [];
      }
    } else {
      pool = `${req.group}/destination/${req.destination}`;
      if (!this.recording.captures[pool]) pool = null;
      kept = [...Array(this.rowsOf(pool)).keys()];
    }
    const session = newCommit().slice(0, 16);
    this.state.captures[session] = {
      group: req.group,
      kept: kept.slice(0, maxMatches),
      pool,
      source: (req.source?.id as string | undefined) ?? null,
      destination: req.destination ?? null,
      filter: req.filter?.trim() || null,
      maxMatches,
      deadlineSecs,
      startedAt: this.now(),
      stoppedAt: null,
    };
    this.save();
    return ok({ session, maxMatches, maxBytes: 8 * 1024 * 1024, deadlineSecs });
  }

  /** What `capture` holds now: the kept rows that have arrived, as their batches had them. */
  private held(capture: Capture): { batches: CaptureBatchDto[]; done: boolean; full: boolean } {
    const until = capture.stoppedAt ?? this.now();
    const elapsed = until - capture.startedAt;
    const total = this.rowsOf(capture.pool);
    // The pool's records arrive spread over its span, as the fleet read them.
    const arrived = Math.floor((Math.min(elapsed, POOL_SPAN_MS) / POOL_SPAN_MS) * total);
    const rows = new Set(capture.kept.filter((i) => i < arrived));
    const full = rows.size >= capture.maxMatches;
    const done =
      capture.stoppedAt !== null ||
      full ||
      elapsed >= POOL_SPAN_MS ||
      elapsed >= capture.deadlineSecs * 1000;
    const batches: CaptureBatchDto[] = [];
    let at = 0;
    for (const batch of capture.pool ? this.recording.captures[capture.pool].batches : []) {
      const taken = batch.rows.filter((_, i) => rows.has(at + i));
      at += batch.rows.length;
      if (taken.length) batches.push({ columns: batch.columns, rows: taken });
    }
    return { batches, done, full };
  }

  private captureStatus(session: string): Answer {
    const capture = this.state.captures[session];
    if (!capture)
      return refused([bareError('no capture by that id -- it may already have been saved')]);
    const { batches, done, full } = this.held(capture);
    const workers = [
      ...new Set(batches.flatMap((b) => b.rows.map((r) => r.worker ?? '')).filter(Boolean)),
    ].sort();
    const end: CaptureEndDto = full ? 'matches' : 'deadline';
    const elapsed = (capture.stoppedAt ?? this.now()) - capture.startedAt;
    const status: CaptureStatusDto = {
      session,
      done,
      stopped: capture.stoppedAt !== null,
      maxMatches: capture.maxMatches,
      workers: workers.map((worker) => ({ worker, state: done ? 'answered' : 'filling' })),
      batches,
      ended: done && capture.stoppedAt === null ? workers.map((w) => [w, end]) : [],
      secondsLeft: done ? 0 : Math.max(0, capture.deadlineSecs - Math.floor(elapsed / 1000)),
      waitingOn: [],
      truncated: false,
    };
    return ok({ capture: status });
  }

  private stopCapture(session: string): Answer {
    const capture = this.state.captures[session];
    if (capture && capture.stoppedAt === null && !this.held(capture).done) {
      capture.stoppedAt = this.now();
      this.save();
    }
    return this.captureStatus(session);
  }

  private discardCapture(session: string): Answer {
    const found = session in this.state.captures;
    delete this.state.captures[session];
    this.save();
    return { status: 200, body: { ok: found } };
  }

  private async saveCapture(
    session: string,
    name: string,
    scope: string,
    req: { id: string; name: string; source?: string },
  ): Promise<Answer> {
    const capture = this.state.captures[session];
    if (!capture)
      return refused([bareError('no capture by that id -- it may already have been saved')]);
    const { done } = this.held(capture);
    const pool = capture.pool ? this.recording.captures[capture.pool].sample : undefined;
    if (!done)
      return refused([
        bareError('this capture is still running. Stop it first to save what it holds'),
      ]);
    const { batches } = this.held(capture);
    if (!pool || !batches.length) {
      return refused([
        bareError(
          'this capture caught nothing, and a sample of no records would preview as an empty pipeline rather than as the problem it is',
        ),
      ]);
    }
    const arrived = this.held(capture).batches.reduce((n, b) => n + b.rows.length, 0);
    // How it was taken, on every record as its `__source` fields are (`Captures::capture_fields`).
    const taken: Record<string, [string, string]> = {
      timestamp: ['datetime', new Date(capture.startedAt).toISOString()],
      ...(capture.source ? { source: ['str', capture.source] } : {}),
      ...(capture.destination ? { destination: ['str', capture.destination] } : {}),
      group: ['str', capture.group],
      ...(capture.filter ? { filter: ['str', capture.filter] } : {}),
      config: ['str', this.state.branches.main.commit],
    };
    const fields = Object.fromEntries(Object.entries(taken).map(([k, [, v]]) => [k, v]));
    const records = capture.kept
      .slice(0, arrived)
      .map((i) => ({ ...pool.records[i], __capture: fields }));
    const sample: SampleDto = {
      id: req.id,
      name: req.name,
      source: req.source,
      columns: [
        ...(pool.columns ?? []),
        ...Object.entries(taken).map(([k, [type]]) => ({ name: `__capture.${k}`, type })),
      ],
      records,
    };
    const saved = await this.write(name, scope, `samples/${sample.id}.json`, (_, { samples }) => {
      samples[sample.id] = sample;
    });
    if ((saved.body as { ok: boolean }).ok) {
      delete this.state.captures[session];
      this.save();
      (saved.body as Record<string, unknown>).records = records.length;
    }
    return saved;
  }
}

const NOT_RECORDED: Answer = refused([bareError('not found')], 404);

/** What the demo says to a write it does not play. */
export const DEMO_REFUSAL =
  "The demo can't do this. It runs in your browser, but you can download the full smeltr to try it: https://smeltr.sh/get/";

/** Answer every `/api/` call this page makes from `leader`. */
export function install(leader: DemoLeader) {
  const passOn = window.fetch.bind(window);
  window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const href = input instanceof Request ? input.url : String(input);
    const url = new URL(href, location.href);
    if (url.origin !== location.origin || !url.pathname.startsWith('/api/'))
      return passOn(input, init);
    const method = (
      init?.method ?? (input instanceof Request ? input.method : 'GET')
    ).toUpperCase();
    const body = typeof init?.body === 'string' ? init.body : null;
    const answer = await leader.answer(method, url, body);
    if (method !== 'GET') window.dispatchEvent(new Event(CHANGED_EVENT));
    return new Response(JSON.stringify(answer.body), {
      status: answer.status,
      headers: { 'content-type': 'application/json' },
    });
  };
}
