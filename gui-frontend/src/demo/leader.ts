// The leader the website demo's GUI talks to, played in the visitor's own browser.
//
// The GUI reaches its leader only through `fetch('/api/...')`, so the demo answers those calls here and the GUI runs
// unchanged. The visitor's branches, bundles, samples and captures live in this tab (`sessionStorage`, since the GUI
// reloads itself). What only the engine can answer -- a preview, a route preview, a save's validation, a grok
// pattern's captures, a line breaker's verdict, which records a capture filter keeps, a scope's tests, a branch's map
// and what a captured sample reads as -- goes to the demo's preview service (`src/bin/smeltr-demo`), with the bundle
// and sample it is about. Everything else is what the staged fleet answered when `just demo-fixtures` recorded it,
// moved to the visitor's now.

import type {
  ApiError,
  CaptureBatchDto,
  CaptureEndDto,
  CaptureStatusDto,
  Lock,
  LockedResult,
  SampleDto,
  SampleRecordDto,
} from '../api';
import { CHANGED_EVENT } from './guide';
import { bareError } from '../errors.svelte';

/** What `tools/screenshots/demo.mjs` recorded and every page reads. The rest it recorded is in parts of their own,
 * each read the first time it is asked for (`Parts`). */
export interface Recording {
  /** What every recorded time is relative to. */
  recordedAt: number;
  /** The last answer to each GET, by `keyOf`. */
  answers: Record<string, { status: number; body: unknown }>;
  /** The live answers, read every five seconds, by `keyOf`. */
  moving: Record<string, unknown[]>;
  /** When each of `moving`'s reads was made: the times in a read are relative to it, not to `recordedAt`. */
  movingAt: number[];
}

/** The recording's parts, by key: an answer by `keyOf`, as `Recording.answers` holds one, or by
 * `<group>/source/<id>` or `<group>/destination/<id>` what main's capture of it took (`Pool`). Undefined for a key no
 * part holds. */
export type Parts = (key: string) => Promise<unknown>;

/** What one capture of main's took. A source's carries the sample a save made of it too, record for row. */
export interface Pool {
  batches: CaptureBatchDto[];
  sample?: Required<Pick<SampleDto, 'columns' | 'records'>>;
}
type Bundle = Record<string, unknown> & { pipelines?: { id: string; steps?: unknown[] }[] };

const SHARED = 'shared';
/** How often the recording read the live answers. */
const MOVING_EVERY_MS = 5000;
/** How long an emulated capture takes to read its pool. */
const POOL_SPAN_MS = 8000;
const STORE = 'smeltr-demo';
/** The leader's own words for a capture it no longer holds (`capture_api::NO_CAPTURE`). */
const NO_CAPTURE = 'no capture by that id -- it may have been saved, or swept after finishing';

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
  /** Its change, once opened for review, as the leader lists it (`branch_api::ChangeOut`). */
  change?: Change;
  /** Oldest first, one for each save: the commit it left and what the scope it changed held there, so a review can
   * show one save at a time. Only that scope, as git keeps only what a commit changed: a whole copy each save would
   * outgrow the tab's storage. */
  past?: Earlier[];
  /** What happened to it, oldest first, as its change's timeline shows it: every save, and once opened, what
   * reviewers did. */
  events?: TimelineEvent[];
}

/** What a branch holds at one commit. */
type Files = Pick<Branch, 'bundles' | 'samples' | 'tests'>;

/** The one scope a save changed, as it was at `commit`, the save's parent. No `bundle`: the scope was not there. */
interface Earlier {
  commit: string;
  scope: string;
  bundle?: Bundle;
  samples?: Record<string, SampleDto>;
  tests?: Record<string, TestDto>;
}

/** A change, as `branch_api::ChangeOut` lists it. */
interface Change {
  state: 'open' | 'closed';
  title: string;
  description: string;
  /** Unix seconds. */
  opened: number;
  updated: number;
  reason: string;
  author: string;
  authorId?: string;
}

/** One thing that happened to a change, as `branch_api::TimelineEventOut` writes it. */
interface TimelineEvent {
  kind: string;
  id?: string;
  time: number;
  author: string;
  authorId?: string;
  title?: string;
  description?: string;
  text?: string;
  commit?: string;
  parent?: string;
  anchor?: unknown;
  at?: string;
  replyTo?: string;
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
  fields: string[];
  rows: unknown[];
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
const refused = (status: number, errors: ApiError[]): Answer => ({
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

/** The service failing to answer, as the leader refuses. */
const unreachable = (message: string, status: number): Answer =>
  refused(status, [bareError(message)]);

/** The sections a bundle stores as one file each, by the path within an area the GUI writes them at: the section's
 * key in a bundle, and its file. */
const SECTIONS: Record<string, [string, string]> = {
  '/sources': ['sources', 'sources.json'],
  '/destinations': ['destinations', 'destinations.json'],
  '/routes': ['routes', 'routes.json'],
  '/lookups': ['lookupTables', 'lookup-tables.json'],
  '/grok-patterns': ['grokPatterns', 'grok-patterns.json'],
  // One file per schema; a save is told as the directory that holds them.
  '/schemas': ['schemas', 'schemas'],
};

/** The kind of object each section of an area's path holds, as the service names it. */
const KINDS: Record<string, string> = {
  sources: 'source',
  routes: 'route',
  pipelines: 'pipeline',
  destinations: 'destination',
  lookups: 'lookupTable',
  'grok-patterns': 'grokPattern',
};

/** `/api/areas/<area><within>` as its area and the path within it; `null` for a path in no area. */
export function areaOf(path: string): { area: string; within: string } | null {
  const found = path.match(/^\/api\/areas\/([^/]+)(\/.*)?$/);
  return found && { area: decodeURIComponent(found[1]), within: found[2] ?? '' };
}

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
  schemas: 'schema',
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

/** `id` as `config_file::slugify` makes it a file's name. Not `slugify.ts`, which folds accents the leader drops. */
function slugged(id: string): string {
  const slug = id
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return slug || 'pipeline';
}

/** The sample or test `id` names among `held`, found by the file it is stored in, as the leader's
 * `read_sample` and `read_test` find it: two ids that slug alike are one file. */
function stored<T extends { id: string }>(
  held: Record<string, T> | undefined,
  id: string,
): T | undefined {
  return held?.[id] ?? Object.values(held ?? {}).find((o) => slugged(o.id) === slugged(id));
}

/** Every scope of `branch` as the service writes it out: its bundle, with the samples and tests beside it. */
function scopesOf(branch: Files) {
  return Object.fromEntries(
    Object.entries(branch.bundles).map(([scope, config]) => [
      scope,
      {
        config,
        samples: Object.values(branch.samples[scope] ?? {}),
        tests: Object.values(branch.tests[scope] ?? {}),
      },
    ]),
  );
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
    private readonly parts: Parts = async () => undefined,
  ) {
    this.state = this.load() ?? this.seed();
  }

  /** Each part asked for, read once. */
  private readonly read = new Map<string, Promise<unknown>>();

  private part(key: string): Promise<unknown> {
    let found = this.read.get(key);
    if (!found) {
      found = this.parts(key);
      this.read.set(key, found);
    }
    return found;
  }

  private async pool(key: string | null): Promise<Pool | undefined> {
    return key === null ? undefined : ((await this.part(key)) as Pool | undefined);
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

  private async recorded(key: string): Promise<Answer | undefined> {
    const answer =
      this.recording.answers[key] ??
      ((await this.part(key)) as Recording['answers'][string] | undefined);
    return answer && { status: answer.status, body: this.moved(answer.body) };
  }

  /** What the recording answered to `url`, or to the same question on another branch it read, which is the nearest
   * the demo holds to a branch it never saw. */
  private async nearest(url: URL): Promise<Answer> {
    const found = await this.recorded(keyOf(url));
    if (found || !url.searchParams.has('branch')) return found ?? NOT_RECORDED;
    for (const branch of ['main', 'work']) {
      const other = new URL(url);
      other.searchParams.set('branch', branch);
      const answer = await this.recorded(keyOf(other));
      if (answer) return answer;
    }
    // A scope the fleet had no tests in.
    if (areaOf(url.pathname)?.within === '/tests') return ok({ tests: [] });
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
      const area = areaOf(url.pathname);
      if (!branch || !area || url.searchParams.has('at')) continue;
      const { area: scope, within } = area;
      const b = body as { config?: Bundle; commit?: string; sample?: SampleDto; test?: TestDto };
      if (within === '' && b.config && b.commit) {
        if (!configs.has(branch)) configs.set(branch, new Map());
        configs.get(branch)!.set(scope, { config: b.config, commit: b.commit });
      }
      if (within.startsWith('/samples/') && b.sample) {
        ((samples[branch] ??= {})[scope] ??= {})[b.sample.id] = b.sample;
      }
      if (within.startsWith('/tests/') && b.test) {
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
            change?: Change;
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
      // A change the fleet was recorded with, with what the recording read of its history.
      if (entry?.change) {
        const change = this.moved(entry.change) as Change;
        const timeline = this.recording.answers[
          `/api/branches/${encodeURIComponent(name)}/timeline`
        ]?.body as { events?: TimelineEvent[] } | undefined;
        branches[name].change = change;
        branches[name].events = timeline?.events
          ? (this.moved(timeline.events) as TimelineEvent[])
          : [
              {
                kind: 'opened',
                id: newCommit(),
                time: change.opened,
                author: change.author,
                authorId: change.authorId,
                title: change.title,
                description: change.description,
              },
            ];
      }
    }
    return { branches, captures: {} };
  }

  /** Whether this tab holds branch `name`. A tab starts from the recording, so a branch made in another tab is not
   * one. */
  holds(name: string): boolean {
    return Object.hasOwn(this.state.branches, name);
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
      let answer: unknown;
      try {
        answer = JSON.parse(text);
      } catch {
        return unreachable(text || `HTTP ${res.status}`, res.ok ? 502 : res.status);
      }
      return { status: res.status, body: answer };
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

  /** Commit `edit` to `scope` of `name` once the service says a save would take what it leaves, with the subject the
   * leader's save of the same kind writes. */
  private async write(
    name: string,
    scope: string,
    path: string,
    subject: string,
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
    const was: Earlier = {
      commit: branch.commit,
      scope,
      bundle: branch.bundles[scope],
      samples: branch.samples[scope],
      tests: branch.tests[scope],
    };
    branch.bundles[scope] = bundle;
    branch.samples[scope] = held.samples;
    branch.tests[scope] = held.tests;
    const parent = branch.commit;
    (branch.past ??= []).push(was);
    branch.commit = newCommit();
    branch.commits += 1;
    branch.lastSave = Math.floor(this.now() / 1000);
    (branch.events ??= []).push({
      ...this.by(branch.lastSave),
      kind: 'saved',
      text: subject,
      commit: branch.commit,
      parent,
    });
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
      return refused(422, named);
    }
    return undefined;
  }

  /** What `name`'s change breaks of a schema in each group it touches, by group, as the leader's review reads it:
   * what main already broke is left out. A group whose bundle does not load is left out, as the leader leaves it out. */
  private async schemaFindings(name: string): Promise<Answer> {
    const branch = this.branch(name);
    if (!branch) return refused(404, [bareError(`no branch ${JSON.stringify(name)}`)]);
    const main = this.branch('main')!;
    const groups = new Set(
      branch.touches.flatMap((file) => /^groups\/([^/]+)\//.exec(file)?.[1] ?? []),
    );
    const own = (b: Branch, group: string) =>
      this.bundleOf(b, group, b.bundles[group] ?? { version: 1 });
    const findings: object[] = [];
    for (const group of groups) {
      const answer = await this.ask('/schema-findings', {
        ...own(branch, group),
        base: own(main, group),
      });
      const body = answer.body as { ok?: boolean; findings?: object[] };
      if (body.ok) findings.push(...(body.findings ?? []).map((f) => ({ group, ...f })));
    }
    return ok({ findings });
  }

  /** The leader's answer to `method url`, with `body` as sent. */
  async answer(method: string, url: URL, body: string | null): Promise<Answer> {
    const path = url.pathname;
    const q = url.searchParams;
    const name = q.get('branch') ?? 'main';
    const area = areaOf(path);
    const scope = area?.area ?? SHARED;
    const within = area?.within;
    const json = () => (body ? JSON.parse(body) : {});
    const at = q.has('at');
    // The tests of a change, across every scope it touches.
    const change = path.match(/^\/api\/branches\/([^/]+)\/(tests|test-runs)$/);

    // A branch read at its own commit, or at main's (the editor's "changed on this branch"), is what this tab holds;
    // the recording read neither for a branch the visitor started.
    // The demo stores what a leader would read back, so nothing in it is unreadable.
    if (method === 'GET' && within === '/unreadable' && this.branch(name)) {
      return ok({ files: [], commit: q.get('at') ?? this.branch(name)!.commit });
    }
    if (method === 'GET' && at) {
      const commit = q.get('at');
      const held = this.at(commit);
      const own = held?.bundles[scope] ?? (scope === SHARED ? undefined : { version: 1 });
      if (within === '' && held && own) return ok({ config: own, commit });
      // A change's review reads each sample and test it changed at both of its ends.
      const file = within?.match(/^\/(samples|tests)\/([^/]+)$/);
      const id = file && decodeURIComponent(file[2]);
      const sample = file?.[1] === 'samples' ? stored(held?.samples[scope], id!) : undefined;
      if (sample) return ok({ sample, commit });
      const test = file?.[1] === 'tests' ? stored(held?.tests[scope], id!) : undefined;
      if (test) return ok({ test, commit });
    }

    if (method === 'GET' && !at) {
      const branch = this.branch(name);
      const own =
        branch && (branch.bundles[scope] ?? (scope === SHARED ? undefined : { version: 1 }));
      if (within === '' && branch && own) {
        return ok({ config: own, commit: branch.commit });
      }
      if (within === '/pipelines' && branch && own) {
        return ok({ pipelines: own.pipelines ?? [], commit: branch.commit });
      }
      const pipeline = within?.match(/^\/pipelines\/([^/]+)$/);
      if (pipeline && branch && own) {
        const id = decodeURIComponent(pipeline[1]);
        const found = (own.pipelines ?? []).find((p) => p.id === id);
        if (found) return ok({ pipeline: found, commit: branch.commit });
        const missing = refused(404, [bareError(`no pipeline ${JSON.stringify(id)}`)]);
        return {
          ...missing,
          body: { ...(missing.body as object), pipeline: null, commit: branch.commit },
        };
      }
      if (within === '/pipeline-callers' && branch && own) {
        return ok({ callers: callersOf(own.pipelines ?? []) });
      }
      if (within === '/samples' && branch) {
        const samples = Object.values(branch.samples[scope] ?? {}).map((s) => ({
          id: s.id,
          name: s.name,
          source: s.source,
          records: s.records.length,
          captured: (s.records[0]?.__capture as { timestamp?: string } | undefined)?.timestamp,
        }));
        return ok({ samples, commit: branch.commit });
      }
      const sample = within?.match(/^\/samples\/([^/]+)$/);
      if (sample && branch) {
        const found = stored(branch.samples[scope], decodeURIComponent(sample[1]));
        if (found) return ok({ sample: found, commit: branch.commit });
        const missing = refused(404, [bareError('there is no sample by that name on this branch')]);
        return {
          ...missing,
          body: { ...(missing.body as object), sample: null, commit: branch.commit },
        };
      }
      if (path === '/api/branches') return this.branches();
      if (path === '/api/preview/schema-standards') return this.ask('/schemas/standards', {});
      if (within === '/tests' && branch) return this.tests(branch, scope, []);
      if (change?.[2] === 'tests') {
        const of = this.branch(decodeURIComponent(change[1]));
        if (of) return this.tests(of, null, []);
      }
      const test = within?.match(/^\/tests\/([^/]+)$/);
      if (test && branch) {
        const found = stored(branch.tests[scope], decodeURIComponent(test[1]));
        if (found) return ok({ test: found });
        return refused(404, [bareError('there is no test by that name on this branch')]);
      }
      if (path === '/api/map' && branch) return this.map(branch);
      const connections = within?.match(/^\/([^/]+)\/([^/]+)\/connections$/);
      if (connections && KINDS[connections[1]] && branch) {
        // An object `shared/` declares runs in every group; a group's own, in that group alone.
        const groups = (scope === SHARED ? this.groups() : [scope]).map((id) => ({
          id,
          config: branch.bundles[id] ?? { version: 1 },
        }));
        return this.ask('/connections', {
          shared: branch.bundles[SHARED],
          groups,
          kind: KINDS[connections[1]],
          id: decodeURIComponent(connections[2]),
        });
      }
      const findings = path.match(/^\/api\/branches\/([^/]+)\/schema-findings$/);
      if (findings) return this.schemaFindings(decodeURIComponent(findings[1]));
      if (path === '/api/main/diff') {
        const between = await this.between(q.get('from'), q.get('to'), q.get('path'));
        if (between) return between;
      }
      const read = path.match(/^\/api\/branches\/([^/]+)\/(timeline|diff)$/);
      if (read?.[2] === 'timeline') return this.timeline(decodeURIComponent(read[1]));
      if (read?.[2] === 'diff') return this.diff(decodeURIComponent(read[1]), q.get('path'));
      // The demo's visitor saves what they capture, so the leader holds none for them.
      if (path === '/api/captures/mine') return ok({});
      const capture = path.match(/^\/api\/captures\/([^/]+)$/);
      if (capture) return this.captureStatus(decodeURIComponent(capture[1]));
      const records = path.match(/^\/api\/captures\/([^/]+)\/records$/);
      if (records) return this.captureRecords(decodeURIComponent(records[1]));
      const live = this.live(url, branch);
      if (live) return live;
      return this.nearest(url);
    }

    if (method === 'PUT' && within !== undefined && SECTIONS[within]) {
      const [section, file] = SECTIONS[within];
      const items = json();
      return this.write(name, scope, file, `edit ${section}`, (b) => {
        b[section] = items;
      });
    }
    const pipeline = within?.match(/^\/pipelines\/([^/]+)$/);
    if (pipeline && (method === 'PUT' || method === 'DELETE')) {
      const id = decodeURIComponent(pipeline[1]);
      const next = method === 'PUT' ? json() : null;
      const named = next?.name ?? id;
      const subject = next ? `edit pipeline ${named}` : `delete pipeline ${id}`;
      return this.write(name, scope, `pipelines/${id}.json`, subject, (b) => {
        const pipelines = (b.pipelines ?? []).filter((p) => p.id !== id);
        const was = (b.pipelines ?? []).findIndex((p) => p.id === id);
        if (next) pipelines.splice(was < 0 ? pipelines.length : was, 0, next);
        b.pipelines = pipelines;
      });
    }
    const test = within?.match(/^\/tests\/([^/]+)$/);
    if (test && method === 'PUT') {
      const id = decodeURIComponent(test[1]);
      const lock = json() as Lock;
      const answer = await this.lockTest(name, scope, lock);
      const held = answer.body as LockedResult;
      if (!held.ok) return answer;
      const { name: testName, pipeline, sample, lockedAt } = lock;
      const locked: TestDto = {
        id,
        name: testName,
        ...(pipeline ? { pipeline } : {}),
        sample,
        lockedAt,
        rowsIn: held.rowsIn ?? 0,
        fields: held.fields ?? [],
        rows: held.rows ?? [],
      };
      return this.write(
        name,
        scope,
        `tests/${id}.json`,
        `lock test ${testName}`,
        (_, { tests }) => {
          tests[id] = locked;
        },
      );
    }
    if (test && method === 'DELETE') {
      const id = decodeURIComponent(test[1]);
      return this.write(name, scope, `tests/${id}.json`, `delete test ${id}`, (_, { tests }) => {
        delete tests[id];
      });
    }
    if ((within === '/test-runs' || change?.[2] === 'test-runs') && method === 'POST') {
      const on = change ? decodeURIComponent(change[1]) : name;
      const branch = this.branch(on) ?? this.state.branches.main;
      const { ids } = json() as { ids?: string[] };
      return this.tests(branch, change ? null : scope, ids ?? [], true);
    }

    const sample = within?.match(/^\/samples\/([^/]+)$/);
    if (sample && method === 'PUT') {
      const id = decodeURIComponent(sample[1]);
      const saved = { ...(json() as SampleDto), id };
      return this.write(
        name,
        scope,
        `samples/${id}.json`,
        `save sample ${saved.name}`,
        (_, { samples }) => {
          samples[id] = saved;
        },
      );
    }
    if (sample && method === 'DELETE') {
      const id = decodeURIComponent(sample[1]);
      return this.write(
        name,
        scope,
        `samples/${id}.json`,
        `delete sample ${id}`,
        (_, { samples }) => {
          delete samples[id];
        },
      );
    }

    if (path === '/api/branches' && method === 'POST') {
      return this.startBranch(String((json() as { name?: unknown }).name ?? ''));
    }
    const named = path.match(/^\/api\/branches\/([^/]+)$/);
    if (named && method === 'DELETE') return this.deleteBranch(decodeURIComponent(named[1]));
    if (named && method === 'PATCH') return this.retitle(decodeURIComponent(named[1]), json());
    const review = path.match(/^\/api\/branches\/([^/]+)\/(open|close|comments|reviews)$/);
    if (review) {
      const of = decodeURIComponent(review[1]);
      const call = `${method} ${review[2]}`;
      if (call === 'POST open') return this.open(of, json());
      if (call === 'POST close') return this.close(of, json());
      if (call === 'POST comments') return this.comment(of, json());
      if (call === 'POST reviews') return this.review(of, json());
    }
    const thread = path.match(/^\/api\/branches\/([^/]+)\/threads\/([^/]+)$/);
    if (thread && method === 'POST') {
      const resolved = (json() as { resolved?: unknown }).resolved === true;
      return this.resolve(decodeURIComponent(thread[1]), decodeURIComponent(thread[2]), resolved);
    }
    const capture = path.match(/^\/api\/captures\/([^/]+)$/);
    if (capture && method === 'DELETE') return this.discardCapture(decodeURIComponent(capture[1]));
    if (within === '/undo' && method === 'POST') return this.undo(name, scope, json());

    if (method === 'POST') {
      if (within === '/preview/pipeline') return this.preview(name, scope, json());
      if (within === '/preview/routes') return this.routePreview(name, scope, json());
      if (within === '/preview/test-lock') return this.lockTest(name, scope, json());
      if (within === '/samples') {
        const { capture, ...save } = json() as {
          capture: string;
          id: string;
          name: string;
          source?: string;
        };
        return this.saveCapture(capture, name, scope, save);
      }
      if (path === '/api/preview/grok') return this.ask('/grok-captures', json());
      if (path === '/api/preview/line-breaker') return this.ask('/line-breaker', json());
      if (path === '/api/preview/format') return this.ask('/format', json());
      if (path === '/api/preview/onboard') return this.ask('/onboard/propose', json());
      if (path === '/api/preview/schema-read') return this.ask('/schemas/read', json());
      if (path === '/api/preview/schema-export') return this.ask('/schemas/export', json());
      if (path === '/api/captures') return this.startCapture(json());
      const stop = path.match(/^\/api\/captures\/([^/]+)\/stop$/);
      if (stop) return this.stopCapture(decodeURIComponent(stop[1]));
      if (path === '/api/auth/session/activity') return ok();
    }

    if (method === 'GET') return this.nearest(url);
    // `results` too: a test run's caller reads it whether or not the run was refused.
    return { status: 403, body: { ok: false, errors: [bareError(DEMO_REFUSAL)], results: [] } };
  }

  /** A live answer, as the fleet moved when it was recorded, round and round. The map's measurements and a node's
   * stats on a branch the recording did not watch are main's: one fleet runs whichever branch it is drawn from. */
  private live(url: URL, branch: Branch | undefined): Answer | undefined {
    const onMain = new URL(url);
    if (onMain.searchParams.has('branch')) onMain.searchParams.set('branch', 'main');
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
      tests.push(...(a.tests ?? []).map((t) => ({ area: s, ...t })));
      results.push(...(a.results ?? []));
    }
    return run ? ok({ results }) : ok({ tests });
  }

  /** What locking `lock` on branch `name` holds, as the leader runs it (`test_api::preview_lock`). */
  private async lockTest(name: string, scope: string, lock: Lock): Promise<Answer> {
    const branch = this.branch(name) ?? this.state.branches.main;
    return this.ask('/test-lock', {
      ...this.bundleOf(branch, scope),
      samples: Object.values(branch.samples[scope] ?? {}),
      lock,
    });
  }

  /** `name` started at `main`, as the leader starts a branch on purpose (`git::create_branch`). */
  private startBranch(name: string): Answer {
    const bad = badBranchName(name);
    if (bad) return refused(422, [bareError(bad)]);
    if (name === 'main') {
      return refused(422, [bareError('main is what deploys move -- it is not a branch to start')]);
    }
    if (this.state.branches[name]) {
      return refused(409, [bareError(`branch ${JSON.stringify(name)} is already there`)]);
    }
    const started = this.branch(name, true)!;
    started.started = true;
    this.save();
    return ok({ head: started.commit });
  }

  /** `name` gone, with the work it carries, as the leader deletes a branch. */
  private deleteBranch(name: string): Answer {
    if (name === 'main')
      return refused(422, [bareError('main is what deploys move and is not deleted')]);
    if (!this.state.branches[name]) {
      return refused(404, [bareError(`there is no branch ${JSON.stringify(name)}`)]);
    }
    delete this.state.branches[name];
    this.save();
    return ok();
  }

  /** Who the visitor is: the account the recording was made signed in as. */
  private by(time: number): { time: number; author: string; authorId?: string } {
    const session = this.recording.answers['/api/auth/session']?.body as
      { displayName?: string; userId?: string } | undefined;
    return { time, author: session?.displayName ?? '', authorId: session?.userId };
  }

  /** `name` and its change, or why the leader would refuse a review call on it: a branch it does not have, or, with
   * `open`, a change that is not open (`git::review_change` and its siblings). */
  private reviewed(
    name: string,
    open: boolean,
  ): { branch: Branch; change: Change } | { refusal: Answer } {
    const branch = name === 'main' ? undefined : this.state.branches[name];
    const q = JSON.stringify(name);
    if (!branch) return { refusal: refused(404, [bareError(`there is no branch ${q}`)]) };
    if (!branch.change) {
      return { refusal: refused(409, [bareError(`${q} was never opened for review`)]) };
    }
    if (open && branch.change.state !== 'open') {
      return { refusal: refused(409, [bareError(`${q} is not open for review`)]) };
    }
    return { branch, change: branch.change };
  }

  private marked(
    branch: Branch,
    event: Omit<TimelineEvent, 'time' | 'author' | 'authorId' | 'id'>,
  ) {
    (branch.events ??= []).push({
      ...this.by(Math.floor(this.now() / 1000)),
      id: newCommit(),
      ...event,
    });
    this.save();
    return ok();
  }

  /** `name` opened for review, or opened again after a close, as `branch_api::open_change` opens it. */
  private open(name: string, req: { title?: string; description?: string }): Answer {
    const branch = name === 'main' ? undefined : this.state.branches[name];
    const q = JSON.stringify(name);
    if (!branch) return refused(404, [bareError(`there is no branch ${q}`)]);
    if (branch.commits === 0) {
      return refused(409, [bareError(`${q} changes nothing yet -- save something on it first`)]);
    }
    const title = (req.title ?? '').trim();
    const description = (req.description ?? '').trim();
    if (!title) return refused(422, [bareError(NO_TITLE)]);
    if (branch.change?.state === 'open') {
      return refused(409, [bareError(`${q} is already open for review`)]);
    }
    const kind = branch.change ? 'reopened' : 'opened';
    const { time, author, authorId } = this.by(Math.floor(this.now() / 1000));
    branch.change = {
      state: 'open',
      title,
      description,
      opened: time,
      updated: time,
      reason: '',
      author,
      authorId,
    };
    return this.marked(branch, { kind, title, description });
  }

  /** `name`'s change closed, keeping the branch, as `git::close_change` closes it. */
  private close(name: string, req: { reason?: string }): Answer {
    const found = this.reviewed(name, true);
    if ('refusal' in found) return found.refusal;
    const reason = (req.reason ?? '').trim() || NO_REASON;
    Object.assign(found.change, {
      state: 'closed',
      reason,
      updated: Math.floor(this.now() / 1000),
    });
    return this.marked(found.branch, { kind: 'closed', text: reason });
  }

  /** `name`'s change renamed, as `git::retitle_change` renames it: whoever opened it still did. */
  private retitle(name: string, req: { title?: string; description?: string }): Answer {
    const title = (req.title ?? '').trim();
    const description = (req.description ?? '').trim();
    if (!title) return refused(422, [bareError(NO_TITLE)]);
    const found = this.reviewed(name, true);
    if ('refusal' in found) return found.refusal;
    Object.assign(found.change, { title, description, updated: Math.floor(this.now() / 1000) });
    return this.marked(found.branch, { kind: 'renamed', title, description });
  }

  /** The comment `id` on `branch`'s change, when it started a thread, or why the leader's `thread_root` refuses it. */
  private threadRoot(branch: Branch, name: string, id: string): TimelineEvent | Answer {
    const found = (branch.events ?? []).find((e) => e.id === id.toLowerCase());
    if (found?.kind === 'comment' && !found.replyTo) return found;
    if (found?.kind === 'comment') {
      return refused(422, [
        bareError(`${id} is a reply: a thread is answered at the comment that started it`),
      ]);
    }
    if (found) return refused(422, [bareError(`${id} is not a comment`)]);
    return refused(404, [
      bareError(`there is no comment ${id} on ${JSON.stringify(name)}'s change`),
    ]);
  }

  /** A comment on `name`'s change, open or closed, as `git::comment_on_change` writes it. */
  private comment(
    name: string,
    req: { body?: string; anchor?: TimelineEvent['anchor']; replyTo?: string },
  ): Answer {
    const body = (req.body ?? '').trim();
    if (!body) return refused(422, [bareError('a comment needs something said in it')]);
    const found = this.reviewed(name, false);
    if ('refusal' in found) return found.refusal;
    if (req.anchor && req.replyTo) {
      return refused(422, [
        bareError('a reply is about what its thread is about, so it takes no anchor'),
      ]);
    }
    let replyTo: string | undefined;
    if (req.replyTo) {
      const root = this.threadRoot(found.branch, name, req.replyTo);
      if ('status' in root) return root;
      replyTo = root.id;
    }
    const at = req.anchor || replyTo ? found.branch.commit : undefined;
    return this.marked(found.branch, {
      kind: 'comment',
      text: body,
      ...(req.anchor ? { anchor: req.anchor } : {}),
      ...(at ? { at } : {}),
      ...(replyTo ? { replyTo } : {}),
    });
  }

  /** The thread comment `id` started on `name`'s change resolved, or opened again, as `git::resolve_thread` does. */
  private resolve(name: string, id: string, resolved: boolean): Answer {
    const found = this.reviewed(name, false);
    if ('refusal' in found) return found.refusal;
    const root = this.threadRoot(found.branch, name, id);
    if ('status' in root) return root;
    const latest = (found.branch.events ?? [])
      .filter((e) => (e.kind === 'resolved' || e.kind === 'unresolved') && e.replyTo === root.id)
      .at(-1);
    if ((latest?.kind === 'resolved') === resolved) {
      return refused(409, [
        bareError(resolved ? 'this thread is already resolved' : 'this thread is not resolved'),
      ]);
    }
    return this.marked(found.branch, {
      kind: resolved ? 'resolved' : 'unresolved',
      replyTo: root.id,
    });
  }

  /** A verdict on `name`'s change, as `git::review_change` records it. */
  private review(name: string, req: { verdict?: string; body?: string }): Answer {
    const body = (req.body ?? '').trim();
    if (req.verdict !== 'approve' && req.verdict !== 'request-changes') {
      return refused(422, [bareError(`unknown verdict ${JSON.stringify(req.verdict)}`)]);
    }
    if (req.verdict === 'request-changes' && !body) {
      return refused(422, [bareError('asking for changes needs to say what to change')]);
    }
    const found = this.reviewed(name, true);
    if ('refusal' in found) return found.refusal;
    const kind = req.verdict === 'approve' ? 'approved' : 'changes-requested';
    return this.marked(found.branch, { kind, text: body });
  }

  /** What happened to `name`'s change, oldest first, as `git::change_timeline` tells it: a reply sits on its thread's
   * anchor. The demo keeps no file's history, so no comment is shown outdated. */
  private timeline(name: string): Answer {
    const found = this.reviewed(name, false);
    if ('refusal' in found) return found.refusal;
    const events = found.branch.events ?? [];
    const anchorOf = new Map(
      events.filter((e) => e.kind === 'comment').map((e) => [e.id, e.anchor]),
    );
    return ok({
      events: events.map((e) => {
        if (e.kind !== 'comment') return e;
        const anchor = anchorOf.get(e.replyTo ?? e.id);
        return { ...e, ...(anchor ? { anchor } : {}), outdated: false };
      }),
    });
  }

  /** What some branch held at `commit`, at its latest save or an earlier one, or `undefined` for a commit the tab
   * never held. */
  private at(commit: string | null): Files | undefined {
    for (const b of Object.values(this.state.branches)) {
      if (b.commit === commit) return b;
      const from = b.past?.findIndex((e) => e.commit === commit) ?? -1;
      if (from < 0) continue;
      // Each save undone, newest first, back to the one that left `commit`.
      const files: Files = {
        bundles: { ...b.bundles },
        samples: { ...b.samples },
        tests: { ...b.tests },
      };
      for (const e of b.past!.slice(from).reverse()) {
        for (const [key, value] of [
          ['bundles', e.bundle],
          ['samples', e.samples],
          ['tests', e.tests],
        ] as const) {
          if (value === undefined) delete files[key][e.scope];
          else (files[key] as Record<string, unknown>)[e.scope] = value;
        }
      }
      return files;
    }
    return undefined;
  }

  /** What landed between two commits the tab holds, as `branch_api::commit_diff` answers it: every file, and the
   * text of `path`'s alone when it names one. `undefined` for a commit the tab no longer holds. */
  private async between(
    from: string | null,
    to: string | null,
    path: string | null,
  ): Promise<Answer | undefined> {
    const [before, after] = [this.at(from), this.at(to)];
    if (!before || !after) return undefined;
    const [all, one] = await Promise.all([
      this.ask('/diff', { base: scopesOf(before), branch: scopesOf(after) }),
      path ? this.ask('/diff', { base: scopesOf(before), branch: scopesOf(after), path }) : null,
    ]);
    if (!one || !(all.body as { ok?: boolean }).ok) return all;
    return {
      ...one,
      body: { ...(one.body as object), files: (all.body as { files: unknown }).files },
    };
  }

  /** What `name` changed since it left `main`, file by file, as the leader's diff tells it; with `path`, that file's
   * alone. The service writes both sides as the leader stores them. */
  private diff(name: string, path: string | null): Promise<Answer> | Answer {
    const branch = name === 'main' ? undefined : this.state.branches[name];
    if (!branch) return refused(404, [bareError(`there is no branch ${JSON.stringify(name)}`)]);
    return this.ask('/diff', {
      base: scopesOf(this.state.branches.main),
      branch: scopesOf(branch),
      ...(path ? { path } : {}),
    });
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
      return refused(422, [bareError(`there is no section ${JSON.stringify(section)} to undo in`)]);
    if (!branch) {
      return refused(409, [
        bareError(`${JSON.stringify(name)} has no saves, so there is nothing to undo`),
      ]);
    }
    const listOf = (b: Bundle | undefined) => (b?.[section] as { id: string }[] | undefined) ?? [];
    const was = listOf(this.state.branches.main.bundles[scope]);
    const back = putBack(was, listOf(branch.bundles[scope]), id);
    if (!back) {
      return refused(404, [
        bareError(
          `there is no ${noun} ${JSON.stringify(id)} on ${JSON.stringify(name)} or where it left main`,
        ),
      ]);
    }
    if (back.same) {
      return refused(409, [
        bareError(
          `${noun} ${JSON.stringify(back.name)} is as it was where ${JSON.stringify(name)} left main: there is nothing to undo`,
        ),
      ]);
    }
    const file =
      section === 'pipelines'
        ? `pipelines/${id}.json`
        : Object.values(SECTIONS).find(([key]) => key === section)![1];
    return this.write(name, scope, file, `undo changes to ${noun} ${back.name}`, (b) => {
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
        ...(b.change ? { change: b.change } : {}),
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
      as_sample?: boolean;
      after_dot: unknown;
      capture_filter: unknown;
    },
  ): Promise<Answer> {
    const branch = this.branch(name) ?? this.state.branches.main;
    const sample = req.sample === null ? undefined : this.sampleOn(branch, scope, req.sample);
    if (req.sample !== null && !sample) {
      return refused(404, [
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
      asSample: req.as_sample ?? false,
      afterDot: req.after_dot ?? undefined,
      captureFilter: req.capture_filter ?? undefined,
    });
  }

  private async routePreview(
    name: string,
    scope: string,
    req: { config: string; sample: string; source: string | null; at_micros: number | null },
  ): Promise<Answer> {
    const branch = this.branch(name) ?? this.state.branches.main;
    const sample = this.sampleOn(branch, scope, req.sample);
    if (!sample)
      return refused(404, [
        bareError(`there is no sample ${JSON.stringify(req.sample)} on this branch`),
      ]);
    return this.ask('/route-preview', {
      ...this.bundleOf(branch, scope, JSON.parse(req.config)),
      sample: { ...sample, source: sample.source ?? req.source ?? undefined },
      atMicros: req.at_micros,
    });
  }

  /** The recorded capture a source's capture reads: its own, or one of main's sources reading the same topics. */
  private async poolFor(group: string, source: Record<string, unknown>): Promise<string | null> {
    const own = `${group}/source/${source.id}`;
    if (await this.pool(own)) return own;
    const topics = new Set((source.topics as string[] | undefined) ?? []);
    const main = this.state.branches.main;
    for (const g of this.groups()) {
      for (const s of (main.bundles[g]?.sources as Record<string, unknown>[] | undefined) ?? []) {
        const key = `${g}/source/${s.id}`;
        const read = (s.topics as string[] | undefined) ?? [];
        if (s.type === source.type && read.some((t) => topics.has(t)) && (await this.pool(key))) {
          return key;
        }
      }
    }
    return null;
  }

  private async rowsOf(pool: string | null): Promise<number> {
    return ((await this.pool(pool))?.batches ?? []).reduce((n, b) => n + b.rows.length, 0);
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
      pool = await this.poolFor(req.group, req.source);
      kept = [...Array(await this.rowsOf(pool)).keys()];
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
          sample: { id: 'pool', name: 'pool', ...(await this.pool(pool))!.sample },
        });
        const a = answer.body as { ok?: boolean; errors?: ApiError[]; kept?: number[] };
        if (!a.ok) return refused(422, a.errors ?? []);
        kept = a.kept ?? [];
      }
    } else {
      pool = `${req.group}/destination/${req.destination}`;
      if (!(await this.pool(pool))) pool = null;
      kept = [...Array(await this.rowsOf(pool)).keys()];
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
  private async held(
    capture: Capture,
  ): Promise<{ batches: CaptureBatchDto[]; done: boolean; full: boolean }> {
    const until = capture.stoppedAt ?? this.now();
    const elapsed = until - capture.startedAt;
    const total = await this.rowsOf(capture.pool);
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
    for (const batch of (await this.pool(capture.pool))?.batches ?? []) {
      const taken = batch.rows.filter((_, i) => rows.has(at + i));
      at += batch.rows.length;
      if (taken.length) batches.push({ columns: batch.columns, rows: taken });
    }
    return { batches, done, full };
  }

  private async captureStatus(session: string): Promise<Answer> {
    const capture = this.state.captures[session];
    if (!capture) return refused(404, [bareError(NO_CAPTURE)]);
    const { batches, done, full } = await this.held(capture);
    const workers = [
      ...new Set(batches.flatMap((b) => b.rows.map((r) => r.worker ?? '')).filter(Boolean)),
    ].sort();
    const end: CaptureEndDto = full ? 'matches' : 'deadline';
    const elapsed = (capture.stoppedAt ?? this.now()) - capture.startedAt;
    const records = batches.reduce((n, b) => n + b.rows.length, 0);
    const status: CaptureStatusDto = {
      session,
      done,
      stopped: capture.stoppedAt !== null,
      maxMatches: capture.maxMatches,
      workers: workers.map((worker) => ({ worker, state: done ? 'answered' : 'filling' })),
      records,
      // The demo's capture only ever grows, so its count marks each change.
      revision: records,
      ended: done && capture.stoppedAt === null ? workers.map((w) => [w, end]) : [],
      secondsLeft: done ? 0 : Math.max(0, capture.deadlineSecs - Math.floor(elapsed / 1000)),
      waitingOn: [],
      truncated: false,
    };
    return ok({ capture: status });
  }

  private async captureRecords(session: string): Promise<Answer> {
    const capture = this.state.captures[session];
    if (!capture) return refused(404, [bareError(NO_CAPTURE)]);
    const { batches } = await this.held(capture);
    const pool = (await this.pool(capture.pool))?.sample;
    const workers = batches.flatMap((b) => b.rows.map((r) => r.worker));
    const fields = this.takenFields(capture);
    const records = capture.kept.slice(0, workers.length).map((i, n) => ({
      ...(workers[n] === undefined ? {} : { worker: workers[n] }),
      record: stamped(pool?.records[i] ?? {}, fields),
    }));
    return ok({ columns: this.takenColumns(capture, pool?.columns ?? []), records });
  }

  /** How `capture` was taken, by `__capture` field: each one's type and value (`Captures::capture_fields`). */
  private taken(capture: Capture): Record<string, [string, string]> {
    return {
      timestamp: ['datetime', new Date(capture.startedAt).toISOString()],
      ...(capture.source ? { source: ['str', capture.source] } : {}),
      ...(capture.destination ? { destination: ['str', capture.destination] } : {}),
      group: ['str', capture.group],
      ...(capture.filter ? { filter: ['str', capture.filter] } : {}),
      config: ['str', this.state.branches.main.commit],
    };
  }

  private takenFields(capture: Capture): Record<string, string> {
    return Object.fromEntries(Object.entries(this.taken(capture)).map(([k, [, v]]) => [k, v]));
  }

  /** A pool's columns with `capture`'s `__capture` fields in place of the ones the recorder's own save stamped. */
  private takenColumns(capture: Capture, columns: { name: string; type: string }[]) {
    return [
      ...columns.filter((c) => !c.name.startsWith('__capture.')),
      ...Object.entries(this.taken(capture)).map(([k, [type]]) => ({
        name: `__capture.${k}`,
        type,
      })),
    ];
  }

  private async stopCapture(session: string): Promise<Answer> {
    const capture = this.state.captures[session];
    if (capture && capture.stoppedAt === null && !(await this.held(capture)).done) {
      capture.stoppedAt = this.now();
      this.save();
    }
    return this.captureStatus(session);
  }

  private discardCapture(session: string): Answer {
    const found = session in this.state.captures;
    delete this.state.captures[session];
    this.save();
    return found ? ok() : refused(404, [bareError(NO_CAPTURE)]);
  }

  private async saveCapture(
    session: string,
    name: string,
    scope: string,
    req: { id: string; name: string; source?: string },
  ): Promise<Answer> {
    const capture = this.state.captures[session];
    if (!capture) return refused(404, [bareError(NO_CAPTURE)]);
    const { done, batches } = await this.held(capture);
    const pool = (await this.pool(capture.pool))?.sample;
    if (!done)
      return refused(409, [
        bareError('this capture is still running. Stop it first to save what it holds'),
      ]);
    if (!pool || !batches.length) {
      return refused(409, [
        bareError(
          'this capture caught nothing, and a sample of no records would preview as an empty pipeline rather than as the problem it is',
        ),
      ]);
    }
    const arrived = batches.reduce((n, b) => n + b.rows.length, 0);
    const fields = this.takenFields(capture);
    const records = capture.kept.slice(0, arrived).map((i) => stamped(pool.records[i], fields));
    const sample: SampleDto = {
      id: req.id,
      name: req.name,
      source: capture.source ?? req.source,
      columns: this.takenColumns(capture, pool.columns ?? []),
      records,
    };
    const saved = await this.write(
      name,
      scope,
      `samples/${sample.id}.json`,
      `save capture as sample ${sample.name}`,
      (_, { samples }) => {
        samples[sample.id] = sample;
      },
    );
    if ((saved.body as { ok: boolean }).ok) {
      delete this.state.captures[session];
      this.save();
      (saved.body as Record<string, unknown>).records = records.length;
    }
    return saved;
  }
}

/** `record` with how a capture took it as its `__capture` fields, keeping the fields it lists as mistyped
 * (`io::MISTYPED_FIELDS`). */
function stamped(record: SampleRecordDto, fields: Record<string, string>): SampleRecordDto {
  const own = record.__capture as { mistyped?: unknown } | undefined;
  return {
    ...record,
    __capture: own?.mistyped === undefined ? fields : { ...fields, mistyped: own.mistyped },
  };
}

const NOT_RECORDED: Answer = refused(404, [bareError('not found')]);

/** The leader's own words for a change opened or renamed without a title (`git::open_change`). */
const NO_TITLE = 'a change needs a title: it is what the deploy commit is called';
/** The reason the leader gives a change closed without one (`git::NO_REASON`). */
const NO_REASON = 'Closed without a reason';

/** What the demo says to a write it does not play. */
export const DEMO_REFUSAL =
  "The demo can't do this. It runs in your browser, but you can download the full smeltr to try it: https://smeltr.sh/get/";

/** What the GUI sent as text, inflated when the GUI gzipped it. */
async function sentText(init?: RequestInit): Promise<string | null> {
  const body = init?.body;
  if (typeof body === 'string') return body;
  if (body == null || new Headers(init?.headers).get('content-encoding') !== 'gzip') return null;
  const stream = new Response(body).body!.pipeThrough(new DecompressionStream('gzip'));
  return new Response(stream).text();
}

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
    const body = await sentText(init);
    const answer = await leader.answer(method, url, body);
    if (method !== 'GET') window.dispatchEvent(new Event(CHANGED_EVENT));
    return new Response(JSON.stringify(answer.body), {
      status: answer.status,
      headers: { 'content-type': 'application/json' },
    });
  };
}
