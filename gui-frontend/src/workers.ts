// The status rollup behind the Cluster page's Workers table. `ThreadHealth`
// only reflects a thread that is currently heartbeating, so a credential
// with no matching thread is "not heartbeating now", a state distinct from
// "never existed"; this is what makes it visible.

import type {
  DirectiveAction,
  Listening,
  ResourceReport,
  ThreadHealth,
  WorkerCredentialDto,
  WarningDto,
  WarningSeverity,
  WorkerKind,
} from './api';

export type WorkerStatus = 'revoked' | 'online' | 'silent' | 'neverHeartbeated' | 'stopped';

/** Revoked beats everything; then the freshest of the worker's own
 * threads decides, against the leader's own `silentAfterMs` so this page
 * and `workerSilent` agree. `neverHeartbeated` covers both "enrolled but never started" and
 * "was heartbeating before the leader restarted": the registry is
 * ephemeral and cannot tell those apart. `stopped` is a worker whose last
 * heartbeat said it was stopping: its quiet since is what it announced. */
export function computeWorkerStatus(
  worker: WorkerCredentialDto,
  threads: ThreadHealth[],
  silentAfterMs: number,
): WorkerStatus {
  if (worker.revoked) return 'revoked';
  const ownThreads = threads.filter((t) => t.workerId === worker.workerId);
  if (ownThreads.length === 0) return 'neverHeartbeated';
  if (freshest(ownThreads)?.stoppedAtMillis !== undefined) return 'stopped';
  return ownThreads.some((t) => t.lastSeenMillisAgo <= silentAfterMs) ? 'online' : 'silent';
}

/** How many of these are threads. A `worker` report is the process
 * itself -- the bootstrap placeholder, or the deploy-time gauges -- and
 * is no thread. A worker older than the pool states no kind, and its
 * report is a thread. */
export function runningThreads(threads: ThreadHealth[]): number {
  return threads.filter((t) => t.kind !== 'worker').length;
}

/** The worker's most recent word about itself. `threads` is ordered by id so
 * the table does not reshuffle, but a retired thread keeps its row in the
 * registry forever -- a worker's `-bootstrap` report outlives the heartbeat
 * that wrote it -- so the first by id can be the oldest answer. The freshest
 * thread is the one that spoke last, which is what a per-worker fact means. */
function freshest(threads: ThreadHealth[]): ThreadHealth | undefined {
  return threads.reduce<ThreadHealth | undefined>(
    (best, t) => (best === undefined || t.lastSeenMillisAgo < best.lastSeenMillisAgo ? t : best),
    undefined,
  );
}

/** One row of the Workers table. */
export type WorkerRow = {
  workerId: string;
  status: WorkerStatus;
  /** What its being gone means, from the enrollment code that brought it in;
   * `null` for a worker heartbeating without a roster entry. */
  kind: WorkerKind | null;
  /** Ordered by thread id so the list does not reshuffle between polls. */
  threads: ThreadHealth[];
  partitionsOwned: number;
  /** The group this worker's threads report, or `null` with no threads.
   * Every thread in a process takes its group from that process's
   * bootstrap config. */
  groupId: string | null;
  /** Freshest heartbeat across the worker's threads; `null` with none. */
  lastSeenMillisAgo: number | null;
  /** This worker's own startup setting (`smeltr.toml`), or `null` with no
   * threads or no limit -- a worker never stops on its own by default, and
   * two workers in the same group can set this differently. */
  stopAfterUnreachableSecs: number | null;
  /** The commit this worker reported running, or `null` with no threads or
   * nothing deployed. Every thread in a process reports its process's, so
   * the first is the worker's. */
  commit: string | null;
  /** This worker's own build, or `null` with no threads or one older than the field. */
  smeltrVersion: string | null;
  /** The release that build is, or `null` on any other build or one older than the field. */
  release: string | null;
  /** What this worker is spending against what it is allowed to run in -- its freshest thread's
   * word, the same as `commit`. `null` with no threads, or one older than the field. */
  resources: ResourceReport | null;
  /** Where each receiver it runs listens, by its freshest thread's word; empty while nothing runs. */
  listens: Listening[];
  /** The name its host goes by, or `null` where the host would not say. */
  hostName: string | null;
  /** What it does when asked; empty for a worker that does nothing. */
  accepts: DirectiveAction[];
  /** What it has been asked and has not yet taken. */
  asked: DirectiveAction | null;
};

/** What a worker can be asked right now: only while it is heartbeating. A
 * silent worker may be gone without a word, and would take the request
 * whenever it came back. */
export function canAsk(row: WorkerRow, action: DirectiveAction): boolean {
  return row.status === 'online' && row.accepts.includes(action);
}

/** Whether a worker can be removed: not while it is heartbeating, since its
 * process would go on moving data with nobody able to see it. */
export function canRemove(row: WorkerRow): boolean {
  return row.status !== 'online';
}

/** The union of the credential roster and the live threads, each worker
 * with its own threads attached. The roster is the only place a worker
 * that never started, or was revoked, appears; the threads are the only
 * place live detail lives, and they survive a failed store read, which
 * is when `workers` arrives `null`. A worker heartbeating without a
 * roster entry is assumed not revoked. */
export function rollupWorkers(
  workers: WorkerCredentialDto[] | null,
  threads: ThreadHealth[],
  silentAfterMs: number,
): WorkerRow[] {
  const byId = new Map<string, WorkerCredentialDto>();
  for (const w of workers ?? []) byId.set(w.workerId, w);
  for (const t of threads) {
    if (!byId.has(t.workerId)) byId.set(t.workerId, { workerId: t.workerId, revoked: false });
  }

  return [...byId.values()]
    .map((w) => {
      const own = threads
        .filter((t) => t.workerId === w.workerId)
        .sort((a, b) => a.threadId.localeCompare(b.threadId));
      const latest = freshest(own);
      return {
        workerId: w.workerId,
        status: computeWorkerStatus(w, threads, silentAfterMs),
        kind: w.kind ?? null,
        threads: own,
        partitionsOwned: own.reduce((n, t) => n + t.partitionsOwned.length, 0),
        groupId: latest?.groupId ?? null,
        lastSeenMillisAgo: latest?.lastSeenMillisAgo ?? null,
        stopAfterUnreachableSecs: latest?.stopAfterUnreachableSecs ?? null,
        commit: latest?.commit ?? null,
        smeltrVersion: latest?.smeltrVersion ?? null,
        release: latest?.release ?? null,
        resources: latest?.resources ?? null,
        listens: latest?.supervising?.listens ?? [],
        hostName: latest?.supervising?.hostName ?? null,
        accepts: w.accepts ?? [],
        asked: w.asked ?? null,
      };
    })
    .sort((a, b) => a.workerId.localeCompare(b.workerId));
}
/** How a worker's row reads on Status: the worst of its heartbeat and the issues on what it runs. */
export type WorkerHealth = 'healthy' | WarningSeverity;

/** The worst of the worker's heartbeat (silent, never heartbeated and revoked are failed), the
 * warnings about the worker itself, and the warnings about the sources, destinations,
 * pipelines and group of its group. A warning names a group, not the worker running
 * its subject, so an object's warning lands on every worker in that group. */
export function workerHealth(row: WorkerRow, warnings: WarningDto[]): WorkerHealth {
  if (row.status === 'silent' || row.status === 'neverHeartbeated' || row.status === 'revoked') {
    return 'failed';
  }
  const own = warnings.filter((w) =>
    w.subjectKind === 'worker'
      ? w.subjectId === row.workerId
      : // A stopped worker runs nothing in its group.
        row.status === 'online' &&
        GROUP_OBJECTS.has(w.subjectKind) &&
        row.groupId !== null &&
        w.group === row.groupId,
  );
  if (own.some((w) => w.severity === 'failed')) return 'failed';
  return own.length > 0 ? 'warning' : 'healthy';
}

const GROUP_OBJECTS: ReadonlySet<WarningDto['subjectKind']> = new Set([
  'source',
  'destination',
  'pipeline',
  'group',
]);

/** Counts a displayed roster by the same status labels the Status page uses. */
export type WorkerCounts = Record<WorkerStatus, number>;

export function countWorkers(rows: WorkerRow[]): WorkerCounts {
  return rows.reduce<WorkerCounts>(
    (counts, row) => {
      counts[row.status] += 1;
      return counts;
    },
    { revoked: 0, online: 0, silent: 0, neverHeartbeated: 0, stopped: 0 },
  );
}

/** Narrow the locally held roster. The leader already delivered every worker;
 * this only decides which rows are visible, so no status is inferred here. */
export function filterWorkers(
  rows: WorkerRow[],
  query: string,
  status: WorkerStatus | 'all',
): WorkerRow[] {
  const needle = query.trim().toLocaleLowerCase();
  return rows.filter((row) => {
    if (status !== 'all' && row.status !== status) return false;
    if (!needle) return true;
    return [row.workerId, row.groupId ?? '', row.commit ?? ''].some((value) =>
      value.toLocaleLowerCase().includes(needle),
    );
  });
}

/** How far a deploy has reached: of the online workers, how many report running `target`.
 * A worker that is not heartbeating cannot take a deploy, so it is left out rather than
 * holding the count short forever. */
export type Rollout = { on: number; of: number };

export function rollout(rows: WorkerRow[], target: string): Rollout {
  const online = rows.filter((w) => w.status === 'online');
  return { on: online.filter((w) => onCommit(w, target)).length, of: online.length };
}

/** Whether a worker runs `target`. Either side may be abbreviated. */
export function onCommit(row: WorkerRow, target: string): boolean {
  return (
    !!row.commit && !!target && (row.commit.startsWith(target) || target.startsWith(row.commit))
  );
}

/** Where a sender reaches `listening`: its address, or, on every address of the host, the host's name. A host with
 * no name reads `[::]`, every address of either family. */
export function listensAt(listening: Listening, hostName: string | null): string {
  const colon = listening.address.lastIndexOf(':');
  const ip = listening.address.slice(0, colon);
  const port = listening.address.slice(colon + 1);
  const every = ip === '0.0.0.0' || ip === '[::]';
  return `${every ? (hostName ?? '[::]') : ip}:${port}`;
}
