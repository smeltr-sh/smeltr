import { describe, expect, it } from 'vitest';
import {
  canAsk,
  canRemove,
  countWorkers,
  computeWorkerStatus,
  filterWorkers,
  listensAt,
  rollout,
  rollupWorkers,
  runningThreads,
  workerHealth,
} from './workers';
import type {
  ResourceReport,
  ThreadHealth,
  ThreadKind,
  WarningDto,
  WorkerCredentialDto,
} from './api';

const SILENT_AFTER_MS = 4_500;

function thread(
  workerId: string,
  threadId: string,
  opts: {
    lastSeen?: number;
    groupId?: string;
    partitions?: number;
    kind?: ThreadKind;
    stopAfterUnreachableSecs?: number | null;
    commit?: string | null;
    resources?: ResourceReport;
  } = {},
): ThreadHealth {
  const {
    lastSeen = 100,
    groupId = 'ingest-1',
    partitions = 0,
    kind = 'reader',
    stopAfterUnreachableSecs = null,
    commit = null,
    resources,
  } = opts;
  return {
    workerId,
    threadId,
    groupId,
    kind,
    partitionsOwned: Array.from({ length: partitions }, (_, i) => ({
      topic: 'logs',
      partition: i,
    })),
    lastSeenMillisAgo: lastSeen,
    stopAfterUnreachableSecs,
    commit,
    ...(resources !== undefined ? { resources } : {}),
    metrics: [],
  };
}

const w1: WorkerCredentialDto = { workerId: 'w1', revoked: false };

describe('computeWorkerStatus', () => {
  it('revoked wins even over a fresh heartbeat', () => {
    const revoked: WorkerCredentialDto = { workerId: 'w1', revoked: true };
    expect(computeWorkerStatus(revoked, [thread('w1', 't0')], SILENT_AFTER_MS)).toBe('revoked');
  });

  it('a thread heartbeating within the cutoff is online, past it silent', () => {
    expect(computeWorkerStatus(w1, [thread('w1', 't0', { lastSeen: 1000 })], SILENT_AFTER_MS)).toBe(
      'online',
    );
    expect(
      computeWorkerStatus(w1, [thread('w1', 't0', { lastSeen: 20_000 })], SILENT_AFTER_MS),
    ).toBe('silent');
  });

  it('one fresh thread among silent ones is enough for online', () => {
    const threads = [
      thread('w1', 't0', { lastSeen: 20_000 }),
      thread('w1', 't1', { lastSeen: 500 }),
      thread('w1', 't2', { lastSeen: 30_000 }),
    ];
    expect(computeWorkerStatus(w1, threads, SILENT_AFTER_MS)).toBe('online');
  });

  it('no thread of its own never heartbeated, whatever other workers report', () => {
    expect(computeWorkerStatus(w1, [], SILENT_AFTER_MS)).toBe('neverHeartbeated');
    expect(computeWorkerStatus(w1, [thread('w2', 't0')], SILENT_AFTER_MS)).toBe('neverHeartbeated');
  });

  it('a worker whose last word was that it is stopping is stopped, not silent', () => {
    const stopped = { ...thread('w1', 't0', { lastSeen: 60_000 }), stoppedAtMillis: 1 };
    expect(computeWorkerStatus(w1, [stopped], SILENT_AFTER_MS)).toBe('stopped');
    expect(
      computeWorkerStatus({ workerId: 'w1', revoked: true }, [stopped], SILENT_AFTER_MS),
      'a revocation is still the operator’s last word',
    ).toBe('revoked');
    expect(countWorkers(rollupWorkers([w1], [stopped], SILENT_AFTER_MS))).toEqual({
      online: 0,
      silent: 0,
      neverHeartbeated: 0,
      revoked: 0,
      stopped: 1,
    });
  });
});

describe('rollupWorkers', () => {
  it('attaches each worker only its own threads, ordered by thread id', () => {
    const workers: WorkerCredentialDto[] = [{ workerId: 'w2', revoked: false }, w1];
    const threads = [thread('w1', 't2'), thread('w2', 't0'), thread('w1', 't1')];
    const rows = rollupWorkers(workers, threads, SILENT_AFTER_MS);

    expect(rows.map((r) => r.workerId)).toEqual(['w1', 'w2']);
    expect(rows[0].threads.map((t) => t.threadId)).toEqual(['t1', 't2']);
    expect(rows[1].threads.map((t) => t.threadId)).toEqual(['t0']);
  });

  it('sums partitions and reports the freshest heartbeat', () => {
    const threads = [
      thread('w1', 't0', { lastSeen: 9_000, partitions: 3 }),
      thread('w1', 't1', { lastSeen: 300, partitions: 2 }),
    ];
    const [row] = rollupWorkers([w1], threads, SILENT_AFTER_MS);

    expect(row.partitionsOwned).toBe(5);
    expect(row.lastSeenMillisAgo).toBe(300);
    expect(row.groupId).toBe('ingest-1');
  });

  it('a worker with no threads still gets a row', () => {
    const [row] = rollupWorkers([w1], [], SILENT_AFTER_MS);

    expect(row.status).toBe('neverHeartbeated');
    expect(row.threads).toEqual([]);
    expect(row.lastSeenMillisAgo).toBeNull();
    expect(row.groupId).toBeNull();
    expect(row.stopAfterUnreachableSecs).toBeNull();
    expect(row.commit).toBeNull();
    expect(row.resources).toBeNull();
  });

  it('carries the kind the roster holds, and none for a worker it does not know', () => {
    const roster: WorkerCredentialDto[] = [
      { workerId: 'vm', revoked: false, kind: 'permanent' },
      { workerId: 'pod', revoked: false, kind: 'ephemeral' },
    ];
    const rows = rollupWorkers(roster, [thread('unrostered', 't0')], SILENT_AFTER_MS);

    expect(rows.map((r) => [r.workerId, r.kind])).toEqual([
      ['pod', 'ephemeral'],
      ['unrostered', null],
      ['vm', 'permanent'],
    ]);
  });

  it('reports what the worker itself started with, not a per-group setting', () => {
    const threads = [thread('w1', 't0', { stopAfterUnreachableSecs: 300 }), thread('w2', 't0')];
    const rows = rollupWorkers([w1, { workerId: 'w2', revoked: false }], threads, SILENT_AFTER_MS);

    expect(rows.find((r) => r.workerId === 'w1')?.stopAfterUnreachableSecs).toBe(300);
    expect(rows.find((r) => r.workerId === 'w2')?.stopAfterUnreachableSecs).toBeNull();
  });

  it('reports the commit each worker says it is running, which may differ across a group', () => {
    const threads = [
      thread('w1', 't0', { commit: 'c0ffee' }),
      thread('w2', 't0', { commit: 'deadbee' }),
      thread('w3', 't0'),
    ];
    const rows = rollupWorkers(null, threads, SILENT_AFTER_MS);

    expect(rows.find((r) => r.workerId === 'w1')?.commit).toBe('c0ffee');
    expect(rows.find((r) => r.workerId === 'w2')?.commit).toBe('deadbee');
    expect(rows.find((r) => r.workerId === 'w3')?.commit).toBeNull();
  });

  it('reports what the worker itself is spending, off its freshest thread like commit', () => {
    const resources: ResourceReport = {
      cpuPercent: 42,
      cpuLimited: true,
      memoryUsedBytes: 100,
      memoryLimitBytes: 200,
      memoryLimited: true,
    };
    const threads = [thread('w1', 't0', { resources })];
    const [row] = rollupWorkers([w1], threads, SILENT_AFTER_MS);

    expect(row.resources).toEqual(resources);
  });

  it('reports where its receivers listen, off its freshest thread like commit', () => {
    const listens = [{ source: 'web', transport: 'tcp' as const, address: '[::]:8088' }];
    const threads = [{ ...thread('w1', 't0'), supervising: { listens, hostName: 'host-a' } }];
    const [row] = rollupWorkers([w1], threads, SILENT_AFTER_MS);

    expect(row.listens).toEqual(listens);
    expect(row.hostName).toBe('host-a');
  });

  it('reads every address as the host, and as [::] on a host with no name', () => {
    const at = (address: string, host: string | null) =>
      listensAt({ source: 's', transport: 'tcp', address }, host);

    expect(at('[::]:8088', 'host-a')).toBe('host-a:8088');
    expect(at('0.0.0.0:8088', 'host-a')).toBe('host-a:8088');
    expect(at('0.0.0.0:8088', null)).toBe('[::]:8088');
    expect(at('10.0.0.5:8090', 'host-a')).toBe('10.0.0.5:8090');
    expect(at('[fe80::1]:514', 'host-a')).toBe('[fe80::1]:514');
  });

  it('reads a per-worker fact off the thread that spoke last, not the first by id', () => {
    // A worker's `-bootstrap` report is written once, before anything deploys, and the registry
    // never evicts it -- so it sits beside the live threads carrying no commit and a stale age.
    const threads = [
      thread('w1', 'a-bootstrap', { lastSeen: 90_000, commit: null }),
      thread('w1', 'z-reader', { lastSeen: 200, commit: 'c0ffee' }),
    ];
    const [row] = rollupWorkers(null, threads, SILENT_AFTER_MS);

    expect(row.commit).toBe('c0ffee');
    expect(row.lastSeenMillisAgo).toBe(200);
  });

  it('survives a failed store read: threads alone still produce rows', () => {
    const rows = rollupWorkers(null, [thread('w1', 't0')], SILENT_AFTER_MS);

    expect(rows.map((r) => r.workerId)).toEqual(['w1']);
    expect(rows[0].status).toBe('online');
  });

  it('a heartbeating worker missing from the roster is still listed', () => {
    const rows = rollupWorkers([w1], [thread('w2', 't0')], SILENT_AFTER_MS);

    expect(rows.map((r) => r.workerId)).toEqual(['w1', 'w2']);
  });

  it('a revoked worker keeps its threads on the row', () => {
    const rows = rollupWorkers(
      [{ workerId: 'w1', revoked: true }],
      [thread('w1', 't0', { lastSeen: 50 })],
      SILENT_AFTER_MS,
    );

    expect(rows[0].status).toBe('revoked');
    expect(rows[0].threads).toHaveLength(1);
  });
});

describe('runningThreads', () => {
  it('does not count the worker itself: the bootstrap report, or the deploy gauges', () => {
    expect(runningThreads([thread('w1', 'w1-bootstrap', { kind: 'worker' })])).toBe(0);
    expect(
      runningThreads([
        thread('w1', 'reader-0', { kind: 'reader' }),
        thread('w1', 'deploy', { kind: 'worker' }),
      ]),
    ).toBe(1);
  });

  it('counts every kind of real thread, and one that states no kind', () => {
    const threads = [
      thread('w1', 'reader-0', { kind: 'reader' }),
      thread('w1', 'transform-0', { kind: 'transform' }),
      thread('w1', 'egress-0', { kind: 'egress' }),
      // A worker older than the pool.
      { ...thread('w1', 'legacy'), kind: null },
    ];
    expect(runningThreads(threads)).toBe(4);
  });
});
describe('status page worker helpers', () => {
  const rows = rollupWorkers(
    [
      { workerId: 'alpha', revoked: false },
      { workerId: 'bravo', revoked: false },
      { workerId: 'charlie', revoked: true },
    ],
    [
      thread('alpha', 't0', { groupId: 'edge', commit: 'c0ffee' }),
      thread('bravo', 't0', { lastSeen: 30_000, groupId: 'core', commit: 'deadbee' }),
      thread('charlie', 't0', { groupId: 'edge' }),
    ],
    SILENT_AFTER_MS,
  );

  it('counts the roster without treating a revoked heartbeat as online', () => {
    expect(countWorkers(rows)).toEqual({
      online: 1,
      silent: 1,
      neverHeartbeated: 0,
      revoked: 1,
      stopped: 0,
    });
  });

  it('filters by status and any identifying worker field', () => {
    expect(filterWorkers(rows, '', 'silent').map((row) => row.workerId)).toEqual(['bravo']);
    expect(filterWorkers(rows, 'edge', 'all').map((row) => row.workerId)).toEqual([
      'alpha',
      'charlie',
    ]);
    expect(filterWorkers(rows, 'DEADBEE', 'all').map((row) => row.workerId)).toEqual(['bravo']);
  });
});

describe('worker actions', () => {
  const both: WorkerCredentialDto = {
    workerId: 'w1',
    revoked: false,
    accepts: ['restart', 'shutdown'],
  };

  it('asks only a heartbeating worker, and only what it said it does', () => {
    const [live] = rollupWorkers([both], [thread('w1', 't0')], SILENT_AFTER_MS);
    expect(canAsk(live, 'restart')).toBe(true);
    expect(canAsk(live, 'shutdown')).toBe(true);

    const [inLeader] = rollupWorkers(
      [{ ...both, accepts: ['restart'] }],
      [thread('w1', 't0')],
      SILENT_AFTER_MS,
    );
    expect(canAsk(inLeader, 'shutdown')).toBe(false);

    const [older] = rollupWorkers([w1], [thread('w1', 't0')], SILENT_AFTER_MS);
    expect(canAsk(older, 'restart')).toBe(false);

    const [never] = rollupWorkers([both], [], SILENT_AFTER_MS);
    expect(canAsk(never, 'restart')).toBe(false);
  });

  it('removes only a worker that is not heartbeating', () => {
    const [live] = rollupWorkers([both], [thread('w1', 't0')], SILENT_AFTER_MS);
    const [never] = rollupWorkers([both], [], SILENT_AFTER_MS);
    const [revoked] = rollupWorkers([{ ...both, revoked: true }], [], SILENT_AFTER_MS);
    expect(canRemove(live)).toBe(false);
    expect(canRemove(never)).toBe(true);
    expect(canRemove(revoked)).toBe(true);
  });

  it('carries what a worker has been asked and not yet taken', () => {
    const [row] = rollupWorkers(
      [{ ...both, asked: 'restart' }],
      [thread('w1', 't0')],
      SILENT_AFTER_MS,
    );
    expect(row.asked).toBe('restart');
  });
});

describe('rollout', () => {
  it('counts the online workers on the target, and leaves out a silent one', () => {
    const rows = rollupWorkers(
      null,
      [
        thread('a', 't0', { commit: 'c0ffee1234' }),
        thread('b', 't0', { commit: 'beef' }),
        thread('c', 't0', { commit: 'beef', lastSeen: 20_000 }),
      ],
      SILENT_AFTER_MS,
    );
    expect(rollout(rows, 'c0ffee1234')).toEqual({ on: 1, of: 2 });
  });

  it('matches an abbreviated commit either way round', () => {
    const rows = rollupWorkers(null, [thread('a', 't0', { commit: 'c0ffee' })], SILENT_AFTER_MS);
    expect(rollout(rows, 'c0ffee1234')).toEqual({ on: 1, of: 1 });
  });

  it('a worker running nothing is not on it', () => {
    const rows = rollupWorkers(null, [thread('a', 't0')], SILENT_AFTER_MS);
    expect(rollout(rows, 'c0ffee')).toEqual({ on: 0, of: 1 });
  });
});

describe('workerHealth', () => {
  function warning(
    subjectKind: WarningDto['subjectKind'],
    subjectId: string,
    group: string | null,
    severity: WarningDto['severity'],
  ): WarningDto {
    return {
      condition: 'sourceBlocked',
      subjectKind,
      subjectId,
      group,
      severity,
      sinceMillis: null,
      message: '',
    };
  }
  const [online] = rollupWorkers(null, [thread('w1', 't0', { groupId: 'g' })], SILENT_AFTER_MS);

  it('an online worker with no issues is healthy', () => {
    expect(workerHealth(online, [])).toBe('healthy');
  });

  it('a heartbeat that is not coming is failed', () => {
    const [silent] = rollupWorkers(
      null,
      [thread('w1', 't0', { lastSeen: 20_000 })],
      SILENT_AFTER_MS,
    );
    const [never] = rollupWorkers([w1], [], SILENT_AFTER_MS);
    const [revoked] = rollupWorkers([{ workerId: 'w1', revoked: true }], [], SILENT_AFTER_MS);
    expect([silent, never, revoked].map((r) => workerHealth(r, []))).toEqual([
      'failed',
      'failed',
      'failed',
    ]);
  });

  it('a stopped worker is not marked for its heartbeat alone', () => {
    const [stopped] = rollupWorkers(
      null,
      [{ ...thread('w1', 't0', { groupId: 'g' }), stoppedAtMillis: 1 }],
      SILENT_AFTER_MS,
    );
    expect(stopped.status).toBe('stopped');
    expect(workerHealth(stopped, [])).toBe('healthy');
    expect(workerHealth(stopped, [warning('source', 'in', 'g', 'failed')])).toBe('healthy');
  });

  it('an object in its group marks it, whatever the object is', () => {
    for (const kind of ['source', 'destination', 'pipeline', 'group', 'identity'] as const) {
      expect(workerHealth(online, [warning(kind, 'x', 'g', 'warning')])).toBe('warning');
    }
  });

  it('takes the worst of them', () => {
    expect(
      workerHealth(online, [
        warning('destination', 'out', 'g', 'warning'),
        warning('source', 'in', 'g', 'failed'),
      ]),
    ).toBe('failed');
  });

  it('a warning about itself marks it, one about another worker does not', () => {
    expect(workerHealth(online, [warning('worker', 'w1', 'g', 'warning')])).toBe('warning');
    expect(workerHealth(online, [warning('worker', 'w2', 'g', 'failed')])).toBe('healthy');
  });

  it('another group, the leader and a worker with no group are not marked', () => {
    expect(
      workerHealth(online, [
        warning('source', 'in', 'other', 'failed'),
        warning('leader', 'leader', null, 'failed'),
      ]),
    ).toBe('healthy');
    const [ungrouped] = rollupWorkers([w1], [], SILENT_AFTER_MS);
    expect(
      workerHealth({ ...ungrouped, status: 'online' }, [warning('group', 'g', null, 'failed')]),
    ).toBe('healthy');
  });
});
