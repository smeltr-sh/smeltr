// A worker's memory, part by part, as the fit allotted it (`src/worker/memory.rs`). The worker
// thread states each part's max as `smeltr_memory_part_bytes`; what a part holds now comes from
// whichever thread measures it. Most parts have no such measure, and those read as absent.

import type { MetricSampleDto, ThreadHealth } from './api';

/** Whose part this is: a source's, a destination's, or the worker's own. */
export type PartOwner = { kind: 'source' | 'destination'; id: string } | null;

export type MemoryPart = {
  part: string;
  owner: PartOwner;
  maxBytes: number;
  /** What it holds now, or `null` where nothing measures it -- never 0 in its place. */
  currentBytes: number | null;
  /** `currentBytes` is the highest seen, not a reading now. */
  peak: boolean;
};

/** The runtime process: everything the worker holds but the supervisor. */
export type RuntimeMemory = {
  /** The whole process now against its parts' max together; over it, something the parts leave out holds memory. */
  whole: MemoryPart;
  /** Its own overhead first, then the worker's other parts, then each source's, then each destination's. */
  parts: MemoryPart[];
};

export type WorkerMemory = {
  supervisor: MemoryPart | null;
  runtime: RuntimeMemory;
  /** What the worker expects to hold with every part full: each part's max together. */
  expectedMaxBytes: number;
};

export const PART_METRIC = 'smeltr_memory_part_bytes';

// The order a reader meets the parts in, and which owner each belongs to.
const WORKER_PARTS = [
  'supervisor',
  'runtime',
  'lanes',
  'columnar',
  'destination pool',
  'receiver pool',
];
const SOURCE_PARTS = ['inbox', 'kafka client', 'requests', 'reserve', 'record', 'answer', 'result'];
const DESTINATION_PARTS = ['buffer', 'reserve', 'senders', 'open files'];

function label(sample: MetricSampleDto, name: string): string | undefined {
  return sample.labels.find(([k]) => k === name)?.[1];
}

/** The threads of the worker's latest heartbeat, which all arrive in one request and so share an
 * age. The registry keeps a retired thread's row, and its last gauges with it, so a stopped
 * deployment's parts would otherwise outlive it. */
function current(threads: ThreadHealth[]): ThreadHealth[] {
  const freshest = Math.min(...threads.map((t) => t.lastSeenMillisAgo));
  return threads.filter((t) => t.lastSeenMillisAgo === freshest);
}

/** The latest value of `metric` whose `labelName` is `labelValue`, or with no such label asked,
 * the latest value at all. */
function latest(
  threads: ThreadHealth[],
  metric: string,
  labelName?: string,
  labelValue?: string,
): number | null {
  for (const t of threads) {
    for (const m of t.metrics) {
      if (m.metric !== metric) continue;
      if (labelName !== undefined && label(m, labelName) !== labelValue) continue;
      return m.value.value;
    }
  }
  return null;
}

/** Every value of `metric` added up, or `null` where none was reported. */
function total(threads: ThreadHealth[], metric: string): number | null {
  const values = threads.flatMap((t) => t.metrics).filter((m) => m.metric === metric);
  return values.length ? values.reduce((n, m) => n + m.value.value, 0) : null;
}

/** `>>> 0` sends a part this list does not know to the end of its owner's, rather than the front. */
function rank(p: MemoryPart): [number, string, number] {
  if (p.owner === null) return [0, '', WORKER_PARTS.indexOf(p.part) >>> 0];
  const order = p.owner.kind === 'source' ? SOURCE_PARTS : DESTINATION_PARTS;
  return [p.owner.kind === 'source' ? 1 : 2, p.owner.id, order.indexOf(p.part) >>> 0];
}

/** Each part of the worker's memory against its max, from that worker's own threads; `null` from
 * a worker that states no parts. */
export function workerMemory(threads: ThreadHealth[]): WorkerMemory | null {
  const fresh = current(threads);
  const stating = fresh.find((t) => t.metrics.some((m) => m.metric === PART_METRIC));
  if (!stating) return null;

  const columnarPeak = fresh
    .filter((t) => t.kind === 'transform')
    .flatMap((t) => t.metrics)
    .filter((m) => m.metric === 'smeltr_memory_peak_bytes')
    .reduce<number | null>((most, m) => Math.max(most ?? 0, m.value.value), null);

  const parts = stating.metrics
    .filter((m) => m.metric === PART_METRIC)
    .map((m): MemoryPart => {
      const part = label(m, 'part') ?? '';
      const source = label(m, 'source');
      const destination = label(m, 'destination');
      const owner: PartOwner =
        source !== undefined
          ? { kind: 'source', id: source }
          : destination !== undefined
            ? { kind: 'destination', id: destination }
            : null;
      let currentBytes: number | null = null;
      let peak = false;
      if (owner?.kind === 'destination' && (part === 'buffer' || part === 'reserve')) {
        currentBytes = latest(fresh, 'smeltr_destination_queue_bytes', 'destination', owner.id);
      } else if (owner?.kind === 'destination' && part === 'open files') {
        currentBytes = latest(fresh, 'smeltr_destination_open_bytes', 'destination', owner.id);
      } else if (owner?.kind === 'source' && part !== 'kafka client') {
        // Each source states one part of its own, and that part is what it holds.
        currentBytes = latest(fresh, 'smeltr_source_held_bytes', 'source', owner.id);
      } else if (owner === null && part === 'lanes') {
        currentBytes = total(fresh, 'smeltr_source_lane_bytes');
      } else if (owner === null && part === 'supervisor') {
        // The supervisor holds no buffers, so its whole process is its part. The runtime's whole process holds every
        // other part too, so it measures the runtime as a whole, not its own part.
        currentBytes = latest(fresh, 'smeltr_process_memory_bytes', 'process', 'supervisor');
      } else if (owner === null && part === 'destination pool') {
        currentBytes = latest(fresh, 'smeltr_destination_pool_bytes');
      } else if (owner === null && part === 'receiver pool') {
        currentBytes = latest(fresh, 'smeltr_receiver_pool_bytes');
      } else if (owner === null && part === 'columnar') {
        currentBytes = columnarPeak;
        peak = currentBytes !== null;
      }
      return { part, owner, maxBytes: m.value.value, currentBytes, peak };
    })
    .sort((a, b) => {
      const [ka, ia, pa] = rank(a);
      const [kb, ib, pb] = rank(b);
      return ka - kb || ia.localeCompare(ib) || pa - pb || a.part.localeCompare(b.part);
    });

  const isSupervisor = (p: MemoryPart) => p.owner === null && p.part === 'supervisor';
  const inRuntime = parts.filter((p) => !isSupervisor(p));
  const max = (ps: MemoryPart[]) => ps.reduce((n, p) => n + p.maxBytes, 0);
  return {
    supervisor: parts.find(isSupervisor) ?? null,
    runtime: {
      whole: {
        part: 'runtime',
        owner: null,
        maxBytes: max(inRuntime),
        currentBytes: latest(fresh, 'smeltr_process_memory_bytes', 'process', 'runtime'),
        peak: false,
      },
      parts: inRuntime,
    },
    expectedMaxBytes: max(parts),
  };
}

/** A part holding more than its max. A reserve does this by design: its queue borrows from the
 * pool above it. */
export function isOver(p: MemoryPart): boolean {
  return p.currentBytes !== null && p.currentBytes > p.maxBytes;
}
