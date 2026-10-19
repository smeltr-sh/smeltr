import { describe, expect, it } from 'vitest';
import type { MetricSampleDto, ThreadHealth, ThreadKind } from './api';
import { PART_METRIC, isOver, workerMemory } from './memoryParts';

function gauge(metric: string, labels: [string, string][], value: number): MetricSampleDto {
  return { metric, labels, value: { kind: 'gauge', value } };
}

const part = (name: string, max: number, owner?: [string, string]) =>
  gauge(PART_METRIC, owner ? [['part', name], owner] : [['part', name]], max);

function thread(
  kind: ThreadKind,
  metrics: MetricSampleDto[],
  threadId: string = kind,
  lastSeenMillisAgo = 0,
): ThreadHealth {
  return {
    workerId: 'w1',
    threadId,
    groupId: 'g',
    kind,
    partitionsOwned: [],
    lastSeenMillisAgo,
    stopAfterUnreachableSecs: null,
    commit: null,
    metrics,
  };
}

/** Every part, the supervisor's first. */
const all = (memory: ReturnType<typeof workerMemory>) =>
  memory ? [memory.supervisor, ...memory.runtime.parts].filter((p) => p !== null) : [];

const find = (memory: ReturnType<typeof workerMemory>, name: string, ownerId?: string) =>
  all(memory).find((p) => p.part === name && (p.owner?.id ?? undefined) === ownerId);

describe('workerMemory', () => {
  it('is null for a worker that states no parts', () => {
    expect(workerMemory([thread('worker', [gauge('smeltr_memory_allocated_bytes', [], 5)])])).toBe(
      null,
    );
    expect(workerMemory([])).toBe(null);
  });

  it('matches each destination queue to its own destination', () => {
    const memory = workerMemory([
      thread('worker', [
        part('buffer', 100, ['destination', 'a']),
        part('buffer', 200, ['destination', 'b']),
      ]),
      thread('egress', [gauge('smeltr_destination_queue_bytes', [['destination', 'a']], 10)], 'e1'),
      thread('egress', [gauge('smeltr_destination_queue_bytes', [['destination', 'b']], 20)], 'e2'),
    ]);
    expect(find(memory, 'buffer', 'a')?.currentBytes).toBe(10);
    expect(find(memory, 'buffer', 'b')?.currentBytes).toBe(20);
    expect(memory?.expectedMaxBytes).toBe(300);
  });

  it('matches what a source holds to its own source, not to a destination of the same id', () => {
    const memory = workerMemory([
      thread('worker', [
        part('reserve', 64, ['source', 'x']),
        part('reserve', 32, ['destination', 'x']),
      ]),
      thread('reader', [gauge('smeltr_source_held_bytes', [['source', 'x']], 7)]),
    ]);
    expect(all(memory).find((p) => p.owner?.kind === 'source')?.currentBytes).toBe(7);
    expect(all(memory).find((p) => p.owner?.kind === 'destination')?.currentBytes).toBe(null);
  });

  it('reads a Kafka inbox off its own source, and the lanes as every lane together', () => {
    const memory = workerMemory([
      thread('worker', [part('lanes', 100), part('inbox', 64, ['source', 'k'])]),
      thread(
        'reader',
        [
          gauge('smeltr_source_lane_bytes', [['source', 'k']], 12),
          gauge('smeltr_source_held_bytes', [['source', 'k']], 5),
        ],
        'r1',
      ),
      thread('reader', [gauge('smeltr_source_lane_bytes', [['source', 'j']], 30)], 'r2'),
    ]);
    expect(find(memory, 'inbox', 'k')?.currentBytes).toBe(5);
    expect(find(memory, 'lanes')?.currentBytes).toBe(42);
  });

  it("reads each source's own part off what it holds, but never its Kafka client", () => {
    const memory = workerMemory([
      thread('worker', [
        part('record', 64, ['source', 'o']),
        part('answer', 64, ['source', 'a']),
        part('result', 64, ['source', 's']),
        part('inbox', 64, ['source', 'k']),
        part('kafka client', 64, ['source', 'k']),
      ]),
      thread('reader', [
        gauge('smeltr_source_held_bytes', [['source', 'o']], 1),
        gauge('smeltr_source_held_bytes', [['source', 'a']], 2),
        gauge('smeltr_source_held_bytes', [['source', 's']], 3),
        gauge('smeltr_source_held_bytes', [['source', 'k']], 4),
      ]),
    ]);
    expect(find(memory, 'record', 'o')?.currentBytes).toBe(1);
    expect(find(memory, 'answer', 'a')?.currentBytes).toBe(2);
    expect(find(memory, 'result', 's')?.currentBytes).toBe(3);
    expect(find(memory, 'inbox', 'k')?.currentBytes).toBe(4);
    expect(find(memory, 'kafka client', 'k')?.currentBytes).toBe(null);
  });

  it("reads a destination's open files off what they buffer", () => {
    const memory = workerMemory([
      thread('worker', [part('open files', 64, ['destination', 'd'])]),
      thread('egress', [gauge('smeltr_destination_open_bytes', [['destination', 'd']], 9)]),
    ]);
    expect(find(memory, 'open files', 'd')?.currentBytes).toBe(9);
  });

  it("reads the supervisor's part off its process, and the runtime's process as the runtime's whole", () => {
    const memory = workerMemory([
      thread('worker', [
        part('supervisor', 150),
        part('runtime', 150),
        gauge('smeltr_process_memory_bytes', [['process', 'supervisor']], 40),
        gauge('smeltr_process_memory_bytes', [['process', 'runtime']], 400),
      ]),
    ]);
    expect(find(memory, 'supervisor')?.currentBytes).toBe(40);
    expect(memory?.supervisor?.currentBytes).toBe(40);
    expect(find(memory, 'runtime')?.currentBytes).toBe(null);
    expect(memory?.runtime.whole).toMatchObject({ currentBytes: 400, maxBytes: 150 });
    expect(isOver(memory!.runtime.whole)).toBe(true);
    expect(memory?.expectedMaxBytes).toBe(300);
  });

  it('keeps a reserve over its max as it is', () => {
    const memory = workerMemory([
      thread('worker', [part('reserve', 100, ['destination', 'd'])]),
      thread('egress', [gauge('smeltr_destination_queue_bytes', [['destination', 'd']], 250)]),
    ]);
    const reserve = find(memory, 'reserve', 'd')!;
    expect(reserve.currentBytes).toBe(250);
    expect(isOver(reserve)).toBe(true);
  });

  it('leaves a part nothing measures absent, not zero', () => {
    const memory = workerMemory([
      thread('worker', [
        part('supervisor', 10),
        part('columnar', 50),
        part('senders', 8, ['destination', 'd']),
      ]),
    ]);
    for (const p of all(memory)) {
      expect(p.currentBytes).toBe(null);
      expect(isOver(p)).toBe(false);
    }
    expect(find(memory, 'columnar')?.peak).toBe(false);
  });

  it('reads the pools off the worker, and columnar as the highest transform peak', () => {
    const memory = workerMemory([
      thread('worker', [
        part('columnar', 500),
        part('destination pool', 300),
        part('receiver pool', 200),
        gauge('smeltr_destination_pool_bytes', [], 30),
        gauge('smeltr_receiver_pool_bytes', [], 0),
      ]),
      thread('transform', [gauge('smeltr_memory_peak_bytes', [], 40)], 't1'),
      thread('transform', [gauge('smeltr_memory_peak_bytes', [], 90)], 't2'),
    ]);
    expect(find(memory, 'destination pool')?.currentBytes).toBe(30);
    // A measured zero is a reading, unlike a part nothing measures.
    expect(find(memory, 'receiver pool')?.currentBytes).toBe(0);
    expect(find(memory, 'columnar')).toMatchObject({ currentBytes: 90, peak: true });
  });

  it('orders worker parts, then sources, then destinations', () => {
    const memory = workerMemory([
      thread('worker', [
        part('buffer', 1, ['destination', 'd']),
        part('inbox', 1, ['source', 's']),
        part('runtime', 1),
        part('supervisor', 1),
      ]),
    ]);
    expect(all(memory).map((p) => p.part)).toEqual(['supervisor', 'runtime', 'inbox', 'buffer']);
  });

  it('takes the parts from the freshest worker thread alone', () => {
    const memory = workerMemory([
      thread('worker', [part('supervisor', 1), part('lanes', 1)], 'w-old', 60_000),
      thread('worker', [part('supervisor', 2)], 'w-new', 100),
    ]);
    expect(memory?.supervisor).toEqual({
      part: 'supervisor',
      owner: null,
      maxBytes: 2,
      currentBytes: null,
      peak: false,
    });
    expect(memory?.runtime.parts).toEqual([]);
  });

  it('shows nothing once the deployment stops, though its retired rows remain', () => {
    const memory = workerMemory([
      thread('worker', [part('supervisor', 1)], 'deploy', 60_000),
      thread(
        'egress',
        [gauge('smeltr_destination_queue_bytes', [['destination', 'a']], 10)],
        'e1',
        60_000,
      ),
      thread('worker', [], 'bootstrap', 100),
    ]);
    expect(memory).toBe(null);
  });
});
