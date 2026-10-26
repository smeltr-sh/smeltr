import { describe, expect, it } from 'vitest';
import {
  bufferFor,
  DEFAULT_BATCH_BYTES,
  estimateLink,
  IN_FLIGHT,
  MESSAGE_OVERHEAD_BYTES,
  MIN_LINGER_MS,
} from './linkEstimate';

// The same cases `derive_sizes`' test pins in `src/io/kafka.rs`, on the producer's share.
const MiB = 1024 * 1024;

describe('estimateLink', () => {
  it('a near broker with a small buffer gets librdkafka’s own sizes', () => {
    const near = estimateLink({
      bufferBytes: 128 * MiB,
      roundTripMs: 2,
      partitions: 64,
      brokerCeiling: 1_048_588,
      recordBytes: 300,
    });
    expect(near.producerBytes).toBe(64 * MiB);
    expect(near.batchBytes).toBe(DEFAULT_BATCH_BYTES);
    expect(near.lingerMs).toBe(MIN_LINGER_MS);
    expect(near.requestsInFlight).toBe(Math.floor((64 * MiB) / DEFAULT_BATCH_BYTES));
  });

  it('a far broker spreads the producer’s share over the partitions and the requests in flight', () => {
    const far = estimateLink({
      bufferBytes: 128 * MiB,
      roundTripMs: 120,
      partitions: 12,
      brokerCeiling: 8 * MiB,
      recordBytes: MESSAGE_OVERHEAD_BYTES,
    });
    expect(far.batchBytes).toBe(Math.floor((64 * MiB) / 12 / 5));
    expect(far.lingerMs).toBe(60);
    expect(far.requestsInFlight).toBe(12 * 5);
    // Records as large as their overhead: half the share is theirs, held a linger and a round trip.
    expect(far.inFlightBytes).toBe((64 * MiB) / 2);
    expect(far.ceilingPerSecond).toBeCloseTo((32 * MiB) / 0.18, -3);
  });

  it('a batch the broker caps gets more requests in flight, so the buffer is still on the wire', () => {
    const capped = estimateLink({
      bufferBytes: 2 * 1024 * MiB,
      roundTripMs: 300,
      partitions: 1,
      brokerCeiling: 8 * MiB,
      recordBytes: 1_000_000,
    });
    expect(capped.batchBytes).toBe(8 * MiB);
    expect(capped.requestsInFlight).toBe(1024 / 8);
    expect(capped.inFlightBytes).toBeCloseTo(1024 * MiB, -6);
  });

  it('a ceiling below librdkafka’s floor is brought inside the range and wins', () => {
    const small = estimateLink({ ...smallInput(), brokerCeiling: 500_000 });
    expect(small.batchBytes).toBe(500_000);
    expect(estimateLink(smallInput()).batchBytes).toBe(1_000);
  });

  it('linger never passes a second, and zero partitions count as one', () => {
    const slow = estimateLink({
      bufferBytes: 128 * MiB,
      roundTripMs: 5_000,
      partitions: 0,
      brokerCeiling: 1_000_000_000,
      recordBytes: 300,
    });
    expect(slow.lingerMs).toBe(1_000);
    expect(slow.requestsInFlight).toBe(IN_FLIGHT);
  });

  it('small records leave less of the buffer for their own bytes', () => {
    const at = (recordBytes: number) =>
      estimateLink({ ...smallInput(), recordBytes }).inFlightBytes;
    expect(at(64)).toBeLessThan(at(1_000));
  });
});

describe('bufferFor', () => {
  it('is the buffer whose ceiling is the throughput asked for', () => {
    const buffer = bufferFor(100e6, 200, 276);
    const reached = estimateLink({
      bufferBytes: buffer,
      roundTripMs: 200,
      partitions: 12,
      brokerCeiling: 1_048_588,
      recordBytes: 276,
    }).ceilingPerSecond;
    expect(reached / 100e6).toBeCloseTo(1, 3);
  });
});

function smallInput() {
  return {
    bufferBytes: 2 * 1024 * MiB,
    roundTripMs: 300,
    partitions: 1,
    brokerCeiling: 0,
    recordBytes: 300,
  };
}
