import { describe, expect, it } from 'vitest';
import { DEFAULT_BATCH_BYTES, estimateLink, MIN_LINGER_MS } from './linkEstimate';

// The same cases `derive_sizes`' test pins in `src/io/kafka.rs`, on the producer's share.
const MiB = 1024 * 1024;

describe('estimateLink', () => {
  it('a near broker with a small buffer gets librdkafka’s own sizes', () => {
    const near = estimateLink({
      bufferBytes: 128 * MiB,
      roundTripMs: 2,
      partitions: 64,
      brokerCeiling: 1_048_588,
    });
    expect(near.producerBytes).toBe(64 * MiB);
    expect(near.batchBytes).toBe(DEFAULT_BATCH_BYTES);
    expect(near.lingerMs).toBe(MIN_LINGER_MS);
  });

  it('a far broker spreads the producer’s share over the partitions and the requests in flight', () => {
    const far = estimateLink({
      bufferBytes: 128 * MiB,
      roundTripMs: 120,
      partitions: 12,
      brokerCeiling: 8 * MiB,
    });
    expect(far.batchBytes).toBe(Math.floor((64 * MiB) / 12 / 5));
    expect(far.lingerMs).toBe(60);
    // Every byte of the share is in flight, so the ceiling is the share over the round trip.
    // Within the floor rounding of one batch times sixty requests.
    expect(far.ceilingPerSecond).toBeCloseTo((64 * MiB) / 0.12, -3);
  });

  it('the broker’s ceiling caps the batch, and then the ceiling per second too', () => {
    const capped = estimateLink({
      bufferBytes: 2 * 1024 * MiB,
      roundTripMs: 300,
      partitions: 1,
      brokerCeiling: 8 * MiB,
    });
    expect(capped.batchBytes).toBe(8 * MiB);
    expect(capped.ceilingPerSecond).toBeCloseTo((8 * MiB * 5) / 0.3, 0);
  });

  it('a ceiling below librdkafka’s floor is brought inside the range and wins', () => {
    const small = estimateLink({
      bufferBytes: 2 * 1024 * MiB,
      roundTripMs: 300,
      partitions: 1,
      brokerCeiling: 500_000,
    });
    expect(small.batchBytes).toBe(500_000);
    expect(estimateLink({ ...smallInput(), brokerCeiling: 0 }).batchBytes).toBe(1_000);
  });

  it('linger never passes a second, and zero partitions count as one', () => {
    const slow = estimateLink({
      bufferBytes: 128 * MiB,
      roundTripMs: 5_000,
      partitions: 0,
      brokerCeiling: 1_000_000_000,
    });
    expect(slow.lingerMs).toBe(1_000);
    expect(slow.requestsInFlight).toBe(5);
  });
});

function smallInput() {
  return { bufferBytes: 2 * 1024 * 1024 * 1024, roundTripMs: 300, partitions: 1, brokerCeiling: 0 };
}
