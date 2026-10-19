// What the worker will make of a buffer over a link, worked out ahead of deployment. A mirror of
// `derive_sizes` in `src/io/kafka.rs`: the worker's rule is the truth, this is the same arithmetic
// in the browser so an admin can size the buffer before anything is deployed. The test pins the two
// together on the same cases; nothing else does.

/** Requests librdkafka keeps in flight per connection, which idempotence fixes at five. */
export const IN_FLIGHT = 5;
/** librdkafka's own `batch.size`, the floor a derived batch never goes under. */
export const DEFAULT_BATCH_BYTES = 1_000_000;
/** The range `message.max.bytes` may take; a broker ceiling outside it is brought inside. */
export const MIN_MESSAGE_BYTES = 1_000;
export const MAX_MESSAGE_BYTES = 1_000_000_000;
/** The bounds on linger: never below what the poll loop already costs, never a whole second. */
export const MIN_LINGER_MS = 5;
export const MAX_LINGER_MS = 1_000;
/** A broker's default `message.max.bytes`, the ceiling an admin who never changed it has. */
export const DEFAULT_BROKER_CEILING = 1_048_588;

export type LinkInputs = {
  /** The destination's whole `bufferBytes`. */
  bufferBytes: number;
  roundTripMs: number;
  partitions: number;
  /** The largest request the topic takes, `message.max.bytes` on the broker or the topic. */
  brokerCeiling: number;
};

export type LinkEstimate = {
  /** The producer's share of the buffer, what is in flight over the link. */
  producerBytes: number;
  batchBytes: number;
  lingerMs: number;
  /** Requests the producer can have out at once: five per partition. */
  requestsInFlight: number;
  /** The most one worker could push, in bytes per second, if its buffer were full the whole time:
   * what the requests in flight carry, over the round trip, and never more than the producer's share
   * over it. */
  ceilingPerSecond: number;
};

const clamp = (n: number, lo: number, hi: number) => Math.min(Math.max(n, lo), hi);

export function estimateLink(input: LinkInputs): LinkEstimate {
  const producerBytes = input.bufferBytes - Math.floor(input.bufferBytes / 2);
  const partitions = Math.max(1, Math.floor(input.partitions));
  const ceiling = clamp(input.brokerCeiling, MIN_MESSAGE_BYTES, MAX_MESSAGE_BYTES);
  const share = Math.floor(producerBytes / partitions / IN_FLIGHT);
  const batchBytes = clamp(share, Math.min(DEFAULT_BATCH_BYTES, ceiling), ceiling);
  const lingerMs = clamp(input.roundTripMs / 2, MIN_LINGER_MS, MAX_LINGER_MS);
  const requestsInFlight = partitions * IN_FLIGHT;
  const roundTripS = Math.max(input.roundTripMs, 1) / 1000;
  const inFlightBytes = Math.min(producerBytes, batchBytes * requestsInFlight);
  return {
    producerBytes,
    batchBytes,
    lingerMs,
    requestsInFlight,
    ceilingPerSecond: inFlightBytes / roundTripS,
  };
}
