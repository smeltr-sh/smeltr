// What the worker will make of a buffer over a link, worked out ahead of deployment. A mirror of
// `derive_sizes` and `held_bytes` in `src/io/kafka.rs`: the worker's rule is the truth, this is the
// same arithmetic in the browser so an admin can size the buffer before anything is deployed. The
// test pins the two together on the same cases; nothing else does.

/** The fewest requests librdkafka keeps in flight per connection; a batch is sized for this many. */
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
/** What the producer counts each queued record at beyond its payload and key. */
export const MESSAGE_OVERHEAD_BYTES = 225;

export type LinkInputs = {
  /** The destination's whole `bufferBytes`. */
  bufferBytes: number;
  roundTripMs: number;
  partitions: number;
  /** The largest request the topic takes, `message.max.bytes` on the broker or the topic. */
  brokerCeiling: number;
  /** The average record as written, payload and key. */
  recordBytes: number;
};

export type LinkEstimate = {
  /** The producer's share of the buffer. */
  producerBytes: number;
  /** The records' own bytes the producer's share holds once each is counted with its overhead. */
  inFlightBytes: number;
  batchBytes: number;
  lingerMs: number;
  /** Requests the producer keeps out at once on each connection. */
  requestsInFlight: number;
  /** The most one worker could push, in bytes per second, if its buffer were full the whole time: the
   * records it holds, over the time each is held, a linger and a round trip. */
  ceilingPerSecond: number;
};

const clamp = (n: number, lo: number, hi: number) => Math.min(Math.max(n, lo), hi);

/** The share of what the producer holds that is the records' own bytes. */
function payloadShare(recordBytes: number): number {
  const record = Math.max(1, recordBytes);
  return record / (record + MESSAGE_OVERHEAD_BYTES);
}

/** How long a record is held: the linger it waits to fill its batch, then the round trip. */
function heldSeconds(roundTripMs: number): number {
  return (Math.max(roundTripMs, 1) + clamp(roundTripMs / 2, MIN_LINGER_MS, MAX_LINGER_MS)) / 1000;
}

export function estimateLink(input: LinkInputs): LinkEstimate {
  const producerBytes = input.bufferBytes - Math.floor(input.bufferBytes / 2);
  const partitions = Math.max(1, Math.floor(input.partitions));
  const ceiling = clamp(input.brokerCeiling, MIN_MESSAGE_BYTES, MAX_MESSAGE_BYTES);
  const share = Math.floor(producerBytes / partitions / IN_FLIGHT);
  const batchBytes = clamp(share, Math.min(DEFAULT_BATCH_BYTES, ceiling), ceiling);
  const lingerMs = clamp(input.roundTripMs / 2, MIN_LINGER_MS, MAX_LINGER_MS);
  const requestsInFlight = Math.max(Math.floor(producerBytes / batchBytes), IN_FLIGHT);
  // Each request carries a batch for every partition its broker leads.
  const inFlightBytes = Math.min(
    producerBytes * payloadShare(input.recordBytes),
    batchBytes * partitions * requestsInFlight,
  );
  return {
    producerBytes,
    inFlightBytes,
    batchBytes,
    lingerMs,
    requestsInFlight,
    ceilingPerSecond: inFlightBytes / heldSeconds(input.roundTripMs),
  };
}

/** The `bufferBytes` whose ceiling per worker is `perSecond` over this link, with records this size. */
export function bufferFor(perSecond: number, roundTripMs: number, recordBytes: number): number {
  const producerBytes = (perSecond * heldSeconds(roundTripMs)) / payloadShare(recordBytes);
  return Math.ceil(2 * producerBytes);
}
