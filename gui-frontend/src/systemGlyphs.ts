import type { MapNode, MapSystem } from './api';
import type { Glyph } from './stepGlyphs';

// A pail: handle, drafted rim, tapered body. One sign for an object store, read from or written to:
// which cloud holds the bucket is the node's name, and the card's role says which way it flows.
const OBJECT_STORE_GLYPH: Glyph = {
  d: 'M15 18 A17 17 0 0 1 49 18 H43 A11 11 0 0 0 21 18 Z M8 18 H56 L59 26 H5 Z M9 31 H55 L48 58 H16 Z',
};

// Three partitions side by side: a stream of records.
const STREAM_GLYPH: Glyph = {
  d: 'M8 20 H16 A4 4 0 0 1 20 24 V40 A4 4 0 0 1 16 44 H8 A4 4 0 0 1 4 40 V24 A4 4 0 0 1 8 20 Z M30 20 H38 A4 4 0 0 1 42 24 V40 A4 4 0 0 1 38 44 H30 A4 4 0 0 1 26 40 V24 A4 4 0 0 1 30 20 Z M52 20 H60 A4 4 0 0 1 64 24 V40 A4 4 0 0 1 60 44 H52 A4 4 0 0 1 48 40 V24 A4 4 0 0 1 52 20 Z',
};

// A table: a header over rows of cells, what a query returns.
const TABLE_GLYPH: Glyph = {
  d: 'M6 10 H58 V54 H6 Z M10 22 H30 V30 H10 Z M34 22 H54 V30 H34 Z M10 34 H30 V40 H10 Z M34 34 H54 V40 H34 Z M10 44 H30 V50 H10 Z M34 44 H54 V50 H34 Z',
  evenodd: true,
};

/** The system behind a source or destination, as the map draws it (STYLE_GUIDE.md, Glyphs). */
export const SYSTEM_GLYPHS: Record<MapSystem, Glyph> = {
  kafka: STREAM_GLYPH,
  // Event Hubs is read as Kafka is: the same stream.
  eventHubs: STREAM_GLYPH,
  elasticsearch: {
    d: 'M8 16 Q8 8 32 8 Q56 8 56 16 V48 Q56 56 32 56 Q8 56 8 48 Z M12 16 A20 5 0 1 0 52 16 A20 5 0 1 0 12 16 Z',
    evenodd: true,
  },
  // A funnel: a collector that takes events in.
  splunkHec: {
    d: 'M6 12 H58 L38 34 V52 L26 46 V34 Z',
  },
  // A paper dart: a request sent to an address.
  http: {
    d: 'M4 30 L60 8 L46 56 L31 40 L52 15 L24 36 Z',
  },
  s3: OBJECT_STORE_GLYPH,
  s3Sqs: OBJECT_STORE_GLYPH,
  azureBlobQueue: OBJECT_STORE_GLYPH,
  gcsPubSub: OBJECT_STORE_GLYPH,
  gcs: OBJECT_STORE_GLYPH,
  azureBlob: OBJECT_STORE_GLYPH,
  // A folder: a directory on a file server.
  sftp: {
    d: 'M6 16 A4 4 0 0 1 10 12 H26 L32 18 H54 A4 4 0 0 1 58 22 V50 A4 4 0 0 1 54 54 H10 A4 4 0 0 1 6 50 Z',
  },
  postgres: TABLE_GLYPH,
  mysql: TABLE_GLYPH,
  metrics: {
    d: 'M32 8 C42 24 48 34 48 42 A16 16 0 1 1 16 42 C16 34 22 24 32 8 Z M23 38 h7 v12 h-7 Z M34 30 h7 v20 h-7 Z',
    evenodd: true,
  },
  // The metrics droplet, cut with lines rather than bars: the same process, saying something else.
  logs: {
    d: 'M32 8 C42 24 48 34 48 42 A16 16 0 1 1 16 42 C16 34 22 24 32 8 Z M21 35 h22 v5 h-22 Z M21 44 h22 v5 h-22 Z',
    evenodd: true,
  },
  // An iceberg: a peak above a drafted waterline, and the larger mass below it.
  iceberg: {
    d: 'M22 25 L30 9 L35 15 L40 6 L48 25 Z M4 29 H60 L63 33 H1 Z M10 37 H56 L50 48 L38 61 L24 58 L12 47 Z',
  },
  // A delta: the letter, an outlined triangle.
  delta: {
    d: 'M32 6 L60 56 H4 Z M32 22 L46.5 48 H17.5 Z',
    evenodd: true,
  },
};

/** The system behind a source or destination, as a card's second line names it. */
export const SYSTEM_LABELS: Record<MapSystem, string> = {
  kafka: 'Kafka',
  eventHubs: 'Azure Event Hubs',
  elasticsearch: 'Elasticsearch',
  splunkHec: 'Splunk HEC',
  http: 'HTTP',
  s3: 'S3',
  s3Sqs: 'S3 via SQS',
  azureBlobQueue: 'Azure Blob via Storage queue',
  gcsPubSub: 'GCS via Pub/Sub',
  gcs: 'GCS',
  azureBlob: 'Azure Blob',
  sftp: 'SFTP',
  postgres: 'PostgreSQL',
  mysql: 'MySQL / MariaDB',
  metrics: 'smeltr metrics',
  logs: 'smeltr logs',
  iceberg: 'Iceberg',
  delta: 'Delta',
};

/** A pipeline: the block a process is drawn as. */
export const PIPELINE_GLYPH: Glyph = {
  d: 'M18 8 H46 A10 10 0 0 1 56 18 V46 A10 10 0 0 1 46 56 H18 A10 10 0 0 1 8 46 V18 A10 10 0 0 1 18 8 Z',
};

/** A system this build does not know: a plain dot, so the node still draws. */
export const UNKNOWN_SYSTEM_GLYPH: Glyph = { d: 'M32 14 A18 18 0 1 0 32 50 A18 18 0 1 0 32 14 Z' };

export function mapGlyph(node: Pick<MapNode, 'kind' | 'system'>): Glyph {
  if (node.kind === 'pipeline' || !node.system) return PIPELINE_GLYPH;
  // `MapSystem` mirrors the leader's enum by hand, so a leader can name a system this build
  // has no glyph for. That is one unmarked node, never a map that fails to draw.
  return (SYSTEM_GLYPHS as Partial<Record<string, Glyph>>)[node.system] ?? UNKNOWN_SYSTEM_GLYPH;
}
