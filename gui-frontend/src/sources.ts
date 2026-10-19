/** What a source is, by its type: its name, what the map draws it as, and a new one of it. */
import type { Framing, MapSystem, ObjectStore, SourceDto } from './api';
import type { CatalogType } from './TypeCatalog.svelte';
import { SOURCE_KIND, type SourceKind } from './sourceKinds';
import { SYSTEM_GLYPHS, SYSTEM_LABELS } from './systemGlyphs';
import { suggestId } from './ids';

/** Absent is `lines`, so choosing it writes nothing. Only `lines` takes `lineBreaker`, so choosing another
 * drops it. */
export function setFraming(d: { framing?: Framing; lineBreaker?: string }, framing: Framing) {
  if (framing === 'lines') delete d.framing;
  else {
    d.framing = framing;
    delete d.lineBreaker;
  }
}

/** The severities a logs source may name, most severe first. Mirrors `logs::LEVEL_LABELS`. */
export const LEVELS = ['error', 'warn', 'info', 'debug', 'trace'];
/** What a source carries when it names no level. Mirrors `LevelLabel::DEFAULT`. */
export const DEFAULT_LEVEL = 'info';

/** A source that reads the objects a queue announces. */
export type ObjectSource = Extract<SourceDto, { type: 's3Sqs' | 'azureBlobQueue' | 'gcsPubSub' }>;
/** The store each object source reads, whose access block its form draws. */
export const OBJECT_STORE: Record<ObjectSource['type'], ObjectStore> = {
  s3Sqs: 's3',
  azureBlobQueue: 'azureBlob',
  gcsPubSub: 'gcs',
};
export function isObjectSource(s: SourceDto): s is ObjectSource {
  return s.type in OBJECT_STORE;
}
/** A source that finds its objects by listing where they are. */
export type ListingSource = Extract<
  SourceDto,
  { type: 's3List' | 'gcsList' | 'azureBlobList' | 'sftp' }
>;
/** The store each listing source lists, whose access block its form draws, and which the map draws it as. */
export const LISTING_STORE: Record<ListingSource['type'], ObjectStore> = {
  s3List: 's3',
  gcsList: 'gcs',
  azureBlobList: 'azureBlob',
  sftp: 'sftp',
};
function isListingType(type: SourceDto['type']): type is ListingSource['type'] {
  return type in LISTING_STORE;
}
export function isListing(s: SourceDto): s is ListingSource {
  return isListingType(s.type);
}
/** What one listed entry is called, and what holds them, per listing source. */
export const LISTED: Record<ListingSource['type'], { noun: string; within: string }> = {
  s3List: { noun: 'object', within: 'bucket' },
  gcsList: { noun: 'object', within: 'bucket' },
  azureBlobList: { noun: 'blob', within: 'container' },
  sftp: { noun: 'file', within: 'directory' },
};
/** A listing source is named for the store and that it lists it, as the queue source on the same store is named
 * for its queue. SFTP has only the one way in, so its name is enough. */
const LISTING_LABELS: Record<ListingSource['type'], string> = {
  s3List: `${SYSTEM_LABELS.s3} listing`,
  gcsList: `${SYSTEM_LABELS.gcs} listing`,
  azureBlobList: `${SYSTEM_LABELS.azureBlob} listing`,
  sftp: SYSTEM_LABELS.sftp,
};
/** A source that polls an API: one worker asks it, and the leader keeps where it got to. */
export type ApiSource = Extract<SourceDto, { type: 'httpApi' | 'oktaLogs' }>;
/** An API source is named for the API it reads; the map draws every one as HTTP, which is how it reads. */
const API_LABELS: Record<ApiSource['type'], string> = {
  httpApi: 'HTTP API',
  oktaLogs: 'Okta System Log',
};
function isApiType(type: SourceDto['type']): type is ApiSource['type'] {
  return type in API_LABELS;
}
/** A source senders connect to: every worker of its group listens on its port. */
export type ReceiverSource = Extract<
  SourceDto,
  {
    type:
      | 'httpReceiver'
      | 'hecReceiver'
      | 's2sReceiver'
      | 'otlpReceiver'
      | 'syslogReceiver'
      | 'prometheusReceiver'
      | 'elasticReceiver';
  }
>;
/** A receiver is named for what its senders speak. The map draws every one as HTTP, as the leader's map does. */
const RECEIVER_LABELS: Record<ReceiverSource['type'], string> = {
  httpReceiver: 'HTTP receiver',
  hecReceiver: 'HEC receiver',
  s2sReceiver: 'S2S receiver',
  otlpReceiver: 'OTLP receiver',
  syslogReceiver: 'Syslog receiver',
  prometheusReceiver: 'Prometheus receiver',
  elasticReceiver: 'Elasticsearch receiver',
};
/** The paths a receiver serves besides its health check, as its row says them. One not reached over HTTP has
 * none; a syslog receiver over UDP says its transport there, as its port is not the TCP port of that number. */
const RECEIVER_PATHS: Record<ReceiverSource['type'], (s: ReceiverSource) => string> = {
  httpReceiver: (s) => (s.type === 'httpReceiver' ? (s.path ?? '/') : '/'),
  hecReceiver: () => '/services/collector',
  s2sReceiver: () => '',
  otlpReceiver: (s) => `/v1/${(s.type === 'otlpReceiver' && s.signal) || 'logs'}`,
  syslogReceiver: (s) => (s.type === 'syslogReceiver' && s.transport === 'udp' ? '/udp' : ''),
  prometheusReceiver: () => '/api/v1/write',
  elasticReceiver: () => '/_bulk',
};
function isReceiverType(type: SourceDto['type']): type is ReceiverSource['type'] {
  return type in RECEIVER_LABELS;
}
export function isReceiver(s: SourceDto): s is ReceiverSource {
  return isReceiverType(s.type);
}
/** A source type's name, as its row, the add dialog and the catalog say it. */
export function typeLabel(type: SourceDto['type']): string {
  if (isListingType(type)) return LISTING_LABELS[type];
  if (isReceiverType(type)) return RECEIVER_LABELS[type];
  return isApiType(type) ? API_LABELS[type] : SYSTEM_LABELS[type];
}
/** The system the map draws a source type as. */
function systemOf(type: SourceDto['type']): MapSystem {
  if (isListingType(type)) return LISTING_STORE[type];
  return isApiType(type) || isReceiverType(type) ? 'http' : type;
}
/** A source that runs a query against a database. */
export type SqlSource = Extract<SourceDto, { type: 'postgres' | 'mysql' }>;
export function isSql(s: SourceDto): s is SqlSource {
  return s.type === 'postgres' || s.type === 'mysql';
}
/** A source read as a Kafka consumer: Kafka's own, or Event Hubs through its Kafka endpoint. */
export type StreamSource = Extract<SourceDto, { type: 'kafka' | 'eventHubs' }>;
export function isStream(s: SourceDto): s is StreamSource {
  return s.type === 'kafka' || s.type === 'eventHubs';
}

/** What a source reads, as its row says it. */
export function reads(s: SourceDto): string {
  if (s.type === 'kafka') return s.topics.join(', ');
  if (s.type === 'eventHubs') return s.eventHubs.join(', ');
  if (s.type === 's3Sqs' || s.type === 'azureBlobQueue') return s.queueUrl;
  if (s.type === 'gcsPubSub') return s.subscription;
  if (s.type === 's3List' || s.type === 'gcsList') return `${s.bucket}/${s.prefix ?? ''}`;
  if (s.type === 'azureBlobList') return `${s.container}/${s.prefix ?? ''}`;
  if (s.type === 'sftp') return `${s.directory ?? '~'}/${s.prefix ?? ''}`;
  if (s.type === 'metrics') return `every ${s.bucketSeconds}s`;
  if (s.type === 'httpApi') return s.url;
  if (s.type === 'oktaLogs') return 'System Log';
  if (isSql(s)) return `${s.database}: ${s.mode.type}`;
  if (isReceiver(s)) return `:${s.port}${RECEIVER_PATHS[s.type](s)}`;
  return `${s.minLevel ?? DEFAULT_LEVEL} and above`;
}

/** Every type a source may be, with the category the add dialog lists it under. */
export const SOURCE_TYPES: CatalogType<SourceDto['type'], SourceKind>[] = (
  [
    ['kafka', 'Streams'],
    ['eventHubs', 'Streams'],
    ['s3Sqs', 'Object storage'],
    ['azureBlobQueue', 'Object storage'],
    ['gcsPubSub', 'Object storage'],
    ['s3List', 'Object storage'],
    ['gcsList', 'Object storage'],
    ['azureBlobList', 'Object storage'],
    ['sftp', 'Object storage'],
    ['httpApi', 'Web'],
    ['oktaLogs', 'Identity'],
    ['postgres', 'Databases'],
    ['mysql', 'Databases'],
    ['httpReceiver', 'Web'],
    ['hecReceiver', 'Log analytics'],
    ['s2sReceiver', 'Log analytics'],
    ['otlpReceiver', 'Observability'],
    ['syslogReceiver', 'Observability'],
    ['prometheusReceiver', 'Observability'],
    ['elasticReceiver', 'Log analytics'],
    ['metrics', 'smeltr'],
    ['logs', 'smeltr'],
  ] as const
).map(([type, category]) => ({
  type,
  category,
  label: typeLabel(type),
  glyph: SYSTEM_GLYPHS[systemOf(type)],
  kind: SOURCE_KIND[type],
}));

/** A new source of `type`, named after it and clear of every id in `taken`. */
export function blankSource(type: SourceDto['type'], taken: string[]): SourceDto {
  const id = suggestId(type, taken);
  if (type === 'kafka') return { id, type, brokers: '', topics: [] };
  if (type === 'eventHubs')
    return { id, type, namespace: '', eventHubs: [], consumerGroup: '', connectionString: '' };
  if (type === 's3Sqs') return { id, type, queueUrl: '' };
  if (type === 'azureBlobQueue') return { id, type, queueUrl: '', account: '' };
  if (type === 'gcsPubSub') return { id, type, subscription: '' };
  if (type === 's3List' || type === 'gcsList') return { id, type, bucket: '' };
  if (type === 'azureBlobList') return { id, type, container: '', account: '' };
  if (type === 'sftp') return { id, type, host: '', username: '', password: '', hostKey: '' };
  if (type === 'httpApi') return { id, type, url: '' };
  if (type === 'oktaLogs') return { id, type, org: '', clientId: '', privateKey: '' };
  if (type === 'postgres' || type === 'mysql')
    return {
      id,
      type,
      host: '',
      database: '',
      user: '',
      query: '',
      mode: { type: 'everyPoll' },
      fields: [],
    };
  // The port its senders expect; the save refuses one another receiver of the group holds.
  if (type === 'httpReceiver') return { id, type, port: 8080 };
  if (type === 'hecReceiver') return { id, type, port: 8088 };
  if (type === 's2sReceiver') return { id, type, port: 9997 };
  if (type === 'otlpReceiver') return { id, type, port: 4318 };
  if (type === 'syslogReceiver') return { id, type, port: 6514 };
  if (type === 'prometheusReceiver') return { id, type, port: 9090 };
  if (type === 'elasticReceiver') return { id, type, port: 9200 };
  if (type === 'metrics') return { id, type, bucketSeconds: 10 };
  return { id, type: 'logs' };
}
