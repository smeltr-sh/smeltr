/** What a destination is, by its type: its name, where it writes, and a new one of it. */
import {
  LIMITS,
  OBJECT_PREFIX,
  type CloudStore,
  type DestinationDto,
  type ObjectStore,
} from './api';
import { byteCount } from './bytes';
import { accessModified } from './ConnectorAccess.svelte';
import { requestDeliveryModified } from './RequestDelivery.svelte';
import type { HeaderedDestination } from './http';
import type { CatalogType } from './TypeCatalog.svelte';
import { SYSTEM_GLYPHS, SYSTEM_LABELS } from './systemGlyphs';
import { suggestId } from './ids';

/** A destination that writes objects to a cloud's bucket or files to an SFTP server's directory. */
export type ObjectDestination = Extract<DestinationDto, { type: ObjectStore }>;
/** An object destination reached over HTTPS, which takes a storage class and an identity. */
export type CloudDestination = Extract<DestinationDto, { type: CloudStore }>;

export function isObjectStore(d: DestinationDto): d is ObjectDestination {
  return isCloud(d) || d.type === 'sftp';
}

export function isCloud(d: DestinationDto): d is CloudDestination {
  return d.type === 's3' || d.type === 'gcs' || d.type === 'azureBlob';
}

/** Drives the Advanced header's marker: the access fields and every shared setting it hides. */
export function objectModified(d: ObjectDestination): boolean {
  return (
    accessModified(d.type, d) ||
    (d.type === 's3' && d.kmsKeyId !== undefined) ||
    (d.compression ?? 'gzip') !== 'gzip' ||
    (isCloud(d) && d.storageClass !== undefined) ||
    (d.inFlight ?? LIMITS.inFlight.default) !== LIMITS.inFlight.default ||
    byteCount(d.maxObjectBytes, LIMITS.maxObjectBytes.default) !== LIMITS.maxObjectBytes.default ||
    byteCount(d.bufferBytes, LIMITS.objectBufferBytes.default) !==
      LIMITS.objectBufferBytes.default ||
    (d.stallAfterSecs ?? LIMITS.stallAfterSecs.default) !== LIMITS.stallAfterSecs.default ||
    (d.fileIntervalSecs ?? LIMITS.fileIntervalSecs.default) !== LIMITS.fileIntervalSecs.default
  );
}

/** Drives the HTTP and Prometheus Advanced header's marker: the identity and every delivery setting it hides. */
export function httpModified(d: HeaderedDestination): boolean {
  return (
    d.identity !== undefined ||
    (d.type === 'http' && d.zoneUrls !== undefined) ||
    (d.type === 'http' && (d.compression ?? 'none') !== 'none') ||
    requestDeliveryModified(d, 'maxRequestBytes', LIMITS.maxRequestBytes.default)
  );
}

/** Drives the Log Analytics Advanced header's marker: the identity, the sign-in's auth endpoint and every
 * delivery setting it hides. */
export function logAnalyticsModified(
  d: Extract<DestinationDto, { type: 'logAnalytics' }>,
): boolean {
  return (
    d.identity !== undefined ||
    accessModified('logAnalytics', d) ||
    requestDeliveryModified(d, 'maxRequestBytes', LIMITS.logAnalyticsRequestBytes.default)
  );
}

/** Where a destination writes, as its row says it. */
/** Whether `d` sends a credential besides one in its URL, as `UrlField` and `ZoneUrls` take it: Elasticsearch's
 * auth, Splunk's token, or an HTTP or Prometheus credential header. */
export function sendsCredential(d: DestinationDto): boolean {
  if (d.type === 'elasticsearch') return d.auth !== undefined || d.password !== undefined;
  if (d.type === 'splunkHec') return true;
  if (d.type === 'http' || d.type === 'prometheus')
    return Object.keys(d.credentials ?? {}).length > 0;
  return false;
}

export function writes(d: DestinationDto): string {
  switch (d.type) {
    case 'kafka':
      return d.defaultTopic;
    case 'eventHubs':
      return d.defaultEventHub;
    case 'elasticsearch':
      return d.defaultIndex;
    case 'splunkHec':
      return d.index ?? '';
    case 'http':
    case 'prometheus':
      return '';
    case 'logAnalytics':
      return d.stream;
    case 's3':
    case 'gcs':
      return d.bucket;
    case 'azureBlob':
      return d.container;
    case 'sftp':
      return d.directory ?? '~';
    case 'iceberg':
      return d.namespace && d.table ? `${d.namespace}.${d.table}` : d.table;
    case 'delta':
      return d.unityCatalog?.table ?? d.location ?? '';
  }
}

/** The prefix a destination writes under, as its field shows it. */
export const prefixOf = (d: ObjectDestination) => d.prefix ?? OBJECT_PREFIX;

/** What a destination connects to, as its row says it. */
export function at(d: DestinationDto): string {
  switch (d.type) {
    case 'kafka':
      return d.brokers;
    case 'eventHubs':
      return d.namespace;
    case 'elasticsearch':
    case 'splunkHec':
    case 'http':
    case 'prometheus':
      return d.url;
    case 'logAnalytics':
      return d.endpoint;
    case 'azureBlob':
      return d.account;
    case 'sftp':
      return d.host;
    case 'iceberg':
      return d.catalog;
    case 'delta':
      return d.unityCatalog?.workspace ?? '';
    default:
      return '';
  }
}

/** A destination the map draws as the HTTP requests it sends, so it has no system label or glyph of its own. */
const HTTP_LABELS: Record<'prometheus' | 'logAnalytics', string> = {
  prometheus: 'Prometheus remote_write',
  logAnalytics: 'Log Analytics workspace',
};
function isHttpDrawn(type: DestinationDto['type']): type is keyof typeof HTTP_LABELS {
  return type in HTTP_LABELS;
}

/** A destination type as the page names it. */
export function typeLabel(type: DestinationDto['type']): string {
  return isHttpDrawn(type) ? HTTP_LABELS[type] : SYSTEM_LABELS[type];
}

/** Every type a destination may be, with the category the add dialog lists it under. */
export const DESTINATION_TYPES: CatalogType<DestinationDto['type']>[] = (
  [
    ['kafka', 'Streams'],
    ['eventHubs', 'Streams'],
    ['elasticsearch', 'Log analytics'],
    ['splunkHec', 'Log analytics'],
    ['logAnalytics', 'Log analytics'],
    ['http', 'Web'],
    ['prometheus', 'Observability'],
    ['s3', 'Object storage'],
    ['gcs', 'Object storage'],
    ['azureBlob', 'Object storage'],
    ['sftp', 'Object storage'],
    ['iceberg', 'Tables'],
    ['delta', 'Tables'],
  ] as const
).map(([type, category]) => ({
  type,
  category,
  label: typeLabel(type),
  glyph: SYSTEM_GLYPHS[isHttpDrawn(type) ? 'http' : type],
}));

/** A new destination of `type`, named after it and clear of every id in `taken`. */
export function blankDestination(type: DestinationDto['type'], taken: string[]): DestinationDto {
  const id = suggestId(type, taken);
  switch (type) {
    case 'kafka':
      return { id, type, brokers: '', defaultTopic: '' };
    case 'eventHubs':
      return { id, type, namespace: '', defaultEventHub: '', connectionString: '' };
    case 'elasticsearch':
      return { id, type, url: '', defaultIndex: '' };
    case 'splunkHec':
      return { id, type, url: '', token: '' };
    case 'http':
    case 'prometheus':
      return { id, type, url: '' };
    case 'logAnalytics':
      return { id, type, endpoint: '', ruleId: '', stream: '' };
    case 's3':
    case 'gcs':
      return { id, type, bucket: '' };
    case 'azureBlob':
      return { id, type, account: '', container: '' };
    case 'sftp':
      return { id, type, host: '', username: '', password: '', hostKey: '' };
    case 'iceberg':
      return { id, type, catalog: '', namespace: '', table: '' };
    case 'delta':
      return { id, type, location: '' };
  }
}
