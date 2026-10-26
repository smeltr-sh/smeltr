// What a Delta destination may be, in the terms the form asks it in.
//
// The Rust side is the authority: `config_file::load::check_delta` refuses each of these. Everything here says the
// same thing earlier, in the field that caused it.

import type { DestinationDto } from './api';
import { accessModified } from './ConnectorAccess.svelte';
import { PLAIN_HTTP_TLS, tlsSet } from './http';

export type DeltaDestination = Extract<DestinationDto, { type: 'delta' }>;

/** How the destination names its table: by its root in S3, or as a Unity Catalog table, which gives the root. One
 * choice, so the form never names both. */
export type TableNaming = 'location' | 'unityCatalog';

export const TABLE_NAMINGS: Record<TableNaming, string> = {
  location: 'a path in S3',
  unityCatalog: 'a Unity Catalog table',
};

/** The storage fields `check_unity_catalog` refuses beside a Unity Catalog table: the catalog gives the credentials
 * for the table's storage. `externalId` goes with `roleArn`. */
const CATALOG_GIVES = [
  'accessKeyId',
  'secretAccessKey',
  'roleArn',
  'externalId',
  'authEndpoint',
] as const;

/** The naming `d` makes. */
export function namingOf(d: DeltaDestination): TableNaming {
  return d.unityCatalog !== undefined ? 'unityCatalog' : 'location';
}

/** Unsets the other naming's fields, and every storage field the catalog gives, and names `choice`'s, empty until
 * they are filled in. */
export function setNaming(d: DeltaDestination, choice: TableNaming) {
  if (choice === 'location') {
    delete d.unityCatalog;
    d.location ??= '';
    return;
  }
  delete d.location;
  for (const field of CATALOG_GIVES) delete d[field];
  d.unityCatalog ??= { workspace: '', table: '', clientId: '', clientSecret: '' };
}

/** The schemes a table's root may be named with. Mirrors `check_delta`. */
const SCHEMES = ['s3://', 's3a://'];

/** Why the location cannot be saved, or nothing. An empty one is the save's to name. */
export function locationRefusal(location: string): string | undefined {
  const l = location.trim();
  if (l === '') return undefined;
  const rest = SCHEMES.map((s) => (l.startsWith(s) ? l.slice(s.length) : undefined)).find(
    (r) => r !== undefined,
  );
  if (rest !== undefined && rest.split('/')[0] !== '') return undefined;
  return "Give the table's root in S3, as s3://bucket/path.";
}

/** Why the partition columns cannot be saved, or nothing: Delta partitions by top-level columns, by their values, so
 * a term is a column's name and nothing more, named once. */
export function partitionRefusal(columns: string[]): string | undefined {
  const seen = new Set<string>();
  for (const column of columns) {
    if (column.trim() === '' || /[.(),]/.test(column))
      return `"${column}" is no column name: a Delta table is partitioned by top-level columns, as host.`;
    if (seen.has(column)) return `"${column}" is named twice: name each column once.`;
    seen.add(column);
  }
  return undefined;
}

/** Why the endpoint cannot be saved, or nothing. A certificate is presented only over TLS. */
export function endpointRefusal(d: DeltaDestination): string | undefined {
  if (d.endpoint === undefined || /^https:\/\//i.test(d.endpoint.trim())) return undefined;
  if (tlsSet(d)) return PLAIN_HTTP_TLS;
  return undefined;
}

/** Why the workspace's address cannot be saved, or nothing. Every API is asked under the workspace's root, so a URL
 * copied from a browser, with a path or `?o=`, is no base. An empty one is the save's to name. */
export function workspaceRefusal(workspace: string): string | undefined {
  const w = workspace.trim();
  if (w === '') return undefined;
  const refused =
    "Give the workspace's https:// URL and nothing after its host, as https://dbc-1234.cloud.databricks.com.";
  let u: URL;
  try {
    u = new URL(w);
  } catch {
    return refused;
  }
  const root = u.pathname === '/' && u.search === '' && u.hash === '' && !/[?#]$/.test(w);
  if (!['http:', 'https:'].includes(u.protocol) || u.hostname === '' || !root) return refused;
  return undefined;
}

/** Why the Unity Catalog table's name cannot be saved, or nothing. An empty one is the save's to name. */
export function catalogTableRefusal(table: string): string | undefined {
  const t = table.trim();
  if (t === '') return undefined;
  const parts = t.split('.');
  if (parts.length === 3 && parts.every((p) => p.trim() !== '')) return undefined;
  return 'Name the Unity Catalog table by its three parts, as catalog.schema.table.';
}

/** Why the client id cannot be saved, or nothing. */
export function clientIdRefusal(clientId: string): string | undefined {
  if (clientId.trim() !== '') return undefined;
  return 'Name the OAuth client id of the service principal it signs in as.';
}

/** Why the region must be named, or nothing. */
export function regionRefusal(d: DeltaDestination): string | undefined {
  if (d.unityCatalog === undefined || (d.region ?? '').trim() !== '') return undefined;
  return "Name the bucket's region, as us-west-2.";
}

/** Drives the Storage header's marker: every field it hides. Beside a Unity Catalog table it hides the endpoint
 * alone, since the region is asked for in the open and the catalog gives the rest. */
export function storageModified(d: DeltaDestination): boolean {
  return d.unityCatalog !== undefined ? d.endpoint !== undefined : accessModified('s3', d);
}
