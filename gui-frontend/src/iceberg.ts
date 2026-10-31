// What an Iceberg destination may be, in the terms the form asks it in.
//
// The Rust side is the authority: `config_file::load::check_iceberg` refuses each of these. Everything here says the
// same thing earlier, in the field that caused it.

import type { DestinationDto } from './api';
import { accessModified, credentialOf } from './ConnectorAccess.svelte';
import { tlsRefusal } from './http';

export type IcebergDestination = Extract<DestinationDto, { type: 'iceberg' }>;

/** How the destination signs in to its catalog: as no one, with a bearer token, with OAuth client credentials, or
 * with AWS signatures. One choice, so the form never names two at once. */
export type CatalogSignIn = 'none' | 'token' | 'client' | 'aws';

export const SIGN_INS: Record<CatalogSignIn, string> = {
  none: 'none',
  token: 'a bearer token',
  client: 'OAuth client credentials',
  aws: 'AWS signatures',
};

/** The services an Iceberg catalog signed with AWS signatures answers as: Glue's Iceberg endpoint, and S3 Tables
 * (`config_file::load::SIGNING_NAMES`). */
export const SIGNING_NAMES = ['glue', 's3tables'] as const;

/** The sign-in `d` names. */
export function signInOf(d: IcebergDestination): CatalogSignIn {
  if (d.signingName !== undefined) return 'aws';
  if (d.clientId !== undefined || d.clientSecret !== undefined) return 'client';
  if (d.token !== undefined) return 'token';
  return 'none';
}

/** Unsets every other sign-in's fields and names `choice`'s, empty until they are filled in. */
export function setSignIn(d: IcebergDestination, choice: CatalogSignIn) {
  if (choice !== 'token') delete d.token;
  if (choice !== 'client') {
    delete d.clientId;
    delete d.clientSecret;
    delete d.tokenUrl;
    delete d.scope;
  }
  if (choice !== 'aws') delete d.signingName;
  if (choice === 'token') d.token ??= '';
  if (choice === 'client') {
    d.clientId ??= '';
    d.clientSecret ??= '';
  }
  if (choice === 'aws') d.signingName ??= SIGNING_NAMES[0];
}

/** The AWS region a catalog's address names, as `glue.us-east-1.amazonaws.com` does. Mirrors
 * `io::iceberg::table::signing_region`. */
export function signingRegion(url: string): string | undefined {
  let host: string;
  try {
    host = new URL(url.trim()).hostname.toLowerCase();
  } catch {
    return undefined;
  }
  const labels = host.split('.');
  const aws = labels.indexOf('amazonaws');
  if (aws < 2) return undefined;
  const region = labels[aws - 1];
  // A region is letters, then a number: `us-east-1`, `us-gov-west-1`.
  return region.includes('-') && /\d$/.test(region) ? region : undefined;
}

/** Why the catalog's address cannot carry what the destination sends over it, or nothing. Plain HTTP negotiates no
 * TLS for the settings to answer. Whether it is a URL at all is the save's to say. */
export function catalogRefusal(d: IcebergDestination): string | undefined {
  return tlsRefusal(d.catalog, d);
}

/** Why the namespace cannot be saved, or nothing. An empty one is the save's to name. */
export function namespaceRefusal(namespace: string): string | undefined {
  const ns = namespace.trim();
  if (ns === '' || !ns.split('.').some((level) => level.trim() === '')) return undefined;
  return 'A level is empty. Join its levels with single dots, as logs.web.';
}

const POSITIVE_U32 = (n: string) =>
  /^\+?\d+$/.test(n.trim()) && Number(n) > 0 && Number(n) < 2 ** 32;

/** Why a partition term cannot be read, or nothing. Mirrors `io::iceberg::transform::partition_term`. */
export function partitionRefusal(term: string): string | undefined {
  const refused = `"${term}" is no partition term: write a column, or year, month, day or hour of one (day(ts)), or bucket(n, column) or truncate(width, column).`;
  const t = term.trim();
  let column: string;
  const open = t.indexOf('(');
  if (open < 0) {
    column = t;
  } else {
    const name = t.slice(0, open).trim();
    const rest = t.slice(open + 1);
    if (!rest.endsWith(')')) return refused;
    const inner = rest.slice(0, -1);
    const comma = inner.indexOf(',');
    if (['year', 'month', 'day', 'hour'].includes(name) && comma < 0) {
      column = inner;
    } else if ((name === 'bucket' || name === 'truncate') && comma >= 0) {
      if (!POSITIVE_U32(inner.slice(0, comma))) return refused;
      column = inner.slice(comma + 1);
    } else {
      return refused;
    }
  }
  const path = column.trim().split('.');
  return path.some((name) => name === '' || /[(),]/.test(name)) ? refused : undefined;
}

/** Why the region must be named, or nothing: an AWS signature is for a region, and the catalog's address names
 * none. */
export function regionRefusal(d: IcebergDestination): string | undefined {
  if (d.signingName === undefined || d.region !== undefined) return undefined;
  if (signingRegion(d.catalog) !== undefined) return undefined;
  return "The catalog's address names no AWS region to sign its calls for. Set the region under Catalog and storage, or use the service's own address, such as https://glue.us-east-1.amazonaws.com/iceberg.";
}

/** Why the signing service cannot be saved, or nothing. */
export function signingNameRefusal(d: IcebergDestination): string | undefined {
  const name = d.signingName?.trim();
  if (name === undefined || (SIGNING_NAMES as readonly string[]).includes(name)) return undefined;
  return `The signing service is ${SIGNING_NAMES.join(' or ')}.`;
}

/** Drives the Catalog and storage header's marker: every field it hides. */
export function catalogStorageModified(d: IcebergDestination): boolean {
  return (
    d.warehouse !== undefined ||
    d.tokenUrl !== undefined ||
    d.scope !== undefined ||
    credentialOf('iceberg', d) !== 'catalog' ||
    accessModified('iceberg', d)
  );
}
