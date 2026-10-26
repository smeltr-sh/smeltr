// What an HTTP destination's headers and URL may be, in the terms the form asks them in.
//
// The Rust side is the authority: `config_file::load` refuses each of these. Everything here says the same thing
// earlier, in the row or field that caused it.

import type { DestinationDto, TlsDto } from './api';

export type HttpDestination = Extract<DestinationDto, { type: 'http' }>;
/** A destination that POSTs to a URL with headers: `http`, and `prometheus` with the remote_write body. */
export type HeaderedDestination = Extract<DestinationDto, { type: 'http' | 'prometheus' }>;

/** Headers smeltr writes from the batch itself, or that the client owns. */
const OWNED_HEADERS = new Set([
  'content-type',
  'content-encoding',
  'content-length',
  'transfer-encoding',
  'host',
  'cookie',
]);

/** What a `prometheus` destination owns besides: remote_write names its own version. */
const PROMETHEUS_OWNED_HEADERS = new Set([...OWNED_HEADERS, 'x-prometheus-remote-write-version']);

/** RFC 7230's `token`: what a header name is made of. */
const TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

/** Why a header row cannot be saved, or nothing. `maps` is the destination's two tables as saved: header names are
 * case-insensitive, so two spellings across or within them are one header with two values. */
export function headerRefusal(
  name: string,
  maps: Pick<HeaderedDestination, 'headers' | 'credentials'> & {
    type?: HeaderedDestination['type'];
  },
): string | undefined {
  const trimmed = name.trim();
  if (trimmed === '') return undefined;
  if (!TOKEN.test(trimmed)) return "not a header name: letters, digits and !#$%&'*+-.^_`|~ only";
  const lower = trimmed.toLowerCase();
  const owned = maps.type === 'prometheus' ? PROMETHEUS_OWNED_HEADERS : OWNED_HEADERS;
  if (owned.has(lower)) return 'smeltr sets this header itself';
  const names = [...Object.keys(maps.headers ?? {}), ...Object.keys(maps.credentials ?? {})];
  if (names.filter((k) => k.toLowerCase() === lower).length > 1)
    return 'set twice; give it one value';
  return undefined;
}

/** Why a committed header's value cannot be saved, or nothing. A line break would end the header early. */
export function headerValueRefusal(value: string): string | undefined {
  // A tab is legal inside a field value (RFC 7230 `field-content`); C1 is control as Rust's `char::is_control` reads it.
  return /[\x00-\x08\x0a-\x1f\x7f-\x9f]/.test(value)
    ? 'a header value cannot hold a line break or other control character'
    : undefined;
}

/** Whether `url` names this host: `localhost` or a loopback address. Mirrors `config_file::load::on_loopback`. */
export function onLoopback(url: string): boolean {
  const trimmed = url.trim();
  const rest = trimmed.includes('://') ? trimmed.slice(trimmed.indexOf('://') + 3) : trimmed;
  const authority = rest.split(/[/?#]/)[0];
  const hostPort = authority.slice(authority.lastIndexOf('@') + 1);
  const host = hostPort.startsWith('[') ? hostPort.slice(1).split(']')[0] : hostPort.split(':')[0];
  return host.toLowerCase() === 'localhost' || /^127(\.\d{1,3}){3}$/.test(host) || host === '::1';
}

/** Whether `url` names a user or a password before its host. */
export function hasUserInfo(url: string): boolean {
  const trimmed = url.trim();
  if (!trimmed.includes('://')) return false;
  const authority = trimmed.slice(trimmed.indexOf('://') + 3).split(/[/?#]/)[0];
  return authority.includes('@');
}

/** Said wherever a credential would cross the network unencrypted. It never stops the save. */
export const UNENCRYPTED = 'Its credentials will cross the network unencrypted.';

/** Whether a credential sent to `url` would cross the network unencrypted: plain http:// off this host. */
export function plainOffHost(url: string): boolean {
  return /^http:\/\//i.test(url.trim()) && !onLoopback(url);
}

/** What to say about a URL a connector sends credentials to, or nothing. Neither stops the save. `sends` is whether
 * a request carries a credential besides one in the URL; `instead` is where this connector names one. */
export function urlWarning(
  url: string,
  sends: boolean,
  instead = 'Put the user and password in Basic auth instead of the URL.',
): string | undefined {
  const userInfo = hasUserInfo(url);
  const said = [
    (sends || userInfo) && plainOffHost(url) ? UNENCRYPTED : undefined,
    userInfo ? instead : undefined,
  ].filter((s) => s !== undefined);
  return said.length > 0 ? said.join(' ') : undefined;
}

/** Whether `tls` sets anything, as `config_file::dto::TlsDto::is_set` reads it. */
export function tlsSet(tls: TlsDto): boolean {
  return (
    tls.certificate !== undefined || tls.authorities !== undefined || tls.trustPublic !== undefined
  );
}

/** What a connector reached over plain HTTP is told of its TLS settings. */
export const PLAIN_HTTP_TLS =
  'Plain HTTP negotiates no TLS, so the certificate and authorities would go unused. Use an https:// URL.';

/** Why `url` cannot carry `tls`, or nothing. Plain HTTP negotiates no TLS for the settings to answer. Whether it is
 * a URL at all is the save's to say. */
export function tlsRefusal(url: string, tls: TlsDto): string | undefined {
  if (tlsSet(tls) && /^http:\/\//i.test(url.trim())) return PLAIN_HTTP_TLS;
  return undefined;
}

/** What a batch of an HTTP destination is, as the form offers it: `body` and `format` together. Text and bytes in a
 * batch go a line each or as a JSON array of strings, bytes as base64. */
export type HttpBatched = 'ndjson' | 'jsonArray' | 'text' | 'textArray' | 'bytes' | 'bytesArray';

/** The batched choice `h` makes. Only meaningful when `h.body` is not `single`. */
export function batchedFormat(h: HttpDestination): HttpBatched {
  const array = h.body === 'jsonArray';
  if (h.format === 'bytes') return array ? 'bytesArray' : 'bytes';
  if (h.format === 'text') return array ? 'textArray' : 'text';
  return array ? 'jsonArray' : 'ndjson';
}

/** Sets `h`'s `body` and `format` to the batched choice `v`, leaving out what is default. A field named for text
 * or bytes stays only while the format reads one of the same kind. */
export function setBatched(h: HttpDestination, v: HttpBatched): void {
  if (v === 'jsonArray' || v === 'textArray' || v === 'bytesArray') h.body = 'jsonArray';
  else delete h.body;
  const format =
    v === 'text' || v === 'textArray'
      ? 'text'
      : v === 'bytes' || v === 'bytesArray'
        ? 'bytes'
        : undefined;
  if (format !== h.format) delete h.field;
  if (format) h.format = format;
  else delete h.format;
}

/** Sets `h` to batched or to one record per request. */
export function setSingle(h: HttpDestination, single: boolean): void {
  if (single) h.body = 'single';
  else delete h.body;
}
