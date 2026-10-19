import { describe, expect, it } from 'vitest';
import {
  batchedFormat,
  headerRefusal,
  headerValueRefusal,
  onLoopback,
  setBatched,
  setSingle,
  identityRefusal,
  urlWarning,
  type HttpDestination,
} from './http';

const http = (over: Partial<HttpDestination> = {}): HttpDestination => ({
  id: 'h',
  type: 'http',
  url: 'http://collector.internal/ingest',
  ...over,
});

describe('a header row', () => {
  it('takes an ordinary header in either table', () => {
    expect(headerRefusal('X-Source', {})).toBeUndefined();
    expect(headerRefusal('Authorization', {})).toBeUndefined();
    expect(headerRefusal('X-Api-Key', {})).toBeUndefined();
    expect(headerRefusal('', {})).toBeUndefined();
  });

  it('refuses a header smeltr sets itself, in any case', () => {
    for (const name of [
      'Content-Type',
      'content-encoding',
      'CONTENT-LENGTH',
      'Transfer-Encoding',
      'Host',
      'Cookie',
    ])
      expect(headerRefusal(name, {})).toMatch(/smeltr sets/);
  });

  it('refuses the remote_write version header on a prometheus destination only', () => {
    const name = 'X-Prometheus-Remote-Write-Version';
    expect(headerRefusal(name, { type: 'prometheus' })).toMatch(/smeltr sets/);
    expect(headerRefusal(name, { type: 'http' })).toBeUndefined();
  });

  it('refuses a name that is not an HTTP token', () => {
    expect(headerRefusal('X Source', {})).toMatch(/not a header name/);
    expect(headerRefusal('X-Source:', {})).toMatch(/not a header name/);
    expect(headerRefusal("X-!#$%&'*+.^_`|~1", {})).toBeUndefined();
  });

  it('refuses one header set twice, across the tables or within one, whatever its case', () => {
    const across = { headers: { 'x-tenant': 'a' }, credentials: { 'X-Tenant': 'T' } };
    expect(headerRefusal('X-Tenant', across)).toMatch(/set twice/);
    expect(headerRefusal('x-tenant', across)).toMatch(/set twice/);
    const within = { headers: { 'X-Source': 'a', 'x-source': 'b' } };
    expect(headerRefusal('X-Source', within)).toMatch(/set twice/);
    expect(headerRefusal('X-Source', { headers: { 'X-Source': 'a' } })).toBeUndefined();
  });

  it('refuses a value holding a line break or control character, and takes a tab', () => {
    expect(headerValueRefusal('a\r\nX-Evil: 1')).toMatch(/line break/);
    expect(headerValueRefusal('a\u0000')).toMatch(/control/);
    expect(headerValueRefusal('a\u0085')).toMatch(/control/);
    expect(headerValueRefusal('a\tb')).toBeUndefined();
    expect(headerValueRefusal('plain value')).toBeUndefined();
  });
});

describe('the URL', () => {
  it('names this host by localhost or a loopback address', () => {
    expect(onLoopback('http://localhost:8080/x')).toBe(true);
    expect(onLoopback('http://127.0.0.1/')).toBe(true);
    expect(onLoopback('http://[::1]:9000')).toBe(true);
    expect(onLoopback('http://user@LOCALHOST')).toBe(true);
    expect(onLoopback('http://collector.internal')).toBe(false);
    expect(onLoopback('http://localhost.evil.com')).toBe(false);
  });

  it('warns about credentials over plain HTTP off this host, and saves them', () => {
    expect(identityRefusal('http://collector.internal', undefined)).toBeUndefined();
    expect(urlWarning('http://collector.internal', true)).toMatch(/unencrypted/);
    expect(urlWarning('http://collector.internal', false)).toBeUndefined();
    expect(urlWarning('http://localhost:8080', true)).toBeUndefined();
    expect(urlWarning('HTTPS://collector.internal', true)).toBeUndefined();
  });

  it('recommends Basic auth for a user and password in the URL', () => {
    expect(urlWarning('https://me:pw@collector.internal', false)).toMatch(/^Put the user/);
    expect(urlWarning('http://me@collector.internal/a@b', false)).toMatch(
      /unencrypted.*Basic auth/,
    );
    expect(urlWarning('http://me:pw@localhost', false)).not.toMatch(/unencrypted/);
    expect(urlWarning('https://collector.internal/a@b', false)).toBeUndefined();
  });

  it('refuses an identity on plain HTTP, even on this host', () => {
    expect(identityRefusal('http://localhost', 'acme')).toMatch(/identity/);
    expect(identityRefusal('https://x', 'acme')).toBeUndefined();
  });

  it('leaves whether the address is a URL to the save', () => {
    expect(identityRefusal('collector.internal/ingest', 'acme')).toBeUndefined();
    expect(identityRefusal('ftp://collector.internal', 'acme')).toBeUndefined();
  });
});

describe('the batched and single choices', () => {
  it('reads body and format as one batched choice', () => {
    expect(batchedFormat(http())).toBe('ndjson');
    expect(batchedFormat(http({ body: 'jsonArray' }))).toBe('jsonArray');
    expect(batchedFormat(http({ format: 'text', field: 'msg' }))).toBe('text');
  });

  it('writes a batched choice as body and format, leaving out defaults', () => {
    const h = http({ format: 'text', field: 'msg' });
    setBatched(h, 'jsonArray');
    expect(h).toEqual(http({ body: 'jsonArray' }));
    setBatched(h, 'text');
    expect(h).toEqual(http({ format: 'text' }));
  });

  it('keeps bytes when a single destination is batched, as base64 lines', () => {
    const h = http({ body: 'single', format: 'bytes', field: 'payload' });
    setSingle(h, false);
    expect(h).toEqual(http({ format: 'bytes', field: 'payload' }));
    expect(batchedFormat(h)).toBe('bytes');
    setSingle(h, true);
    expect(h).toEqual(http({ body: 'single', format: 'bytes', field: 'payload' }));
  });

  it('batches text as lines or a JSON array, keeping its field', () => {
    const h = http({ format: 'text', field: 'msg' });
    setBatched(h, 'textArray');
    expect(h).toEqual(http({ body: 'jsonArray', format: 'text', field: 'msg' }));
    expect(batchedFormat(h)).toBe('textArray');
    setBatched(h, 'text');
    expect(h).toEqual(http({ format: 'text', field: 'msg' }));
    setBatched(h, 'bytesArray');
    expect(h).toEqual(http({ body: 'jsonArray', format: 'bytes' }));
  });

  it('batches bytes as base64 lines or a JSON array, keeping their field', () => {
    const h = http({ format: 'bytes', field: 'payload' });
    setBatched(h, 'bytesArray');
    expect(h).toEqual(http({ body: 'jsonArray', format: 'bytes', field: 'payload' }));
    expect(batchedFormat(h)).toBe('bytesArray');
    setBatched(h, 'text');
    expect(h).toEqual(http({ format: 'text' }));
  });
});
