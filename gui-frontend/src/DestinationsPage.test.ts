import { describe, expect, it } from 'vitest';
import type { DestinationDto } from './api';
import type { HttpDestination } from './http';
import {
  httpModified,
  isCloud,
  isObjectStore,
  objectModified,
  writes,
  type ObjectDestination,
} from './destinations';

const azure = (): Extract<ObjectDestination, { type: 'azureBlob' }> => ({
  id: 'a',
  type: 'azureBlob',
  account: 'acct',
  container: 'logs',
});
const s3 = (): Extract<ObjectDestination, { type: 's3' }> => ({ id: 's', type: 's3', bucket: 'b' });
const sftp = (): Extract<ObjectDestination, { type: 'sftp' }> => ({
  id: 'f',
  type: 'sftp',
  host: 'sftp.example.com',
  username: 'smeltr',
  password: 'SFTP_PASSWORD',
  hostKey: 'ssh-ed25519 AAAA',
});

describe('the object Advanced marker', () => {
  it('is off at the defaults', () => {
    expect(objectModified(s3())).toBe(false);
    expect(objectModified({ ...s3(), compression: 'gzip', maxObjectBytes: 64 * 1024 * 1024 })).toBe(
      false,
    );
  });

  it('is on for an access field, the KMS key or a shared setting it hides', () => {
    expect(objectModified({ ...s3(), region: 'eu-west-1' })).toBe(true);
    expect(objectModified({ ...s3(), kmsKeyId: 'k' })).toBe(true);
    expect(objectModified({ ...azure(), storageClass: 'Cool' })).toBe(true);
    expect(objectModified({ ...s3(), compression: 'zstd' })).toBe(true);
    expect(objectModified({ ...s3(), maxObjectBytes: '32 MiB' })).toBe(true);
    expect(objectModified({ ...s3(), fileIntervalSecs: 600 })).toBe(true);
    expect(objectModified({ ...s3(), fileIntervalSecs: 60 })).toBe(false);
  });
});

describe('an SFTP destination', () => {
  it('is an object destination, and no cloud: it takes no storage class or TLS settings', () => {
    expect(isObjectStore(sftp())).toBe(true);
    expect(isCloud(sftp())).toBe(false);
    expect(isCloud(s3())).toBe(true);
  });

  it('marks Advanced for a shared setting, and not for its credential or default port', () => {
    expect(objectModified(sftp())).toBe(false);
    expect(objectModified({ ...sftp(), privateKey: 'K', password: undefined })).toBe(false);
    expect(objectModified({ ...sftp(), compression: 'zstd' })).toBe(true);
  });
});

describe('the HTTP Advanced marker', () => {
  const http = (): HttpDestination => ({ id: 'h', type: 'http', url: 'https://x' });

  it('is off at the defaults, and for what the main form shows', () => {
    expect(httpModified(http())).toBe(false);
    expect(
      httpModified({ ...http(), compression: 'none', body: 'single', headers: { 'X-A': '1' } }),
    ).toBe(false);
  });

  it('is on for a TLS setting, compression or a delivery setting', () => {
    expect(httpModified({ ...http(), certificate: 'acme' })).toBe(true);
    expect(httpModified({ ...http(), trustPublic: false })).toBe(true);
    expect(httpModified({ ...http(), compression: 'gzip' })).toBe(true);
    expect(httpModified({ ...http(), maxRequestBytes: '4 MiB' })).toBe(true);
    expect(httpModified({ ...http(), stallAfterSecs: 5 })).toBe(true);
  });
});

describe('an Iceberg destination', () => {
  it('is no object destination: it writes a table, not objects', () => {
    const iceberg: DestinationDto = {
      id: 'i',
      type: 'iceberg',
      catalog: 'https://catalog.example.com',
      namespace: 'logs',
      table: 'web',
    };
    expect(isObjectStore(iceberg)).toBe(false);
    expect(isCloud(iceberg)).toBe(false);
  });
});

describe('a Delta destination', () => {
  it('is no object destination: it writes a table, not objects', () => {
    const delta: DestinationDto = { id: 'd', type: 'delta', location: 's3://lake/web' };
    expect(isObjectStore(delta)).toBe(false);
    expect(isCloud(delta)).toBe(false);
  });

  it('writes its root in S3, or its Unity Catalog table', () => {
    expect(writes({ id: 'd', type: 'delta', location: 's3://lake/web' })).toBe('s3://lake/web');
    const catalogued: DestinationDto = {
      id: 'd',
      type: 'delta',
      unityCatalog: {
        workspace: 'https://dbc-1234.cloud.databricks.com',
        table: 'main.web.access_log',
        clientId: 'smeltr-writer',
        clientSecret: 'DATABRICKS_CLIENT_SECRET',
      },
      region: 'us-west-2',
    };
    expect(writes(catalogued)).toBe('main.web.access_log');
  });
});

describe('an Event Hubs destination', () => {
  it('writes its default event hub', () => {
    const hub: DestinationDto = {
      id: 'e',
      type: 'eventHubs',
      namespace: 'acme-logs',
      defaultEventHub: 'alerts',
      connectionString: 'EVENTHUBS_CONNECTION_STRING',
    };
    expect(writes(hub)).toBe('alerts');
    expect(isObjectStore(hub)).toBe(false);
  });
});
