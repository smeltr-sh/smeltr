import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';
import IcebergFields from './IcebergFields.svelte';
import { tableDeliveryModified } from './TableDelivery.svelte';
import {
  catalogRefusal,
  catalogStorageModified,
  namespaceRefusal,
  partitionRefusal,
  regionRefusal,
  setSignIn,
  signInOf,
  signingNameRefusal,
  signingRegion,
  type IcebergDestination,
} from './iceberg';

const iceberg = (): IcebergDestination => ({
  id: 'lake',
  type: 'iceberg',
  catalog: 'https://polaris.example.com/api/catalog',
  namespace: 'analytics.web',
  table: 'access_log',
});

describe('an Iceberg destination', () => {
  it('is refused nothing when it names a catalog, a namespace and a table', () => {
    const d = iceberg();
    expect(catalogRefusal(d)).toBeUndefined();
    expect(namespaceRefusal(d.namespace)).toBeUndefined();
    expect(regionRefusal(d)).toBeUndefined();
    expect(signingNameRefusal(d)).toBeUndefined();
  });

  it('signs in one way at a time, and switching unsets the last way', () => {
    const d = iceberg();
    expect(signInOf(d)).toBe('none');
    setSignIn(d, 'client');
    expect(d).toMatchObject({ clientId: '', clientSecret: '' });
    d.tokenUrl = 'https://auth.example.com/token';
    d.scope = 'PRINCIPAL_ROLE:ALL';
    setSignIn(d, 'token');
    expect(signInOf(d)).toBe('token');
    expect(d.token).toBe('');
    for (const key of ['clientId', 'clientSecret', 'tokenUrl', 'scope'])
      expect(d).not.toHaveProperty(key);
    setSignIn(d, 'aws');
    expect(d.signingName).toBe('glue');
    expect(d).not.toHaveProperty('token');
    setSignIn(d, 'none');
    expect(d).toEqual(iceberg());
  });

  it('refuses TLS settings over plain HTTP, and leaves whether the catalog is a URL to the save', () => {
    expect(catalogRefusal({ ...iceberg(), catalog: 'polaris:8181' })).toBeUndefined();
    expect(catalogRefusal({ ...iceberg(), catalog: '' })).toBeUndefined();
    const plain = { ...iceberg(), catalog: 'http://localhost:8181' };
    expect(catalogRefusal(plain)).toBeUndefined();
    expect(catalogRefusal({ ...plain, authorities: 'lake-ca' })).toMatch(/https/);
  });

  it('refuses a namespace with an empty level', () => {
    expect(namespaceRefusal('logs..web')).toMatch(/single dots/);
    expect(namespaceRefusal('.logs')).toMatch(/single dots/);
    expect(namespaceRefusal('logs')).toBeUndefined();
  });

  it('reads partition terms as the load does', () => {
    for (const good of ['host', 'day(ts)', ' bucket(16, user.id) ', 'truncate(4,name)', 'hour(ts)'])
      expect(partitionRefusal(good)).toBeUndefined();
    for (const bad of ['', 'day()', 'bucket(0, id)', 'bucket(id)', 'week(ts)', 'day(ts', 'a..b'])
      expect(partitionRefusal(bad)).toMatch(/no partition term/);
  });

  it('needs a region to sign for when the catalog address names none', () => {
    const glue = { ...iceberg(), catalog: 'https://glue.us-east-2.amazonaws.com/iceberg' };
    expect(signingRegion(glue.catalog)).toBe('us-east-2');
    expect(regionRefusal({ ...glue, signingName: 'glue' })).toBeUndefined();
    const proxied = { ...iceberg(), signingName: 's3tables' };
    expect(regionRefusal(proxied)).toMatch(/Set the region under Catalog and storage/);
    expect(regionRefusal({ ...proxied, region: 'eu-west-1' })).toBeUndefined();
    expect(signingRegion('https://amazonaws.com')).toBeUndefined();
    expect(signingNameRefusal({ ...iceberg(), signingName: 's3' })).toMatch(/glue or s3tables/);
  });

  it('marks each Advanced section for what it hides, and not at the defaults', () => {
    const d = iceberg();
    expect(catalogStorageModified(d)).toBe(false);
    expect(tableDeliveryModified({ ...d, compression: 'zstd', fileIntervalSecs: 60 })).toBe(false);
    expect(catalogStorageModified({ ...d, warehouse: 'analytics' })).toBe(true);
    expect(catalogStorageModified({ ...d, region: 'eu-west-1' })).toBe(true);
    expect(catalogStorageModified({ ...d, accessKeyId: 'AWS_ACCESS_KEY_ID' })).toBe(true);
    expect(tableDeliveryModified({ ...d, compression: 'snappy' })).toBe(true);
    expect(tableDeliveryModified({ ...d, fileIntervalSecs: 120 })).toBe(true);
  });

  it('shows the sign-in its config names, and that sign-in only', () => {
    const shown = (d: IcebergDestination) =>
      render(IcebergFields, { props: { value: d, secrets: [] } }).body;
    const none = shown(iceberg());
    expect(none).toContain('catalog sign-in');
    expect(none).not.toMatch(/client id\b/);
    const client = shown({ ...iceberg(), clientId: 'smeltr', clientSecret: 'CATALOG_SECRET' });
    expect(client).toMatch(/client id\b/);
    expect(client).not.toContain('signing service');
    const aws = shown({ ...iceberg(), signingName: 's3tables' });
    expect(aws).toContain('signing service');
    expect(aws).toContain('Set the region under Catalog and storage');
  });
});
