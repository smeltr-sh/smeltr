import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';
import DeltaFields from './DeltaFields.svelte';
import {
  catalogTableRefusal,
  clientIdRefusal,
  endpointRefusal,
  locationRefusal,
  namingOf,
  partitionRefusal,
  regionRefusal,
  setNaming,
  storageModified,
  workspaceRefusal,
  type DeltaDestination,
} from './delta';

const delta = (): DeltaDestination => ({
  id: 'lake',
  type: 'delta',
  location: 's3://lake/tables/web',
});

describe('a Delta destination', () => {
  it('is refused nothing when it names a location in S3', () => {
    const d = delta();
    expect(locationRefusal(d.location ?? '')).toBeUndefined();
    expect(partitionRefusal([])).toBeUndefined();
    expect(endpointRefusal(d)).toBeUndefined();
  });

  it('reads a location as the load does', () => {
    for (const bad of ['lake/tables/web', 'https://lake/tables/web', 's3://', 's3:///tables/web'])
      expect(locationRefusal(bad)).toMatch(/s3:\/\/bucket\/path/);
    for (const fine of ['s3://lake/tables/web', 's3a://lake/tables/web', 's3://lake', ''])
      expect(locationRefusal(fine)).toBeUndefined();
  });

  it('partitions by top-level columns, each named once', () => {
    expect(partitionRefusal(['host', 'day'])).toBeUndefined();
    for (const bad of ['http.host', 'day(ts)', ' '])
      expect(partitionRefusal([bad])).toMatch(/top-level columns/);
    expect(partitionRefusal(['host', 'day', 'host'])).toMatch(/"host" is named twice/);
  });

  it('refuses TLS settings on a plain HTTP endpoint', () => {
    const d = { ...delta(), certificate: 'lake-client' };
    expect(endpointRefusal(d)).toBeUndefined();
    expect(endpointRefusal({ ...d, endpoint: 'https://minio.example.com' })).toBeUndefined();
    expect(endpointRefusal({ ...d, endpoint: 'http://localhost:9000' })).toMatch(/certificate/);
    expect(endpointRefusal({ ...delta(), endpoint: 'http://localhost:9000' })).toBeUndefined();
    expect(endpointRefusal({ ...delta(), endpoint: 'http://minio:9000' })).toBeUndefined();
  });

  it("shows the location's refusal, and the credential its config names", () => {
    const shown = (d: DeltaDestination) =>
      render(DeltaFields, { props: { value: d, secrets: [] } }).body;
    const host = shown(delta());
    expect(host).toContain('location');
    expect(host).not.toContain('access key id');
    expect(host).toContain("the host's credentials");
    expect(host).not.toContain('s3://bucket/path');
    const keyed = shown({ ...delta(), accessKeyId: 'AWS_ACCESS_KEY_ID', secretAccessKey: '' });
    expect(keyed).toContain('access key id');
    expect(shown({ ...delta(), location: 'lake/web' })).toContain('s3://bucket/path');
  });
});

const catalogued = (): DeltaDestination => ({
  id: 'lake',
  type: 'delta',
  unityCatalog: {
    workspace: 'https://dbc-1234.cloud.databricks.com',
    table: 'main.web.access_log',
    clientId: 'smeltr-writer',
    clientSecret: 'DATABRICKS_CLIENT_SECRET',
  },
  region: 'us-west-2',
});

describe('a Unity Catalog Delta destination', () => {
  it('is refused nothing when it names a workspace, a table, a client and a region', () => {
    const d = catalogued();
    const uc = d.unityCatalog!;
    expect(workspaceRefusal(uc.workspace)).toBeUndefined();
    expect(catalogTableRefusal(uc.table)).toBeUndefined();
    expect(clientIdRefusal(uc.clientId)).toBeUndefined();
    expect(regionRefusal(d)).toBeUndefined();
  });

  it("reads the workspace as the workspace's root, over https", () => {
    for (const bad of [
      'dbc-1234.cloud.databricks.com',
      'https://dbc-1234.cloud.databricks.com/browse',
      'https://dbc-1234.cloud.databricks.com/?o=1234',
      'https://dbc-1234.cloud.databricks.com/#tables',
      'ftp://dbc-1234.cloud.databricks.com',
    ])
      expect(workspaceRefusal(bad)).toMatch(/nothing after its host/);
    for (const fine of [
      'http://dbc-1234.cloud.databricks.com',
      'https://dbc-1234.cloud.databricks.com',
      'https://dbc-1234.cloud.databricks.com/',
      'http://localhost:8080',
      '',
    ])
      expect(workspaceRefusal(fine)).toBeUndefined();
  });

  it('names the table by its three parts', () => {
    for (const bad of ['access_log', 'web.access_log', 'main..access_log', 'a.b.c.d'])
      expect(catalogTableRefusal(bad)).toMatch(/catalog\.schema\.table/);
    expect(catalogTableRefusal('')).toBeUndefined();
  });

  it('names a client id', () => {
    expect(clientIdRefusal('')).toMatch(/OAuth client id/);
    expect(clientIdRefusal(' ')).toMatch(/OAuth client id/);
  });

  it("names the bucket's region, which the catalog does not give", () => {
    const { region: _, ...d } = catalogued();
    expect(regionRefusal(d)).toMatch(/Name the bucket's region/);
    expect(regionRefusal({ ...d, region: ' ' })).toMatch(/Name the bucket's region/);
    expect(regionRefusal(delta())).toBeUndefined();
  });

  it('names one table at a time, and drops the storage credentials the catalog gives', () => {
    const d: DeltaDestination = {
      ...delta(),
      accessKeyId: 'AWS_ACCESS_KEY_ID',
      secretAccessKey: 'AWS_SECRET_ACCESS_KEY',
      roleArn: 'arn:aws:iam::1:role/w',
      externalId: 'x',
      authEndpoint: 'https://sts.example.com',
      endpoint: 'https://minio.example.com',
      region: 'us-west-2',
    };
    expect(namingOf(d)).toBe('location');
    setNaming(d, 'unityCatalog');
    expect(namingOf(d)).toBe('unityCatalog');
    expect(d).toEqual({
      id: 'lake',
      type: 'delta',
      endpoint: 'https://minio.example.com',
      region: 'us-west-2',
      unityCatalog: { workspace: '', table: '', clientId: '', clientSecret: '' },
    });
    setNaming(d, 'location');
    expect(d).toEqual({
      id: 'lake',
      type: 'delta',
      endpoint: 'https://minio.example.com',
      region: 'us-west-2',
      location: '',
    });
  });

  it('marks Storage only for the endpoint, since the region is asked in the open', () => {
    expect(storageModified(catalogued())).toBe(false);
    expect(storageModified({ ...catalogued(), endpoint: 'https://minio.example.com' })).toBe(true);
    expect(storageModified({ ...delta(), region: 'us-west-2' })).toBe(true);
  });

  it("shows the catalog's fields and the region, and no storage credential", () => {
    const shown = (d: DeltaDestination) =>
      render(DeltaFields, { props: { value: d, secrets: [] } }).body;
    const body = shown(catalogued());
    for (const label of ['workspace', 'client id', 'client secret', 'region'])
      expect(body).toContain(label);
    expect(body).toContain('main.web.access_log');
    expect(body).not.toContain('s3://lake');
    expect(body).not.toContain("the host's credentials");
    expect(body).not.toContain('role ARN');
    expect(body).not.toContain("Name the bucket's region");
    const { region: _, ...regionless } = catalogued();
    expect(shown(regionless)).toContain("Name the bucket's region");
    expect(
      shown({ ...catalogued(), unityCatalog: { ...catalogued().unityCatalog!, table: 'web' } }),
    ).toContain('catalog.schema.table');
  });
});
