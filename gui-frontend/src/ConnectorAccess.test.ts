import { describe, expect, it } from 'vitest';
import type { SourceDto } from './api';
import {
  accessModified,
  credentialOf,
  setCredential,
  setField,
  shown,
} from './ConnectorAccess.svelte';

const azure = () => ({ id: 'a', type: 'azureBlob', account: 'acct', container: 'logs' });
const sqs = (): Extract<SourceDto, { type: 's3Sqs' }> => ({
  id: 'q',
  type: 's3Sqs',
  queueUrl: 'https://sqs.eu-west-1.amazonaws.com/1/q',
});

describe('an object store credential', () => {
  it("is the host's when the config names none", () => {
    expect(credentialOf('azureBlob', azure())).toBe('host');
    expect(credentialOf('gcs', { id: 'g', type: 'gcs', bucket: 'b' })).toBe('host');
    expect(credentialOf('s3', sqs())).toBe('host');
  });

  it('names every field of the one chosen, and none of another', () => {
    const d = azure();
    setCredential('azureBlob', d, 'sasToken');
    expect(d).toMatchObject({ sasToken: '' });
    setCredential('azureBlob', d, 'servicePrincipal');
    expect(d).not.toHaveProperty('sasToken');
    expect(d).toMatchObject({ tenantId: '', clientId: '', clientSecret: '' });
    expect(credentialOf('azureBlob', d)).toBe('servicePrincipal');
  });

  it("names nothing when it is the host's", () => {
    const d = sqs();
    setCredential('s3', d, 'accessKey');
    expect(d).toMatchObject({ accessKeyId: '', secretAccessKey: '' });
    setCredential('s3', d, 'host');
    expect(d).toEqual(sqs());
  });

  it('takes the auth endpoint with the service principal it serves', () => {
    const d = azure();
    setCredential('azureBlob', d, 'servicePrincipal');
    setField('azureBlob', d, 'authEndpoint', 'https://login.example');
    setCredential('azureBlob', d, 'accessKey');
    expect(d).not.toHaveProperty('authEndpoint');
    expect(shown('azureBlob', d, 'authEndpoint')).toBe(false);
  });
});

describe('an SFTP credential', () => {
  const server = () => ({
    id: 'f',
    type: 'sftp',
    host: 'sftp.example.com',
    username: 'smeltr',
    hostKey: 'ssh-ed25519 AAAA',
  });

  it('is a password when the config names none, since a server has no host credential', () => {
    expect(credentialOf('sftp', server())).toBe('password');
  });

  it('is a password or a private key, never both', () => {
    const d = server();
    setCredential('sftp', d, 'privateKey');
    expect(d).toMatchObject({ privateKey: '' });
    expect(credentialOf('sftp', d)).toBe('privateKey');
    setCredential('sftp', d, 'password');
    expect(d).not.toHaveProperty('privateKey');
    expect(d).toMatchObject({ password: '' });
    expect(credentialOf('sftp', d)).toBe('password');
  });

  it('leaves the server, user and host key alone, and has nothing under Advanced', () => {
    const d = server();
    setCredential('sftp', d, 'privateKey');
    expect(d).toMatchObject(server());
    expect(accessModified('sftp', { ...d, port: 2222 })).toBe(false);
  });
});

describe('a field that needs another', () => {
  it('is shown only once that one is set, and unset with it', () => {
    const d = sqs();
    expect(shown('s3', d, 'externalId')).toBe(false);
    setField('s3', d, 'roleArn', 'arn:aws:iam::1:role/w');
    setField('s3', d, 'externalId', 'x');
    expect(shown('s3', d, 'externalId')).toBe(true);
    setField('s3', d, 'roleArn', undefined);
    expect(d).not.toHaveProperty('externalId');
  });
});

describe('the access Advanced marker', () => {
  it('is on for a field the Advanced half shows, and off for the credential', () => {
    expect(accessModified('s3', sqs())).toBe(false);
    expect(accessModified('s3', { ...sqs(), accessKeyId: 'k', secretAccessKey: 's' })).toBe(false);
    expect(accessModified('s3', { ...sqs(), region: 'eu-west-1' })).toBe(true);
  });
});

describe('an Event Hubs sign-in', () => {
  const hubs = (): Extract<SourceDto, { type: 'eventHubs' }> => ({
    id: 'h',
    type: 'eventHubs',
    namespace: 'acme',
    eventHubs: ['logs'],
    consumerGroup: 'smeltr',
  });

  it("is the host's managed identity when the config names none", () => {
    expect(credentialOf('eventHubs', hubs())).toBe('host');
  });

  it('is a connection string or a service principal, never both', () => {
    const d = hubs();
    setCredential('eventHubs', d, 'connectionString');
    expect(d).toMatchObject({ connectionString: '' });
    expect(credentialOf('eventHubs', d)).toBe('connectionString');
    setCredential('eventHubs', d, 'servicePrincipal');
    expect(d).not.toHaveProperty('connectionString');
    expect(d).toMatchObject({ tenantId: '', clientId: '', clientSecret: '' });
    setField('eventHubs', d, 'authEndpoint', 'https://login.microsoftonline.us');
    expect(accessModified('eventHubs', d)).toBe(true);
    setCredential('eventHubs', d, 'host');
    expect(d).toEqual(hubs());
  });

  it('names a user-assigned managed identity by its client id alone', () => {
    const d = hubs();
    setCredential('eventHubs', d, 'servicePrincipal');
    setCredential('eventHubs', d, 'managedIdentity');
    expect(d).toEqual({ ...hubs(), managedIdentityClientId: '' });
    expect(credentialOf('eventHubs', d)).toBe('managedIdentity');
    setCredential('eventHubs', d, 'host');
    expect(d).toEqual(hubs());
  });
});
