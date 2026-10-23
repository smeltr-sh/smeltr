import { describe, expect, it } from 'vitest';
import {
  issuerProblem,
  keepsStoredSecret,
  missingForSignIn,
  partialScheme,
} from './OidcSettings.svelte';

describe('missingForSignIn', () => {
  const complete = {
    issuer: 'https://example.okta.com',
    clientId: 'smeltr',
    secretHeld: true,
  };

  it('lacks nothing when every part is there', () => {
    expect(missingForSignIn(complete)).toEqual([]);
  });

  it('names each missing part, in the order the form shows them', () => {
    expect(missingForSignIn({ issuer: ' ', clientId: '', secretHeld: false })).toEqual([
      'issuer URL',
      'client ID',
      'client secret',
    ]);
  });

  it('needs a secret, new or stored', () => {
    expect(missingForSignIn({ ...complete, secretHeld: false })).toEqual(['client secret']);
  });
});

describe('issuerProblem', () => {
  it('takes what the leader takes', () => {
    for (const ok of [
      'https://example.okta.com',
      'https://login.example.com/realms/smeltr/',
      'http://localhost:8180/realms/smeltr',
      'http://127.0.0.1:8080',
      'http://[0:0:0:0:0:0:0:1]:8080',
      '',
    ]) {
      expect(issuerProblem(ok), ok).toBe('');
    }
  });

  it('refuses what the leader refuses', () => {
    for (const bad of [
      'https://',
      'https://idp?x=1',
      'https://idp#top',
      'https://user@idp',
      'https://id p.example.com',
      'http://id.example.com',
      'ftp://id.example.com',
      'id.example.com',
    ]) {
      expect(issuerProblem(bad), bad).not.toBe('');
    }
  });

  it('holds its tongue while a scheme is still being typed', () => {
    expect(partialScheme('ht')).toBe(true);
    expect(partialScheme('https:/')).toBe(true);
    expect(partialScheme('https://i')).toBe(false);
  });
});

describe('keepsStoredSecret', () => {
  const stored = { clientSecretSet: true, issuer: 'https://idp', clientId: 'smeltr' };

  it('keeps it while the issuer and client id stay the same', () => {
    expect(keepsStoredSecret(stored, ' https://idp ', 'smeltr')).toBe(true);
  });

  it('forgets it when either changes, as the leader does', () => {
    expect(keepsStoredSecret(stored, 'https://other', 'smeltr')).toBe(false);
    expect(keepsStoredSecret(stored, 'https://idp', 'other')).toBe(false);
  });

  it('has nothing to keep when none is stored', () => {
    expect(keepsStoredSecret({ ...stored, clientSecretSet: false }, 'https://idp', 'smeltr')).toBe(
      false,
    );
  });
});
