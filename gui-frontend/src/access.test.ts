import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  allows,
  areaOfPath,
  canEdit,
  deployBlocker,
  heldAreas,
  holds,
  levelIn,
  levelOf,
  settleScope,
} from './access';
import { currentScope, setScope } from './api';
import { load, signedOut } from './session';

function signInAs(access: { administrator: boolean; areas: Record<string, string> }) {
  vi.spyOn(globalThis, 'fetch').mockImplementation(
    async () =>
      new Response(
        JSON.stringify({
          authenticated: true,
          userId: 'u_1',
          displayName: 'Lee Park',
          administrator: access.administrator,
          mustChangePassword: false,
          access,
        }),
        { headers: { 'content-type': 'application/json' } },
      ),
  );
  return load();
}

beforeEach(() => signedOut());
afterEach(() => {
  vi.restoreAllMocks();
  setScope('shared');
});

const LEE = { shared: 'viewer', payments: 'viewer', search: 'editor', storefront: 'deployer' };

describe('what a person may do', () => {
  it('gives an administrator every level in every area', async () => {
    await signInAs({ administrator: true, areas: {} });
    expect(levelIn('anything')).toBe('deployer');
    expect(heldAreas()).toEqual([]);
  });

  it('orders levels, so a deployer edits and a viewer does not', async () => {
    await signInAs({ administrator: false, areas: LEE });
    expect(canEdit('storefront')).toBe(true);
    expect(canEdit('search')).toBe(true);
    expect(canEdit('payments')).toBe(false);
    expect(holds('default', 'viewer')).toBe(false);
  });

  it('lists the areas held, shared first', async () => {
    await signInAs({ administrator: false, areas: LEE });
    expect(heldAreas()).toEqual(['shared', 'payments', 'search', 'storefront']);
  });

  it('reads shared for anyone holding an area, as the leader does', async () => {
    await signInAs({ administrator: false, areas: { payments: 'viewer' } });
    expect(levelIn('shared')).toBe('viewer');
    expect(canEdit('shared')).toBe(false);
    expect(heldAreas()).toEqual(['shared', 'payments']);
    setScope('search');
    settleScope();
    expect(currentScope()).toBe('shared');
  });

  it('moves a scope that is not held to the first one that is', async () => {
    await signInAs({ administrator: false, areas: LEE });
    setScope('default');
    settleScope();
    expect(currentScope()).toBe('shared');
    setScope('search');
    settleScope();
    expect(currentScope()).toBe('search');
  });

  it('needs a deployer in every area a change touches', async () => {
    await signInAs({ administrator: false, areas: LEE });
    expect(deployBlocker(['groups/storefront/sources.json'])).toBe('');
    expect(
      deployBlocker(['groups/storefront/sources.json', 'groups/search/routes.json', 'shared/x']),
    ).toBe('Deploying this takes a deployer in search, shared.');
  });

  it('needs an administrator for a path in no area, as the leader does', async () => {
    await signInAs({ administrator: false, areas: { shared: 'deployer', storefront: 'deployer' } });
    expect(deployBlocker(['groups/storefront/sources.json', 'README.md'])).toBe(
      'Deploying this takes an administrator: it changes README.md.',
    );
    expect(deployBlocker(['groups/storefront/sources.json', 'shared/routes.json'])).toBe('');
  });

  it('judges another account the way it judges the person signed in', async () => {
    await signInAs({ administrator: true, areas: {} });
    const touches = ['groups/storefront/sources.json', 'shared/routes.json'];
    expect(
      deployBlocker(touches, { administrator: false, areas: { storefront: 'deployer' } }),
    ).toBe('Deploying this takes a deployer in shared.');
    expect(
      deployBlocker(touches, {
        administrator: false,
        areas: { storefront: 'deployer', shared: 'deployer' },
      }),
    ).toBe('');
    expect(deployBlocker(touches, { administrator: true, areas: {} })).toBe('');
  });
});

describe('one rule for any account', () => {
  it('reads shared as viewer for anyone holding an area, and more only by name', () => {
    const access = { administrator: false, areas: { payments: 'deployer' as const } };
    expect(levelOf(access, 'shared')).toBe('viewer');
    expect(allows(access, 'shared', 'deployer')).toBe(false);
    expect(allows(access, 'payments', 'deployer')).toBe(true);
    expect(levelOf({ administrator: false, areas: {} }, 'shared')).toBeNull();
    expect(levelOf({ administrator: true, areas: {} }, 'anything')).toBe('deployer');
  });
});

describe('areaOfPath', () => {
  it('reads the area a path is in as policy::area_of_path does', () => {
    expect(areaOfPath('shared/sources.json')).toBe('shared');
    expect(areaOfPath('groups/west/pipelines/a.json')).toBe('west');
    expect(areaOfPath('shared/')).toBeNull();
    expect(areaOfPath('shared')).toBeNull();
    expect(areaOfPath('groups/west/')).toBeNull();
    expect(areaOfPath('groups//x')).toBeNull();
    expect(areaOfPath('sources.json')).toBeNull();
    expect(areaOfPath('sharedfoo/x')).toBeNull();
  });
});
