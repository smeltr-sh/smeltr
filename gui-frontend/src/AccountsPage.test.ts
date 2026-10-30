// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import type { Question } from './Confirm.svelte';

// The page asks through the app's one dialog, which App mounts; here each test answers it.
const ask = vi.hoisted(() => vi.fn<(q: Question) => Promise<boolean | null>>());
vi.mock('./Confirm.svelte', () => ({ ask }));

import AccountsPage from './AccountsPage.svelte';

let page: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
let calls: { method: string; url: string; body: unknown }[];
let resets: number;

const ROLES = [
  { name: 'administrator', builtIn: true, groups: ['smeltr-admins'], areas: {} },
  {
    name: 'payments',
    builtIn: false,
    groups: ['smeltr-payments', 'smeltr-oncall'],
    areas: { payments: 'deployer', shared: 'viewer' },
  },
];

const ACCOUNTS = [
  {
    id: 'u-ada',
    username: 'ada',
    displayName: 'Ada',
    enabled: true,
    authSource: 'local',
    createdAt: 0,
    updatedAt: 0,
    roles: ['administrator'],
    groups: [],
    access: {},
  },
  {
    id: 'u-cy',
    username: 'cy',
    displayName: 'Cy',
    enabled: true,
    authSource: 'local',
    createdAt: 0,
    updatedAt: 0,
    roles: ['payments'],
    groups: [],
    access: {},
  },
  {
    id: 'u-bo',
    displayName: 'Bo',
    providerLogin: 'bo@example.com',
    enabled: true,
    authSource: 'oidc',
    createdAt: 0,
    updatedAt: 0,
    roles: ['payments'],
    groups: ['everyone', 'smeltr-payments', 'smeltr-search'],
    access: {},
  },
  {
    id: 'u-exp',
    displayName: 'exporter',
    enabled: true,
    authSource: 'key',
    createdAt: 1,
    updatedAt: 1,
    roles: ['payments'],
    groups: [],
    access: {},
    keys: [
      { id: 'key_1', label: 'nightly', createdAt: 0, expiresAt: null, lastUsedAt: null },
      {
        id: 'key_2',
        label: 'weekly',
        createdAt: 0,
        // Made a moment ago for 10 days, which it still reads.
        expiresAt: Date.now() + 10 * 86_400_000 - 5_000,
        lastUsedAt: null,
      },
    ],
  },
];

function json(body: unknown) {
  return new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
}

function stubFetch() {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string, init?: RequestInit) => {
      const method = init?.method ?? 'GET';
      if (method !== 'GET') {
        calls.push({ method, url, body: init?.body ? JSON.parse(String(init.body)) : null });
      }
      if (url.startsWith('/api/roles/')) return json({ ok: true, errors: [] });
      if (url === '/api/roles') return json({ ok: true, errors: [], roles: ROLES });
      if (url.startsWith('/api/worker-groups')) {
        return json({
          ok: true,
          errors: [],
          groups: [{ id: 'payments', name: 'Payments', deployed: true, hue: 180 }],
          sharedHue: 330,
        });
      }
      if (url.endsWith('/reset-password')) {
        resets += 1;
        return json({
          ok: true,
          errors: [],
          userId: 'u-ada',
          username: 'ada',
          temporaryPassword: `pw-${resets}`,
        });
      }
      if (url.startsWith('/api/accounts/') && method === 'PATCH')
        return json({ ok: true, errors: [], ...ACCOUNTS[0] });
      if (url.endsWith('/keys') && method === 'POST')
        return json({
          ok: true,
          errors: [],
          id: 'key_3',
          label: '',
          createdAt: 0,
          expiresAt: null,
          key: 'smk_secret',
        });
      if (url === '/api/agents') return json({ ok: true, errors: [], userId: 'u-new' });
      if (url.startsWith('/api/agents/')) return json({ ok: true, errors: [], roles: [] });
      if (url === '/api/accounts' && method === 'POST')
        return json({ ok: true, errors: [], ...ACCOUNTS[3] });
      if (url === '/api/accounts') return json({ ok: true, errors: [], accounts: ACCOUNTS });
      throw new Error(`unexpected ${url}`);
    }),
  );
}

async function rolesTab() {
  stubFetch();
  page = mount(AccountsPage, { target, props: { section: 'roles' } });
  await vi.waitFor(() => expect(target.querySelector('tbody tr')).not.toBeNull());
}

async function accountsTab() {
  stubFetch();
  page = mount(AccountsPage, { target, props: { section: 'accounts' } });
  await vi.waitFor(() => expect(target.querySelector('tbody tr')).not.toBeNull());
}

function button(text: string, within: ParentNode = target): HTMLButtonElement | undefined {
  return [...within.querySelectorAll('button')].find((b) => b.textContent?.trim() === text);
}

/** The open row's form, so its Cancel is not the dialogs'. */
function form(): HTMLFormElement {
  return target.querySelector('tr.fold form')!;
}

function row(name: string): HTMLTableRowElement {
  return [...target.querySelectorAll('tbody tr')].find(
    (r) => r.querySelector('td .name')?.textContent?.trim() === name,
  ) as HTMLTableRowElement;
}

/** The accounts the roster shows, by display name. */
function shown(): string[] {
  return [...target.querySelectorAll('tbody tr:not(.fold) td .name')].map(
    (n) => n.textContent?.trim() ?? '',
  );
}

/** Types into the dialog's field labelled `label`. */
function fill(label: string, value: string) {
  const field = [...target.querySelectorAll('dialog label, dialog .field')].find((l) =>
    l.textContent?.trim().startsWith(label),
  );
  const input = field?.querySelector('input') ?? null;
  if (input === null) throw new Error(`no field ${label}`);
  input.value = value;
  input.dispatchEvent(new Event('input'));
  flushSync();
}

/** Lets a click's awaited question settle, answered or not. */
async function settle() {
  await new Promise((r) => setTimeout(r, 0));
  flushSync();
}

async function manage(name: string) {
  button('Manage', row(name))!.click();
  await settle();
}

/** Picks an area's level the way a reader does. Svelte reads a select's choice back with
 * `:checked`, which happy-dom does not match on an option, so this select answers it itself. */
function choose(area: string, level: string) {
  const select = target.querySelector<HTMLSelectElement>(`select[aria-label="${area} level"]`)!;
  select.value = level;
  const own = select.querySelector.bind(select);
  select.querySelector = ((q: string) =>
    q === ':checked' ? select.selectedOptions[0] : own(q)) as typeof select.querySelector;
  select.dispatchEvent(new Event('change'));
}

async function addRole(name: string) {
  button('+ Add role')!.click();
  await vi.waitFor(() => expect(target.querySelector('dialog input')).not.toBeNull());
  const input = target.querySelector<HTMLInputElement>('dialog input')!;
  input.value = name;
  input.dispatchEvent(new Event('input'));
  flushSync();
  button('Add')!.click();
  flushSync();
}

beforeEach(() => {
  calls = [];
  resets = 0;
  ask.mockReset();
  history.replaceState(null, '', '/#/admin/accounts/roles');
  target = document.createElement('div');
  document.body.append(target);
});

afterEach(() => {
  if (page) unmount(page);
  page = undefined;
  target.remove();
  vi.unstubAllGlobals();
});

describe('the Accounts tab', () => {
  it('shows each account its roles, whatever its sign-in', async () => {
    await accountsTab();
    const pills = (name: string) =>
      [...row(name).querySelectorAll('td:nth-child(4) span.pill')].map((p) =>
        p.textContent?.trim(),
      );
    expect(pills('Ada')).toEqual(['administrator']);
    expect(pills('Cy')).toEqual(['payments']);
    expect(pills('Bo')).toEqual(['payments']);
    expect(pills('exporter')).toEqual(['payments']);
  });

  it('offers each account what its kind of sign-in has', async () => {
    await accountsTab();
    const offered = async (name: string) => {
      await manage(name);
      const fold = target.querySelector('tr.fold .row')!;
      const texts = [...fold.querySelectorAll('button')].map((b) => b.textContent?.trim());
      button('Close', row(name))!.click();
      await settle();
      return texts;
    };
    expect(await offered('Cy')).toEqual([
      'Rename',
      'Change access',
      'Reset password',
      'Revoke sessions',
      'Disable account',
    ]);
    expect(await offered('Bo')).toEqual(['Rename', 'Revoke sessions', 'Disable account']);
    expect(await offered('exporter')).toEqual([
      'Rename',
      'Change access',
      '+ New key',
      'Disable account',
    ]);
  });

  it('lists a key account by its keys, each with when it expires', async () => {
    await accountsTab();
    expect(row('exporter').textContent).toContain('Key');
    expect(row('exporter').textContent).toContain('2 keys');
    await manage('exporter');
    const keys = target.querySelector('table[aria-label="Keys"]')!;
    const cells = (label: string) =>
      [...keys.querySelectorAll('tbody tr')]
        .find((r) => r.textContent?.includes(label))!
        .querySelector('td:nth-child(3)')!
        .textContent?.trim();
    expect(cells('nightly')).toBe('never');
    expect(cells('weekly')).toBe('in 10 days');
  });

  it('makes a key that expires in the days given, or never', async () => {
    await accountsTab();
    await manage('exporter');
    button('+ New key')!.click();
    flushSync();
    const days = form().querySelector<HTMLInputElement>('input[type="number"]')!;
    days.value = '0';
    days.dispatchEvent(new Event('input'));
    flushSync();
    expect(button('Create key')!.disabled).toBe(true);
    days.value = '30';
    days.dispatchEvent(new Event('input'));
    flushSync();
    button('Create key')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({
      method: 'POST',
      url: '/api/accounts/u-exp/keys',
      body: { label: '', expiresInDays: 30 },
    });

    await vi.waitFor(() => expect(target.querySelector('.handover')).not.toBeNull());
    button('+ New key')!.click();
    flushSync();
    button('Create key')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(2));
    expect(calls[1].body).toEqual({ label: '', expiresInDays: null });
  });

  it('finds an account by its display name, username or provider login', async () => {
    await accountsTab();
    const search = target.querySelector<HTMLInputElement>('input[type="search"]')!;
    const find = (text: string) => {
      search.value = text;
      search.dispatchEvent(new Event('input'));
      flushSync();
      return shown().sort();
    };
    expect(find('ADA')).toEqual(['Ada']);
    expect(find('cy')).toEqual(['Cy']);
    expect(find('example.com')).toEqual(['Bo']);
    expect(find('nobody')).toEqual([]);
    expect(target.textContent).toContain('No account matches.');
    expect(find('')).toEqual(['Ada', 'Bo', 'Cy', 'exporter']);
  });

  it('filters the accounts by how they sign in', async () => {
    await accountsTab();
    const chips = target.querySelector('[aria-label="Filter accounts by sign-in"]')!;
    const pick = (label: string) => {
      [...chips.querySelectorAll('button')]
        .find((b) => b.textContent?.trim().startsWith(label))!
        .click();
      flushSync();
      return shown().sort();
    };
    expect(pick('Password')).toEqual(['Ada', 'Cy']);
    expect(pick('SSO')).toEqual(['Bo']);
    expect(pick('Key')).toEqual(['exporter']);
    expect(pick('All')).toEqual(['Ada', 'Bo', 'Cy', 'exporter']);
  });

  it('adds a local account holding the roles it is given', async () => {
    await accountsTab();
    button('+ Add account')!.click();
    flushSync();
    fill('Display name', 'Dee');
    fill('Username', 'dee');
    target.querySelector<HTMLInputElement>('dialog .roles input')!.click();
    flushSync();
    button('Add')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({
      method: 'POST',
      url: '/api/accounts',
      body: {
        authSource: 'local',
        username: 'dee',
        displayName: 'Dee',
        roles: ['administrator'],
      },
    });
  });

  it('stops at a refused change of access, the form still open', async () => {
    await accountsTab();
    const answer = globalThis.fetch;
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string, init?: RequestInit) => {
        if (init?.method !== 'PATCH') return answer(url, init);
        calls.push({ method: 'PATCH', url, body: JSON.parse(String(init.body)) });
        return new Response(JSON.stringify({ ok: false, errors: [{ message: 'refused' }] }), {
          status: 409,
          headers: { 'content-type': 'application/json' },
        });
      }),
    );
    await manage('exporter');
    button('Change access')!.click();
    flushSync();
    // The administrator role given beside the role it holds.
    form().querySelector<HTMLInputElement>('.roles input')!.click();
    flushSync();
    button('Save access')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    await settle();
    expect(calls).toEqual([
      {
        method: 'PATCH',
        url: '/api/accounts/u-exp',
        body: { roles: ['payments', 'administrator'] },
      },
    ]);
    expect(button('Save access')).toBeDefined();
  });

  it('adds an account that signs in with a key', async () => {
    await accountsTab();
    button('+ Add account')!.click();
    flushSync();
    button('Key')!.click();
    flushSync();
    expect(target.querySelector('dialog input[autocomplete="off"]')).not.toBeNull();
    const name = target.querySelector<HTMLInputElement>('dialog input')!;
    name.value = 'exporter';
    name.dispatchEvent(new Event('input'));
    flushSync();
    button('Add')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({
      method: 'POST',
      url: '/api/accounts',
      body: { authSource: 'key', displayName: 'exporter', roles: [] },
    });
  });

  it('offers a role for each group an account holds that gives no role', async () => {
    history.replaceState(null, '', '/#/admin/accounts');
    await accountsTab();
    expect(button('+ smeltr-payments', row('Bo'))).toBeUndefined();
    button('+ everyone', row('Bo'))!.click();
    await settle();
    expect(location.hash).toBe('#/admin/accounts/roles');
  });

  it('resets a password only once it is confirmed', async () => {
    await accountsTab();
    await manage('Ada');
    expect(button('Reset password')).toBeDefined();
    ask.mockResolvedValueOnce(false);
    button('Reset password')!.click();
    await settle();
    expect(ask.mock.calls[0][0].action).toBe('Reset password');
    expect(calls).toEqual([]);

    ask.mockResolvedValueOnce(true);
    button('Reset password')!.click();
    await vi.waitFor(() => expect(target.querySelector('.handover')).not.toBeNull());
    expect(calls).toEqual([
      { method: 'POST', url: '/api/accounts/u-ada/reset-password', body: null },
    ]);
  });

  it('disables an account only once it is confirmed', async () => {
    await accountsTab();
    await manage('Ada');
    ask.mockResolvedValueOnce(false);
    button('Disable account')!.click();
    await settle();
    expect(ask.mock.calls[0][0].action).toBe('Disable account');
    expect(calls).toEqual([]);

    ask.mockResolvedValueOnce(true);
    button('Disable account')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({
      method: 'PATCH',
      url: '/api/accounts/u-ada',
      body: { enabled: false },
    });
  });

  it('shows only the newest password when one account is reset twice', async () => {
    await accountsTab();
    await manage('Ada');
    ask.mockResolvedValue(true);
    button('Reset password')!.click();
    await vi.waitFor(() => expect(target.textContent).toContain('pw-1'));
    button('Reset password')!.click();
    await vi.waitFor(() => expect(target.textContent).toContain('pw-2'));
    expect(target.querySelectorAll('.handover')).toHaveLength(1);
    expect(target.textContent).not.toContain('pw-1');
  });
});

describe('the Roles tab', () => {
  it('shows the groups that give each role and what it gives, shared first', async () => {
    await rolesTab();
    const pills = (name: string, column: number) =>
      [...row(name).querySelectorAll(`td:nth-child(${column}) .pill`)].map((p) =>
        p.textContent?.replace(/\s+/g, ' ').trim(),
      );
    expect(pills('administrator', 2)).toEqual(['smeltr-admins']);
    expect(pills('administrator', 3)).toEqual(['everything']);
    expect(pills('payments', 2)).toEqual(['smeltr-payments', 'smeltr-oncall']);
    expect(pills('payments', 3)).toEqual(['shared viewer', 'payments deployer']);
  });

  it('changes only the groups of the administrator role, which stays', async () => {
    await rolesTab();
    await manage('administrator');
    expect(target.querySelector('select')).toBeNull();
    expect(button('Remove role')).toBeUndefined();
    const groups = form().querySelector('textarea')!;
    groups.value = 'smeltr-admins\n smeltr-ops \n\nsmeltr-ops';
    groups.dispatchEvent(new Event('input'));
    flushSync();
    button('Save')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({
      method: 'PUT',
      url: '/api/roles/administrator',
      body: { groups: ['smeltr-admins', 'smeltr-ops'], areas: {} },
    });
  });

  it('saves the level chosen in each area, and none where none is chosen', async () => {
    await rolesTab();
    await manage('payments');
    choose('shared', 'editor');
    choose('payments', '');
    flushSync();
    button('Save')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({
      method: 'PUT',
      url: '/api/roles/payments',
      body: { groups: ['smeltr-payments', 'smeltr-oncall'], areas: { shared: 'editor' } },
    });
  });

  it('removes a role only once it is confirmed', async () => {
    await rolesTab();
    await manage('payments');
    ask.mockResolvedValueOnce(false);
    button('Remove role')!.click();
    await settle();
    expect(ask).toHaveBeenCalledTimes(1);
    expect(calls).toEqual([]);

    ask.mockResolvedValueOnce(true);
    button('Remove role')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({ method: 'DELETE', url: '/api/roles/payments', body: null });
  });

  it('does not save a new role until it gives something', async () => {
    await rolesTab();
    await addRole('smeltr-search');
    expect(row('smeltr-search')).toBeDefined();
    expect(button('Save')!.disabled).toBe(true);
    button('Cancel', form())!.click();
    flushSync();
    expect(row('smeltr-search')).toBeUndefined();
  });

  it('asks before another row drops an unsaved role', async () => {
    await rolesTab();
    await addRole('smeltr-search');
    ask.mockResolvedValueOnce(false);
    await manage('payments');
    await settle();
    expect(ask).toHaveBeenCalledTimes(1);
    expect(row('smeltr-search')).toBeDefined();
    expect(row('smeltr-search').classList.contains('expanded')).toBe(true);

    ask.mockResolvedValueOnce(true);
    await manage('payments');
    await vi.waitFor(() => expect(row('smeltr-search')).toBeUndefined());
    expect(row('payments').classList.contains('expanded')).toBe(true);
  });

  it('asks before a second new role drops the first', async () => {
    await rolesTab();
    await addRole('smeltr-search');
    ask.mockResolvedValueOnce(false);
    button('+ Add role')!.click();
    await settle();
    expect(ask).toHaveBeenCalledTimes(1);
    expect(target.querySelector('dialog input')).toBeNull();
    expect(row('smeltr-search')).toBeDefined();

    ask.mockResolvedValueOnce(true);
    await addRole('smeltr-logs');
    expect(row('smeltr-search')).toBeUndefined();
    expect(row('smeltr-logs')).toBeDefined();
  });

  it('refuses a name a role already has', async () => {
    await rolesTab();
    button('+ Add role')!.click();
    flushSync();
    await vi.waitFor(() => expect(target.querySelector('dialog input')).not.toBeNull());
    const name = target.querySelector<HTMLInputElement>('dialog input')!;
    name.value = 'administrator';
    name.dispatchEvent(new Event('input'));
    flushSync();
    expect(button('Add')!.disabled).toBe(true);
    name.value = 'smeltr-search';
    name.dispatchEvent(new Event('input'));
    flushSync();
    expect(button('Add')!.disabled).toBe(false);
  });
});
