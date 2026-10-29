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
  { name: 'smeltr-admins', administrator: true, areas: {} },
  {
    name: 'smeltr-payments',
    administrator: false,
    areas: { payments: 'deployer', shared: 'viewer' },
  },
];

const ACCOUNTS = [
  {
    id: 'u-ada',
    username: 'ada',
    displayName: 'Ada',
    enabled: true,
    administrator: true,
    authSource: 'local',
    createdAt: 0,
    updatedAt: 0,
    roles: [],
    groups: [],
    access: {},
  },
  {
    id: 'u-bo',
    displayName: 'Bo',
    providerLogin: 'bo@example.com',
    enabled: true,
    administrator: false,
    authSource: 'oidc',
    createdAt: 0,
    updatedAt: 0,
    roles: ['smeltr-payments', 'smeltr-search'],
    groups: ['everyone', 'smeltr-payments', 'smeltr-search'],
    access: {},
  },
  {
    id: 'u-exp',
    displayName: 'exporter',
    enabled: true,
    administrator: false,
    authSource: 'key',
    createdAt: 1,
    updatedAt: 1,
    roles: ['smeltr-payments'],
    groups: [],
    access: {},
    keys: [{ id: 'key_1', label: 'nightly', createdAt: 0, lastUsedAt: null }],
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
      if (url === '/api/agents') return json({ ok: true, errors: [], userId: 'u-new' });
      if (url.startsWith('/api/agents/')) return json({ ok: true, errors: [], roles: [] });
      if (url === '/api/accounts' && method === 'POST')
        return json({ ok: true, errors: [], ...ACCOUNTS[2] });
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
  it('shows each account its roles, a local one as administrator', async () => {
    await accountsTab();
    const pills = (name: string) =>
      [...row(name).querySelectorAll('td:nth-child(4) span.pill')].map((p) =>
        p.textContent?.trim(),
      );
    expect(pills('Ada')).toEqual(['administrator']);
    expect(pills('Bo')).toEqual(['smeltr-payments', 'smeltr-search']);
    expect(pills('exporter')).toEqual(['smeltr-payments']);
  });

  it('lists a key account among the accounts, by its key, with its own actions', async () => {
    await accountsTab();
    expect(row('exporter').textContent).toContain('Key');
    expect(row('exporter').textContent).toContain('1 key');
    await manage('exporter');
    expect(button('+ New key')).toBeDefined();
    expect(button('Reset password')).toBeUndefined();
    expect(target.querySelector('table[aria-label="Keys"]')?.textContent).toContain('nightly');
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
      body: { authSource: 'key', displayName: 'exporter', roles: [], administrator: false },
    });
  });

  it('offers a role for each group an account holds that no role names', async () => {
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
  it('shows what each role gives, shared first', async () => {
    await rolesTab();
    expect(row('smeltr-admins').textContent).toContain('administrator');
    const pills = [...row('smeltr-payments').querySelectorAll('.pill')].map((p) =>
      p.textContent?.replace(/\s+/g, ' ').trim(),
    );
    expect(pills).toEqual(['shared viewer', 'payments deployer']);
  });

  it('saves an administrator role with no areas of its own', async () => {
    await rolesTab();
    await manage('smeltr-payments');
    const administrator = target.querySelector<HTMLInputElement>('input[type="checkbox"]')!;
    administrator.click();
    flushSync();
    expect(target.querySelector('select')).toBeNull();
    button('Save')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({
      method: 'PUT',
      url: '/api/roles/smeltr-payments',
      body: { administrator: true, areas: {} },
    });
  });

  it('saves the level chosen in each area, and none where none is chosen', async () => {
    await rolesTab();
    await manage('smeltr-payments');
    choose('shared', 'editor');
    choose('payments', '');
    flushSync();
    button('Save')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({
      method: 'PUT',
      url: '/api/roles/smeltr-payments',
      body: { administrator: false, areas: { shared: 'editor' } },
    });
  });

  it('removes a role only once it is confirmed', async () => {
    await rolesTab();
    await manage('smeltr-payments');
    ask.mockResolvedValueOnce(false);
    button('Remove role')!.click();
    await settle();
    expect(ask).toHaveBeenCalledTimes(1);
    expect(calls).toEqual([]);

    ask.mockResolvedValueOnce(true);
    button('Remove role')!.click();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toEqual({ method: 'DELETE', url: '/api/roles/smeltr-payments', body: null });
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
    await manage('smeltr-payments');
    await settle();
    expect(ask).toHaveBeenCalledTimes(1);
    expect(row('smeltr-search')).toBeDefined();
    expect(row('smeltr-search').classList.contains('expanded')).toBe(true);

    ask.mockResolvedValueOnce(true);
    await manage('smeltr-payments');
    await vi.waitFor(() => expect(row('smeltr-search')).toBeUndefined());
    expect(row('smeltr-payments').classList.contains('expanded')).toBe(true);
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
    name.value = 'smeltr-admins';
    name.dispatchEvent(new Event('input'));
    flushSync();
    expect(button('Add')!.disabled).toBe(true);
    name.value = 'smeltr-search';
    name.dispatchEvent(new Event('input'));
    flushSync();
    expect(button('Add')!.disabled).toBe(false);
  });
});
