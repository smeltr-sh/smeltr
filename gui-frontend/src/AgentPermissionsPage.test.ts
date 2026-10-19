// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import AgentPermissionsPage from './AgentPermissionsPage.svelte';
import { load, signedOut } from './session';

let page: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
let saved: unknown[];
let stored: unknown;

function json(body: unknown) {
  return new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
}

/** Signs in as an editor of `payments`, with `permissions` stored for their agent. */
async function open(permissions: unknown) {
  stored = permissions;
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string, init?: RequestInit) => {
      if (url === '/api/auth/session') {
        return json({
          authenticated: true,
          administrator: false,
          mustChangePassword: false,
          csrf: 'csrf',
          access: { administrator: false, areas: { payments: 'editor' } },
        });
      }
      if (url === '/api/me/agent-permissions') {
        if (init?.method === 'PUT') {
          stored = JSON.parse(String(init.body));
          saved.push(stored);
        }
        return json(stored);
      }
      if (url.startsWith('/api/worker-groups')) {
        return json({
          ok: true,
          errors: [],
          groups: [
            { id: 'payments', name: 'Payments', deployed: true, hue: 180 },
            { id: 'search', name: 'Search', deployed: true, hue: 60 },
          ],
          sharedHue: 330,
        });
      }
      throw new Error(`unexpected ${url}`);
    }),
  );
  await load();
  page = mount(AgentPermissionsPage, { target });
  await vi.waitFor(() => expect(target.querySelector('form')).not.toBeNull());
}

function button(text: string): HTMLButtonElement {
  return [...target.querySelectorAll('button')].find((b) => b.textContent?.trim() === text)!;
}

/** Picks a level the way a reader does. Svelte reads a select's choice back with `:checked`, which happy-dom does
 * not match on an option, so this select answers it itself. */
function choose(select: HTMLSelectElement, value: string) {
  select.value = value;
  const own = select.querySelector.bind(select);
  select.querySelector = ((q: string) =>
    q === ':checked' ? select.selectedOptions[0] : own(q)) as typeof select.querySelector;
  select.dispatchEvent(new Event('change'));
  flushSync();
}

beforeEach(() => {
  saved = [];
  target = document.createElement('div');
  document.body.append(target);
});

afterEach(() => {
  if (page) unmount(page);
  page = undefined;
  target.remove();
  signedOut();
  vi.unstubAllGlobals();
});

describe('agent permissions', () => {
  it('offers the areas the person holds, each beside what they hold there', async () => {
    await open({ kind: 'upto', default: 'viewer', areas: {} });
    const areas = [...target.querySelectorAll('.area-name')].map((a) => a.textContent);
    expect(areas).toEqual(['shared', 'payments']);
    expect(target.textContent).toContain('you hold editor');
  });

  it('saves a level for every area and another for the areas given one', async () => {
    await open({ kind: 'upto', default: 'viewer', areas: {} });
    choose(
      target.querySelector<HTMLSelectElement>('select[aria-label="payments level"]')!,
      'editor',
    );
    button('Save').click();
    await vi.waitFor(() => expect(saved).toHaveLength(1));
    expect(saved[0]).toEqual({ kind: 'upto', default: 'viewer', areas: { payments: 'editor' } });
  });

  it('saves everything the person holds', async () => {
    await open({ kind: 'upto', default: 'viewer', areas: {} });
    button('Everything you hold').click();
    flushSync();
    expect(target.querySelector('select')).toBeNull();
    button('Save').click();
    await vi.waitFor(() => expect(saved).toHaveLength(1));
    expect(saved[0]).toEqual({ kind: 'everything' });
  });

  it('keeps an area the saved permissions name that the person no longer holds', async () => {
    await open({ kind: 'upto', default: 'editor', areas: { search: 'deployer' } });
    const areas = [...target.querySelectorAll('.area-name')].map((a) => a.textContent);
    expect(areas).toEqual(['shared', 'payments', 'search']);
    button('Save').click();
    await vi.waitFor(() => expect(saved).toHaveLength(1));
    expect(saved[0]).toEqual({ kind: 'upto', default: 'editor', areas: { search: 'deployer' } });
  });
});
