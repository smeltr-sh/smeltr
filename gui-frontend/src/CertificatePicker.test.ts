// @vitest-environment happy-dom
import { afterEach, expect, it, vi } from 'vitest';
import { flushSync, mount, tick, unmount } from 'svelte';
import CertificatePicker from './CertificatePicker.svelte';

const listed = (name: string, forServers: boolean, forClients: boolean) => ({
  name,
  description: '',
  versions: [{ version: 1, setAt: 0, usable: true }],
  chain: [],
  served: false,
  forServers,
  forClients,
});

let picker: ReturnType<typeof mount> | undefined;
afterEach(() => {
  if (picker) unmount(picker);
  picker = undefined;
  document.body.innerHTML = '';
  vi.unstubAllGlobals();
});

async function options(usableAs: 'server' | 'client', value?: string): Promise<string[]> {
  vi.stubGlobal(
    'fetch',
    vi.fn(
      async () =>
        new Response(
          JSON.stringify([
            listed('web', true, false),
            listed('mtls', false, true),
            listed('both', true, true),
          ]),
          { headers: { 'content-type': 'application/json' } },
        ),
    ),
  );
  picker = mount(CertificatePicker, {
    target: document.body,
    props: { value, usableAs, optional: true },
  });
  await vi.waitFor(() => expect(document.querySelectorAll('option').length).toBeGreaterThan(1));
  await tick();
  flushSync();
  return [...document.querySelectorAll('option')].map((o) => o.textContent!.trim());
}

// A worker refuses a certificate it cannot present from its side, so the picker does not offer one.
it('offers only certificates usable from the side they are presented from', async () => {
  expect(await options('client')).toEqual(['none', 'mtls', 'both']);
});

it('keeps a chosen certificate that is not usable, and says so', async () => {
  expect(await options('server', 'mtls')).toEqual([
    'none',
    'web',
    'both',
    'mtls (not usable as a server certificate)',
  ]);
});
