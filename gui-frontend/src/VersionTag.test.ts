// @vitest-environment happy-dom
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import VersionTag from './VersionTag.svelte';
import type { SystemInfoDto } from './api';

let tag: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;

/** The tag's text once the leader has answered `info`. */
async function shown(info: SystemInfoDto, administrator: boolean): Promise<HTMLElement> {
  vi.stubGlobal(
    'fetch',
    vi.fn(
      async () =>
        new Response(JSON.stringify(info), { headers: { 'content-type': 'application/json' } }),
    ),
  );
  tag = mount(VersionTag, { target, props: { administrator } });
  await vi.waitFor(() => expect(target.textContent?.trim()).not.toBe(''));
  flushSync();
  return target;
}

beforeEach(() => {
  target = document.createElement('div');
  document.body.append(target);
});

afterEach(() => {
  if (tag) unmount(tag);
  tag = undefined;
  target.remove();
  vi.unstubAllGlobals();
});

const newer = { release: '1.3.0', url: 'https://example.com/releases' };

it('names a release by its number', async () => {
  const el = await shown({ smeltrVersion: '2026.10.24+main.abc', release: '1.2.0' }, false);
  expect(el.textContent?.trim()).toBe('v1.2.0');
});

it('shows any other build by its whole build string', async () => {
  const el = await shown({ smeltrVersion: '2026.10.24+main.abc' }, true);
  expect(el.textContent?.trim()).toBe('2026.10.24+main.abc');
});

it('offers a newer release to an administrator, on the page where the check is set', async () => {
  const el = await shown(
    { smeltrVersion: '2026.10.24+main.abc', release: '1.2.0', newerRelease: newer },
    true,
  );
  const link = el.querySelector('a');
  expect(link?.textContent).toBe('v1.3.0 available');
  expect(link?.getAttribute('href')).toBe('#/admin/system/general');
});

it('shows anyone else only the version', async () => {
  const el = await shown(
    { smeltrVersion: '2026.10.24+main.abc', release: '1.2.0', newerRelease: newer },
    false,
  );
  expect(el.querySelector('a')).toBeNull();
  expect(el.textContent?.trim()).toBe('v1.2.0');
});
