// @vitest-environment happy-dom
import { afterEach, beforeEach, expect, it } from 'vitest';
import { mount, unmount } from 'svelte';
import Version from './Version.svelte';
import type { SystemInfoDto } from './api';

let shown: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;

function render(props: { info: SystemInfoDto; build?: boolean; newerHref?: string }): HTMLElement {
  shown = mount(Version, { target, props });
  return target;
}

beforeEach(() => {
  target = document.createElement('div');
  document.body.append(target);
});

afterEach(() => {
  if (shown) unmount(shown);
  shown = undefined;
  target.remove();
});

const build = '2026.10.24+main.abc';
const newer = { release: '1.3.0', url: 'https://example.com/releases/1.3.0' };

it('names a release by its number', () => {
  expect(render({ info: { smeltrVersion: build, release: '1.2.0' } }).textContent?.trim()).toBe(
    'v1.2.0',
  );
});

it('shows any other build by its whole build string', () => {
  expect(render({ info: { smeltrVersion: build } }).textContent?.trim()).toBe(build);
});

it('offers a newer release only where it is told to lead', () => {
  const info = { smeltrVersion: build, release: '1.2.0', newerRelease: newer };
  expect(render({ info }).querySelector('a')).toBeNull();
  unmount(shown!);
  const link = render({ info, newerHref: '#/admin/system/general' }).querySelector('a');
  expect(link?.textContent).toBe('v1.3.0 available');
  expect(link?.getAttribute('href')).toBe('#/admin/system/general');
  expect(link?.getAttribute('target')).toBeNull();
});

it('opens a link outside the app in a tab of its own', () => {
  const info = { smeltrVersion: build, release: '1.2.0', newerRelease: newer };
  const link = render({ info, newerHref: newer.url }).querySelector('a');
  expect(link?.getAttribute('target')).toBe('_blank');
});
