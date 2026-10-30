// @vitest-environment happy-dom
import { afterEach, expect, it, vi } from 'vitest';
import { createRawSnippet, flushSync, mount, unmount } from 'svelte';
import Help from './Help.svelte';

afterEach(() => {
  document.body.innerHTML = '';
  vi.unstubAllGlobals();
});

it('stops following its `?` once it is gone with the help open', () => {
  const frames = new Map<number, FrameRequestCallback>();
  let next = 0;
  vi.stubGlobal(
    'requestAnimationFrame',
    (f: FrameRequestCallback) => (frames.set(++next, f), next),
  );
  vi.stubGlobal('cancelAnimationFrame', (id: number) => frames.delete(id));
  const help = mount(Help, {
    target: document.body,
    props: { label: 'x', children: createRawSnippet(() => ({ render: () => '<p>help</p>' })) },
  });
  flushSync();
  document
    .querySelector('[popover]')!
    .dispatchEvent(Object.assign(new Event('beforetoggle'), { newState: 'open' }));
  expect(frames.size).toBe(1);
  unmount(help);
  expect(frames.size).toBe(0);
});
