// @vitest-environment happy-dom
import { afterEach, expect, it, vi } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import LineBreakerRefusal from './LineBreakerRefusal.svelte';

const WHY =
  'this line breaker ends an event where "\\n" matches, which has no capture group to mark the break';

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

function refusing(asked: string[]) {
  return vi.fn(async (_url: string, init?: RequestInit) => {
    const { pattern } = JSON.parse(String(init?.body)) as { pattern: string };
    asked.push(pattern);
    const body =
      pattern === '\\n' ? { ok: false, errors: [{ message: WHY }] } : { ok: true, errors: [] };
    return new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
  });
}

function said(target: HTMLElement): string[] {
  return [...target.querySelectorAll('.refused')].map((e) => e.textContent ?? '');
}

it('says why a save would refuse a pattern once it sits still, and nothing for one it takes', async () => {
  vi.useFakeTimers();
  const asked: string[] = [];
  vi.stubGlobal('fetch', refusing(asked));
  const target = document.createElement('div');
  const props = $state({ pattern: '\\n' });
  const box = mount(LineBreakerRefusal, { target, props });
  flushSync();
  expect(asked).toEqual([]);

  await vi.advanceTimersByTimeAsync(500);
  flushSync();
  expect(said(target)).toEqual([WHY]);

  props.pattern = '(\\n)';
  flushSync();
  expect(said(target)).toEqual([]);
  await vi.advanceTimersByTimeAsync(500);
  flushSync();
  expect(said(target)).toEqual([]);

  props.pattern = '\\n';
  flushSync();
  expect(said(target)).toEqual([WHY]);
  expect(asked).toEqual(['\\n', '(\\n)']);
  unmount(box);
});

it('asks nothing of a blank pattern', async () => {
  vi.useFakeTimers();
  const asked: string[] = [];
  vi.stubGlobal('fetch', refusing(asked));
  const target = document.createElement('div');
  const box = mount(LineBreakerRefusal, { target, props: { pattern: '  ' } });
  await vi.advanceTimersByTimeAsync(500);
  expect(asked).toEqual([]);
  unmount(box);
});
