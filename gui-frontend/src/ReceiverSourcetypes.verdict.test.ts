// @vitest-environment happy-dom
import { afterEach, expect, it, vi } from 'vitest';
import { createRawSnippet, flushSync, mount, unmount } from 'svelte';
import ReceiverSourcetypes from './ReceiverSourcetypes.svelte';

const help = createRawSnippet(() => ({ render: () => '<span>help</span>' }));
const WHY =
  'this line breaker ends an event where "\\n" matches, which has no capture group to mark the break';

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it('says under a row why a save would refuse its line breaker, once the typing stops', async () => {
  vi.useFakeTimers();
  const asked: string[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn(async (_url: string, init?: RequestInit) => {
      const { pattern } = JSON.parse(String(init?.body)) as { pattern: string };
      asked.push(pattern);
      const body =
        pattern === '\\n' || pattern === 'constructor'
          ? { ok: false, errors: [{ message: WHY }] }
          : { ok: true, errors: [] };
      return new Response(JSON.stringify(body), {
        headers: { 'content-type': 'application/json' },
      });
    }),
  );
  const target = document.createElement('div');
  const value = {
    sourcetypes: {
      bad: { lineBreaker: '\\n' },
      good: { lineBreaker: '(\\n)' },
      named: { lineBreaker: 'constructor' },
    },
  };
  const form = mount(ReceiverSourcetypes, { target, props: { value, help } });
  flushSync();
  expect(asked).toEqual([]);

  await vi.advanceTimersByTimeAsync(500);
  flushSync();
  expect(asked.sort()).toEqual(['(\\n)', '\\n', 'constructor']);
  const refused = [...target.querySelectorAll('.refused')].map((e) => e.textContent);
  expect(refused).toEqual([WHY, WHY]);
  unmount(form);
});
