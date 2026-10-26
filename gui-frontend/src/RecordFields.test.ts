// @vitest-environment happy-dom
import { afterEach, beforeEach, expect, it } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import RecordFields from './RecordFields.svelte';
import type { Step } from './pipeline';

let shown: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;

beforeEach(() => {
  target = document.createElement('div');
  document.body.append(target);
});

afterEach(() => {
  if (shown) unmount(shown);
  shown = undefined;
  target.remove();
});

const parser = { type: 'comment', id: 'p', text: 'parse' } as Step;

it('keeps a sign on every field of a run, drawn on the first and quiet on the rest', () => {
  shown = mount(RecordFields, {
    target,
    props: {
      before: {},
      after: { a: 1, b: 2, c: 3 },
      touched: () => [{ step: parser, number: 1 }],
    },
  });
  flushSync();
  const signs = [...target.querySelectorAll('.by')];
  expect(signs).toHaveLength(3);
  expect(signs.map((s) => s.classList.contains('quiet'))).toEqual([false, true, true]);
  expect(signs[2].querySelector('button')?.getAttribute('aria-label')).toBe(
    'Step 1 · Comment · parse',
  );
});
