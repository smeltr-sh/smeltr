import { describe, expect, it } from 'vitest';

// An error drawn with its own `class="error"` is one the error bar does not know about, which is how errors came to
// sit at the foot of pages where nobody saw them. These draw it for everyone else.
const DRAWERS = [
  './Field.svelte',
  './ValidatedField.svelte',
  './ErrorBar.svelte',
  './InlineError.svelte',
];

const sources = import.meta.glob<string>('./**/*.svelte', {
  query: '?raw',
  import: 'default',
  eager: true,
});

describe('errors', () => {
  it('are drawn only by the error components', () => {
    const drawn = Object.entries(sources)
      .filter(([path]) => !DRAWERS.includes(path))
      .flatMap(([path, source]) =>
        source
          .split('\n')
          .map((line, i) => ({ path, line: i + 1, text: line.trim() }))
          .filter(({ text }) => /class="([^"]*\s)?error(\s[^"]*)?"/.test(text)),
      )
      .map(({ path, line, text }) => `${path}:${line} ${text}`);
    expect(drawn, 'use ErrorBar (a call), InlineError (beside a control) or Field').toEqual([]);
  });
});
