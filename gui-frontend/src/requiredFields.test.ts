import { describe, expect, it } from 'vitest';

// A control the browser refuses empty belongs to a field marked `required`, so its `*` and its `?` say so too.
// The other way is not checked: most required fields refuse empty in the save, not in the browser. It reads the
// source, so it sees only a literal `required`, not `required={…}`.
const sources = import.meta.glob<string>('./**/*.svelte', {
  query: '?raw',
  import: 'default',
  eager: true,
});

/** The opening tag starting at `start`, to its first `>` that is not an arrow's. */
function openingTag(source: string, start: number): string {
  for (let i = start; i < source.length; i++) {
    if (source[i] === '>' && source[i - 1] !== '=') return source.slice(start, i + 1);
  }
  return source.slice(start);
}

describe('required fields', () => {
  it('mark the Field of every control the browser requires', () => {
    const unmarked = Object.entries(sources).flatMap(([path, source]) =>
      [...source.matchAll(/<(input|textarea|UnitInput)\b/g)]
        .filter((m) => /\srequired[\s/>]/.test(openingTag(source, m.index)))
        .flatMap((m) => {
          const field = source.lastIndexOf('<Field', m.index);
          if (field < 0 || source.lastIndexOf('</Field>', m.index) > field) return [];
          if (/\srequired[\s=>]/.test(openingTag(source, field))) return [];
          return [`${path}:${source.slice(0, m.index).split('\n').length}`];
        }),
    );
    expect(unmarked, 'add `required` to the Field').toEqual([]);
  });
});
