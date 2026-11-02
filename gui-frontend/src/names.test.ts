import { describe, expect, it } from 'vitest';
import { keySpans, pushKey } from './names';

describe('keySpans', () => {
  it('splits at dots and takes a key in brackets whole, decoded', () => {
    expect(keySpans('a["b.c"].d').map((k) => k.key)).toEqual(['a', 'b.c', 'd']);
  });

  it('round-trips every key through its spelling', () => {
    const keys = [
      'app.kubernetes.io/name',
      'a[0]',
      'say "hi"',
      'back\\slash',
      '',
      'x-y',
      '@timestamp',
    ];
    const name = keys.reduce(pushKey, 'root');
    expect(keySpans(name).map((k) => k.key)).toEqual(['root', ...keys]);
  });
});
