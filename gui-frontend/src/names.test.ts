import { describe, expect, it } from 'vitest';
import { keySpans, parsePath, pushKey } from './names';

describe('parsePath', () => {
  it('accepts a dotted path and plain keys as they are', () => {
    expect(parsePath('kubernetes.labels.app')).toEqual({
      ok: true,
      name: 'kubernetes.labels.app',
    });
    expect(parsePath('@timestamp')).toEqual({ ok: true, name: '@timestamp' });
    expect(parsePath('x-y.z')).toEqual({ ok: true, name: 'x-y.z' });
  });

  it('accepts a key in brackets that needs them', () => {
    expect(parsePath('labels["app.kubernetes.io/name"]')).toEqual({
      ok: true,
      name: 'labels["app.kubernetes.io/name"]',
    });
    expect(parsePath('["say \\"hi\\""]')).toEqual({ ok: true, name: '["say \\"hi\\""]' });
  });

  it('spells a bracketed plain key bare', () => {
    expect(parsePath('a["b"]')).toEqual({ ok: true, name: 'a.b' });
    expect(parsePath('["\\u0061"].b')).toEqual({ ok: true, name: 'a.b' });
  });

  it('refuses text after a bracketed key that is not a dot or a bracket', () => {
    expect(parsePath('a["b"]c').ok).toBe(false);
  });

  it('refuses a bracket that does not close on a string', () => {
    for (const text of ['labels["app', 'a[0]', 'a[b]', 'a["b"']) {
      const parsed = parsePath(text);
      expect(parsed.ok, text).toBe(false);
      if (!parsed.ok) expect(parsed.message).toContain('a key in brackets is a string in quotes');
    }
  });

  it('refuses a key that must be written in brackets, and says how', () => {
    expect(parsePath('say "hi"')).toEqual({
      ok: false,
      message: '`say "hi"`: write the key `say "hi"` in brackets, as `["say \\"hi\\""]`',
    });
  });

  it('refuses an empty key or an empty name', () => {
    for (const text of ['a.', '.a', 'a..b', 'a.["b"]']) {
      expect(parsePath(text)).toEqual({ ok: false, message: `\`${text}\` has an empty key` });
    }
    expect(parsePath('')).toEqual({ ok: false, message: 'a name is needed' });
  });
});

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
