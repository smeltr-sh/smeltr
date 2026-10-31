import { afterEach, describe, expect, it, vi } from 'vitest';
import { store, stored, storedJson } from './stored';

function memory(): Storage {
  const kept = new Map<string, string>();
  return {
    getItem: (k: string) => kept.get(k) ?? null,
    setItem: (k: string, v: string) => void kept.set(k, v),
    removeItem: (k: string) => void kept.delete(k),
  } as Storage;
}

const refusing = {
  getItem: () => {
    throw new DOMException('denied', 'SecurityError');
  },
  setItem: () => {
    throw new DOMException('full', 'QuotaExceededError');
  },
  removeItem: () => {
    throw new DOMException('denied', 'SecurityError');
  },
} as unknown as Storage;

describe('stored', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('keeps, reads back and forgets a value, each area apart', () => {
    vi.stubGlobal('localStorage', memory());
    vi.stubGlobal('sessionStorage', memory());
    store('k', 'local');
    store('k', 'tab', 'session');
    expect(stored('k')).toBe('local');
    expect(stored('k', 'session')).toBe('tab');
    store('k', null);
    expect(stored('k')).toBeNull();
    expect(stored('k', 'session')).toBe('tab');
  });

  it('reads JSON, and the fallback for what does not parse', () => {
    vi.stubGlobal('localStorage', memory());
    store('k', '{"a":true}');
    expect(storedJson('k', {})).toEqual({ a: true });
    store('k', '{');
    expect(storedJson('k', { fallback: 1 })).toEqual({ fallback: 1 });
    expect(storedJson('missing', [])).toEqual([]);
  });

  it('forgets quietly in a browser that refuses storage or has none', () => {
    vi.stubGlobal('localStorage', refusing);
    expect(() => store('k', 'v')).not.toThrow();
    expect(stored('k')).toBeNull();
    expect(storedJson('k', 7)).toBe(7);
    vi.stubGlobal('localStorage', undefined);
    expect(() => store('k', 'v')).not.toThrow();
    expect(stored('k')).toBeNull();
  });
});
