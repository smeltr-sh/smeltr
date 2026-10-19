import { describe, expect, it } from 'vitest';

import { bytes } from './bytes';

describe('bytes', () => {
  it('stays in bytes below a kibibyte, with no decimal', () => {
    expect(bytes(0)).toBe('0 B');
    expect(bytes(972)).toBe('972 B');
    expect(bytes(1023)).toBe('1023 B');
  });

  it('climbs a unit at 1024', () => {
    expect(bytes(1024)).toBe('1.0 KiB');
    expect(bytes(1024 * 1024)).toBe('1.0 MiB');
    expect(bytes(1024 ** 4)).toBe('1.0 TiB');
  });

  it('carries a decimal below ten and drops it above, so the digits all say something', () => {
    expect(bytes(9.5 * 1024)).toBe('9.5 KiB');
    expect(bytes(15 * 1024 * 1024)).toBe('15 MiB');
  });

  /** A budget past the largest unit still reads as a number rather than running off the scale. */
  it('stops at tebibytes', () => {
    expect(bytes(2048 * 1024 ** 4)).toBe('2048 TiB');
  });
});

import { byteCount, exactBytes, parseBytes } from './bytes';

describe('parseBytes', () => {
  it('reads either unit family, or bare bytes', () => {
    expect(parseBytes('4 GiB')).toBe(4 * 1024 ** 3);
    expect(parseBytes('4GB')).toBe(4e9);
    expect(parseBytes('4 gb')).toBe(4e9);
    expect(parseBytes('1.5 MiB')).toBe(1.5 * 1024 ** 2);
    expect(parseBytes('512')).toBe(512);
    expect(parseBytes(' 512 B ')).toBe(512);
    expect(parseBytes('1,000 KB')).toBe(1e6);
    // The same fractions the loader takes.
    expect(parseBytes('.5 MiB')).toBe(512 * 1024);
    expect(parseBytes('1. KiB')).toBe(1024);
  });

  it('refuses what is not a size', () => {
    expect(parseBytes('')).toBeUndefined();
    expect(parseBytes('GB')).toBeUndefined();
    expect(parseBytes('4 parsecs')).toBeUndefined();
    expect(parseBytes('1.5 B')).toBeUndefined();
    expect(parseBytes('-4 MiB')).toBeUndefined();
  });
});

describe('exactBytes', () => {
  it('uses the largest binary unit that divides exactly, else bytes', () => {
    expect(exactBytes(64 * 1024 ** 2)).toBe('64 MiB');
    expect(exactBytes(1024 ** 3)).toBe('1 GiB');
    expect(exactBytes(1.5 * 1024 ** 2)).toBe('1536 KiB');
    expect(exactBytes(4_000_000)).toBe('4000000 B');
    expect(exactBytes(0)).toBe('0 B');
  });
});

describe('byteCount', () => {
  it('reads a number, a size, or falls back', () => {
    expect(byteCount(42, 7)).toBe(42);
    expect(byteCount('64 MiB', 7)).toBe(64 * 1024 ** 2);
    expect(byteCount(undefined, 7)).toBe(7);
    expect(byteCount('soon', 7)).toBe(7);
  });
});
