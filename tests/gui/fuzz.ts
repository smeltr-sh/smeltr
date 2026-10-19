// What a seeded GUI fuzzer shares with the crate's own (`src/fuzz.rs`): the same PRNG, so a seed means the same
// draws in both, the seed range `SMELTR_FUZZ_SEED` and `SMELTR_FUZZ_SEEDS` choose, and the check that every option
// the fuzzer knows was reached.

const MASK = (1n << 64n) - 1n;

/** xorshift64*, as `fuzz::Rng`. */
export class Rng {
  private state: bigint;

  /** A generator for `seed`, 0 included. */
  constructor(seed: number) {
    this.state = ((BigInt(seed) * 0x9e3779b97f4a7c15n) & MASK) | 1n;
  }

  nextU64(): bigint {
    let x = this.state;
    x ^= x >> 12n;
    x = (x ^ (x << 25n)) & MASK;
    x ^= x >> 27n;
    this.state = x;
    return (x * 0x2545f4914f6cdd1dn) & MASK;
  }

  /** In `0..n`. `n` must not be 0. */
  below(n: number): number {
    return Number(this.nextU64() % BigInt(n));
  }

  /**
   * An index into a list of `len`, or `undefined` when it is empty. It draws once either way, so a list whose length
   * timing decides leaves the draws after it as they were.
   */
  index(len: number): number | undefined {
    const x = this.nextU64();
    return len > 0 ? Number(x % BigInt(len)) : undefined;
  }

  pick<T>(xs: readonly T[]): T {
    return xs[this.below(xs.length)];
  }

  chance(oneIn: number): boolean {
    return this.below(oneIn) === 0;
  }
}

function envNumber(name: string): number | undefined {
  const v = process.env[name];
  return v !== undefined && /^\d+$/.test(v) ? Number(v) : undefined;
}

/** `SMELTR_FUZZ_SEED` is set: one seed is replayed, so what holds only over a range is not checked. */
export function replaying(): boolean {
  return envNumber('SMELTR_FUZZ_SEED') !== undefined;
}

/** The seeds to run: `count` from 0, or what `SMELTR_FUZZ_SEED` or `SMELTR_FUZZ_SEEDS` asks for in its place. */
export function seeds(count: number): number[] {
  const one = envNumber('SMELTR_FUZZ_SEED');
  if (one !== undefined) return [one];
  const n = envNumber('SMELTR_FUZZ_SEEDS') ?? count;
  return Array.from({ length: n }, (_, i) => i);
}

/** The options a fuzzer knows, and which of them its seeds reached. */
export class Coverage {
  private reached = new Set<string>();
  private expected: readonly string[];
  constructor(expected: readonly string[]) {
    this.expected = expected;
  }

  reach(name: string) {
    this.reached.add(name);
  }

  /** The options no seed reached, in order. */
  missed(): string[] {
    return [...this.expected].sort().filter((o) => !this.reached.has(o));
  }
}
