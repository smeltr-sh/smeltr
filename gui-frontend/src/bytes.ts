// A count of bytes as a reader wants it: the largest unit that leaves a number with digits in
// front of the point. Binary units, because every budget it describes is stated in them.

const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];

/** `bytes` at the unit that says it shortest -- `15.0 MiB`, `972 B`. One decimal below ten so a
 * queue filling reads as moving; none above it, where the digit says nothing. Bytes are whole,
 * so `B` never carries a decimal. */
export function bytes(n: number): string {
  let value = n;
  let unit = 0;
  while (value >= 1024 && unit < UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(unit > 0 && value < 10 ? 1 : 0)} ${UNITS[unit]}`;
}

/** What a bundle holds for a byte figure: a number of bytes, or a size as typed (`"4 GiB"`, `"250 MB"`).
 * The loader accepts either and writes back the exact binary form (`exactBytes`). */
export type ByteCount = number | string;

const BINARY: [string, number][] = [
  ['TiB', 1024 ** 4],
  ['GiB', 1024 ** 3],
  ['MiB', 1024 ** 2],
  ['KiB', 1024],
];
const DECIMAL: [string, number][] = [
  ['TB', 1e12],
  ['GB', 1e9],
  ['MB', 1e6],
  ['KB', 1e3],
];

/** `"4 GiB"`, `"4GB"`, `"1.5 MiB"`, `"512"` or `"512 B"` to bytes, case ignored; `undefined` for anything else,
 * including a fraction that is not a whole number of bytes. The same rules as `config_file::bytes::parse`, so what
 * this accepts the save accepts. */
export function parseBytes(text: string): number | undefined {
  const m = /^\s*([0-9_,]*\.?[0-9]*)\s*([A-Za-z]*)\s*$/.exec(text);
  if (!m || !/[0-9]/.test(m[1])) return undefined;
  const number = Number(m[1].replace(/[_,]/g, ''));
  const unit = m[2];
  let scale = 1;
  if (unit !== '' && unit.toUpperCase() !== 'B') {
    const found = [...BINARY, ...DECIMAL].find(
      ([name]) => name.toLowerCase() === unit.toLowerCase(),
    );
    if (!found) return undefined;
    scale = found[1];
  }
  const bytes = number * scale;
  if (!Number.isFinite(bytes) || !Number.isInteger(bytes) || bytes < 0) return undefined;
  return bytes;
}

/** `n` in the largest binary unit that divides it exactly, else in bytes: `64 MiB`, `1 GiB`, `4000000 B`. The form
 * the bundle carries, so a field shows what the file will say. `bytes()` rounds, which a config value must not. */
export function exactBytes(n: number): string {
  if (n > 0) {
    for (const [name, scale] of BINARY) {
      if (n % scale === 0) return `${n / scale} ${name}`;
    }
  }
  return `${n} B`;
}

/** The bytes a stored figure means: a number as it is, a size parsed, and `fallback` for absent or unreadable. */
export function byteCount(value: ByteCount | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  if (typeof value === 'number') return value;
  return parseBytes(value) ?? fallback;
}
