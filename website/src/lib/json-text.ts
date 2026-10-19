/**
 * JSON that keeps a number's written form. JavaScript has one number type, so
 * `JSON.parse` reads the engine's `2.0` as `2`; here a number written with a
 * fraction or an exponent becomes a `Decimal` that prints as written.
 */
export class Decimal {
  constructor(readonly text: string) {}
}

/** `text` parsed as JSON, each fractional number a `Decimal`. */
export function parse(text: string): any {
  let i = 0;
  const space = () => {
    while (/\s/.test(text[i])) i++;
  };
  const value = (): unknown => {
    space();
    const c = text[i];
    if (c === '{') {
      i++;
      const obj: Record<string, unknown> = {};
      space();
      if (text[i] === '}') return (i++, obj);
      for (;;) {
        space();
        const key = value() as string;
        space();
        i++; // ':'
        obj[key] = value();
        space();
        if (text[i++] === '}') return obj;
      }
    }
    if (c === '[') {
      i++;
      const arr: unknown[] = [];
      space();
      if (text[i] === ']') return (i++, arr);
      for (;;) {
        arr.push(value());
        space();
        if (text[i++] === ']') return arr;
      }
    }
    // A string, number or literal is one token, which JSON.parse reads.
    const token = /^("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null)/.exec(
      text.slice(i),
    );
    if (!token) throw new SyntaxError(`JSON at ${i}`);
    i += token[0].length;
    return /^-?\d.*[.eE]/.test(token[0]) ? new Decimal(token[0]) : JSON.parse(token[0]);
  };
  return value();
}

/** `v` as compact JSON, each `Decimal` as written. */
export function stringify(v: unknown): string {
  if (v instanceof Decimal) return v.text;
  if (Array.isArray(v)) return `[${v.map(stringify).join(',')}]`;
  if (v !== null && typeof v === 'object')
    return `{${Object.entries(v)
      .map(([k, x]) => `${JSON.stringify(k)}:${stringify(x)}`)
      .join(',')}}`;
  return JSON.stringify(v);
}
