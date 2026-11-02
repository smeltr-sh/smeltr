// A name as the language spells it, and as the compiler does (`plan::tree`): keys joined by dots, except a key that
// is empty or holds any of `.` `[` `]` `"` `\`, which is written as a JSON string in brackets with no dot before it.
// `kubernetes.labels["app.kubernetes.io/name"]`, `["say \"hi\""]`, `x-y.z`, `@timestamp`.

/** Whether `key` is written bare in a name. */
export function isPlain(key: string): boolean {
  return key !== '' && !/[.[\]"\\]/.test(key);
}

/** `key`, one key, appended to the name `path`. */
export function pushKey(path: string, key: string): string {
  if (!isPlain(key)) return `${path}[${JSON.stringify(key)}]`;
  return path === '' ? key : `${path}.${key}`;
}

/** `name` and `path`, a name under it, joined. */
export function joinName(name: string, path: string): string {
  if (name === '') return path;
  if (path === '') return name;
  return path.startsWith('[') ? name + path : `${name}.${path}`;
}

/** The JSON string `s` starts with, decoded, and its length with its quotes. */
function jsonString(s: string): { key: string; len: number } | null {
  if (!s.startsWith('"')) return null;
  let escaped = false;
  for (let i = 1; i < s.length; i++) {
    const c = s[i];
    if (!escaped && c === '"') {
      const text = s.slice(1, i);
      if (!text.includes('\\')) return { key: text, len: i + 1 };
      try {
        return { key: JSON.parse(s.slice(0, i + 1)) as string, len: i + 1 };
      } catch {
        return null;
      }
    }
    escaped = !escaped && c === '\\';
  }
  return null;
}

const ESCAPES: Record<string, string> = { n: '\n', t: '\t', r: '\r', '0': '\0', '\\': '\\', "'": "'", '"': '"' };

/** The string `s` starts with, in single or double quotes as an expression writes one, decoded, and its length with
 * its quotes. Mirrors the expression lexer's `lex_string`. */
function stringLiteral(s: string): { key: string; len: number } | null {
  const quote = s[0];
  if (quote !== '"' && quote !== "'") return null;
  let key = '';
  for (let i = 1; i < s.length; ) {
    const c = s[i++];
    if (c === quote) return { key, len: i };
    if (c !== '\\') {
      key += c;
      continue;
    }
    const esc = s[i++];
    if (esc === 'u') {
      const hex = s[i] === '{' ? /^\{([0-9a-fA-F]+)\}/.exec(s.slice(i)) : /^[0-9a-fA-F]{4}/.exec(s.slice(i));
      const code = hex ? parseInt(hex[1] ?? hex[0], 16) : NaN;
      if (!hex || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) return null;
      key += String.fromCodePoint(code);
      i += hex[0].length;
    } else if (esc !== undefined && esc in ESCAPES) {
      key += ESCAPES[esc];
    } else {
      return null;
    }
  }
  return null;
}

/** One key of a name: decoded, and where its spelling starts and ends. */
export interface KeySpan {
  key: string;
  start: number;
  end: number;
}

/** The keys of `name`, a name already in this spelling. A bracket that does not close is part of a bare key, as the
 * compiler reads it. */
export function keySpans(name: string): KeySpan[] {
  const spans: KeySpan[] = [];
  let at = 0;
  while (at < name.length) {
    const rest = name.slice(at);
    let key: string;
    let end: number;
    const quoted = rest.startsWith('[') ? jsonString(rest.slice(1)) : null;
    if (quoted && rest[quoted.len + 1] === ']') {
      key = quoted.key;
      end = quoted.len + 2;
    } else {
      const skip = rest.startsWith('[') ? 1 : 0;
      const stop = rest.slice(skip).search(/[.[]/);
      end = stop < 0 ? rest.length : stop + skip;
      key = rest.slice(0, end);
    }
    spans.push({ key, start: at, end: at + end });
    at += rest[end] === '.' ? end + 1 : end;
  }
  return spans;
}

/** `text`, a name as a person types it into a step's field, in its one spelling -- or what is wrong with it. Mirrors
 * the compiler's `tree::parse_name`, so a box this refuses never reaches it. */
export function parsePath(
  text: string,
): { ok: true; name: string } | { ok: false; message: string } {
  const fail = (message: string) => ({ ok: false as const, message });
  let out = '';
  let rest = text;
  while (rest !== '') {
    if (rest.startsWith('[')) {
      // JSON first: it is how a name is spelled, and it has escapes, such as `\b`, an expression has not.
      const quoted = jsonString(rest.slice(1)) ?? stringLiteral(rest.slice(1));
      if (!quoted || rest[quoted.len + 1] !== ']') {
        return fail(`\`${text}\`: a key in brackets is a string in quotes, as in \`a["b.c"]\``);
      }
      out = pushKey(out, quoted.key);
      rest = rest.slice(quoted.len + 2);
    } else {
      const stop = rest.search(/[.[]/);
      const key = stop < 0 ? rest : rest.slice(0, stop);
      if (!isPlain(key)) {
        return fail(
          key === ''
            ? `\`${text}\` has an empty key`
            : `\`${text}\`: write the key \`${key}\` in brackets, as \`[${JSON.stringify(key)}]\``,
        );
      }
      out = pushKey(out, key);
      rest = rest.slice(key.length);
    }
    if (rest.startsWith('.')) {
      const after = rest.slice(1);
      if (after === '' || after.startsWith('[')) return fail(`\`${text}\` has an empty key`);
      rest = after;
    } else if (rest !== '' && !rest.startsWith('[')) {
      return fail(`\`${text}\`: a key in brackets ends the name or meets \`.\` or \`[\``);
    }
  }
  if (out === '') return fail('a name is needed');
  return { ok: true, name: out };
}
