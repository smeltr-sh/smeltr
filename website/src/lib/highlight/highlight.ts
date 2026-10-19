// Syntax coloring for pipeline code and the JSON records it reads and writes.
// The token rules follow the engine's lexer (`src/expr/lex.rs`): the same
// keywords, `$` in names, and a `/` that starts a regex only where no value
// came before it. A word the parser (`src/expr/parse.rs`) takes as a keyword
// only in one place is colored only there, so a field named `case` stays a
// name. `highlight.test.ts` in the GUI holds both lists to the engine's. JSON is a subset, so one tokenizer serves both. The GUI's
// expression fields import it from here, since the site builds alone.

export const KEYWORDS = new Set([
  'let',
  'if',
  'else',
  'for',
  'of',
  'while',
  'break',
  'continue',
  'return',
  'function',
  'as',
]);
// Each word the parser reads as a keyword only where it starts its construct,
// with that place: what comes right before it (`prev`), and what follows it.
type Place = { prev: string; next: string; inSwitch: boolean };
const startsStatement = (prev: string) => ['', '{', '}', ';', ':'].includes(prev);
export const CONTEXTUAL_KEYWORDS: Record<string, (at: Place) => boolean> = {
  try: (at) => at.next.startsWith('{'),
  catch: (at) => at.prev === '}' && /^[({]/.test(at.next),
  finally: (at) => at.prev === '}' && at.next.startsWith('{'),
  throw: (at) => isIdentStart(at.next[0] ?? ''),
  switch: (at) => at.next.startsWith('('),
  // Not a write to a field named `case`.
  case: (at) =>
    at.inSwitch && startsStatement(at.prev) && !/^(:|=(?!=)|[-+*/%]=|\+\+|--)/.test(at.next),
  default: (at) => at.inSwitch && startsStatement(at.prev) && at.next.startsWith(':'),
};
export const LITERALS = new Set(['true', 'false', 'null']);
const TYPES = new Set([
  'bool',
  'i64',
  'f64',
  'str',
  'bytes',
  'datetime',
  'duration',
  'array',
  'variant',
]);

export type TokenKind =
  | 'keyword'
  | 'literal'
  | 'type'
  | 'string'
  | 'number'
  | 'regex'
  | 'comment'
  | 'function'
  | 'key'
  | 'plain';

export interface Token {
  kind: TokenKind;
  text: string;
}

const isIdentStart = (c: string) => /[A-Za-z_$]/.test(c);
const isIdent = (c: string) => /[A-Za-z0-9_$]/.test(c);

/** `src` cut into colored runs; joined, the texts give `src` back unchanged. */
export function tokenize(src: string): Token[] {
  const out: Token[] = [];
  // Whether the last token was a value, so a `/` after it divides.
  let afterValue = false;
  // The last token that was not whitespace.
  let prev = '';
  let prevKind = 'plain' as TokenKind;
  // One frame per open bracket, plus the statement's own. A type name is a
  // type only where the grammar puts a type: after a declaration's `:`,
  // after `as`, and inside `array<…>` or a `{name: T}` exception. A `:`
  // inside an expression's `{…}` is a key's, and a `:` that closes an open
  // `?` is a ternary's; neither starts a type. A `;` ends the statement.
  // A `switch`'s `(…)` is its head, and the `{…}` after it its body.
  type Frame = {
    open: string;
    type: boolean;
    ternaries: number;
    switchHead?: boolean;
    switchBody?: boolean;
  };
  const frames: Frame[] = [{ open: '', type: false, ternaries: 0 }];
  let closedSwitchHead = false;
  const top = () => frames[frames.length - 1];
  let inType = false;
  let i = 0;
  const push = (kind: TokenKind, text: string, value: boolean) => {
    out.push({ kind, text });
    if (kind !== 'plain' || text.trim() !== '') {
      afterValue = value;
      prev = text;
      prevKind = kind;
    }
  };

  while (i < src.length) {
    const c = src[i];
    const rest = src.slice(i);

    if (rest.startsWith('//')) {
      const end = src.indexOf('\n', i);
      const stop = end < 0 ? src.length : end;
      push('comment', src.slice(i, stop), afterValue);
      i = stop;
      continue;
    }
    if (rest.startsWith('/*')) {
      const end = src.indexOf('*/', i + 2);
      const stop = end < 0 ? src.length : end + 2;
      push('comment', src.slice(i, stop), afterValue);
      i = stop;
      continue;
    }
    if (c === '"' || c === "'" || c === '`') {
      let j = i + 1;
      while (j < src.length && src[j] !== c) j += src[j] === '\\' ? 2 : 1;
      const text = src.slice(i, Math.min(j + 1, src.length));
      // A quoted name before a colon is a key, as in JSON, unless the colon
      // closes a ternary.
      const key = /^\s*:/.test(src.slice(j + 1)) && c !== '`' && top().ternaries === 0;
      push(key ? 'key' : 'string', text, true);
      i += text.length;
      continue;
    }
    if (c === '/' && !afterValue) {
      let j = i + 1;
      let inClass = false;
      while (j < src.length && src[j] !== '\n') {
        if (src[j] === '\\') j += 2;
        else if (src[j] === '[') ((inClass = true), j++);
        else if (src[j] === ']') ((inClass = false), j++);
        else if (src[j] === '/' && !inClass) break;
        else j++;
      }
      if (src[j] === '/') {
        j++;
        while (j < src.length && /[a-z]/.test(src[j])) j++;
        push('regex', src.slice(i, j), true);
        i = j;
        continue;
      }
    }
    if (/[0-9]/.test(c) || (c === '-' && /[0-9]/.test(src[i + 1] ?? '') && !afterValue)) {
      const m = /^-?\d+(\.\d+)?([eE][+-]?\d+)?/.exec(rest);
      if (m) {
        push('number', m[0], true);
        i += m[0].length;
        continue;
      }
    }
    if (isIdentStart(c)) {
      let j = i + 1;
      while (j < src.length && isIdent(src[j])) j++;
      const word = src.slice(i, j);
      const at = { prev, next: src.slice(j).trimStart(), inSwitch: !!top().switchBody };
      const kind: TokenKind =
        KEYWORDS.has(word) || CONTEXTUAL_KEYWORDS[word]?.(at)
          ? 'keyword'
          : LITERALS.has(word)
            ? 'literal'
            : TYPES.has(word) && inType
              ? 'type'
              : /^\s*\(/.test(src.slice(j))
                ? 'function'
                : 'plain';
      push(kind, word, kind !== 'keyword');
      if (word === 'as') inType = true;
      i = j;
      continue;
    }
    if (rest.startsWith('??') || rest.startsWith('?.')) {
      push('plain', rest.slice(0, 2), false);
      i += 2;
      continue;
    }
    if (c === '?') top().ternaries++;
    else if (c === ':') {
      if (top().ternaries > 0) top().ternaries--;
      // A declaration's or an exception field's `:`; an object literal's is a key's.
      else inType = top().open !== '{' || top().type;
    } else if (c === '<' && inType && prev === 'array') {
      frames.push({ open: '<', type: true, ternaries: 0 });
    } else if (c === '>' && top().open === '<') {
      frames.pop();
    } else if (c === '{' || c === '(' || c === '[') {
      const type = c === '{' && inType && (prev === ':' || prev === '<');
      const switchHead = c === '(' && prev === 'switch' && prevKind === 'keyword';
      const switchBody = c === '{' && prev === ')' && closedSwitchHead;
      frames.push({ open: c, type, ternaries: 0, switchHead, switchBody });
      inType = false;
    } else if ((c === ')' || c === ']' || c === '}') && frames.length > 1) {
      const frame = frames.pop()!;
      inType = frame.type;
      closedSwitchHead = !!frame.switchHead;
    } else if (c === ';') {
      top().ternaries = 0;
      inType = false;
    } else if (c.trim() !== '' && c !== '.') {
      // Any other operator or separator ends the type; a `,` inside an
      // exception leads to the next field's name.
      inType = false;
    }
    // A closing bracket ends a value; any other punctuation does not.
    push('plain', c, c === ')' || c === ']' || c === '}');
    i++;
  }
  return out;
}

const escape = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/** `src` as escaped HTML, each colored run in a `<span class="tok-KIND">`. */
export function highlight(src: string): string {
  return tokenize(src)
    .map((t) =>
      t.kind === 'plain' ? escape(t.text) : `<span class="tok-${t.kind}">${escape(t.text)}</span>`,
    )
    .join('');
}
