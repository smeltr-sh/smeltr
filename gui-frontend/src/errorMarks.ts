/** The names a compiler message quotes in backticks, as it names every
 * identifier and expression it is about. */
export function quotedTokens(message: string): string[] {
  return [...message.matchAll(/`([^`]+)`/g)].map((m) => m[1]).filter((t) => t.length > 0);
}

const WORD = /[A-Za-z0-9_$]/;

/** Where an error is in a field's text, in UTF-8 bytes, as the compiler counts (`CompileError::span`). */
export interface ByteSpan {
  start: number;
  end: number;
}

/** How many UTF-16 units, as a string counts, the first `bytes` UTF-8 bytes of `text` take. */
function unitsOf(text: string, bytes: number): number {
  return new TextDecoder().decode(new TextEncoder().encode(text).slice(0, bytes)).length;
}

/** `text` cut into runs, the part `span` covers marked. An empty span, as where the text ends too soon, marks the
 * character at it, or the last one where it is past the text. The runs join back into `text`. */
export function spanRuns(text: string, span: ByteSpan): { text: string; mark: boolean }[] {
  return unitRuns(text, unitsOf(text, span.start), unitsOf(text, span.end));
}

/** `spanRuns` for `laid`, `text` laid out in lines: the layout changes only white space, so a place in `text` is
 * the one in `laid` with as many non-white-space characters before it. */
export function laidSpanRuns(
  text: string,
  laid: string,
  span: ByteSpan,
): { text: string; mark: boolean }[] {
  const ink = (s: string, to: number) => [...s.slice(0, to)].filter((c) => !/\s/.test(c)).length;
  const at = (n: number, skip: boolean) => {
    let i = 0;
    for (; i < laid.length && n > 0; i++) if (!/\s/.test(laid[i])) n--;
    while (skip && i < laid.length && /\s/.test(laid[i])) i++;
    return i;
  };
  const start = at(ink(text, unitsOf(text, span.start)), true);
  return unitRuns(laid, start, Math.max(start, at(ink(text, unitsOf(text, span.end)), false)));
}

function unitRuns(text: string, start: number, end: number): { text: string; mark: boolean }[] {
  const last = text.replace(/\s+$/, '').length;
  if (last === 0) return [];
  if (end <= start) {
    start = Math.min(start, last - 1);
    end = start + 1;
  }
  return [
    { text: text.slice(0, start), mark: false },
    { text: text.slice(start, end), mark: true },
    { text: text.slice(end), mark: false },
  ].filter((r) => r.text.length > 0);
}

/** Whether `token` stands at `i` in `text` as a whole name, not inside a
 * longer one: `x` in `x = 1`, never in `max`. */
function standsAt(text: string, token: string, i: number): boolean {
  if (!text.startsWith(token, i)) return false;
  const before = text[i - 1] ?? '';
  const after = text[i + token.length] ?? '';
  return (
    !(WORD.test(token[0]) && WORD.test(before)) && !(WORD.test(token.at(-1)!) && WORD.test(after))
  );
}

/** Whether `token` stands somewhere in `text` as a whole name. */
export function holdsName(text: string, token: string): boolean {
  for (let i = text.indexOf(token); i >= 0; i = text.indexOf(token, i + 1)) {
    if (standsAt(text, token, i)) return true;
  }
  return false;
}

/** The error a field holding `text` marks: `forced` where one is known to be its own, else the first of `errors`
 * that quotes a name `text` holds, else null. */
export function fieldError(text: string, errors: string[], forced: string | null): string | null {
  return forced ?? errors.find((e) => quotedTokens(e).some((t) => holdsName(text, t))) ?? null;
}

/** `text` cut into runs, each marked where one of `tokens` stands as a whole
 * name: every occurrence, the longest token first where two start at one
 * place. The runs join back into `text`. A token with no name in it, as the
 * `.` a parse error quotes, marks nothing: every `.` in the text is not where
 * the error is. */
export function markRuns(text: string, tokens: string[]): { text: string; mark: boolean }[] {
  const longestFirst = [...new Set(tokens)]
    .filter((t) => WORD.test(t))
    .sort((a, b) => b.length - a.length);
  const runs: { text: string; mark: boolean }[] = [];
  let plain = '';
  for (let i = 0; i < text.length;) {
    const hit = longestFirst.find((t) => standsAt(text, t, i));
    if (hit === undefined) {
      plain += text[i++];
      continue;
    }
    if (plain) runs.push({ text: plain, mark: false });
    plain = '';
    runs.push({ text: hit, mark: true });
    i += hit.length;
  }
  if (plain) runs.push({ text: plain, mark: false });
  return runs;
}
