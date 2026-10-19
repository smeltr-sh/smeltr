/** The names a compiler message quotes in backticks, as it names every
 * identifier and expression it is about. */
export function quotedTokens(message: string): string[] {
  return [...message.matchAll(/`([^`]+)`/g)].map((m) => m[1]).filter((t) => t.length > 0);
}

const WORD = /[A-Za-z0-9_$]/;

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
 * place. The runs join back into `text`. */
export function markRuns(text: string, tokens: string[]): { text: string; mark: boolean }[] {
  const longestFirst = [...new Set(tokens)]
    .filter((t) => t.length > 0)
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
