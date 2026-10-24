// The matching behind `ValidatedField`'s function autocomplete: which word
// is being typed, and which functions match it, in what order.

import type { StdFuncEntry } from './stdlib';
import { takes, type Member } from './fieldTypes';

export interface CurrentWord {
  word: string;
  start: number;
  pos: number;
}

/** The identifier run ending at `cursorPos`. `.` is not an identifier
 * character, so `status.toU` yields `toU`. */
export function currentWord(value: string, cursorPos: number): CurrentWord {
  let start = cursorPos;
  while (start > 0 && /[A-Za-z0-9_]/.test(value[start - 1])) start--;
  return { word: value.slice(start, cursorPos), start, pos: cursorPos };
}

/** Where a name's words begin: its first letter, a capital after a lower-case
 * letter (`parseTime`'s `T`), and anything after `_`. */
function wordStart(name: string, i: number): boolean {
  if (i === 0) return true;
  const prev = name[i - 1];
  const here = name[i];
  return prev === '_' || (/[a-z0-9]/.test(prev) && /[A-Z]/.test(here));
}

/** How well `word` matches `name` as a code editor matches it: its letters in
 * order, not necessarily together, so `pt` finds `parseTime`. Letters that
 * start a word in the name and letters that run on together score most, and a
 * shorter name scores a little more. `null` when the letters are not all
 * there in order. */
export function nameScore(name: string, word: string): number | null {
  const lowerName = name.toLowerCase();
  const lower = word.toLowerCase();
  if (lowerName === lower) return 1000;
  let score = lowerName.startsWith(lower) ? 20 : 0;
  let at = -1;
  for (const ch of lower) {
    // A letter that starts one of the name's words beats an earlier one inside a word.
    let next = -1;
    for (let i = at + 1; i < name.length; i++) {
      if (lowerName[i] !== ch) continue;
      if (next === -1) next = i;
      if (i === at + 1 || wordStart(name, i)) {
        next = i;
        break;
      }
    }
    if (next === -1) return null;
    score += next === at + 1 ? 5 : wordStart(name, next) ? 10 : 1;
    at = next;
  }
  return score - name.length * 0.2;
}

/** The most a function's use in the bundle adds to its score: enough to order
 * matches that are about as good, never enough to lift a weak match over one
 * that starts a word. */
const MAX_USAGE_BONUS = 4;

function usageBonus(uses: number): number {
  return Math.min(MAX_USAGE_BONUS, 1.5 * Math.log2(1 + uses));
}

/** Every entry matching `word`, best first: by `nameScore`, then, where no
 * name matches, by a description holding the word, which is what lets a real
 * word (`integer`) find `parseInt` when a guessed name (`toInt`) cannot. How
 * often the bundle calls a name (`usage`) adds a little. Grok macro names are
 * typed directly and their descriptions are prose, so `matchDescription` is off
 * there, or "row" and "with" would surface half the catalog. An empty `word`
 * is the whole list in its own order: the browse-everything case. */
export function rankedSuggestions(
  entries: StdFuncEntry[],
  word: string,
  {
    matchDescription = true,
    usage = new Map(),
  }: { matchDescription?: boolean; usage?: ReadonlyMap<string, number> } = {},
): StdFuncEntry[] {
  if (word.length === 0) return entries;
  const lower = word.toLowerCase();
  // A description match ranks below every name match, whatever its use.
  const scored: { f: StdFuncEntry; byName: boolean; score: number }[] = [];
  for (const f of entries) {
    const byName = nameScore(f.name, word);
    if (byName === null && !(matchDescription && f.description.toLowerCase().includes(lower))) {
      continue;
    }
    const bonus = usageBonus(usage.get(f.name) ?? 0);
    scored.push({ f, byName: byName !== null, score: (byName ?? 0) + bonus });
  }
  return scored
    .sort((a, b) => Number(b.byName) - Number(a.byName) || b.score - a.score)
    .map(({ f }) => f);
}

/** What may follow a dot, best first: an object's fields, then the functions that take what is before the dot, then
 * every other function, each with a warning naming what it takes. Within each, as `rankedSuggestions` orders them.
 * Only a function written on a value (`value.sign()`) can follow a dot. Where `member` is unknown, every such
 * function, as they rank. */
export function memberSuggestions(
  member: Member,
  functions: StdFuncEntry[],
  word: string,
  usage: ReadonlyMap<string, number> = new Map(),
): StdFuncEntry[] {
  const methods = rankedSuggestions(
    functions.filter((f) => /^\w+\.\w+\(/.test(f.signature)),
    word,
    { usage },
  );
  if (member.kind === 'unknown') return methods;
  const fields: StdFuncEntry[] =
    member.kind === 'object'
      ? rankedSuggestions(
          member.fields.map((f) => ({
            name: f.name,
            signature: `${f.name}: ${f.type}`,
            description: '',
            field: true,
          })),
          word,
          { matchDescription: false },
        )
      : [];
  const type = member.kind === 'object' ? 'object' : member.type;
  const kind = member.kind === 'object' ? 'object' : type.startsWith('array<') ? 'array' : null;
  const fit: StdFuncEntry[] = [];
  const misfit: StdFuncEntry[] = [];
  for (const f of methods) {
    // A function the signatures do not type is written on the kind its reference names: `object.keys()`.
    const known = takes(f.name, type);
    const fits = known.fits ?? (kind !== null && f.signature.startsWith(`${kind}.`));
    if (fits) fit.push(f);
    else
      misfit.push({
        ...f,
        warning: known.takes ? `takes ${known.takes}, not ${type}` : `not for ${type}`,
      });
  }
  return [...fields, ...fit, ...misfit];
}

/** True once an unclosed `%{` precedes `cursorPos` with no `:` since -- the
 * macro-name position of a `%{NAME}` or `%{NAME:field}`. A `}` or a `:`
 * since the last `%{` means the cursor has moved past that name, into a
 * field name or past the macro entirely. */
export function inGrokMacroName(text: string, cursorPos: number): boolean {
  const upto = text.slice(0, cursorPos);
  const lastOpen = upto.lastIndexOf('%{');
  if (lastOpen === -1) return false;
  const since = upto.slice(lastOpen + 2);
  return !since.includes('}') && !since.includes(':');
}

/** A description from the website's reference cut into runs, `code` marked
 * as such. A link reads as its text: the entry links to its own page. */
export function describedRuns(text: string): { text: string; code: boolean }[] {
  return text
    .split(/(`[^`]+`)/)
    .filter((piece) => piece.length > 0)
    .map((piece) =>
      piece.startsWith('`')
        ? { text: piece.slice(1, -1), code: true }
        : { text: piece.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1'), code: false },
    );
}
