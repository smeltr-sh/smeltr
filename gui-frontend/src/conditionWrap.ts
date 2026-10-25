// A condition laid out in lines for a box too narrow for it: broken before its `&&` and `||`, the shallowest
// first, so a bracketed group stays on one line wherever it fits on one.
import { tokenize } from '../../website/src/lib/highlight/highlight';

type Break = { at: number; depth: number };

/** Where `src` may break: before each `&&` or `||` that is not in a string or comment and does not start it, with
 * how many brackets it sits inside. */
function breaks(src: string): Break[] {
  const out: Break[] = [];
  let pos = 0;
  let depth = 0;
  for (const t of tokenize(src)) {
    if (t.kind === 'plain' && t.text.length === 1) {
      const c = t.text;
      if ('([{'.includes(c)) depth++;
      else if (')]}'.includes(c)) depth = Math.max(0, depth - 1);
      // The first of the pair: the one before it is not the same operator.
      const op = src.slice(pos, pos + 2);
      if ((op === '&&' || op === '||') && src[pos - 1] !== c && src.slice(0, pos).trim() !== '')
        out.push({ at: pos, depth });
    }
    pos += t.text.length;
  }
  return out;
}

/** `src` cut into lines of at most `cols` characters, each trimmed. A line starts at an operator, never inside a
 * name, number or string, so a run with no operator in it stays whole even when it is longer than `cols`. A
 * bracketed group is broken only where it cannot fit on a line of its own, and then at its own shallowest
 * operators first. */
export function wrapCondition(src: string, cols: number): string[] {
  const all = breaks(src);
  const fit = (a: number, b: number): string[] => {
    const text = src.slice(a, b).trim();
    if (text.length <= cols) return [text];
    const inside = all.filter((x) => x.at > a && x.at < b);
    if (inside.length === 0) return [text];
    const depth = Math.min(...inside.map((x) => x.depth));
    const cuts = [a, ...inside.filter((x) => x.depth === depth).map((x) => x.at), b];
    // The parts between the cuts, packed greedily; a part too long for a line is broken further.
    const out: string[] = [];
    let cur = '';
    for (let k = 0; k + 1 < cuts.length; k++) {
      const part = src.slice(cuts[k], cuts[k + 1]).trim();
      if (cur && cur.length + 1 + part.length <= cols) {
        cur += ' ' + part;
        continue;
      }
      if (cur) out.push(cur);
      const lines = part.length <= cols ? [part] : fit(cuts[k], cuts[k + 1]);
      out.push(...lines.slice(0, -1));
      cur = lines[lines.length - 1];
    }
    if (cur) out.push(cur);
    return out;
  };
  return fit(0, src.length);
}

/** `src` cut into lines of at most `cols` characters at its spaces, packed greedily, each break taking the one space
 * it is at, so the lines joined by a space are `src` exactly. A space inside a match of `held` is never a break, and
 * a run longer than `cols` stays whole. */
export function wrapWords(src: string, cols: number, held?: RegExp): string[] {
  const inside = new Set<number>();
  for (const m of held ? src.matchAll(held) : [])
    for (let i = m.index; i < m.index + m[0].length; i++) inside.add(i);
  const runs: string[] = [];
  let from = 0;
  for (let i = 0; i < src.length; i++)
    if (src[i] === ' ' && !inside.has(i)) {
      runs.push(src.slice(from, i));
      from = i + 1;
    }
  runs.push(src.slice(from));
  const out: string[] = [];
  let cur: string | null = null;
  for (const run of runs) {
    if (cur !== null && cur.length + 1 + run.length <= cols) cur += ' ' + run;
    else {
      if (cur !== null) out.push(cur);
      cur = run;
    }
  }
  if (cur !== null) out.push(cur);
  return out;
}
