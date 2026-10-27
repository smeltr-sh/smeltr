// What the map's search box knows beyond names (design doc "The map", Search): a term filters on
// a field of a node or of its current reading, and the box folds open to the terms worth trying
// on the fleet as it reads now, each with how many objects it matches. Pure, so the page wires it
// to its own state and the tests run it on a fleet of their own.

import {
  SHARED_SCOPE,
  conditionLabel,
  type MapNode,
  type NodeReadingDto,
  type WarningCondition,
  type WarningDto,
} from './api';
import { percent, rows } from './focus';
import { ago } from './time';

/** How a node reads now. A pipeline reports nothing of its own, so it is always `ok`. */
export type Pressure = 'held' | 'near' | 'stale' | 'nodata' | 'ok';

/** The source readings `scope` runs: its group's own, or under `shared/`, which runs in every group, each
 * group's. */
export function sourceReadings(nodes: NodeReadingDto[], scope: string): NodeReadingDto[] {
  return nodes.filter((n) => n.kind === 'source' && (scope === SHARED_SCOPE || n.group === scope));
}

/** A node's current reading, by `nodeKey`. */
export type Readings = Map<string, NodeReadingDto>;

export function pressureOf(node: MapNode, reading: NodeReadingDto | undefined): Pressure {
  if (node.kind === 'pipeline') return 'ok';
  if (!reading) return 'nodata';
  if ((reading.queue?.busiest ?? 0) >= 1) return 'held';
  if ((reading.queue?.nearCapacity ?? 0) > 0) return 'near';
  const fresh =
    reading.rate !== undefined || reading.lag !== undefined || (reading.queue?.fresh ?? 0) > 0;
  if (!fresh) return reading.lastSeenMillis !== undefined || reading.queue ? 'stale' : 'nodata';
  return 'ok';
}

/** Whether the warning catalog names `condition` on this node now. For a figure that shows one
 * fact of one condition, such as a lag's trend; what needs attention reads every warning. */
export function has(reading: NodeReadingDto | undefined, condition: WarningCondition): boolean {
  return reading?.warnings.some((w) => w.condition === condition) ?? false;
}

/** The warning that matters most: a failure before a warning. */
export function worstWarning(warnings: WarningDto[] | undefined): WarningDto | undefined {
  return warnings?.find((w) => w.severity === 'failed') ?? warnings?.[0];
}

/** Rows per second that no route claimed, when any did in the current bucket. */
export function unmatchedRate(reading: NodeReadingDto | undefined): number | null {
  return (reading?.unmatched ?? 0) > 0 ? reading!.unmatched! : null;
}

/** How long a holder may go unheard before its line says so: several of a worker's heartbeats, so one late beat
 * says nothing. */
export const HOLDER_QUIET_MS = 10_000;

/** Which worker holds a listing source and how many entries its ledger has, as a card or a row says it: `held by
 * w-1 · 1,204 in ledger`, with when the holder was last heard once it has gone quiet. `null` when no worker holds
 * it. */
export function holding(
  reading: NodeReadingDto | undefined,
  now: number = Date.now(),
): string | null {
  if (reading?.holder === undefined) return null;
  const heard = reading.holderHeardMillis;
  const quiet =
    heard === undefined
      ? ', not heard since the leader started'
      : now - heard > HOLDER_QUIET_MS
        ? `, last heard ${ago(heard / 1000, now)}`
        : '';
  const ledger =
    // A source that deletes, moves or keeps a cursor has no ledger, and its 0 would read as a fault.
    !reading.ledgerEntries ? '' : ` · ${rows(reading.ledgerEntries)} in ledger`;
  return `held by ${reading.holder}${quiet}${ledger}`;
}

/** Who holds a source in each of `readings`' groups, one `holding` per group that has a holder, joined. `byGroup`
 * names each group first, for a scope whose sources run in every group. */
export function holdings(readings: NodeReadingDto[], byGroup: boolean): string {
  return readings
    .flatMap((r) => {
      const held = holding(r);
      if (held === null) return [];
      return [byGroup ? `${r.group}: ${held}` : held];
    })
    .join('; ');
}

/** One search term: what to type, what it means in words, and what it matches. */
export type Term = {
  term: string;
  label: string;
  test: (node: MapNode, reading: NodeReadingDto | undefined) => boolean;
};

/** The terms for the map's own readings that name a problem. The warning catalog's are made from
 * whatever it names now (`warningTerms`). */
export const ATTENTION_TERMS: Term[] = [
  { term: 'pressure:held', label: 'Backpressure', test: (n, r) => pressureOf(n, r) === 'held' },
  { term: 'pressure:near', label: 'Near capacity', test: (n, r) => pressureOf(n, r) === 'near' },
  {
    term: 'unmatched:',
    label: 'Rows no route takes',
    test: (_n, r) => unmatchedRate(r) !== null,
  },
  {
    term: 'stale:',
    label: 'Readings too old to trust',
    test: (n, r) => pressureOf(n, r) === 'stale',
  },
  { term: 'nodata:', label: 'No readings', test: (n, r) => pressureOf(n, r) === 'nodata' },
];

const FIELD_TERMS: Term[] = [
  {
    term: 'warning:',
    label: 'Any warning',
    test: (_n, r) => (r?.warnings.length ?? 0) > 0,
  },
  {
    term: 'pressure:',
    label: 'Backpressure or near capacity',
    test: (n, r) => ['held', 'near'].includes(pressureOf(n, r)),
  },
  ...ATTENTION_TERMS,
  { term: 'kind:source', label: 'Sources', test: (n) => n.kind === 'source' },
  { term: 'kind:pipeline', label: 'Pipelines', test: (n) => n.kind === 'pipeline' },
  { term: 'kind:destination', label: 'Destinations', test: (n) => n.kind === 'destination' },
  { term: 'shared', label: 'Declared in shared/', test: (n) => n.shared },
];

/** One `warning:` term per condition the catalog names on these nodes now. */
export function warningTerms(
  nodes: MapNode[],
  readingOf: (n: MapNode) => NodeReadingDto | undefined,
): Term[] {
  const named = new Map<WarningCondition, WarningDto['severity']>();
  for (const n of nodes)
    for (const w of readingOf(n)?.warnings ?? [])
      if (named.get(w.condition) !== 'failed') named.set(w.condition, w.severity);
  return [...named]
    .sort(([a, x], [b, y]) => Number(y === 'failed') - Number(x === 'failed') || a.localeCompare(b))
    .map(([condition]) => ({
      term: `warning:${condition.toLowerCase()}`,
      label: conditionLabel(condition),
      test: (_n: MapNode, r: NodeReadingDto | undefined) => has(r, condition),
    }));
}

/** Every term the box answers on these nodes: the fixed ones, one `warning:` per condition named
 * now and one `system:` per system on the map. */
export function termsFor(
  nodes: MapNode[],
  readingOf: (n: MapNode) => NodeReadingDto | undefined = () => undefined,
): Term[] {
  const systems = [...new Set(nodes.flatMap((n) => (n.system ? [n.system] : [])))].sort();
  return [
    ...FIELD_TERMS,
    ...warningTerms(nodes, readingOf),
    ...systems.map((s) => ({
      term: `system:${s.toLowerCase()}`,
      label: s,
      test: (n: MapNode) => n.system === s,
    })),
  ];
}

/** The fields this module answers, so the search can tell a term it knows from one it does not. */
export const TERM_FIELDS = new Set([
  'warning',
  'pressure',
  'unmatched',
  'stale',
  'nodata',
  'kind',
  'system',
  'shared',
]);

/** The term a typed token names, or `null` when it is no term here. `kind:`, `system:` and
 * `warning:` take a prefix, so `kind:dest` is a term while it is being typed; a bare `shared` is a
 * term, every other bare word is a name. */
export function termFor(token: string, terms: Term[]): Term | null {
  const t = token.toLowerCase();
  const exact = terms.find((x) => x.term === t || x.term === `${t}:`);
  if (exact) return exact;
  const prefixed = /^(kind|system|warning):(.+)$/.exec(t);
  if (prefixed) {
    const [, field, value] = prefixed;
    return {
      term: t,
      label: t,
      test: (n, r) =>
        field === 'warning'
          ? (r?.warnings.some((w) => w.condition.toLowerCase().startsWith(value)) ?? false)
          : (field === 'kind' ? n.kind : (n.system ?? '').toLowerCase()).startsWith(value),
    };
  }
  return null;
}

export type Suggestion =
  | { section: string; kind: 'term'; value: string; label: string; detail?: string; count: number }
  | { section: string; kind: 'node'; node: MapNode; label: string; detail: string }
  | { section: string; kind: 'none'; label: string };

/** The most the box lists at once: past it a list is read as noise. */
export const MAX_SUGGESTIONS = 14;

/** What the box offers for `query` as typed. With nothing being typed, the problems the fleet has
 * now -- each with how many objects it matches and the first of them -- then the filters worth
 * having. Mid-word, the terms that word starts and the objects whose name holds it. A term already
 * in the query is not offered again, and a term that would match nothing is left out. */
export function suggestions(
  query: string,
  nodes: MapNode[],
  readingOf: (n: MapNode) => NodeReadingDto | undefined,
): Suggestion[] {
  const terms = termsFor(nodes, readingOf);
  const words = query.split(/\s+/);
  const typing =
    query.length > 0 && !query.endsWith(' ') ? words[words.length - 1].toLowerCase() : '';
  const used = new Set(words.filter((w) => w.length > 0).map((w) => w.toLowerCase()));
  const matching = (t: Term) => nodes.filter((n) => t.test(n, readingOf(n)));
  const out: Suggestion[] = [];

  if (typing === '') {
    for (const t of [...warningTerms(nodes, readingOf), ...ATTENTION_TERMS]) {
      if (used.has(t.term)) continue;
      const hits = matching(t);
      if (hits.length === 0) continue;
      const names = [...new Set(hits.map((n) => n.name))];
      out.push({
        section: 'Needs attention',
        kind: 'term',
        value: t.term,
        label: t.label,
        detail: names.slice(0, 2).join(', ') + (names.length > 2 ? ` +${names.length - 2}` : ''),
        count: hits.length,
      });
    }
    for (const t of terms.filter(
      (t) => t.term.startsWith('kind:') || t.term.startsWith('system:') || t.term === 'shared',
    )) {
      if (used.has(t.term)) continue;
      const count = matching(t).length;
      if (count > 0)
        out.push({ section: 'Filter by', kind: 'term', value: t.term, label: t.label, count });
    }
    return out.slice(0, MAX_SUGGESTIONS);
  }

  for (const t of terms) {
    if (used.has(t.term) && t.term !== typing) continue;
    const byLabel =
      !typing.includes(':') && typing.length >= 3 && t.label.toLowerCase().includes(typing);
    if (!t.term.startsWith(typing) && !byLabel) continue;
    const count = matching(t).length;
    if (count > 0 || t.term === typing)
      out.push({ section: 'Filter by', kind: 'term', value: t.term, label: t.label, count });
  }
  if (!typing.includes(':')) {
    for (const n of nodes) {
      if (n.name.toLowerCase().includes(typing) || n.authorId.toLowerCase().includes(typing))
        out.push({
          section: 'Go to',
          kind: 'node',
          node: n,
          label: n.name,
          detail: `${n.kind} · ${n.group}`,
        });
    }
  } else if (out.length === 0 && !typing.startsWith('calls:')) {
    out.push({ section: '', kind: 'none', label: `Nothing answers ${typing}` });
  }
  return out.slice(0, MAX_SUGGESTIONS);
}

/** The query once a suggestion is picked: the word being typed is replaced by the term, and a
 * space follows it so the box offers the next one. */
export function applySuggestion(query: string, term: string): string {
  const words = query.split(/\s+/).filter((w) => w.length > 0);
  if (query.length > 0 && !query.endsWith(' ')) words.pop();
  return [...words, term].join(' ') + ' ';
}

/** One row of the attention list: what is wrong and where, worst first. A row about a node names
 * it; a catalog warning about something in a group the map draws no node for, such as a worker,
 * names the group and the warning's own subject. */
export type Attention = {
  group: string;
  node?: MapNode;
  /** What the row names: the node's name, or the warning's subject. */
  subject: string;
  text: string;
  /** 0 is a failure, 1 and 2 a warning, 3 a reading too old to trust. */
  rank: number;
  /** The catalog's own sentence, for a row that is a catalog warning. */
  message?: string;
  /** The log site of a worker's logged warning, which tells two of one worker's rows apart. */
  site?: string;
};

function fromWarning(group: string, w: WarningDto, node?: MapNode): Attention {
  return {
    group,
    node,
    subject: node?.name ?? w.subjectId,
    text: conditionLabel(w.condition),
    rank: w.severity === 'failed' ? 0 : 1,
    message: w.message,
    site: w.site,
  };
}

/** Every catalog warning on these nodes, and on the `groups` the map shows, then each node's
 * worst reading of its own. `groups` defaults to the nodes' own; a group drawn as its errors
 * alone has no nodes and is named there. */
export function attention(
  nodes: MapNode[],
  readingOf: (n: MapNode) => NodeReadingDto | undefined,
  groupWarnings: WarningDto[] = [],
  groups: string[] = nodes.map((n) => n.group),
): Attention[] {
  const out: Attention[] = [];
  for (const node of nodes) {
    const r = readingOf(node);
    for (const w of r?.warnings ?? []) out.push(fromWarning(node.group, w, node));
    const own = { group: node.group, node, subject: node.name };
    const p = pressureOf(node, r);
    // A full buffer behind a failure is that failure's effect, and the failure already says so.
    if (p === 'held' && worstWarning(r?.warnings)?.severity !== 'failed')
      out.push({ ...own, text: 'Backpressure', rank: 0 });
    else if (p === 'near')
      out.push({ ...own, text: `${percent(r?.queue?.busiest ?? null)} buffer`, rank: 1 });
    else if (unmatchedRate(r) !== null)
      out.push({ ...own, text: `${perSecond(unmatchedRate(r)!)}/s unmatched`, rank: 2 });
    else if (p === 'stale') out.push({ ...own, text: 'Stale', rank: 3 });
  }
  const drawn = new Set(groups);
  // One subject can hold one condition twice, as a connector does for its certificate and its authorities.
  // A worker holds its logged warnings once per site, and each site is its own row.
  const listed = new Set<string>();
  for (const w of groupWarnings) {
    const key = `${w.group}|${w.subjectKind}|${w.subjectId}|${w.condition}|${w.site ?? ''}`;
    if (w.group === null || !drawn.has(w.group) || listed.has(key)) continue;
    listed.add(key);
    out.push(fromWarning(w.group, w));
  }
  return out.sort(
    (a, b) =>
      a.rank - b.rank ||
      a.subject.localeCompare(b.subject) ||
      a.group.localeCompare(b.group) ||
      a.text.localeCompare(b.text),
  );
}

/** A group's header line: its worst problem, or that nothing reports. `ok` groups are the ones a
 * large fleet starts collapsed. */
export type GroupSummary = {
  text: string;
  level: 'alarm' | 'warn' | 'dim' | 'ok';
  first?: Attention;
};

/** `rows` is this group's share of `attention`, worst first. */
export function groupSummary(
  nodes: MapNode[],
  readingOf: (n: MapNode) => NodeReadingDto | undefined,
  rows: Attention[],
): GroupSummary {
  const first = rows[0];
  if (first) {
    const same = rows.filter((r) => r.text === first.text).length;
    return {
      text: same > 1 ? `${first.text} · ${same}` : first.text,
      level: first.rank === 0 ? 'alarm' : first.rank < 3 ? 'warn' : 'dim',
      first,
    };
  }
  const terminals = nodes.filter((n) => n.kind !== 'pipeline');
  if (terminals.length > 0 && terminals.every((n) => pressureOf(n, readingOf(n)) === 'nodata'))
    return { text: 'No readings', level: 'dim' };
  return { text: 'No pressure', level: 'ok' };
}

/** A rate as a card reads it: whole rows from ten a second up, one decimal below, so a trickle
 * never reads as none. */
export function perSecond(rate: number): string {
  return rate >= 10 || Number.isInteger(rate) ? rows(rate) : rate.toFixed(1);
}

/** What a card says about its reading, in one line: the rate, then a source's lag or the worker
 * holding it, or a destination's busiest buffer, which also fills the card's bar. `null` for a pipeline, which
 * reports nothing of its own. */
export type CardLine = {
  rate: string;
  note: string;
  level: 'alarm' | 'warn' | 'dim';
  /** The busiest worker's buffer, 0 to 1 or past it; absent where no worker reports one. */
  fill?: number;
};

export function cardLine(node: MapNode, reading: NodeReadingDto | undefined): CardLine | null {
  if (node.kind === 'pipeline') return null;
  const p = pressureOf(node, reading);
  const rate =
    reading?.rate !== undefined
      ? `${perSecond(reading.rate)} rows/s`
      : p === 'stale'
        ? 'stale'
        : p === 'nodata'
          ? 'no readings'
          : '';
  const unmatched = unmatchedRate(reading);
  const worst = worstWarning(reading?.warnings);
  const level =
    p === 'held' || worst?.severity === 'failed'
      ? 'alarm'
      : p === 'near' || worst !== undefined || unmatched !== null
        ? 'warn'
        : 'dim';
  if (node.kind === 'source') {
    // A source waiting on a destination is only a warning, but nothing else on the card says why it is not reading.
    const note =
      worst?.severity === 'failed' || worst?.condition === 'sourceWaiting'
        ? conditionLabel(worst.condition)
        : unmatched !== null
          ? `${perSecond(unmatched)}/s unmatched`
          : reading?.lag !== undefined
            ? `lag ${rows(reading.lag)}${has(reading, 'lagClimbing') ? ' ↑' : ''}`
            : (holding(reading) ?? '');
    return { rate, note, level };
  }
  const fill = reading?.queue?.busiest;
  return { rate, note: fill === undefined ? '' : percent(fill), level, fill };
}
