import { describe, expect, it } from 'vitest';
import type {
  MapNode,
  NodeReadingDto,
  QueueSummaryDto,
  WarningCondition,
  WarningDto,
  WarningSeverity,
} from './api';
import {
  applySuggestion,
  attention,
  cardLine,
  groupSummary,
  holding,
  holdings,
  MAX_SUGGESTIONS,
  pressureOf,
  suggestions,
  termFor,
  termsFor,
} from './mapTerms';

function node(
  authorId: string,
  kind: MapNode['kind'],
  group = 'g1',
  system?: MapNode['system'],
): MapNode {
  return { group, authorId, name: authorId, kind, system, shared: false, warning: false };
}

function queue(q: Partial<QueueSummaryDto>): QueueSummaryDto {
  return { nearCapacity: 0, fresh: 1, stale: 0, workers: 1, ...q };
}

function reading(r: Partial<NodeReadingDto>): NodeReadingDto {
  return { group: 'g1', kind: 'destination', id: 'x', warnings: [], ...r };
}

function warning(
  condition: WarningCondition,
  severity: WarningSeverity = 'failed',
  subject: Partial<WarningDto> = {},
): WarningDto {
  return {
    condition,
    severity,
    subjectKind: 'source',
    subjectId: 'x',
    group: 'g1',
    sinceMillis: null,
    message: `${condition} sentence`,
    ...subject,
  };
}

// A fleet with one of each problem, read by author id.
const held = node('es-prod', 'destination', 'g1', 'elasticsearch');
const near = node('kafka-out', 'destination', 'g1', 'kafka');
const growing = node('orders', 'source', 'g1', 'kafka');
const stale = node('clicks', 'source', 'g2', 'kafka');
const silent = node('archive', 'destination', 'g2', 's3');
const healthy = node('alerts', 'destination', 'g2', 'kafka');
const pipe = node('normalize', 'pipeline');
const fleet = [held, near, growing, stale, silent, healthy, pipe];
const readings: Record<string, NodeReadingDto> = {
  'es-prod': reading({ rate: 10, queue: queue({ busiest: 1.2 }) }),
  'kafka-out': reading({ rate: 10, queue: queue({ busiest: 0.93, nearCapacity: 1 }) }),
  orders: reading({
    kind: 'source',
    rate: 5,
    lag: 900,
    warnings: [warning('lagClimbing', 'warning')],
  }),
  clicks: reading({ kind: 'source', lastSeenMillis: 1 }),
  alerts: reading({ rate: 3, queue: queue({ busiest: 0.1 }) }),
};
const readingOf = (n: MapNode) => readings[n.authorId];

describe('pressureOf', () => {
  it('reads each node by its worst condition', () => {
    expect(fleet.map((n) => pressureOf(n, readingOf(n)))).toEqual([
      'held',
      'near',
      'ok',
      'stale',
      'nodata',
      'ok',
      'ok',
    ]);
  });

  it("reads only the readings: a warning is the catalog's to say", () => {
    expect(pressureOf(growing, reading({ rate: 5, warnings: [warning('sourceBlocked')] }))).toBe(
      'ok',
    );
  });
});

describe('termFor', () => {
  const terms = termsFor(fleet);

  it('answers a fixed term, with or without its colon', () => {
    expect(termFor('pressure:held', terms)?.label).toBe('Backpressure');
    expect(termFor('stale', terms)?.term).toBe('stale:');
  });

  it('answers a kind or system while it is being typed', () => {
    const t = termFor('kind:dest', terms)!;
    expect(fleet.filter((n) => t.test(n, undefined)).map((n) => n.kind)).toEqual(
      Array(4).fill('destination'),
    );
    expect(termFor('system:elastic', terms)!.test(held, undefined)).toBe(true);
  });

  it('answers a warning by the start of its condition', () => {
    const t = termFor('warning:lag', terms)!;
    expect(fleet.filter((n) => t.test(n, readingOf(n)))).toEqual([growing]);
  });

  it('is no term for a bare word or a field it does not know', () => {
    expect(termFor('orders', terms)).toBeNull();
    expect(termFor('calls:enrich', terms)).toBeNull();
  });
});

describe('termsFor', () => {
  it('names one system term per system on the map', () => {
    const systems = termsFor(fleet)
      .map((t) => t.term)
      .filter((t) => t.startsWith('system:'));
    expect(systems).toEqual(['system:elasticsearch', 'system:kafka', 'system:s3']);
  });
});

describe('suggestions', () => {
  it('with nothing typed, leads with the problems the fleet has now', () => {
    const s = suggestions('', fleet, readingOf);
    const attention = s.filter((x) => x.section === 'Needs attention');
    expect(attention.map((x) => (x.kind === 'term' ? [x.value, x.count] : null))).toEqual([
      ['warning:lagclimbing', 1],
      ['pressure:held', 1],
      ['pressure:near', 1],
      ['stale:', 1],
      ['nodata:', 1],
    ]);
    expect(s[0].kind === 'term' && s[0].label).toBe('Lag climbing');
    expect(s[1].kind === 'term' && s[1].detail).toBe('es-prod');
  });

  it('leaves out a term that would match nothing, or one already in the query', () => {
    const calm = suggestions('', [healthy], readingOf);
    expect(calm.some((x) => x.section === 'Needs attention')).toBe(false);
    const again = suggestions('pressure:held ', fleet, readingOf);
    expect(again.some((x) => x.kind === 'term' && x.value === 'pressure:held')).toBe(false);
  });

  it('mid-word, offers the terms the word starts and the objects whose name holds it', () => {
    const s = suggestions('kind:d', fleet, readingOf);
    expect(s.map((x) => (x.kind === 'term' ? x.value : x.kind))).toEqual(['kind:destination']);
    const byName = suggestions('ord', fleet, readingOf);
    expect(byName.some((x) => x.kind === 'node' && x.node === growing)).toBe(true);
  });

  it('says so when a field term answers nothing', () => {
    expect(suggestions('kind:zzz', fleet, readingOf)).toEqual([
      { section: '', kind: 'none', label: 'Nothing answers kind:zzz' },
    ]);
  });

  it('leaves calls: to the search, which reads callers it does not have', () => {
    expect(suggestions('calls:enrich', fleet, readingOf)).toEqual([]);
  });

  it('lists no more than it can read', () => {
    const many = Array.from({ length: 40 }, (_, i) => node(`orders-${i}`, 'source'));
    expect(suggestions('orders', many, () => undefined)).toHaveLength(MAX_SUGGESTIONS);
  });
});

describe('applySuggestion', () => {
  it('replaces the word being typed, and leaves room for the next', () => {
    expect(applySuggestion('orders kind:d', 'kind:destination')).toBe('orders kind:destination ');
    expect(applySuggestion('orders ', 'pressure:held')).toBe('orders pressure:held ');
    expect(applySuggestion('', 'shared')).toBe('shared ');
  });
});

describe('attention', () => {
  it("lists every catalog warning and each node's worst reading, failures first", () => {
    expect(attention(fleet, readingOf).map((a) => [a.subject, a.text, a.rank])).toEqual([
      ['es-prod', 'Backpressure', 0],
      ['kafka-out', '93.0% buffer', 1],
      ['orders', 'Lag climbing', 1],
      ['clicks', 'Stale', 3],
    ]);
  });

  it('labels a warning as the catalog does, and keeps its sentence', () => {
    const brokers = node('kafka-1', 'destination', 'g1', 'kafka');
    const feed = node('feed', 'source', 'g1', 'kafka');
    const cut: Record<string, NodeReadingDto> = {
      ...readings,
      'kafka-1': reading({ rate: 0, warnings: [warning('destinationUnreachable')] }),
      feed: reading({ kind: 'source', warnings: [warning('sourceUnreachable')] }),
    };
    const list = attention([held, brokers, feed], (n) => cut[n.authorId]);
    expect(list.map((a) => [a.subject, a.text, a.rank])).toEqual([
      ['es-prod', 'Backpressure', 0],
      ['feed', 'Source unreachable', 0],
      ['kafka-1', 'Destination unreachable', 0],
    ]);
    expect(list[2].message).toBe('destinationUnreachable sentence');
  });

  it('leaves out a full buffer that a failure on the same node already explains', () => {
    const refusing = reading({
      rate: 0,
      queue: queue({ busiest: 1.4 }),
      warnings: [warning('destinationRefusing')],
    });
    expect(attention([held], () => refusing).map((a) => a.text)).toEqual(['Refusing writes']);
  });

  it('lists a warning about something in a group the map draws, under that group', () => {
    const silentWorker = warning('workerSilent', 'failed', {
      subjectKind: 'worker',
      subjectId: 'w7',
    });
    const elsewhere = warning('workerSilent', 'failed', { subjectId: 'w8', group: 'g9' });
    const rows = attention([node('quiet', 'destination')], readingOf, [silentWorker, elsewhere]);
    expect(rows.map((a) => [a.group, a.subject, a.text, a.node])).toEqual([
      ['g1', 'w7', 'Worker silent', undefined],
    ]);
    const broken = warning('workerSilent', 'failed', { subjectId: 'w9', group: 'g9' });
    expect(attention([], readingOf, [broken], ['g9']).map((a) => a.subject)).toEqual(['w9']);
    const twice = warning('identityExpiring', 'warning', {
      subjectKind: 'identity',
      subjectId: 'tls',
    });
    expect(attention([node('quiet', 'destination')], readingOf, [twice, twice])).toHaveLength(1);
    const logged = (site: string) =>
      warning('workerLoggedWarnings', 'warning', { subjectKind: 'worker', subjectId: 'w1', site });
    const sites = [logged('worker.fetch_failed'), logged('worker.heartbeat_failed')];
    expect(attention([node('quiet', 'destination')], readingOf, sites)).toHaveLength(2);
  });
});

describe('groupSummary', () => {
  const rowsOf = (nodes: MapNode[], g: string) =>
    attention(nodes, readingOf).filter((a) => a.group === g);

  it("names a group's worst problem and the row that has it", () => {
    const g1 = fleet.filter((n) => n.group === 'g1');
    const rows = rowsOf(g1, 'g1');
    expect(groupSummary(g1, readingOf, rows)).toEqual({
      text: 'Backpressure',
      level: 'alarm',
      first: rows[0],
    });
  });

  it('counts the worst problem when more than one row has it', () => {
    const a = node('a', 'destination');
    const b = node('b', 'destination');
    const down = () => reading({ warnings: [warning('destinationUnreachable')] });
    const rows = attention([a, b], down);
    expect(groupSummary([a, b], down, rows).text).toBe('Destination unreachable · 2');
  });

  it('reads a group nothing reports from as having no readings', () => {
    expect(groupSummary([silent, pipe], readingOf, [])).toEqual({
      text: 'No readings',
      level: 'dim',
    });
  });

  it('reads a group with nothing wrong as calm', () => {
    expect(groupSummary([healthy, pipe], readingOf, [])).toEqual({
      text: 'No pressure',
      level: 'ok',
    });
  });
});

describe('cardLine', () => {
  it("says a destination's rate and its busiest buffer, which fills the bar", () => {
    expect(cardLine(near, readingOf(near))).toEqual({
      rate: '10 rows/s',
      note: '93.0%',
      level: 'warn',
      fill: 0.93,
    });
  });

  it("says a source's lag, and whether it climbs", () => {
    expect(cardLine(growing, readingOf(growing))).toEqual({
      rate: '5 rows/s',
      note: 'lag 900 ↑',
      level: 'warn',
    });
  });

  it("names a failing source's worst warning, in alarm", () => {
    const blocked = reading({ kind: 'source', rate: 0, warnings: [warning('sourceBlocked')] });
    expect(cardLine(growing, blocked)).toEqual({
      rate: '0 rows/s',
      note: 'Source blocked',
      level: 'alarm',
    });
  });

  it('names a source waiting on a destination, though it is only a warning', () => {
    const waiting = reading({
      kind: 'source',
      rate: 0,
      lag: 900,
      warnings: [warning('sourceWaiting', 'warning')],
    });
    expect(cardLine(growing, waiting)).toEqual({
      rate: '0 rows/s',
      note: 'Waiting on destination',
      level: 'warn',
    });
  });

  it('never rounds a trickle down to none', () => {
    expect(cardLine(healthy, reading({ rate: 0.1 }))?.rate).toBe('0.1 rows/s');
    expect(cardLine(healthy, reading({ rate: 1234.4 }))?.rate).toBe('1,234 rows/s');
  });

  it('says when there is no current reading, and nothing for a pipeline', () => {
    expect(cardLine(stale, readingOf(stale))?.rate).toBe('stale');
    expect(cardLine(silent, undefined)?.rate).toBe('no readings');
    expect(cardLine(pipe, undefined)).toBeNull();
  });
});

describe('a held listing source', () => {
  it('names its worker and its ledger, and nothing when no worker holds it', () => {
    expect(
      holding(
        reading({
          kind: 'source',
          holder: 'w-1',
          holderHeardMillis: Date.now(),
          ledgerEntries: 1204,
        }),
      ),
    ).toBe('held by w-1 · 1,204 in ledger');
    expect(holding(reading({ kind: 'source', holder: 'w-1', holderHeardMillis: Date.now() }))).toBe(
      'held by w-1',
    );
    expect(
      holding(
        reading({ kind: 'source', holder: 'w-1', holderHeardMillis: Date.now(), ledgerEntries: 0 }),
      ),
    ).toBe('held by w-1');
    expect(holding(reading({ kind: 'source' }))).toBeNull();
    expect(holding(undefined)).toBeNull();
  });

  it('says when its holder has gone quiet, or has not been heard since the leader started', () => {
    const now = 1_000_000;
    const at = (heard?: number) =>
      holding(reading({ kind: 'source', holder: 'w-1', holderHeardMillis: heard }), now);
    expect(at(now - 9_000)).toBe('held by w-1');
    expect(at(now - 40_000)).toBe('held by w-1, last heard 40s ago');
    expect(at(undefined)).toBe('held by w-1, not heard since the leader started');
  });

  it("names each group's holder in a scope every group runs, and only held groups", () => {
    const g1 = reading({
      kind: 'source',
      group: 'g1',
      holder: 'w-1',
      holderHeardMillis: Date.now(),
      ledgerEntries: 2,
    });
    const g2 = reading({ kind: 'source', group: 'g2' });
    const g3 = reading({
      kind: 'source',
      group: 'g3',
      holder: 'w-3',
      holderHeardMillis: Date.now(),
    });
    expect(holdings([g1, g2, g3], true)).toBe('g1: held by w-1 · 2 in ledger; g3: held by w-3');
    expect(holdings([g1], false)).toBe('held by w-1 · 2 in ledger');
    expect(holdings([], true)).toBe('');
  });

  it("is what its card says, where a Kafka source's says its lag", () => {
    const held = reading({
      kind: 'source',
      rate: 5,
      holder: 'w-1',
      holderHeardMillis: Date.now(),
      ledgerEntries: 3,
    });
    expect(cardLine(node('listed', 'source', 'g1', 's3'), held)).toEqual({
      rate: '5 rows/s',
      note: 'held by w-1 · 3 in ledger',
      level: 'dim',
    });
  });
});

describe('unmatched rows', () => {
  const metrics = node('metrics', 'source', 'g3', 'metrics');
  const logs = node('logs', 'source', 'g3', 'logs');
  const read = (n: MapNode) =>
    n === metrics
      ? reading({ kind: 'source', id: 'metrics', rate: 6.6, unmatched: 6.6, lag: 4 })
      : reading({ kind: 'source', id: 'logs', rate: 3, unmatched: 0 });

  it('take the card ahead of lag', () => {
    expect(cardLine(metrics, read(metrics))).toEqual({
      rate: '6.6 rows/s',
      note: '6.6/s unmatched',
      level: 'warn',
    });
    expect(cardLine(logs, read(logs))?.note).toBe('');
  });

  it('need attention, name the group, and answer a term', () => {
    const rows = attention([metrics, logs], read);
    expect(rows.map((a) => a.text)).toEqual(['6.6/s unmatched']);
    expect(groupSummary([metrics, logs], read, rows)).toEqual({
      text: '6.6/s unmatched',
      level: 'warn',
      first: rows[0],
    });
    const term = termFor('unmatched:', termsFor([metrics, logs]));
    expect([metrics, logs].filter((n) => term!.test(n, read(n)))).toEqual([metrics]);
  });
});
