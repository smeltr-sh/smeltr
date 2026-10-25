import { describe, expect, it } from 'vitest';
import type { FileConfigDto, TestResult, TestSummary } from './api';
import {
  canonicalJson,
  compareRows,
  compareSteps,
  csvRowChanges,
  emptyConfig,
  fileOf,
  isSectionChange,
  jsonDiff,
  objectChanges,
  pipelineSlug,
  scopeOf,
  stepLine,
  testsCovering,
  undoTarget,
  uploadedName,
  type ObjectChange,
} from './objectChanges';

const parse = {
  type: 'parser',
  id: 'p1',
  filter: null,
  disabled: false,
  input: '_raw',
  into: '$',
  kind: { type: 'json' },
  declared: [],
};
const lookup = {
  type: 'lookup',
  id: 'l1',
  filter: null,
  disabled: false,
  input: 'panos.app',
  table: 'app-risk',
  matchKind: 'exact',
  multiplicity: 'best',
  keyColumn: 'app',
  outputs: [{ source: 'risk', field: 'risk' }],
};
const code = {
  type: 'code',
  id: 'c1',
  filter: null,
  disabled: false,
  body: 'if risk >= 5 {\n  event.risk = "high"\n}',
};
const rename = {
  type: 'rename',
  id: 'r1',
  filter: null,
  disabled: false,
  pairs: [{ from: 'a', to: 'b' }],
};

function config(over: Partial<FileConfigDto>): FileConfigDto {
  return { ...emptyConfig(), ...over };
}

describe('stepLine', () => {
  it('names a lookup by its table, input and outputs', () => {
    expect(stepLine(lookup)).toBe('app-risk  panos.app → risk');
  });
  it('gives a code step its expression on one line', () => {
    expect(stepLine(code)).toBe('if risk >= 5 { event.risk = "high" }');
  });
  it('falls back to the editor summary', () => {
    expect(stepLine(rename)).toBe('a → b');
  });
});

describe('compareSteps', () => {
  it('lists every step in order, a removed one where it stood', () => {
    const changed = { ...rename, pairs: [{ from: 'a', to: 'c' }] };
    const steps = compareSteps([parse, code, rename], [parse, lookup, changed]);
    expect(steps.map((s) => [s.status, s.type, s.position])).toEqual([
      ['same', 'parser', 1],
      ['added', 'lookup', 2],
      ['removed', 'code', undefined],
      ['changed', 'rename', 3],
    ]);
  });

  it('compares an if step by its condition, and its branches step by step', () => {
    const before = { type: 'if', id: 'i1', condition: 'x', then: [code], else: [] };
    const after = { type: 'if', id: 'i1', condition: 'x', then: [code, lookup], else: [] };
    const [step] = compareSteps([before], [after]);
    expect(step.status).toBe('same');
    expect(step.then?.map((s) => s.status)).toEqual(['same', 'added']);
    expect(step.else).toEqual([]);
  });

  it('marks a step that runs in another order as moved', () => {
    const steps = compareSteps([parse, code, rename], [parse, rename, code]);
    expect(steps.map((s) => [s.status, s.type, s.moved])).toEqual([
      ['same', 'parser', undefined],
      ['same', 'rename', undefined],
      ['changed', 'code', true],
    ]);
  });
});

describe('compareRows', () => {
  it('marks rows added and removed, counting repeats', () => {
    const rows = compareRows(
      [
        ['tor', '5'],
        ['ssh', '1'],
      ],
      [
        ['tor', '5'],
        ['tor', '5'],
      ],
    );
    expect(rows.map((r) => [r.status, r.cells.join(',')])).toEqual([
      ['same', 'tor,5'],
      ['added', 'tor,5'],
      ['removed', 'ssh,1'],
    ]);
  });
});

describe('objectChanges', () => {
  it('finds added, removed and changed objects, labelled by name', () => {
    const before = config({
      pipelines: [{ id: 'x7f3', name: 'panos-traffic', steps: [parse] }],
      sources: [{ id: 'old', type: 'kafka' } as never],
    });
    const after = config({
      pipelines: [{ id: 'x7f3', name: 'panos-traffic', steps: [parse, lookup] }],
      lookupTables: [
        { id: 'app-risk', type: 'inline', headers: ['app', 'risk'], rows: [['tor', '5']] },
      ],
    });
    const changes = objectChanges([{ scope: 'network', before, after }]);
    expect(changes.map((c) => [c.section, c.status, c.label])).toEqual([
      ['sources', 'removed', 'old'],
      ['lookupTables', 'added', 'app-risk'],
      ['pipelines', 'changed', 'panos-traffic'],
    ]);
    const table = changes[1];
    expect(table.headers).toEqual(['app', 'risk']);
    expect(table.rows).toEqual([{ status: 'added', cells: ['tor', '5'] }]);
    expect(table.fields.map((f) => f.key)).toEqual(['type', 'headers']);
    expect(changes[2].fields).toEqual([]);
    expect(changes[2].steps?.map((s) => s.status)).toEqual(['same', 'added']);
  });

  it('leaves out an object whose only difference is nothing', () => {
    const side = config({ pipelines: [{ id: 'a', name: 'a', steps: [parse] }] });
    expect(objectChanges([{ scope: 'shared', before: side, after: side }])).toEqual([]);
  });

  it('names changed fields of an ordinary object', () => {
    const before = config({ routes: [{ id: 'r', source: 's', condition: null, pipeline: 'p' }] });
    const after = config({ routes: [{ id: 'r', source: 's', condition: 'x', pipeline: 'p' }] });
    const [change] = objectChanges([{ scope: 'shared', before, after }]);
    expect(change.fields).toEqual([
      { key: 'condition', kind: 'changed', before: null, after: 'x' },
    ]);
  });

  it('finds a route that only moved, since routes are tried in order', () => {
    const route = (id: string) => ({ id, source: 's', condition: null, pipeline: 'p' });
    const before = config({ routes: [route('r1'), route('r2'), route('r3')] });
    const after = config({ routes: [route('r2'), route('r3'), route('r1')] });
    const changes = objectChanges([{ scope: 'shared', before, after }]);
    expect(changes.map((c) => [c.id, c.status, c.moved])).toEqual([['r1', 'changed', true]]);
  });

  it('opens a group the change creates with its own card, named from its manifest', () => {
    const route = { id: 'r', source: 's', condition: null, pipeline: 'p' };
    const changes = objectChanges([
      {
        scope: 'north',
        before: config({}),
        after: config({ name: 'North', routes: [route] }),
        manifest: 'A',
      },
    ]);
    expect(changes.map((c) => [c.section, c.noun, c.status, c.label])).toEqual([
      ['manifest', 'worker group', 'added', 'North'],
      ['routes', 'route', 'added', 'r'],
    ]);
    expect(changes[0].fields.map((f) => [f.key, f.after])).toEqual([
      ['name', 'North'],
      ['version', 1],
    ]);
  });

  it('names what changed in a manifest, and still shows one whose fields read the same', () => {
    const before = config({ name: 'North' });
    const renamed = objectChanges([
      { scope: 'north', before, after: config({ name: 'Northern' }), manifest: 'M' },
    ]);
    expect(renamed.map((c) => [c.status, c.label])).toEqual([['changed', 'Northern']]);
    expect(renamed[0].fields).toEqual([
      { key: 'name', kind: 'changed', before: 'North', after: 'Northern' },
    ]);
    const repaired = objectChanges([{ scope: 'north', before, after: before, manifest: 'M' }]);
    expect(repaired.map((c) => [c.status, c.fields])).toEqual([['changed', []]]);
  });

  it('says a group the change deletes is removed, by the name it had', () => {
    const [group] = objectChanges([
      { scope: 'north', before: config({ name: 'North' }), after: config({}), manifest: 'D' },
    ]);
    expect([group.status, group.label, group.after]).toEqual(['removed', 'North', undefined]);
  });

  it('gives shared no group card, since it has no manifest', () => {
    const side = config({});
    expect(objectChanges([{ scope: 'shared', before: side, after: side, manifest: 'M' }])).toEqual(
      [],
    );
  });
});

describe('sample and test cards', () => {
  const side = config({});
  const sample = (name: string, raws: string[]) => ({
    id: 'orders',
    name,
    source: 'kafka',
    records: raws.map((raw) => ({ _raw: raw })),
  });
  const test = (expected: unknown[]) => ({
    id: 'orders-out',
    name: 'Orders out',
    pipeline: 'p',
    sample: 'orders',
    lockedAt: 1,
    rowsIn: 2,
    expected,
  });

  it('reads a sample or test from its path within a bundle, and nothing else', () => {
    expect(fileOf('samples/orders.json')).toEqual({ kind: 'samples', id: 'orders' });
    expect(fileOf('tests/orders-out.json')).toEqual({ kind: 'tests', id: 'orders-out' });
    expect(fileOf('pipelines.json')).toBeNull();
    expect(fileOf('samples/nested/x.json')).toBeNull();
  });

  it('shows a changed sample by its fields and the records it added and removed, after the sections', () => {
    const route = { id: 'r', source: 's', condition: null, pipeline: 'p' };
    const changes = objectChanges([
      {
        scope: 'shared',
        before: side,
        after: config({ routes: [route] }),
        files: [
          {
            kind: 'samples',
            id: 'orders',
            status: 'M',
            before: sample('Orders', ['a', 'b']),
            after: sample('Orders EU', ['a', 'c']),
          },
        ],
      },
    ]);
    expect(changes.map((c) => [c.section, c.noun, c.status, c.label])).toEqual([
      ['routes', 'route', 'added', 'r'],
      ['samples', 'sample', 'changed', 'Orders EU'],
    ]);
    const [, card] = changes;
    expect(card.fields).toEqual([
      { key: 'name', kind: 'changed', before: 'Orders', after: 'Orders EU' },
    ]);
    expect(card.headers).toEqual(['record']);
    expect(card.rows).toEqual([
      { status: 'same', cells: ['a'] },
      { status: 'added', cells: ['c'] },
      { status: 'removed', cells: ['b'] },
    ]);
  });

  it('shows a re-locked test by what it now expects, a dropped row as dropped', () => {
    const [card] = objectChanges([
      {
        scope: 'north',
        before: side,
        after: side,
        files: [
          {
            kind: 'tests',
            id: 'orders-out',
            status: 'M',
            before: test([{ a: 1 }, null]),
            after: test([{ a: 2 }, null]),
          },
        ],
      },
    ]);
    expect([card.noun, card.label, card.fields]).toEqual(['test', 'Orders out', []]);
    expect(card.rows).toEqual([
      { status: 'added', cells: ['{"a":2}'] },
      { status: 'same', cells: ['dropped'] },
      { status: 'removed', cells: ['{"a":1}'] },
    ]);
  });

  it('keeps the card of a file that does not read, named by its id, as git says it changed', () => {
    const [card] = objectChanges([
      {
        scope: 'shared',
        before: side,
        after: side,
        files: [{ kind: 'tests', id: 'broken', status: 'A' }],
      },
    ]);
    expect([card.status, card.label, card.fields, card.rows]).toEqual(['added', 'broken', [], []]);
    expect(isSectionChange(card)).toBe(false);
  });
});

describe('lookup file cards', () => {
  const csv = (path: string) => ({
    id: 'geo',
    type: 'csv' as const,
    path,
    delimiter: ',',
    headers: ['ip'],
  });

  it('gives a file its own card only where no table card names it', () => {
    const changes = objectChanges([
      {
        scope: 'shared',
        before: config({ lookupTables: [csv('lookups/0123456789abcdef-geo.csv')] }),
        after: config({ lookupTables: [csv('lookups/fedcba9876543210-geo.csv')] }),
        lookups: new Map([
          ['lookups/0123456789abcdef-geo.csv', 'D'],
          ['lookups/fedcba9876543210-geo.csv', 'A'],
          ['lookups/aaaaaaaaaaaaaaaa-hosts.csv', 'A'],
        ]),
      },
    ]);
    expect(changes.map((c) => [c.section, c.noun, c.status, c.label])).toEqual([
      ['lookupTables', 'lookup table', 'changed', 'geo'],
      ['lookups', 'lookup file', 'added', 'hosts.csv'],
    ]);
  });

  it('cards a file whose bytes changed under a path its table keeps', () => {
    const side = config({ lookupTables: [csv('lookups/geo.csv')] });
    const [card] = objectChanges([
      { scope: 'shared', before: side, after: side, lookups: new Map([['lookups/geo.csv', 'M']]) },
    ]);
    expect([card.noun, card.status, card.id]).toEqual([
      'lookup file',
      'changed',
      'lookups/geo.csv',
    ]);
  });

  it('keeps the card of bytes changed in place even where the table changed too', () => {
    const was = { ...csv('lookups/geo.csv'), delimiter: ',' };
    const now = { ...csv('lookups/geo.csv'), delimiter: ';' };
    const changes = objectChanges([
      {
        scope: 'shared',
        before: config({ lookupTables: [was] }),
        after: config({ lookupTables: [now] }),
        lookups: new Map([['lookups/geo.csv', 'M']]),
      },
    ]);
    expect(changes.map((c) => c.noun)).toEqual(['lookup table', 'lookup file']);
    expect(changes[1].delimiters).toEqual({ before: ',', after: ';' });
  });

  it('names an upload as its uploader did', () => {
    expect(uploadedName('lookups/0123456789abcdef-geo.csv')).toBe('geo.csv');
    expect(uploadedName('lookups/geo.csv')).toBe('geo.csv');
  });

  it('compares a CSV file by rows under the newer header', () => {
    const { headers, rows } = csvRowChanges(
      [
        ['ip', 'host'],
        ['10.0.0.1', 'a'],
        ['10.0.0.2', 'b'],
      ],
      [
        ['ip', 'host'],
        ['10.0.0.1', 'a'],
        ['10.0.0.3', 'c'],
      ],
    );
    expect(headers).toEqual(['ip', 'host']);
    expect(rows.map((r) => [r.status, r.cells[0]])).toEqual([
      ['same', '10.0.0.1'],
      ['added', '10.0.0.3'],
      ['removed', '10.0.0.2'],
    ]);
  });
});

describe('files that do not read', () => {
  const src = (id: string) =>
    ({ id, type: 'kafka' }) as unknown as FileConfigDto['sources'][number];

  it('names the file that broke instead of every object in it as removed', () => {
    const changes = objectChanges([
      {
        scope: 'shared',
        before: config({ sources: [src('a'), src('b')] }),
        after: config({}),
        unreadable: {
          before: new Map(),
          after: new Map([['sources.json', 'invalid JSON: EOF']]),
        },
      },
    ]);
    expect(changes.map((c) => [c.section, c.noun, c.label])).toEqual([
      ['unreadable', 'file', 'sources.json'],
    ]);
    expect(changes[0].fields).toEqual([
      { key: 'does not read', kind: 'added', after: 'invalid JSON: EOF' },
    ]);
  });

  it('says a broken pipeline file reads again, without calling its pipeline added', () => {
    const changes = objectChanges([
      {
        scope: 'north',
        before: config({}),
        after: config({ pipelines: [{ id: 'geoip', name: 'GeoIP', steps: [] }] }),
        unreadable: { before: new Map([['pipelines/geoip.json', 'bad']]), after: new Map() },
      },
    ]);
    expect(changes.map((c) => [c.label, c.fields[0].kind])).toEqual([
      ['pipelines/geoip.json', 'removed'],
    ]);
  });

  it('finds a pipeline by the file the leader names it by', () => {
    expect(pipelineSlug('Orders West!')).toBe('orders-west');
    expect(pipelineSlug('--Ünï--')).toBe('n');
    expect(pipelineSlug('***')).toBe('pipeline');
    expect(pipelineSlug('\u212Aelvin')).toBe('elvin');
    const changes = objectChanges([
      {
        scope: 'shared',
        before: config({ pipelines: [{ id: 'Orders West', name: 'Orders West', steps: [] }] }),
        after: config({}),
        unreadable: { before: new Map(), after: new Map([['pipelines/orders-west.json', 'bad']]) },
      },
    ]);
    expect(changes.map((c) => c.noun)).toEqual(['file']);
  });

  it('gives a group whose manifest broke only the file card', () => {
    const changes = objectChanges([
      {
        scope: 'north',
        before: config({ name: 'North' }),
        after: config({}),
        manifest: 'M',
        unreadable: { before: new Map(), after: new Map([['system.json', 'invalid JSON']]) },
      },
    ]);
    expect(changes.map((c) => [c.noun, c.label])).toEqual([['file', 'system.json']]);
  });

  it('leaves a file broken the same way at both ends, and samples and tests, to other cards', () => {
    const same = new Map([
      ['routes.json', 'bad'],
      ['samples/x.json', 'bad'],
    ]);
    expect(
      objectChanges([
        {
          scope: 'shared',
          before: config({}),
          after: config({}),
          unreadable: {
            before: same,
            after: new Map([
              ['routes.json', 'bad'],
              ['samples/x.json', 'worse'],
            ]),
          },
        },
      ]).map((c) => c.label),
    ).toEqual([]);
  });
});

describe('scopeOf', () => {
  it('reads a group from its path, and shared otherwise', () => {
    expect(scopeOf('groups/network/pipelines/a.json')).toBe('network');
    expect(scopeOf('sources.json')).toBe('shared');
  });
});

describe('canonicalJson', () => {
  it('ignores key order', () => {
    expect(canonicalJson({ b: 1, a: { d: 2, c: 3 } })).toBe(
      canonicalJson({ a: { c: 3, d: 2 }, b: 1 }),
    );
  });
});

describe('testsCovering', () => {
  it('matches by the summary, falling back to the result', () => {
    const results: TestResult[] = [
      { id: 't1', name: 't1', scope: 'net', pipeline: 'p' },
      { id: 't2', name: 't2', scope: 'net' },
      { id: 't3', name: 't3', scope: 'other', pipeline: 'p' },
    ];
    const summaries = new Map<string, TestSummary>([
      [
        'net t2',
        {
          id: 't2',
          name: 't2',
          scope: 'net',
          sample: 's',
          rowsIn: 1,
          rowsOut: 1,
          pipelines: ['p'],
        },
      ],
    ]);
    expect(testsCovering(results, summaries, 'net', 'p').map((t) => t.id)).toEqual(['t1', 't2']);
  });
});

describe('jsonDiff', () => {
  it('keeps common lines and marks the rest', () => {
    expect(jsonDiff({ a: 1, b: 2 }, { a: 1, b: 3 })).toEqual([
      { kind: 'same', text: '{' },
      { kind: 'same', text: '  "a": 1,' },
      { kind: 'removed', text: '  "b": 2' },
      { kind: 'added', text: '  "b": 3' },
      { kind: 'same', text: '}' },
    ]);
  });
  it('shows an added object whole', () => {
    expect(jsonDiff(undefined, 1)).toEqual([{ kind: 'added', text: '1' }]);
  });
});

describe('a source deploying moves', () => {
  const kafka = (start?: unknown) =>
    ({ id: 'orders', type: 'kafka', start }) as unknown as FileConfigDto['sources'][number];
  const moves = (before: unknown, after: unknown) =>
    objectChanges([
      {
        scope: 'shared',
        before: config({ sources: [kafka(before)] }),
        after: config({ sources: [kafka(after)] }),
      },
    ])[0]?.movesSource;

  it('is a Kafka source whose start changed', () => {
    expect(moves({ type: 'earliest' }, { type: 'timestamp', millis: 1 })).toBe(true);
    expect(moves({ type: 'timestamp', millis: 1 }, { type: 'timestamp', millis: 2 })).toBe(true);
  });

  it('is not one that names the start it already had', () => {
    expect(moves(undefined, { type: 'earliest' })).toBeUndefined();
  });
});

describe('undoTarget', () => {
  const card = (section: ObjectChange['section'], id: string): ObjectChange => ({
    scope: 'shared',
    section,
    noun: 'file',
    id,
    label: id,
    status: 'changed',
    fields: [],
  });
  it('puts back an object by its section and id', () => {
    expect(undoTarget(card('routes', 'r1'))).toEqual({ section: 'routes', id: 'r1' });
  });
  it('puts back a sample, a test, a lookup file or a file that does not read by its path', () => {
    expect(undoTarget(card('samples', 's'))).toEqual({ path: 'samples/s.json' });
    expect(undoTarget(card('tests', 't'))).toEqual({ path: 'tests/t.json' });
    expect(undoTarget(card('lookups', 'lookups/geo.csv'))).toEqual({ path: 'lookups/geo.csv' });
    expect(undoTarget(card('unreadable', 'routes.json'))).toEqual({ path: 'routes.json' });
  });
  it("leaves a group's own card read only", () => {
    expect(undoTarget(card('manifest', 'west'))).toBeNull();
  });
});
