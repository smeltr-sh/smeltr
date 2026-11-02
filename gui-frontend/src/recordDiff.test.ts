import { describe, expect, it } from 'vitest';
import { isInternal } from './api';
import {
  pathLines,
  runMarks,
  changeFitsInline,
  countStates,
  declaredRecord,
  diffRecords,
  diffTree,
  fieldAt,
  leafLines,
  leafMap,
  parseDeclared,
  typeAt,
  pathsOf,
  PAGE_LINES,
  recordLines,
  splitPath,
  withoutRemoved,
  type FieldLeaf,
  type RecordLine,
} from './recordDiff';

const leaf = (path: string, state: FieldLeaf['state']): FieldLeaf => ({ path, state });

/** `label` at `depth`, which is what a reader actually sees. */
function shape(lines: RecordLine[]): string[] {
  return lines.map((l) => `${'  '.repeat(l.depth)}${l.kind === 'more' ? '…' : l.label}`);
}

describe('diffRecords', () => {
  it('classifies every leaf path either side has', () => {
    const fields = diffRecords(
      { keep: 1, drop: 2, move: 'before' },
      { keep: 1, move: 'after', gain: 3 },
    );
    expect(fields).toEqual([
      { path: 'drop', state: 'removed', before: 2 },
      { path: 'gain', state: 'added', after: 3 },
      { path: 'keep', state: 'unchanged', before: 1, after: 1 },
      { path: 'move', state: 'changed', before: 'before', after: 'after' },
    ]);
  });

  it('reaches nested fields by their path rather than comparing objects whole', () => {
    const fields = diffRecords(
      { observer: { ingress: { zone: 'Internet' }, egress: { zone: 'Niprnet' } } },
      { observer: { ingress: { zone: 'Internet-Asym' }, egress: { zone: 'Niprnet' } } },
    );
    expect(fields.map((f) => [f.path, f.state])).toEqual([
      ['observer.egress.zone', 'unchanged'],
      ['observer.ingress.zone', 'changed'],
    ]);
  });

  it('orders by path, so siblings land together', () => {
    const fields = diffRecords(
      {},
      { destination: { port: 443, ip: 'a', nat: { ip: 'b' } }, '@timestamp': 't' },
    );
    expect(fields.map((f) => f.path)).toEqual([
      '@timestamp',
      'destination.ip',
      'destination.nat.ip',
      'destination.port',
    ]);
  });

  it('treats an array as one value', () => {
    expect(diffRecords({ tags: ['a'] }, { tags: ['a', 'b'] })).toEqual([
      { path: 'tags', state: 'changed', before: ['a'], after: ['a', 'b'] },
    ]);
  });

  it('keeps an empty array as its own leaf', () => {
    expect(diffRecords({}, { tags: [] }).map((f) => f.path)).toEqual(['tags']);
  });

  it('keeps an empty object as its own leaf', () => {
    expect(diffRecords({}, { user: {} }).map((f) => f.path)).toEqual(['user']);
  });

  it('a missing record on either side is every field added or removed', () => {
    expect(diffRecords(null, { a: 1 })).toEqual([{ path: 'a', state: 'added', after: 1 }]);
    expect(diffRecords({ a: 1 }, null)).toEqual([{ path: 'a', state: 'removed', before: 1 }]);
  });
});

describe('diffTree', () => {
  it('reads structs and arrays alike, an element at its index', () => {
    const root = diffTree(null, { e: [{ kind: 'K' }, { kind: 'L' }] });
    expect(root.children[0]).toMatchObject({ key: 'e', kind: 'array' });
    expect(root.children[0].children.map((c) => c.path)).toEqual(['e[0]', 'e[1]']);
    expect(root.children[0].children[1].children[0].leaf).toEqual({
      path: 'e[1].kind',
      state: 'added',
      after: 'L',
    });
  });

  it('names the type declared at a path', () => {
    const declared = declaredRecord([
      ['hits', 'array<{host: str, "a.b": i64, ...}>'],
      ['url.path', 'str'],
    ]);
    expect(typeAt(declared, 'url.path')).toBe('str');
    expect(typeAt(declared, 'url')).toBe('object');
    expect(typeAt(declared, 'hits[0]["a.b"]')).toBe('i64');
    expect(typeAt(declared, 'loose')).toBe('undeclared');
    expect(typeAt(null, 'url.path')).toBeNull();
  });

  it('names each value by the type its side declares', () => {
    const declared = declaredRecord([
      ['_raw', 'str'],
      ['hits', 'array<{host: str, "a.b": i64, ...}>'],
      ['tags', 'variant'],
      ['url.path', 'str'],
      ['url.*', 'variant'],
    ]);
    const record = {
      _raw: 'x',
      hits: [{ host: 'h', 'a.b': 1, extra: true }],
      tags: ['a'],
      url: { path: '/', query: 'q' },
      loose: 1,
    };
    const root = diffTree(null, record, undefined, { before: null, after: declared });
    const types = Object.fromEntries(
      recordLines(root, { open: () => true }).map((l) => [
        l.kind === 'more' ? '' : l.path,
        l.kind === 'more' ? '' : l.type,
      ]),
    );
    expect(types).toEqual({
      _raw: 'str',
      hits: 'array<object>',
      'hits[0]': 'object',
      'hits[0]["a.b"]': 'i64',
      'hits[0].extra': 'undeclared',
      'hits[0].host': 'str',
      loose: 'undeclared',
      tags: 'variant',
      'tags[0]': 'undeclared',
      url: 'object',
      'url.path': 'str',
      'url.query': 'undeclared',
    });
  });

  it("takes a removed field's type from the side before, and none from a side that did not compile", () => {
    const before = declaredRecord([['gone', 'datetime']]);
    const root = diffTree({ gone: 't', kept: 1 }, { kept: 1 }, undefined, { before, after: null });
    expect(root.children.map((c) => [c.key, c.type])).toEqual([
      ['gone', 'datetime'],
      ['kept', null],
    ]);
  });

  it('reads back every type the dictionary spells', () => {
    expect(parseDeclared('array<array<{"x y": f64, z: {a: bool, ...}}>>')).toEqual({
      kind: 'array',
      of: {
        kind: 'array',
        of: {
          kind: 'struct',
          open: false,
          fields: new Map<string, unknown>([
            ['x y', { kind: 'scalar', name: 'f64' }],
            [
              'z',
              {
                kind: 'struct',
                open: true,
                fields: new Map([['a', { kind: 'scalar', name: 'bool' }]]),
              },
            ],
          ]),
        },
      },
    });
  });

  it('draws a value whose shape the step changed whole', () => {
    const root = diffTree({ tags: 'a' }, { tags: ['a'] });
    expect(root.children[0]).toMatchObject({ kind: 'leaf' });
    expect(root.children[0].leaf?.state).toBe('changed');
  });

  it('adds up what changed beneath each part', () => {
    const root = diffTree({ a: { b: 1, c: 2 } }, { a: { b: 1, c: 3, d: 4 } });
    expect(root.children[0].marks).toEqual({ added: 1, changed: 1, removed: 0 });
  });

  it('drops a field it is told not to keep, and a struct left empty', () => {
    const root = diffTree(null, { __source: { a: 1 }, x: 1 }, (p) => !isInternal(p));
    expect(root.children.map((c) => c.key)).toEqual(['x']);
  });
});

describe('recordLines', () => {
  it('draws a struct holding one field as a longer name', () => {
    const lines = recordLines(diffTree(null, { destination: { ip: 'a', nat: { ip: 'b' } } }));
    expect(shape(lines)).toEqual(['destination', '  ip', '  nat.ip']);
    expect(shape(recordLines(diffTree(null, { a: { b: { c: 1 } } })))).toEqual(['a.b.c']);
  });

  it('leaves out what holds no change, when asked, and keeps the tree', () => {
    const tree = diffTree(
      { source: { ip: 'a', port: 1 }, host: 'h' },
      { source: { ip: 'a', port: 2, geo: { city: 'c', country: 'SE' } }, host: 'h' },
    );
    expect(shape(recordLines(tree, {}, true))).toEqual([
      'source',
      '  geo',
      '    city',
      '    country',
      '  port',
    ]);
  });

  it('joins a key in brackets without a dot', () => {
    const lines = recordLines(diffTree(null, { labels: { 'a.b': 1 } }));
    expect(shape(lines)).toEqual(['labels["a.b"]']);
  });

  it('starts a top-level key in brackets at the record', () => {
    const after = { 'x.c': 1, a: { 'y.d': 2, e: 3 } };
    expect(shape(recordLines(diffTree(null, after)))).toEqual([
      'a',
      '  e',
      '  ["y.d"]',
      '$["x.c"]',
    ]);
    expect(pathLines(diffTree(null, after)).map((l) => l.kind === 'field' && l.label)).toEqual([
      'a.e',
      'a["y.d"]',
      '$["x.c"]',
    ]);
  });

  it("carries each field's type and state onto its line", () => {
    const declared = declaredRecord([
      ['a.b', 'i64'],
      ['a.c', 'str'],
    ]);
    const lines = recordLines(
      diffTree({ a: { b: 1, c: 'x' } }, { a: { b: 2 } }, undefined, {
        before: declared,
        after: declared,
      }),
    );
    expect(lines.filter((l) => l.kind === 'field').map((l) => [l.label, l.type, l.state])).toEqual([
      ['b', 'i64', 'changed'],
      ['c', 'str', 'removed'],
    ]);
  });

  it('opens what fits in the page and folds what does not', () => {
    const big = Object.fromEntries(Array.from({ length: 200 }, (_, k) => [`l${k}`, k]));
    const lines = recordLines(diffTree(null, { labels: big, user: { name: 'a', id: 1 } }));
    expect(shape(lines)).toEqual(['labels', 'user', '  id', '  name']);
    expect(lines[0]).toMatchObject({ kind: 'group', open: false, size: 200, branch: 'struct' });
    expect(lines[0].kind === 'group' && lines[0].marks).toEqual({
      added: 200,
      changed: 0,
      removed: 0,
    });
  });

  it('draws an opened array a page at a time, however long or deep', () => {
    const deep = (n: number): unknown =>
      n === 0 ? 1 : Array.from({ length: 50 }, () => deep(n - 1));
    const tree = diffTree(null, { e: deep(3) });
    const lines = recordLines(tree, { open: (p) => ['e', 'e[0]', 'e[0][0]'].includes(p) });
    // Each opened level draws one page, then a line for the rest.
    expect(lines.length).toBeLessThan(4 * PAGE_LINES);
    expect(lines.at(-1)).toEqual({ kind: 'more', path: 'e', depth: 1, shown: 40, total: 50 });
  });

  it('draws a page more for each one asked for', () => {
    const tags = Array.from({ length: 100 }, (_, i) => i);
    const tree = diffTree(null, { tags });
    const lines = recordLines(tree, { open: () => true, pages: (p) => (p === 'tags' ? 1 : 0) });
    expect(lines.at(-1)).toMatchObject({ kind: 'more', shown: 2 * PAGE_LINES, total: 100 });
  });

  it('draws one child of a page however long it is, and pages its fields', () => {
    const huge = Object.fromEntries(Array.from({ length: 100 }, (_, k) => [`f${k}`, k]));
    const tree = diffTree(null, { e: [huge, huge] });
    const lines = recordLines(tree, { open: (p) => p === 'e' || p === 'e[0]' });
    expect(lines.filter((l) => l.kind === 'field')).toHaveLength(PAGE_LINES);
    expect(lines.filter((l) => l.kind === 'more')).toEqual([
      { kind: 'more', path: 'e[0]', depth: 2, shown: 40, total: 100 },
    ]);
    // Its sibling, too big for what is left, starts folded.
    expect(lines.at(-1)).toMatchObject({ kind: 'group', path: 'e[1]', open: false });
  });

  it('pages the top level of a wide record', () => {
    const wide = Object.fromEntries(Array.from({ length: 200 }, (_, k) => [`f${k}`, k]));
    const lines = recordLines(diffTree(null, wide));
    expect(lines).toHaveLength(PAGE_LINES + 1);
    expect(lines.at(-1)).toEqual({ kind: 'more', path: '', depth: 0, shown: 40, total: 200 });
  });

  it('keeps a page for the siblings of what the reader opened', () => {
    const tags = Array.from({ length: 100 }, (_, i) => i);
    const lines = recordLines(diffTree(null, { a: tags, b: 1 }), {
      open: (p) => (p === 'a' ? true : undefined),
    });
    expect(lines.at(-1)).toMatchObject({ kind: 'field', label: 'b' });
  });

  it('keeps what the reader folded folded', () => {
    const lines = recordLines(diffTree(null, { user: { a: 1, b: 2 } }), {
      open: (p) => (p === 'user' ? false : undefined),
    });
    expect(shape(lines)).toEqual(['user']);
  });
});

describe('pathLines', () => {
  it('lists every changed field by its whole path, folded or not', () => {
    const lines = pathLines(
      diffTree({ a: { b: 1, c: [1, 2] } }, { a: { b: 1, c: [1, 3] } }),
      {},
      true,
    );
    expect(lines.map((l) => l.kind === 'field' && [l.label, l.state])).toEqual([
      ['a.c[1]', 'changed'],
    ]);
  });

  it('lists every field by its whole path, changed or not', () => {
    const lines = pathLines(diffTree({ a: { b: 1, c: [1, 2] } }, { a: { b: 1, c: [1, 3] } }));
    expect(lines.map((l) => l.kind === 'field' && l.label)).toEqual(['a.b', 'a.c[0]', 'a.c[1]']);
  });

  it('draws a page at a time', () => {
    const tags = Array.from({ length: 100 }, (_, i) => i);
    const lines = pathLines(diffTree(null, { tags }), {}, true);
    expect(lines.at(-1)).toEqual({ kind: 'more', path: '', depth: 0, shown: 40, total: 100 });
  });
});

describe('changeFitsInline', () => {
  it('fits a short rename and refuses a whole log line', () => {
    expect(changeFitsInline('start', 'flow_started')).toBe(true);
    expect(changeFitsInline('Internet', 'Internet-Asym')).toBe(true);
    expect(
      changeFitsInline(
        '<14>Nov 4 00:47:17 fw-PNT-02 2026/11/04 00:47:17,001801003286,TRAFFIC',
        '<14>Nov 4 00:47:17 fw-PNT-02 2026/11/04 00:47:17,REDACTED,TRAFFIC',
      ),
    ).toBe(false);
  });

  it('measures a non-string by the text it draws as', () => {
    expect(changeFitsInline(1, 2)).toBe(true);
    expect(changeFitsInline({ a: 'x'.repeat(40) }, 1)).toBe(false);
  });
});

describe('fieldAt', () => {
  it('reads one field from each side, and says nothing where neither has it', () => {
    const before = leafMap({ a: { b: 1 }, c: 'x', d: [1] });
    const after = leafMap({ a: { b: 2 }, d: [1], e: true });
    expect(fieldAt(before, after, 'a.b')).toEqual({
      path: 'a.b',
      state: 'changed',
      before: 1,
      after: 2,
    });
    expect(fieldAt(before, after, 'c')).toEqual({ path: 'c', state: 'removed', before: 'x' });
    expect(fieldAt(before, after, 'd')?.state).toBe('unchanged');
    expect(fieldAt(before, after, 'e')).toEqual({ path: 'e', state: 'added', after: true });
    expect(fieldAt(before, after, 'missing')).toBeUndefined();
  });
});

describe('pathsOf', () => {
  it('is every path any record has, alphabetically', () => {
    expect(
      pathsOf([
        leafMap({ network: { bytes: 1 }, '@timestamp': 2 }),
        leafMap({ destination: { ip: 3 } }),
      ]),
    ).toEqual(['@timestamp', 'destination.ip', 'network.bytes']);
    expect(pathsOf([])).toEqual([]);
  });
});

describe('withoutRemoved', () => {
  it('lifts what a step removed out of its structs, a removed struct whole', () => {
    const { tree, removed } = withoutRemoved(
      diffTree(
        { _raw: 'x', srcip: '1', old: { a: 1, b: 2 }, keep: { gone: 1, here: 2 }, list: [1, 2] },
        { source: { ip: '1' }, keep: { here: 2 }, list: [1] },
      ),
    );
    expect(removed.map((n) => n.path)).toEqual(['_raw', 'keep.gone', 'old', 'srcip']);
    expect(leafLines(removed).map((l) => l.kind === 'field' && l.label)).toEqual([
      '_raw',
      'keep.gone',
      'old.a',
      'old.b',
      'srcip',
    ]);
    // An array keeps the element it lost, where it was.
    expect(leafLines([tree]).map((l) => l.kind === 'field' && `${l.label} ${l.state}`)).toEqual([
      'keep.here unchanged',
      'list[0] unchanged',
      'list[1] removed',
      'source.ip added',
    ]);
    expect(tree.marks).toEqual({ added: 1, changed: 0, removed: 1 });
  });
});

describe('splitPath', () => {
  it('separates the nesting from the field a reader is looking for', () => {
    expect(splitPath('observer.ingress.zone')).toEqual({
      prefix: 'observer.ingress.',
      leaf: 'zone',
    });
    expect(splitPath('_raw')).toEqual({ prefix: '', leaf: '_raw' });
  });

  it('takes a key in brackets whole, dots and all, as the leaf', () => {
    expect(splitPath('kubernetes.labels["app.kubernetes.io/name"]')).toEqual({
      prefix: 'kubernetes.labels',
      leaf: '["app.kubernetes.io/name"]',
    });
    expect(splitPath('["say \\"hi\\""]')).toEqual({ prefix: '', leaf: '["say \\"hi\\""]' });
    expect(splitPath('["a.b"].c')).toEqual({ prefix: '["a.b"].', leaf: 'c' });
  });
});

describe('countStates', () => {
  it('counts what a step did, ignoring what it left alone', () => {
    expect(
      countStates([
        leaf('a', 'added'),
        leaf('b', 'changed'),
        leaf('c', 'changed'),
        leaf('d', 'removed'),
        leaf('e', 'unchanged'),
      ]),
    ).toEqual({ added: 1, changed: 2, removed: 1 });
  });
});

describe('runMarks', () => {
  const marks = (before: unknown, after: unknown, steps: Record<string, string> = {}) => {
    const lines = recordLines(diffTree(before, after));
    return runMarks(
      lines,
      (p) => steps[p] ?? null,
      (a, b) => a === b,
    ).map((m, i) => {
      const l = lines[i];
      const name = l.kind === 'more' ? 'more' : l.label;
      return `${name} ${m.state}${m.start ? ' start' : ''} @${m.depth}${m.sign ? ` sign:${m.step}` : ''}`;
    });
  };

  it('runs a new struct as one bar, signed once, from its own depth', () => {
    expect(
      marks(
        { a: 1 },
        { a: 1, http: { method: 'GET', status: 200 } },
        {
          'http.method': 'p',
          'http.status': 'p',
        },
      ),
    ).toEqual([
      'a unchanged @0',
      'http added start @0 sign:p',
      'method added @0',
      'status added @0',
    ]);
  });

  it('signs again where the step changes within a run', () => {
    expect(marks({}, { a: 1, b: 2, c: 3 }, { a: 'p', b: 'e', c: 'e' })).toEqual([
      'a added start @0 sign:p',
      'b added @0 sign:e',
      'c added @0',
    ]);
  });

  it('runs a folded struct as changed when what is in it differs, with no one step', () => {
    const lines = recordLines(diffTree({ s: { a: 1 } }, { s: { a: 2, b: 1 } }), {
      open: () => false,
    });
    const steps: Record<string, string> = { 's.a': 'e', 's.b': 'p' };
    expect(
      runMarks(
        lines,
        (p) => steps[p] ?? null,
        (a, b) => a === b,
      ),
    ).toEqual([{ state: 'changed', step: null, start: true, depth: 0, sign: true }]);
  });

  it('starts a run inside an unchanged struct at the depth of the change', () => {
    expect(marks({ s: { ip: 1, port: 1 } }, { s: { ip: 1, port: 2 } }, { 's.port': 'e' })).toEqual([
      's unchanged @0',
      'ip unchanged @1',
      'port changed start @1 sign:e',
    ]);
  });
});
