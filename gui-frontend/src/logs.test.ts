import { describe, expect, it } from 'vitest';
import {
  fields,
  isFiltered,
  NO_FILTERS,
  resetCursor,
  toHash,
  toQuery,
  toggleLevel,
  wasTruncated,
} from './logs';
import { parseRoute } from './nav';

describe('toHash', () => {
  it('is the bare page when nothing is narrowed', () => {
    expect(toHash(NO_FILTERS)).toBe('#/status/logs');
  });

  it('carries every filter, repeating levels', () => {
    const hash = toHash({
      levels: ['error', 'warn'],
      worker: 'w-1',
      site: 'worker.',
      q: 'refused',
    });
    // One key, comma separated: the leader's decoder refuses a repeated one.
    expect(hash).toContain('level=error%2Cwarn');
    expect(hash).toContain('worker=w-1');
    expect(hash).toContain('site=worker.');
    expect(hash).toContain('q=refused');
  });

  /** The whole point of putting filters in the hash: the link is the view. */
  it('round trips through parseRoute', () => {
    const filters = { levels: ['error'], worker: 'w-1', group: 'west', site: 'io.', q: '100%' };
    const route = parseRoute(toHash(filters));
    expect(route).toEqual({ tab: 'status', page: 'logs', ...filters });
  });

  it('round trips an empty filter to the same empty filter', () => {
    expect(parseRoute(toHash(NO_FILTERS))).toEqual({
      tab: 'status',
      page: 'logs',
      levels: [],
      worker: undefined,
      group: undefined,
      site: undefined,
      q: undefined,
    });
  });
});

describe('toQuery', () => {
  it('adds the page size the hash leaves out', () => {
    const query = toQuery({ levels: ['error', 'warn'] }, 50);
    expect(query).toContain('level=error%2Cwarn');
    expect(query).toContain('limit=50');
    expect(toHash({ levels: ['error'] })).not.toContain('limit');
  });

  it('carries a page cursor the same way as any other filter', () => {
    expect(toQuery({ levels: [], before: 42 }, 50)).toContain('before=42');
    expect(toQuery({ levels: [], after: 7 }, 50)).toContain('after=7');
    expect(toHash({ levels: [], before: 42 })).toBe('#/status/logs?before=42');
  });

  /** The whole point of putting the cursor in the hash, same as a filter: it round trips. */
  it('a cursor round trips through parseRoute', () => {
    const filters = { levels: ['error'], before: 42 };
    expect(parseRoute(toHash(filters))).toEqual({ tab: 'status', page: 'logs', ...filters });
  });

  it('an unparsable cursor is dropped rather than crashing the route', () => {
    const route = parseRoute('#/status/logs?before=not-a-number');
    expect(route.page === 'logs' && route.before).toBeUndefined();
  });
});

describe('resetCursor', () => {
  it('drops the page position and keeps everything else', () => {
    const filters = { levels: ['warn'], worker: 'w-1', before: 42, after: 7 };
    expect(resetCursor(filters)).toEqual({ levels: ['warn'], worker: 'w-1' });
  });
});

describe('toggleLevel', () => {
  it('adds, removes, and keeps one order however they were picked', () => {
    expect(toggleLevel([], 'warn')).toEqual(['warn']);
    expect(toggleLevel(['warn'], 'warn')).toEqual([]);
    // Picked least-severe first, still ordered most-severe first, so the
    // same set is always the same hash.
    expect(toggleLevel(['warn'], 'error')).toEqual(['error', 'warn']);
    expect(toggleLevel(['error'], 'warn')).toEqual(['error', 'warn']);
  });
});

describe('isFiltered', () => {
  it('is false only when nothing is narrowed', () => {
    expect(isFiltered(NO_FILTERS)).toBe(false);
    expect(isFiltered({ levels: [], q: '' })).toBe(false);
    expect(isFiltered({ levels: ['info'] })).toBe(true);
    expect(isFiltered({ levels: [], worker: 'w-1' })).toBe(true);
  });
});

describe('fields', () => {
  it('flattens the delivered pairs, whatever the value type', () => {
    const row = {
      fields: [
        ['subject', { kind: 'str', value: 'orders' }],
        ['rows', { kind: 'uint', value: 5 }],
        ['ratio', { kind: 'float', value: 6.5 }],
        ['ok', { kind: 'bool', value: false }],
      ],
    };
    expect(fields(row)).toEqual([
      { name: 'subject', value: 'orders' },
      { name: 'rows', value: '5' },
      { name: 'ratio', value: '6.5' },
      { name: 'ok', value: 'false' },
    ]);
  });

  /** A row is still worth reading when one field of it is not. */
  it('drops what it cannot read rather than the row', () => {
    const row = {
      fields: [
        ['subject', { kind: 'str', value: 'orders' }],
        'not a pair',
        ['bare'],
        [1, { value: 2 }],
        null,
      ],
    };
    expect(fields(row as never)).toEqual([{ name: 'subject', value: 'orders' }]);
  });

  it('is empty for a row whose fields did not parse', () => {
    expect(fields({ fields: [] })).toEqual([]);
    expect(fields({ fields: 'nope' as never })).toEqual([]);
  });
});

describe('wasTruncated', () => {
  it('marks a message that reached the cut', () => {
    expect(wasTruncated('x'.repeat(200), 200)).toBe(true);
    expect(wasTruncated('x'.repeat(199), 200)).toBe(false);
  });
});
