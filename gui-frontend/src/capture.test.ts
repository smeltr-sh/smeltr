import { describe, expect, it } from 'vitest';
import type { CaptureBatch, CaptureStatus, IdentityDto, SourceDto } from './api';
import {
  captureRequest,
  cellText,
  filledColumns,
  filterHelp,
  firstRows,
  heldCount,
  progressText,
  settled,
  shownRecords,
  workerChips,
} from './capture';

const columns = [
  { name: '_raw', type: 'string' },
  { name: '__source.topic', type: 'string' },
];
const row = (worker: string, raw: string) => ({ worker, values: [raw, 't'], failed: [] });

function status(over: Partial<CaptureStatus> = {}): CaptureStatus {
  return {
    session: 's1',
    done: false,
    stopped: false,
    maxMatches: 100,
    workers: [
      { worker: 'w-a', state: 'filling' },
      { worker: 'w-b', state: 'answered' },
      { worker: 'w-c', state: 'waiting' },
    ],
    batches: [
      { columns, rows: [row('w-a', '1'), row('w-b', '2')] },
      { columns, rows: [row('w-a', '3')] },
    ],
    ended: [['w-b', 'matches']],
    secondsLeft: 12,
    waitingOn: ['w-a', 'w-c'],
    truncated: false,
    ...over,
  };
}

describe('workerChips', () => {
  it('names every worker once, in order, with its state and its count', () => {
    expect(workerChips(status())).toEqual([
      { worker: 'w-a', state: 'filling', count: 2 },
      { worker: 'w-b', state: 'answered', count: 1 },
      { worker: 'w-c', state: 'waiting', count: 0 },
    ]);
  });

  it('keeps a worker with no record held, and one nobody waits on', () => {
    // w-d's records were pushed out by the fold. w-e was invited after the
    // capture opened, so it is not in waitingOn.
    const chips = workerChips(
      status({
        workers: [
          ...status().workers,
          { worker: 'w-d', state: 'filling' },
          { worker: 'w-e', state: 'waiting' },
        ],
      }),
    );
    expect(chips).toHaveLength(5);
    expect(chips).toContainEqual({ worker: 'w-d', state: 'filling', count: 0 });
    expect(chips).toContainEqual({ worker: 'w-e', state: 'waiting', count: 0 });
  });

  it('is empty before anyone is known', () => {
    expect(workerChips(status({ workers: [], batches: [], ended: [], waitingOn: [] }))).toEqual([]);
  });
});

const raws = (batches: CaptureBatch[]) => batches.map((b) => b.rows.map((r) => r.values[0]));

describe('shownRecords', () => {
  it('shows everything with no chip chosen, and one worker with one', () => {
    expect(raws(shownRecords(status(), null))).toEqual([['1', '2'], ['3']]);
    expect(raws(shownRecords(status(), 'w-a'))).toEqual([['1'], ['3']]);
    expect(raws(shownRecords(status(), 'w-b'))).toEqual([['2']]);
    expect(shownRecords(status(), 'w-c')).toEqual([]);
  });
});

describe('heldCount', () => {
  it('counts rows across every batch', () => {
    expect(heldCount(status())).toBe(3);
    expect(heldCount(status({ batches: [] }))).toBe(0);
  });
});

describe('firstRows', () => {
  it('takes rows in order across batches, up to the cap', () => {
    expect(raws(firstRows(status().batches, 2))).toEqual([['1', '2']]);
    expect(raws(firstRows(status().batches, 3))).toEqual([['1', '2'], ['3']]);
    expect(raws(firstRows(status().batches, 8))).toEqual([['1', '2'], ['3']]);
  });
});

describe('filledColumns', () => {
  it('leaves out a column null on every row', () => {
    const batch: CaptureBatch = {
      columns: [
        { name: 'a', type: 'string' },
        { name: '__destination.placement', type: 'string' },
        { name: '__destination.id', type: 'string' },
      ],
      rows: [
        { values: ['x', null, null], failed: [] },
        { values: ['y', null, 'id-1'], failed: [] },
      ],
    };
    expect(filledColumns(batch)).toEqual([0, 2]);
  });
});

describe('cellText', () => {
  it('shows a string as it is and anything else as JSON', () => {
    expect(cellText('a b')).toBe('a b');
    expect(cellText(3)).toBe('3');
    expect(cellText({ to: '/_bulk' })).toBe('{"to":"/_bulk"}');
    expect(cellText(false)).toBe('false');
  });
});

describe('progressText', () => {
  it('says where the capture stands', () => {
    expect(progressText(status())).toBe('3 records so far from 2 of 3 workers.');
    expect(progressText(status({ done: true }))).toBe('Finished. 3 records from 2 of 3 workers.');
    expect(progressText(status({ done: true, stopped: true }))).toBe(
      'Stopped with 12s left. 3 records from 2 of 3 workers.',
    );
    expect(
      progressText(
        status({
          batches: [{ columns, rows: [row('w-a', '1')] }],
          workers: [{ worker: 'w-a', state: 'filling' }],
        }),
      ),
    ).toBe('1 record so far from 1 of 1 worker.');
  });
});

describe('settled', () => {
  const answered = [
    { worker: 'w-a', state: 'answered' as const },
    { worker: 'w-b', state: 'answered' as const },
  ];

  it('is not while the capture runs, even with everyone answered', () => {
    expect(settled(status())).toBe(false);
    expect(settled(status({ workers: answered }))).toBe(false);
  });

  it('is not while a finished capture has a worker yet to say why it stopped', () => {
    // A worker that stopped before the capture ended sends its reason after.
    expect(settled(status({ done: true }))).toBe(false);
    expect(settled(status({ done: true, stopped: true }))).toBe(false);
  });

  it('is once a finished capture has heard from every worker', () => {
    expect(settled(status({ done: true, workers: answered }))).toBe(true);
    expect(settled(status({ done: true, workers: [] }))).toBe(true);
  });
});

describe('captureRequest', () => {
  const ask = { group: 'east', filter: '  level == "error" ', maxMatches: 50, deadlineSecs: 20 };

  it('names a destination, with the same filter and bounds as a source', () => {
    expect(captureRequest({ destination: 'search' }, ask)).toEqual({
      group: 'east',
      destination: 'search',
      filter: 'level == "error"',
      maxMatches: 50,
      deadlineSecs: 20,
    });
  });

  it('carries a source and the identity it names', () => {
    const source = { id: 'm', type: 'metrics' } as SourceDto;
    const identity = { id: 'i' } as IdentityDto;
    expect(captureRequest({ source, identity }, ask)).toEqual({
      group: 'east',
      source,
      identity,
      filter: 'level == "error"',
      maxMatches: 50,
      deadlineSecs: 20,
    });
  });

  it('sends a source its start, and a destination none', () => {
    const source = { id: 'k', type: 'kafka' } as SourceDto;
    const start = { type: 'earliest' } as const;
    expect(captureRequest({ source }, { ...ask, start })).toMatchObject({ start });
    expect(captureRequest({ destination: 'search' }, { ...ask, start })).not.toHaveProperty(
      'start',
    );
  });

  it('leaves out an empty filter', () => {
    expect(
      captureRequest({ destination: 'search' }, { ...ask, filter: '  ' }).filter,
    ).toBeUndefined();
  });
});

describe('filterHelp', () => {
  const text = (at: 'source' | 'destination') =>
    filterHelp(at)
      .map((p) => p.text)
      .join('');

  it('says a source filter reads the record as it arrived', () => {
    expect(text('source')).toContain('reads the record as it arrived');
    expect(filterHelp('source').find((p) => p.code)?.text).toBe('_raw');
  });

  it('says a destination filter reads the row as its pipeline finished it', () => {
    expect(text('destination')).toContain('reads the row as its pipeline finished it');
    expect(text('destination')).not.toContain('_raw');
  });
});
