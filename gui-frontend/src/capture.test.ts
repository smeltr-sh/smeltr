import { describe, expect, it } from 'vitest';
import type { CaptureStatus, HeldRecord, SourceDto } from './api';
import {
  captureRequest,
  cellText,
  filterHelp,
  heldRows,
  heldCount,
  progressText,
  settled,
  shownRecords,
  workerChips,
} from './capture';

const columns = [
  { name: '_raw', type: 'str' },
  { name: '__source.topic', type: 'str' },
];
const held = (worker: string, raw: string): HeldRecord => ({
  worker,
  record: { _raw: raw, __source: { topic: 't' } },
});

function status(over: Partial<CaptureStatus> = {}): CaptureStatus {
  return {
    session: 's1',
    revision: 3,
    done: false,
    stopped: false,
    maxMatches: 100,
    workers: [
      { worker: 'w-a', state: 'filling' },
      { worker: 'w-b', state: 'answered' },
      { worker: 'w-c', state: 'waiting' },
    ],
    columns,
    records: [held('w-a', '1'), held('w-b', '2'), held('w-a', '3')],
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
    expect(workerChips(status({ workers: [], records: [], ended: [], waitingOn: [] }))).toEqual([]);
  });
});

const raws = (records: HeldRecord[]) => records.map((r) => r.record._raw);

describe('shownRecords', () => {
  it('shows everything with no chip chosen, and one worker with one', () => {
    expect(raws(shownRecords(status(), null))).toEqual(['1', '2', '3']);
    expect(raws(shownRecords(status(), 'w-a'))).toEqual(['1', '3']);
    expect(raws(shownRecords(status(), 'w-b'))).toEqual(['2']);
    expect(shownRecords(status(), 'w-c')).toEqual([]);
  });
});

describe('heldCount', () => {
  it('counts the records held', () => {
    expect(heldCount(status())).toBe(3);
    expect(heldCount(status({ records: [] }))).toBe(0);
  });
});

describe('heldRows', () => {
  it('shows each record as its fields, with why it failed apart from them', () => {
    const record = {
      __exception: { message: 'elasticsearch refused it' },
      _raw: 'x',
      code: 'abc',
      __capture: { group: 'g', mistyped: ['code', 'gone'] },
    };
    const types = [
      { name: 'code', type: 'i64' },
      { name: 'gone', type: 'f64' },
    ];
    expect(heldRows([{ worker: 'w-a', record }], types)).toEqual([
      {
        record: { _raw: 'x', code: 'abc', __capture: { group: 'g' } },
        exceptions: [
          'elasticsearch refused it',
          'code could not be read as i64',
          'gone could not be read as f64',
        ],
        worker: 'w-a',
      },
    ]);
    // The record the leader sent is left as it was.
    expect(record.__capture.mistyped).toEqual(['code', 'gone']);
    expect(heldRows([{ record: { _raw: 'y' } }], [])).toEqual([
      { record: { _raw: 'y' }, exceptions: [] },
    ]);
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
          records: [held('w-a', '1')],
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

  it('captures a receiver as sent with no filter, since there is no record yet to read', () => {
    const source = { id: 'sys', type: 'syslogReceiver', port: 1514 } as SourceDto;
    expect(captureRequest({ source }, { ...ask, raw: true })).toEqual({
      group: 'east',
      source,
      raw: true,
      maxMatches: 50,
      deadlineSecs: 20,
    });
  });

  it('carries a source as the form has it', () => {
    const source = { id: 'm', type: 'metrics' } as SourceDto;
    expect(captureRequest({ source }, ask)).toEqual({
      group: 'east',
      source,
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
