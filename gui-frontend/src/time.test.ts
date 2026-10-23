import { describe, expect, it } from 'vitest';
import {
  ago,
  clockTime,
  count,
  countdown,
  dateTime,
  isPipelinePath,
  pathSummary,
  shortCommit,
  shortPath,
  expiry,
} from './time';

const NOW = 1_700_000_000_000;
const then = (secondsBack: number) => NOW / 1000 - secondsBack;

describe('ago', () => {
  it('steps up a unit only once the smaller one is full', () => {
    expect(ago(then(0), NOW)).toBe('0s ago');
    expect(ago(then(59), NOW)).toBe('59s ago');
    expect(ago(then(60), NOW)).toBe('1m ago');
    expect(ago(then(59 * 60), NOW)).toBe('59m ago');
    expect(ago(then(60 * 60), NOW)).toBe('1h ago');
    expect(ago(then(23 * 3600), NOW)).toBe('23h ago');
    expect(ago(then(24 * 3600), NOW)).toBe('1d ago');
    expect(ago(then(400 * 24 * 3600), NOW)).toBe('400d ago');
  });

  it('reads a time in the future as now, rather than as a negative age', () => {
    expect(ago(then(-90), NOW)).toBe('0s ago');
  });
});

describe('dateTime', () => {
  it('names the month and day, then the local clock', () => {
    expect(dateTime(new Date(2024, 9, 17, 22, 40).getTime() / 1000)).toBe('Oct 17, 22:40');
    expect(dateTime(new Date(2024, 0, 3, 8, 5).getTime() / 1000)).toBe('Jan 3, 08:05');
  });
});

describe('countdown', () => {
  it('ticks to the second, with days once there is a day left', () => {
    expect(countdown(NOW + 59_000, NOW)).toBe('00:00:59');
    expect(countdown(NOW + (6 * 3600 + 22 * 60 + 51) * 1000, NOW)).toBe('06:22:51');
    expect(countdown(NOW + (86_400 + 6 * 3600 + 22 * 60 + 51) * 1000, NOW)).toBe('1 day 06:22:51');
    expect(countdown(NOW + 2 * 86_400_000, NOW)).toBe('2 days 00:00:00');
  });

  it('stops at zero once the moment passes', () => {
    expect(countdown(NOW - 5_000, NOW)).toBe('00:00:00');
  });
});

describe('clockTime', () => {
  it('pads to two digits, in local time', () => {
    expect(clockTime(new Date(2024, 0, 1, 9, 5).getTime())).toBe('09:05');
    expect(clockTime(new Date(2024, 0, 1, 23, 59).getTime())).toBe('23:59');
    expect(clockTime(new Date(2024, 0, 1, 0, 0).getTime())).toBe('00:00');
  });
});

describe('count', () => {
  it('pluralizes everything but one', () => {
    expect(count(0, 'commit')).toBe('0 commits');
    expect(count(1, 'commit')).toBe('1 commit');
    expect(count(2, 'file')).toBe('2 files');
  });
});

describe('shortCommit', () => {
  it('keeps enough of a sha to name one', () => {
    expect(shortCommit('a41f0c2e9b7d4a1c8f30')).toBe('a41f0c2');
    expect(shortCommit('abc')).toBe('abc');
  });
});

describe('shortPath', () => {
  it('drops the group prefix and the extension, leaving the object', () => {
    expect(shortPath('groups/west/sources.json')).toBe('sources');
    expect(shortPath('shared/pipelines/orders-normalize.json')).toBe('pipelines/orders-normalize');
    expect(shortPath('samples/orders.json')).toBe('samples/orders');
  });

  it('leaves a path it recognizes nothing in alone', () => {
    expect(shortPath('lookups/geo-cities.csv')).toBe('lookups/geo-cities.csv');
  });
});

describe('pathSummary', () => {
  it('names the first few and counts the rest', () => {
    expect(pathSummary([])).toBe('—');
    expect(pathSummary(['groups/west/sources.json'])).toBe('sources');
    expect(pathSummary(['a.json', 'b.json', 'c.json'])).toBe('a · b · c');
    expect(pathSummary(['a.json', 'b.json', 'c.json', 'd.json'])).toBe('a · b · c · 1 more');
  });

  it('names a pipeline file by its kind, never by the id its stem is', () => {
    expect(pathSummary(['groups/west/pipelines/pipeline-x7k2m9q1.json'])).toBe('pipeline');
    expect(
      pathSummary([
        'groups/west/sources.json',
        'groups/west/pipelines/pipeline-x7k2m9q1.json',
        'shared/pipelines/pipeline-a1b2c3d4.json',
        'groups/west/routes.json',
      ]),
    ).toBe('sources · 2 pipelines · routes');
  });

  it('counts the files behind the entries it does not show', () => {
    expect(
      pathSummary([
        'a.json',
        'b.json',
        'c.json',
        'pipelines/pipeline-1.json',
        'pipelines/pipeline-2.json',
      ]),
    ).toBe('a · b · c · 2 more');
  });

  it('names a path the way `label` does, where it does', () => {
    const label = (path: string) =>
      path === 'groups/west/pipelines/pipeline-x7k2m9q1.json' ? 'pipeline orders' : undefined;
    expect(
      pathSummary(
        [
          'groups/west/pipelines/pipeline-x7k2m9q1.json',
          'groups/west/pipelines/pipeline-a1b2c3d4.json',
          'groups/west/sources.json',
        ],
        label,
      ),
    ).toBe('pipeline orders · pipeline · sources');
  });
});

describe('isPipelinePath', () => {
  it('knows a pipeline file in a group, in shared and in a bare bundle', () => {
    expect(isPipelinePath('groups/west/pipelines/pipeline-x7k2m9q1.json')).toBe(true);
    expect(isPipelinePath('shared/pipelines/pipeline-x7k2m9q1.json')).toBe(true);
    expect(isPipelinePath('pipelines/pipeline-x7k2m9q1.json')).toBe(true);
    expect(isPipelinePath('groups/west/sources.json')).toBe(false);
    expect(isPipelinePath('groups/west/pipelines/notes.txt')).toBe(false);
  });
});

describe('expiry', () => {
  const NOW = 1_700_000_000_000;
  const MINUTE = 60_000;
  const HOUR = 60 * MINUTE;
  const DAY = 24 * HOUR;

  it('says what to do about a deadline rather than when it is', () => {
    expect(expiry(NOW + 30 * MINUTE, NOW)).toBe('expires in 30 minutes');
    expect(expiry(NOW + 3 * HOUR, NOW)).toBe('expires in 3 hours');
    expect(expiry(NOW + 10 * DAY, NOW)).toBe('expires in 10 days');
    expect(expiry(NOW + 200 * DAY, NOW)).toBe('expires in 7 months');
    expect(expiry(NOW + 3650 * DAY, NOW)).toBe('expires in 10 years');
  });

  it('pluralizes by what is actually left', () => {
    expect(expiry(NOW + MINUTE, NOW)).toBe('expires in 1 minute');
    expect(expiry(NOW + HOUR, NOW)).toBe('expires in 1 hour');
  });

  // A page showing "expires in 0 minutes" reads as a deadline still to come.
  it('says expired once it has, including exactly on the boundary', () => {
    expect(expiry(NOW, NOW)).toBe('expired');
    expect(expiry(NOW - DAY, NOW)).toBe('expired');
  });
});
