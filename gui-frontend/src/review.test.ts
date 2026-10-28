import { describe, expect, it } from 'vitest';
import type { TestResult, TimelineEvent } from './api';
import { canSubmit, latestVerdicts, reviews, testsTally } from './review';

const ev = (
  kind: TimelineEvent['kind'],
  time: number,
  author: string,
  authorId?: string,
): TimelineEvent => ({ kind, time, author, authorId });

describe('latestVerdicts', () => {
  it('keeps each reviewer last verdict, ordered by when it was given', () => {
    const events = [
      ev('changes-requested', 1, 'Marcus Webb', 'mw'),
      ev('approved', 2, 'Jo Kim', 'jk'),
      ev('comment', 3, 'Marcus Webb', 'mw'),
      ev('approved', 4, 'Marcus Webb', 'mw'),
    ];
    expect(latestVerdicts(events).map((e) => [e.author, e.kind])).toEqual([
      ['Jo Kim', 'approved'],
      ['Marcus Webb', 'approved'],
    ]);
  });

  it('tells reviewers apart by account, and by name where no account is named', () => {
    const events = [
      ev('approved', 1, 'Sam', 'a'),
      ev('approved', 2, 'Sam', 'b'),
      ev('approved', 3, 'token'),
      ev('changes-requested', 4, 'token'),
    ];
    expect(latestVerdicts(events).map((e) => e.time)).toEqual([1, 2, 4]);
  });
});

describe('reviews', () => {
  it('names everyone whose standing verdict asks for changes, over any approval', () => {
    const events = [
      ev('approved', 1, 'Jo Kim', 'jk'),
      ev('changes-requested', 2, 'Marcus Webb', 'mw'),
      ev('changes-requested', 3, 'Ana Diaz', 'ad'),
    ];
    expect(reviews(events)).toEqual({
      state: 'changes-requested',
      names: ['Marcus Webb', 'Ana Diaz'],
    });
  });

  it('names the approvers once no request for changes stands', () => {
    const events = [
      ev('changes-requested', 1, 'Marcus Webb', 'mw'),
      ev('approved', 2, 'Marcus Webb', 'mw'),
    ];
    expect(reviews(events)).toEqual({ state: 'approved', names: ['Marcus Webb'] });
  });

  it('says there are none when nobody gave a verdict', () => {
    expect(reviews([ev('opened', 1, 'Priya Raman'), ev('comment', 2, 'Jo Kim')])).toEqual({
      state: 'none',
    });
  });
});

describe('testsTally', () => {
  const t = (passed: boolean | null): TestResult =>
    passed === null
      ? { id: 'x', name: 'x', area: 's', error: 'no sample' }
      : {
          id: 'x',
          name: 'x',
          area: 's',
          verdict: { passed, expectedRows: 1, actualRows: 1 },
        };

  it('counts a test that could not run as neither passed nor failed', () => {
    expect(testsTally([t(true), t(true), t(false), t(null)])).toEqual({
      total: 4,
      passed: 2,
      failed: 1,
      couldNotRun: 1,
    });
  });

  it('is all zeroes with no tests', () => {
    expect(testsTally([])).toEqual({ total: 0, passed: 0, failed: 0, couldNotRun: 0 });
  });
});

describe('canSubmit', () => {
  it('needs words for a comment and for a request for changes', () => {
    expect(canSubmit('comment', '  ')).toBe(false);
    expect(canSubmit('request-changes', '')).toBe(false);
    expect(canSubmit('comment', 'ok')).toBe(true);
    expect(canSubmit('request-changes', 'fix line 28')).toBe(true);
  });

  it('takes an approval without words', () => {
    expect(canSubmit('approve', '')).toBe(true);
  });
});
