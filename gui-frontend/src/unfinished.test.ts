import { describe, expect, it } from 'vitest';
import { UnfinishedRows } from './unfinished.svelte';
import type { PreviewError, Step } from './pipeline';

const evalOf = (...assignments: { target: string; expr: string }[]): Step => ({
  type: 'eval',
  id: 'e',
  filter: '',
  disabled: false,
  assignments,
});
const sent = (steps: Step[]) => (steps[0] as Extract<Step, { type: 'eval' }>).assignments;
const error = (assignmentIndex: number): PreviewError => ({
  step: 'e',
  function: null,
  assignmentIndex,
  message: 'x',
});

describe('rows still being written', () => {
  it('leaves out a blank row, and a box not yet left blank', () => {
    const blank = { target: '', expr: '' };
    const typing = { target: 'a', expr: '' };
    const done = { target: 'b', expr: '1' };
    const { steps } = new UnfinishedRows().hold([evalOf(blank, typing, done)]);
    expect(sent(steps)).toEqual([done]);
  });

  it('sends a box left blank, so its error shows', () => {
    const rows = new UnfinishedRows();
    const typing = { target: 'a', expr: '' };
    rows.left(typing, 'expr');
    expect(sent(rows.hold([evalOf(typing)]).steps)).toEqual([typing]);
  });

  it('keeps leaving out a blank row even once left', () => {
    const rows = new UnfinishedRows();
    const blank = { target: '', expr: '' };
    rows.left(blank, 'target');
    rows.left(blank, 'expr');
    expect(sent(rows.hold([evalOf(blank)]).steps)).toEqual([]);
  });

  it('puts an error back on the row it names on screen', () => {
    const held = new UnfinishedRows().hold([
      evalOf({ target: '', expr: '' }, { target: 'b', expr: '(' }),
    ]);
    expect(held.restore(error(0)).assignmentIndex).toBe(1);
  });
});
