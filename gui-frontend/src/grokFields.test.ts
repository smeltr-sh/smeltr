import { describe, expect, it } from 'vitest';
import { beginReconcile, endReconcile, fieldsSettled } from './grokFields';

describe('grok field reconciliation', () => {
  it('is settled when nothing is owed', async () => {
    await expect(fieldsSettled()).resolves.toBeUndefined();
  });

  it('waits for the outstanding answer, then settles', async () => {
    beginReconcile();
    let settled = false;
    const waiting = fieldsSettled().then(() => {
      settled = true;
    });

    // A save arriving now must not proceed: the step's `declared` still disagrees with its pattern.
    await Promise.resolve();
    expect(settled).toBe(false);

    endReconcile();
    await waiting;
    expect(settled).toBe(true);
  });

  it('waits for every grok step, not just the first to answer', async () => {
    beginReconcile();
    beginReconcile();
    let settled = false;
    const waiting = fieldsSettled().then(() => {
      settled = true;
    });

    endReconcile();
    await Promise.resolve();
    expect(settled).toBe(false);

    endReconcile();
    await waiting;
    expect(settled).toBe(true);
  });

  it('does not go negative when an edit is superseded after its answer arrived', async () => {
    // The request settles the count, and the effect's cleanup then runs for the same edit.
    beginReconcile();
    endReconcile();
    endReconcile();

    // A later edit must still be awaited rather than read as already settled.
    beginReconcile();
    let settled = false;
    const waiting = fieldsSettled().then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    endReconcile();
    await waiting;
    expect(settled).toBe(true);
  });
});
