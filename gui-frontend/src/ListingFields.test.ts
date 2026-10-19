import { describe, expect, it } from 'vitest';
import type { ListingSettings } from './api';
import { canStartNew, listingModified, setAfterCommit, setStart } from './ListingFields.svelte';

describe('what a listing source does after commit', () => {
  it('writes nothing for keep, the default', () => {
    const d: ListingSettings = { afterCommit: 'delete' };
    setAfterCommit(d, 'keep');
    expect(d).toEqual({});
  });

  it('drops the move prefix with anything but move', () => {
    const d: ListingSettings = { afterCommit: 'move', movePrefix: 'done/' };
    setAfterCommit(d, 'delete');
    expect(d).toEqual({ afterCommit: 'delete' });
  });

  it('drops a start at new objects with anything but keep, which alone records the first listing', () => {
    const d: ListingSettings = {};
    setStart(d, 'new');
    expect(canStartNew(d)).toBe(true);
    setAfterCommit(d, 'move');
    expect(d).toEqual({ afterCommit: 'move' });
    expect(canStartNew(d)).toBe(false);
  });

  it('writes nothing for a start at everything, the default', () => {
    const d: ListingSettings = { start: 'new' };
    setStart(d, 'everything');
    expect(d).toEqual({});
  });
});

describe('the listing Advanced marker', () => {
  it('is on only for a poll interval off its default', () => {
    expect(listingModified({})).toBe(false);
    expect(listingModified({ pollIntervalSecs: 60 })).toBe(false);
    expect(listingModified({ pollIntervalSecs: 300 })).toBe(true);
    expect(listingModified({ afterCommit: 'delete', prefix: 'app/' })).toBe(false);
  });
});
