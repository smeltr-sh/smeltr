import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  anyHeld,
  carry,
  drop,
  dropAll,
  dropScope,
  EDITS_EVENT,
  hold,
  heldFor,
  heldLabel,
  listHeld,
  verdictFor,
} from './edits';
import { setBranch, setScope, SHARED_SCOPE } from './api';

const edit = (value: string, baseline: string, readAtCommit: string | null = 'c1') => ({
  value,
  baseline,
  readAtCommit,
});

beforeEach(() => {
  dropAll();
  setBranch('work');
  setScope(SHARED_SCOPE);
});

describe('a first write starting a branch', () => {
  it("carries what was held on main to it, and leaves another branch's", () => {
    setBranch('other');
    hold('sources', edit('[{"id":"elsewhere"}]', '[]'));
    setBranch('main');
    hold('routes', edit('[{"id":"route-a"}]', '[]', 'c3'));
    setBranch('fresh');
    carry('fresh');
    expect(heldFor('routes')).toMatchObject({
      branch: 'fresh',
      value: '[{"id":"route-a"}]',
      readAtCommit: 'c3',
    });
    expect(listHeld()).toHaveLength(1);
  });
});

describe('holding an edit', () => {
  it('gives back what was held, with the commit it was read at', () => {
    hold('sources', edit('[{"id":"kafka-1"}]', '[]', 'c7'));
    expect(heldFor('sources')).toMatchObject({
      file: 'sources',
      branch: 'work',
      scope: SHARED_SCOPE,
      value: '[{"id":"kafka-1"}]',
      baseline: '[]',
      readAtCommit: 'c7',
    });
  });

  it('has nothing for a file nothing was typed in', () => {
    expect(heldFor('sources')).toBeUndefined();
  });

  /** Otherwise the leave warning fires and the shell lists a row for an edit
   * that was typed and then undone. */
  it('does not hold an edit that matches what the file already says', () => {
    hold('sources', edit('[]', '[]'));
    expect(heldFor('sources')).toBeUndefined();
    expect(anyHeld()).toBe(false);
  });

  it('drops a record that is undone after it was held', () => {
    hold('sources', edit('[1]', '[]'));
    hold('sources', edit('[]', '[]'));
    expect(heldFor('sources')).toBeUndefined();
  });

  it('holds one record per file, so two files do not overwrite each other', () => {
    hold('routes', edit('[1]', '[]'));
    hold('unmatched', edit('"dlq"', 'null'));
    expect(heldFor('routes')?.value).toBe('[1]');
    expect(heldFor('unmatched')?.value).toBe('"dlq"');
  });

  it('forgets one file without touching the other', () => {
    hold('routes', edit('[1]', '[]'));
    hold('unmatched', edit('"dlq"', 'null'));
    drop('unmatched');
    expect(heldFor('routes')?.value).toBe('[1]');
    expect(heldFor('unmatched')).toBeUndefined();
  });
});

// Branch and scope both decide which file a save writes, so both are in the key.
// They differ in whether the work under one can be got back to.
describe('which branch and group a record belongs to', () => {
  it('does not offer one group of work under another', () => {
    hold('sources', edit('[1]', '[]'));
    setScope('payments');
    expect(heldFor('sources')).toBeUndefined();
  });

  /** A scope moves without a reload, so work left in one is still reachable. */
  it('keeps work in a group the reader left, for when they come back', () => {
    hold('sources', edit('[1]', '[]'));
    setScope('payments');
    setScope(SHARED_SCOPE);
    expect(heldFor('sources')?.value).toBe('[1]');
  });

  /** There is no branch event and every way of moving the branch in view
   * reloads, which takes the record with it. A record left on another branch
   * could never be reached, so it is not kept. */
  it('forgets work on a branch the tab is no longer on', () => {
    hold('sources', edit('[1]', '[]'));
    setBranch('al/vendor-returns');
    expect(anyHeld()).toBe(false);
    expect(listHeld()).toEqual([]);
  });

  it('does not bring that work back by returning to the branch', () => {
    hold('sources', edit('[1]', '[]'));
    setBranch('al/vendor-returns');
    anyHeld();
    setBranch('work');
    expect(heldFor('sources')).toBeUndefined();
  });
});

describe('what a page does when its own read lands', () => {
  it('has nothing to say when nothing is held', () => {
    expect(verdictFor('sources', '[]')).toEqual({ kind: 'none' });
  });

  it('puts the edit back when the file did not move', () => {
    hold('sources', edit('[1]', '[]'));
    const verdict = verdictFor('sources', '[]');
    expect(verdict.kind).toBe('restore');
    expect(verdict.kind === 'restore' && verdict.held.value).toBe('[1]');
  });

  /** `user-stories.md`, W9: an open editor notices when what it is working
   * from moves, and says so rather than writing over it. */
  it('says the file moved when the baseline is not what the file says now', () => {
    hold('sources', edit('[1]', '[]', 'c1'));
    const verdict = verdictFor('sources', '[2]');
    expect(verdict.kind).toBe('moved');
    expect(verdict.kind === 'moved' && verdict.fresh).toBe('[2]');
    expect(verdict.kind === 'moved' && verdict.held.readAtCommit).toBe('c1');
  });

  /** The save landed from somewhere else, or the same edit was made twice.
   * Either way there is nothing left to put back. */
  it('has nothing to say when the edit is already what the file says', () => {
    hold('sources', edit('[1]', '[]'));
    expect(verdictFor('sources', '[1]')).toEqual({ kind: 'none' });
    expect(heldFor('sources')).toBeUndefined();
  });
});

describe('what the leave warning and the shell read', () => {
  it('lists a row per file held, naming the group it belongs to', () => {
    hold('sources', edit('[1]', '[]'));
    setScope('payments');
    hold('routes', edit('[2]', '[]'));
    expect(anyHeld()).toBe(true);
    expect(listHeld().map((h) => [h.file, h.scope])).toEqual([
      ['sources', SHARED_SCOPE],
      ['routes', 'payments'],
    ]);
  });

  it('names a pipeline by the label it was held with, and a section by its file', () => {
    hold('pipelines/pipeline-x7k2m9q1', { ...edit('[1]', '[]'), label: 'orders cleanup' });
    hold('pipelines/pipeline-0ld00001', edit('[1]', '[]'));
    hold('sources', edit('[1]', '[]'));
    expect(listHeld().map(heldLabel)).toEqual(['orders cleanup', 'pipeline', 'sources']);
  });

  /** The session is over. The next person to sign in on this machine must not
   * be handed the last one's unsaved work as though it were the file. */
  it('forgets everything, in every group, when the session ends', () => {
    hold('sources', edit('[1]', '[]'));
    setScope('payments');
    hold('routes', edit('[2]', '[]'));
    dropAll();
    expect(anyHeld()).toBe(false);
  });
});

// The pipeline editor restores its own model of the steps but compares a
// signature of them, the form its baseline takes. The model never equals the
// baseline, so compared by it, a clean editor would read as unsaved.
describe('a value held in another form than its signature', () => {
  const held = () => ({
    value: '{"steps":[{"id":"eval-91"}]}',
    signature: '[{"type":"eval"}]',
    baseline: '[]',
    readAtCommit: 'c1',
  });

  it('holds what the page restores from, and compares by the signature', () => {
    hold('pipelines/orders', held());
    expect(heldFor('pipelines/orders')?.value).toBe('{"steps":[{"id":"eval-91"}]}');
  });

  it('is not held when the signature says nothing was typed', () => {
    hold('pipelines/orders', { ...held(), signature: '[]' });
    expect(heldFor('pipelines/orders')).toBeUndefined();
  });

  it('puts the edit back when the signature of the file has not moved', () => {
    hold('pipelines/orders', held());
    expect(verdictFor('pipelines/orders', '[]').kind).toBe('restore');
  });

  /** The value is in another form than the file. Only the signature may decide this. */
  it('has nothing to say when the signature is already what the file says', () => {
    hold('pipelines/orders', held());
    expect(verdictFor('pipelines/orders', '[{"type":"eval"}]')).toEqual({ kind: 'none' });
  });

  it('says the file moved when the signature differs from it', () => {
    hold('pipelines/orders', held());
    expect(verdictFor('pipelines/orders', '[{"type":"grok"}]').kind).toBe('moved');
  });
});

// The shell redraws from this, so it must fire when a file gains a record or
// loses one -- and must not fire on every keystroke into one, which is not news
// to a list of files.
describe('saying that what is held changed', () => {
  let fired = 0;

  beforeEach(() => {
    fired = 0;
    // A window with just enough on it: something to raise the event at, and the
    // storage `setBranch` writes the chip's choice to.
    const store = new Map<string, string>();
    const target = Object.assign(new EventTarget(), {
      localStorage: {
        getItem: (k: string) => store.get(k) ?? null,
        setItem: (k: string, v: string) => void store.set(k, v),
        removeItem: (k: string) => void store.delete(k),
      },
    });
    vi.stubGlobal('window', target);
    target.addEventListener(EDITS_EVENT, () => (fired += 1));
  });

  afterEach(() => vi.unstubAllGlobals());

  it('says so when a held pipeline is renamed, and not when only its value changed', () => {
    hold('pipelines/pipeline-x7k2m9q1', { ...edit('[1]', '[]'), label: 'orders' });
    hold('pipelines/pipeline-x7k2m9q1', { ...edit('[2]', '[]'), label: 'orders' });
    hold('pipelines/pipeline-x7k2m9q1', { ...edit('[2]', '[]'), label: 'orders cleanup' });
    expect(fired).toBe(2);
  });

  it('says so when a file starts holding work', () => {
    hold('sources', edit('[1]', '[]'));
    expect(fired).toBe(1);
  });

  it('stays quiet while the same file is typed into again', () => {
    hold('sources', edit('[1]', '[]'));
    hold('sources', edit('[1,2]', '[]'));
    hold('sources', edit('[1,2,3]', '[]'));
    expect(fired).toBe(1);
  });

  it('says so when the work is undone', () => {
    hold('sources', edit('[1]', '[]'));
    hold('sources', edit('[]', '[]'));
    expect(fired).toBe(2);
  });

  it('says so when a second file starts holding work', () => {
    hold('sources', edit('[1]', '[]'));
    hold('routes', edit('[1]', '[]'));
    expect(fired).toBe(2);
  });

  it('says so once when a branch change forgets what was held', () => {
    hold('sources', edit('[1]', '[]'));
    setBranch('al/vendor-returns');
    anyHeld();
    expect(fired).toBe(2);
  });
});

describe('a group that goes', () => {
  it("takes its held edits with it, and leaves every other group's", () => {
    setScope('edge');
    hold('sources', edit('[1]', '[]'));
    setScope('core');
    hold('sources', edit('[2]', '[]'));
    dropScope('edge');
    expect(listHeld().map((r) => r.scope)).toEqual(['core']);
  });
});
