import { describe, expect, it } from 'vitest';
import { defaultHue, scopeHue } from './scope';
import { SHARED_SCOPE, type WorkerGroupDto } from './api';

const group = (id: string, hue?: number): WorkerGroupDto => ({
  id,
  name: id,
  deployed: true,
  ...(hue === undefined ? {} : { hue }),
});

describe('scopeHue', () => {
  it('draws shared in the hue given for it', () => {
    expect(scopeHue(SHARED_SCOPE, [], 42)).toBe(42);
  });

  it('has no color for shared before its hue is read', () => {
    expect(scopeHue(SHARED_SCOPE, [], null)).toBeNull();
  });

  it('draws a group in the hue its manifest carries', () => {
    expect(scopeHue('prod-west', [group('prod-west', 264)])).toBe(264);
  });

  /** The admin's choice is the group's, whatever it is -- including a hue the
   * leader would never have assigned. */
  it('keeps a hue an admin chose that is no default', () => {
    expect(scopeHue('prod-west', [group('prod-west', 42)])).toBe(42);
  });

  it('has no color for a group it has not read yet', () => {
    expect(scopeHue('prod-west', [])).toBeNull();
  });

  /** A group from before hues existed still has to be drawn, and drawn the
   * same way on every page. */
  it('gives a group with no stored hue a default, stable across calls', () => {
    const groups = [group('prod-west'), group('prod-east'), group('ingest-1')];
    expect(scopeHue('prod-east', groups)).toBe(scopeHue('prod-east', [...groups].reverse()));
    const hues = new Set(groups.map((g) => scopeHue(g.id, groups)));
    expect(hues.size).toBe(groups.length);
  });

  /** A hue that is no default takes none of them. */
  it('numbers the hueless groups around a hue that is not a default', () => {
    const groups = [group('a', 264), group('b'), group('c')];
    expect(scopeHue('b', groups)).toBe(defaultHue(0));
    expect(scopeHue('c', groups)).toBe(defaultHue(1));
  });

  /** The one that bit in a real fleet: a group created with the first default beside a
   * group from before hues existed, both drawn in the same color. */
  it('never gives a hueless group a hue another group already holds', () => {
    const groups = [group('legacy'), group('prod-west', defaultHue(0))];
    expect(scopeHue('legacy', groups)).not.toBe(defaultHue(0));
    expect(scopeHue('legacy', groups)).toBe(defaultHue(1));
  });

  /** A group from before hues existed must not land on the hue the leader
   * already gave `shared` -- `shared` is not in `groups`, so its hue has to
   * be excluded by hand. */
  it('never gives a hueless group the hue shared already holds', () => {
    const groups = [group('legacy')];
    expect(scopeHue('legacy', groups, defaultHue(0))).not.toBe(defaultHue(0));
    expect(scopeHue('legacy', groups, defaultHue(0))).toBe(defaultHue(1));
  });

  it('falls back to repeating only once every default is spoken for', () => {
    const groups = [
      group('legacy'),
      ...Array.from({ length: 12 }, (_, i) => group(`g${i}`, defaultHue(i))),
    ];
    expect(scopeHue('legacy', groups)).toBe(defaultHue(0));
  });
});

describe('defaultHue', () => {
  /** The leader hands out the same list (`worker_groups_api.rs`). */
  it('starts with blue, magenta and teal, and holds twelve distinct hues', () => {
    expect([0, 1, 2].map(defaultHue)).toEqual([240, 330, 180]);
    expect(new Set(Array.from({ length: 12 }, (_, i) => defaultHue(i))).size).toBe(12);
  });

  it('starts round again past the last default', () => {
    expect(defaultHue(12)).toBe(defaultHue(0));
  });
});
