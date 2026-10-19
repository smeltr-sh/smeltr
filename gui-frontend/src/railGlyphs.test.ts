import { describe, expect, it } from 'vitest';
import { TABS, TAB_HOME, parseRoute, section } from './nav';
import { RAIL_GLYPHS } from './railGlyphs';

describe('RAIL_GLYPHS', () => {
  // A folded rail shows only icons, so an item without one would vanish.
  it('draws every rail item outside Admin', () => {
    for (const { tab } of TABS) {
      if (tab === 'admin') continue;
      for (const item of section(parseRoute(TAB_HOME[tab]))?.items ?? []) {
        expect(RAIL_GLYPHS[item.label], `${tab}: ${item.label}`).toBeDefined();
      }
    }
  });
});
