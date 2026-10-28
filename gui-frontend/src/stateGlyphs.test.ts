import { describe, expect, it } from 'vitest';
import { STATE_GLYPHS, severityState } from './stateGlyphs';
import { RAIL_GLYPHS } from './railGlyphs';
import { STEP_GLYPHS } from './stepGlyphs';
import { SYSTEM_GLYPHS } from './systemGlyphs';

describe('STATE_GLYPHS', () => {
  // The shape is what tells the states apart for a reader who cannot tell their colors apart.
  it('draws each state as its own shape', () => {
    const paths = Object.values(STATE_GLYPHS).map((g) => g.d);
    expect(new Set(paths).size).toBe(paths.length);
  });

  it('draws no state the way another glyph set draws something else', () => {
    const others = [RAIL_GLYPHS, STEP_GLYPHS, SYSTEM_GLYPHS].flatMap((set) =>
      Object.values(set).map((g) => g.d),
    );
    for (const g of Object.values(STATE_GLYPHS)) expect(others).not.toContain(g.d);
  });

  it('signs a failed issue as an error', () => {
    expect(severityState('failed')).toBe('error');
    expect(severityState('warning')).toBe('warning');
    expect(severityState('info')).toBe('info');
  });
});
