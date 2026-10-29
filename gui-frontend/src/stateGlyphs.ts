import type { WarningSeverity } from './api';
import type { Glyph } from './stepGlyphs';

/** The four states the GUI signs (STYLE_GUIDE.md, Glyphs). */
export type State = 'info' | 'ok' | 'warning' | 'error';

/** The sign each state carries, each its own shape so none is told from another by color alone.
    A cutout is a hole in the path, so it shows whatever the mark sits on. */
export const STATE_GLYPHS: Record<State, Glyph> = {
  // A dot: something to know.
  info: { d: 'M32 20 A12 12 0 1 1 32 44 A12 12 0 1 1 32 20 Z' },
  // A check: it works.
  ok: { d: 'M7 34 L14 27 L25 38 L50 13 L57 20 L25 52 Z' },
  // A triangle with a mark in it: it works, but may not do what was meant.
  warning: { d: 'M32 5 L61 57 H3 Z M29 22 H35 V40 H29 Z M29 45 H35 V51 H29 Z', evenodd: true },
  // An octagon struck with an X: it failed.
  error: {
    d: 'M21 4 H43 L60 21 V43 L43 60 H21 L4 43 V21 Z M20 25 L25 20 L32 27 L39 20 L44 25 L37 32 L44 39 L39 44 L32 37 L25 44 L20 39 L27 32 Z',
    evenodd: true,
  },
};

/** The sign for an issue's severity: a failed issue is an error. */
export function severityState(severity: WarningSeverity): State {
  return severity === 'failed' ? 'error' : severity;
}
