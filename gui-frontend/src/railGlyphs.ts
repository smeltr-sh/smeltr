import type { Glyph } from './stepGlyphs';
import { SYSTEM_GLYPHS } from './systemGlyphs';

/** The sign each rail item carries (STYLE_GUIDE.md, Glyphs), keyed by the item's label in `nav.ts`.
    Admin's items carry their own icon components instead. */
export const RAIL_GLYPHS: Record<string, Glyph> = {
  // A pulse: the fleet is running.
  Status: { d: 'M6 31 H18 L27 10 L37 39 L42 31 H58 V37 H46 L37 57 L27 26 L22 37 H6 Z' },
  // A mark in a disc: something wants attention.
  Issues: {
    d: 'M8 32 A24 24 0 1 0 56 32 A24 24 0 1 0 8 32 Z M29 16 H35 V37 H29 Z M29 42 H35 V48 H29 Z',
    evenodd: true,
  },
  // smeltr's own logs, the same droplet the map draws for them.
  Logs: SYSTEM_GLYPHS.logs,
  // Leaving an edge: where rows enter the fleet.
  Sources: { d: 'M8 12 H15 V52 H8 Z M20 29 H42 V18 L58 32 L42 46 V35 H20 Z' },
  // An arrow through a block: rows worked on inside it.
  Pipelines: {
    d: 'M16 10 H48 A8 8 0 0 1 56 18 V46 A8 8 0 0 1 48 54 H16 A8 8 0 0 1 8 46 V18 A8 8 0 0 1 16 10 Z M18 17 A3 3 0 0 0 15 20 V44 A3 3 0 0 0 18 47 H46 A3 3 0 0 0 49 44 V20 A3 3 0 0 0 46 17 Z M19 29 H32 V21 L44 32 L32 43 V35 H19 Z',
    evenodd: true,
  },
  // Arriving at an edge: where rows leave the fleet.
  Destinations: { d: 'M6 29 H28 V18 L44 32 L28 46 V35 H6 Z M49 12 H56 V52 H49 Z' },
  // One line splitting in two: which rows go where.
  Routes: {
    d: 'M6 29 H26 L40 15 H48 V7 L60 18 L48 29 V21 H43 L32 32 L43 43 H48 V35 L60 46 L48 57 V49 H40 L26 35 H6 Z',
  },
  // A stack of records: samples, lookup tables, grok patterns.
  Resources: {
    d: 'M10 14 A22 7 0 0 1 54 14 A22 7 0 0 1 10 14 Z M10 22 A22 7 0 0 0 54 22 V30 A22 7 0 0 1 10 30 Z M10 38 A22 7 0 0 0 54 38 V46 A22 7 0 0 1 10 46 Z',
  },
  // A line forking off a line.
  Branches: {
    d: 'M18 14 H25 V50 H18 Z M25 40 C38 40 44 34 44 24 V14 H51 V24 C51 38 41 47 25 47 Z M15 12 A6.5 6.5 0 1 1 28 12 A6.5 6.5 0 1 1 15 12 Z M15 52 A6.5 6.5 0 1 1 28 52 A6.5 6.5 0 1 1 15 52 Z M41 12 A6.5 6.5 0 1 1 54 12 A6.5 6.5 0 1 1 41 12 Z',
  },
  // A clock.
  History: {
    d: 'M8 32 A24 24 0 1 0 56 32 A24 24 0 1 0 8 32 Z M14 32 A18 18 0 1 0 50 32 A18 18 0 1 0 14 32 Z M29 17 H35 V30 H45 V36 H29 Z',
    evenodd: true,
  },
};
