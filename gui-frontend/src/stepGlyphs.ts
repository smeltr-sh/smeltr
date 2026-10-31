import type { StepType } from './pipeline';

/** The sign each step type carries in its badge (STYLE_GUIDE.md, Glyphs). */
/** One path on the 64-unit grid, filled in `currentColor`. */
export type Glyph = { d: string; evenodd?: boolean };

export const STEP_GLYPHS: Record<StepType, Glyph> = {
  parser: {
    d: 'M24 8 H30 V14 H27 V25 C27 29 24 31 20 32 C24 33 27 35 27 39 V50 H30 V56 H24 C19 56 17 52 17 47 V38 C17 35 15 34 11 34 V30 C15 30 17 29 17 26 V17 C17 12 19 8 24 8 Z M40 8 H34 V14 H37 V25 C37 29 40 31 44 32 C40 33 37 35 37 39 V50 H34 V56 H40 C45 56 47 52 47 47 V38 C47 35 49 34 53 34 V30 C49 30 47 29 47 26 V17 C47 12 45 8 40 8 Z',
  },
  eval: { d: 'M13 18 H51 L54 26 H10 Z M13 38 H51 L54 46 H10 Z' },
  code: { d: 'M6 15 L12 9 L35 32 L12 55 L6 49 L23 32 Z M34 47 H54 L57 55 H31 Z' },
  drop: { d: 'M10 14 H54 V21 H10 Z M26 8 H38 V14 H26 Z M14 25 H50 L45 56 H19 Z' },
  rename: { d: 'M50.6 5 L59 13.4 L55 17.4 L46.6 9 Z M45 11 L53 19 L26.2 46.2 L10 54 L17.8 37.8 Z' },
  // `CrossIcon`'s cross: remove, as everywhere else.
  remove: {
    d: 'M14 19 L19 14 L32 27 L45 14 L50 19 L37 32 L50 45 L45 50 L32 37 L19 50 L14 45 L27 32 Z',
  },
  lookup: {
    d: 'M26 8 A18 18 0 1 0 26 44 A18 18 0 1 0 26 8 Z M26 15 A11 11 0 1 1 26 37 A11 11 0 1 1 26 15 Z M36 40 L42 34 L58 50 L52 56 Z',
    evenodd: true,
  },
  unroll: {
    d: 'M8 10 H20 V16 H14 V48 H20 V54 H8 Z M28 12 H54 L56 19 H26 Z M28 28 H54 L56 35 H26 Z M28 44 H54 L56 51 H26 Z',
  },
  call: { d: 'M8 14 H36 V20 H14 V50 H36 V56 H8 Z M30 29 H44 V20 L58 32 L44 44 V35 H30 Z' },
  if: {
    d: 'M32 4 L60 32 L32 60 L4 32 Z M32 11 L53 32 L32 53 L11 32 Z M26 26 C26 21 29 19 32 19 C36 19 39 21.5 39 25.5 C39 29 36.5 30.5 34 32 V36 H30 V30.5 C30 29 31.5 28.5 33.5 27.5 C35 26.5 35.5 26 35.5 25.5 C35.5 23.5 34 22.5 32 22.5 C30 22.5 29.5 24 29.5 26 Z M30 39 H34 V43 H30 Z',
    evenodd: true,
  },
  comment: { d: 'M14 54 L30 10 H38 L22 54 Z M30 54 L46 10 H54 L38 54 Z' },
};
