// What the GUI fuzzer (`specs/gui-fuzz.spec.ts`) draws from, apart from the fuzzer itself, so
// `specs/gui-fuzz-coverage.spec.ts` can name every option without registering the seeds' tests a second time.

/** Who the seed signs in as. Taken in turn rather than drawn, so a handful of seeds holds every one. */
export const WHO = [
  'signed-out',
  'required-change',
  'administrator',
  'viewer',
  'editor',
  'deployer',
] as const;
export type Who = (typeof WHO)[number];
export const THEMES = ['light', 'dark'] as const;
export const VIEWPORTS = {
  wide: { width: 1440, height: 900 },
  narrow: { width: 390, height: 844 },
};
export const ACTIONS = [
  'click',
  'type',
  'choose',
  'go',
  'back',
  'reload',
  'lose-session',
  'keys',
  'sign-in',
] as const;
export type Action = (typeof ACTIONS)[number];

/** Seeds a run takes when nothing asks for more: one for each person, which holds every theme and size too. */
export const DEFAULT_SEEDS = WHO.length;
