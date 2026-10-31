<!--
  What a person may do: its glyph and its name, wherever a level is shown. One glyph per level, so the
  shape is learned and the name need not be read each time (STYLE_GUIDE.md, Glyphs). The
  logo's 64-unit grid, one path, `currentColor`, 18px, the size a compact icon control gets; the
  drafted bar under the deployer's arrow and the administrator's crown is the house's touch.
  An eye reads, a nib writes, an arrow ships, a crown holds everything.
-->
<script lang="ts">
  import type { AccessLevel } from './session';

  let {
    level,
    named = true,
  }: {
    level: AccessLevel | 'administrator';
    /** Off where room is short: the glyph alone, its name on hover and to a screen reader. */
    named?: boolean;
  } = $props();

  const PATHS: Record<AccessLevel | 'administrator', string> = {
    viewer:
      'M6 32 C18 14 46 14 58 32 C46 50 18 50 6 32 Z M32 23 A9 9 0 1 0 32 41 A9 9 0 1 0 32 23 Z M32 28 A4 4 0 1 1 32 36 A4 4 0 1 1 32 28 Z',
    editor:
      'M22 6 H42 V16 L50 30 L32 58 L14 30 L22 16 Z M30.5 26 H33.5 V44 H30.5 Z M32 20 A4 4 0 1 1 32 28 A4 4 0 1 1 32 20 Z',
    deployer: 'M32 8 L50 28 H39 V44 H25 V28 H14 Z M14 50 H50 L54 57 H10 Z',
    administrator: 'M10 18 L22 31 L32 12 L42 31 L54 18 L50 44 H14 Z M14 49 H50 L54 56 H10 Z',
  };
</script>

<span
  class="level"
  title={named ? undefined : level}
  role={named ? undefined : 'img'}
  aria-label={named ? undefined : level}
  ><svg viewBox="0 0 64 64" width="18" height="18" aria-hidden="true">
    <path d={PATHS[level]} fill="currentColor" fill-rule="evenodd" /></svg
  >{#if named}{level}{/if}</span
>

<style>
  /* The name sits on the line's baseline, as the words around it do; the glyph centers on it. */
  .level {
    display: inline-flex;
    align-items: baseline;
    gap: 0.2rem;
  }

  svg {
    align-self: center;
  }
</style>
