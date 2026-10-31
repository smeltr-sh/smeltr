<!--
  What a person may do: its glyph and its name, wherever a level is shown. One glyph per level, so the
  shape is learned and the name need not be read each time (STYLE_GUIDE.md, Glyphs). The
  logo's 64-unit grid, one path, `currentColor`, 18px, the size a compact icon control gets.
  An eye reads, a nib writes, a rocket launches, a star holds everything.
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
      'M4 32 C17 12 47 12 60 32 C47 52 17 52 4 32 Z M12 32 C22 46 42 46 52 32 C42 18 22 18 12 32 Z M32 25 A7 7 0 1 1 32 39 A7 7 0 1 1 32 25 Z',
    editor:
      'M22 6 H42 V16 L50 30 L32 58 L14 30 L22 16 Z M30.5 26 H33.5 V44 H30.5 Z M32 20 A4 4 0 1 1 32 28 A4 4 0 1 1 32 20 Z',
    deployer:
      'M32 4 C44 14 46 30 42 44 H22 C18 30 20 14 32 4 Z M32 18 A5 5 0 1 1 32 28 A5 5 0 1 1 32 18 Z M21 34 L12 50 L22 46 Z M43 34 L52 50 L42 46 Z M26 48 H38 L32 60 Z',
    administrator: 'M32 4 L39 23 L59 23 L43 35 L49 55 L32 43 L15 55 L21 35 L5 23 L25 23 Z',
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
