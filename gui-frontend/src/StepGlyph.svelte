<!-- A step type's sign (`stepGlyphs.ts`), the size of the text around it. -->
<script lang="ts">
  import type { StepType } from './pipeline';
  import { STEP_GLYPHS } from './stepGlyphs';

  let { type }: { type: StepType } = $props();
</script>

<svg viewBox="0 0 64 64" fill="none" aria-hidden="true">
  <path
    d={STEP_GLYPHS[type].d}
    fill="currentColor"
    fill-rule={STEP_GLYPHS[type].evenodd ? 'evenodd' : 'nonzero'}
  />
</svg>

<style>
  svg {
    width: 0.9em;
    height: 0.9em;
    vertical-align: -0.1em;
  }
</style>
