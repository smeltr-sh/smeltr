<!--
  A state's sign (`stateGlyphs.ts`), in the state's own color, which says again what the shape says.

  Give `label` where nothing beside the mark says the state in words; without it the mark is hidden from a screen
  reader.
-->
<script lang="ts">
  import { STATE_GLYPHS, type State } from './stateGlyphs';

  let { state, size = 14, label }: { state: State; size?: number; label?: string } = $props();
</script>

<svg
  class="state-mark {state}"
  viewBox="0 0 64 64"
  width={size}
  height={size}
  role={label ? 'img' : undefined}
  aria-label={label}
  aria-hidden={label ? undefined : 'true'}
>
  <path
    d={STATE_GLYPHS[state].d}
    fill="currentColor"
    fill-rule={STATE_GLYPHS[state].evenodd ? 'evenodd' : 'nonzero'}
  />
</svg>

<style>
  .state-mark {
    flex: none;
    vertical-align: -0.15em;
  }

  .info {
    color: var(--dim);
  }

  .ok {
    color: var(--added);
  }

  .warning {
    color: var(--warn);
  }

  .error {
    color: var(--error);
  }
</style>
