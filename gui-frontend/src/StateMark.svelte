<!--
  A state's sign (STYLE_GUIDE.md, Glyphs): a dot for info, a check for ok, a triangle for a warning, an octagon
  struck with an X for an error. The shape alone tells the four apart, so a reader who cannot tell red from green
  still can; the color says it again. The logo's 64-unit grid, one path, `currentColor`; a cutout is a hole in the
  path (`evenodd`), so it shows whatever the mark sits on.

  Give `label` where nothing beside the mark says the state in words; without it the mark is hidden from a screen
  reader.
-->
<script lang="ts" module>
  import type { WarningSeverity } from './api';

  export type State = 'info' | 'ok' | 'warning' | 'error';

  /** The sign for an issue's severity: a failed issue is an error. */
  export function severityState(severity: WarningSeverity): State {
    return severity === 'failed' ? 'error' : severity;
  }

  const PATHS: Record<State, string> = {
    info: 'M32 20 A12 12 0 1 1 32 44 A12 12 0 1 1 32 20 Z',
    ok: 'M7 34 L14 27 L25 38 L50 13 L57 20 L25 52 Z',
    warning: 'M32 5 L61 57 H3 Z M29 22 H35 V40 H29 Z M29 45 H35 V51 H29 Z',
    error:
      'M21 4 H43 L60 21 V43 L43 60 H21 L4 43 V21 Z M20 25 L25 20 L32 27 L39 20 L44 25 L37 32 L44 39 L39 44 L32 37 L25 44 L20 39 L27 32 Z',
  };
</script>

<script lang="ts">
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
  <path d={PATHS[state]} fill="currentColor" fill-rule="evenodd" />
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
