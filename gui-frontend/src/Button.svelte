<!--
  A button that can be disabled, and then says why (`WhyDisabled`). `disabled` is the reason, so
  nothing disables a button without giving one; `''` leaves it enabled. `npm run check` refuses
  `disabled` on a bare `<button>`, so every button that can be disabled is one of these.
-->
<script module lang="ts">
  /** Why a button is disabled while what it or a button beside it started is still under way. */
  export const BUSY = 'Wait for the running action to finish.';
</script>

<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { HTMLButtonAttributes } from 'svelte/elements';

  let {
    disabled = '',
    children,
    ...rest
  }: Omit<HTMLButtonAttributes, 'disabled' | 'children'> & {
    /** Why the button cannot be pressed now, or `''` when it can. */
    disabled?: string;
    children: Snippet;
  } = $props();
</script>

<button {...rest} disabled={disabled !== ''} data-why={disabled || undefined}
  >{@render children()}</button
>
