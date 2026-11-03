<!--
  A button that can be disabled, and then says why (`WhyDisabled`). `disabled` is the reason, so
  nothing disables a button without giving one; `''` leaves it enabled. `npm run check` refuses
  `disabled` on a bare `<button>`, so every button that can be disabled is one of these.
-->
<script module lang="ts">
  /** Why a button is disabled while what it or a button beside it started is still under way. */
  export const BUSY = 'Wait for the running action to finish.';

  // One reason for each cause that buttons share, so the same cause reads the same everywhere.
  export const UNCHANGED = 'Nothing has changed since the last save.';
  export const SAVE_FIRST = 'Save or discard your changes first.';
  export const INCOMPLETE = 'Fill in or correct the fields first.';
  export const LOADING = 'Wait for the page to finish loading.';
  export const FIRST_PAGE = 'This is the first page.';
  export const LAST_PAGE = 'This is the last page.';
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
    children?: Snippet;
  } = $props();
</script>

<button {...rest} disabled={disabled !== ''} data-why={disabled || undefined}
  >{@render children?.()}</button
>
