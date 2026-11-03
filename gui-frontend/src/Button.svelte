<!--
  A button that can be disabled, and then says why: on hover, and on a press, which a disabled
  button otherwise ignores. `disabled` is the reason, so nothing disables a button without giving
  one; `''` leaves it enabled. Inside a `Locked`, the lock's reason disables it. `npm run check`
  refuses `disabled` on a bare `<button>`, so every button that can be disabled is one of these.

  It stays a native disabled button, which a form does not submit and a `Locked` fieldset disables
  alike; the browser still hands it pointer events, which is all the reason needs.
-->
<script module lang="ts">
  let seq = 0;

  /** Why a button is disabled while what it or a button beside it started is still under way. */
  export const BUSY = 'Wait for the last step to finish.';
</script>

<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { HTMLButtonAttributes } from 'svelte/elements';
  import { lockedBecause } from './Locked.svelte';
  import Popover from './Popover.svelte';

  let {
    disabled = '',
    children,
    ...rest
  }: Omit<HTMLButtonAttributes, 'disabled' | 'children'> & {
    /** Why the button cannot be pressed now, or `''` when it can. */
    disabled?: string;
    children: Snippet;
  } = $props();

  const id = `why-${++seq}`;
  const locked = lockedBecause();
  let why = $derived(locked || disabled);
  let button: HTMLButtonElement;
  let pop = $state<Popover>();
</script>

<button
  bind:this={button}
  {...rest}
  disabled={why !== ''}
  aria-describedby={why ? id : undefined}
  onpointerenter={(e) => {
    if (e.pointerType === 'mouse') pop?.toggle(true);
  }}
  onpointerleave={(e) => {
    if (e.pointerType === 'mouse') pop?.toggle(false);
  }}
  onpointerdown={() => pop?.toggle(true)}>{@render children()}</button
>{#if why}<Popover bind:this={pop} {id} anchor={() => button}>{why}</Popover>{/if}
