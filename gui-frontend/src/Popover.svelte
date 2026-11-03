<!--
  A note floating beside what it is about (`float.ts`). A native `popover`, so it draws in the top
  layer -- no ancestor can clip it, and Escape and click-away come from the platform rather than
  from an effect here.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import { follow } from './float';

  let {
    id,
    anchor,
    open = $bindable(false),
    children,
  }: {
    id: string;
    /** What the note sits under, read each time it opens. */
    anchor: () => Element;
    open?: boolean;
    children: Snippet;
  } = $props();

  let pop: HTMLDivElement;
  let stop: (() => void) | undefined;
  // A page left with the note open drops it without a toggle, and its anchor with it.
  $effect(() => () => stop?.());

  /** Opens the note, closes it, or with no `force` turns it over. */
  export function toggle(force?: boolean): void {
    pop.togglePopover(force);
  }
</script>

<div
  bind:this={pop}
  {id}
  popover
  class="pop"
  onbeforetoggle={(e) => {
    stop?.();
    // Before it is shown, so it lands in place rather than appearing centered and jumping.
    stop =
      (e as ToggleEvent).newState === 'open'
        ? follow(pop, () => anchor().getBoundingClientRect()).stop
        : undefined;
  }}
  ontoggle={(e) => (open = (e as ToggleEvent).newState === 'open')}
>
  {@render children()}
</div>

<style>
  .pop {
    max-width: min(32rem, calc(100vw - 1rem));
    padding: 0.6rem 0.75rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    color: var(--ink);
    font-size: var(--text-note);
    font-weight: 400;
    line-height: 1.55;
    /* The popover is a child of whatever holds what it is about -- a caps heading, a nowrap table
       header -- and inherits text from it. */
    text-transform: none;
    letter-spacing: normal;
    white-space: normal;
    text-align: start;
    /* A detached overlay, the exception STYLE_GUIDE's "Flat" rule names. */
    box-shadow: 0 4px 14px rgb(0 0 0 / 0.18);
  }
</style>
