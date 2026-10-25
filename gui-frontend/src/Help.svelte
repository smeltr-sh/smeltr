<!--
  The explainer register (STYLE_GUIDE.md, "Text"): a `?` beside a label
  that opens the long form on demand. A native `popover`, so it draws in
  the top layer -- no ancestor can clip it, and Escape and click-away
  come from the platform rather than from an effect here.

  The `?` is a span with a button's role, not a `<button>`: a button is
  labelable, so inside a `<label>` it would become the label's control,
  taking the field's name and its clicks. A span leaves the label to the
  field. Without `popovertarget` the platform no longer toggles the
  popover from it, so the handlers below do. It is placed under its
  button by `float.ts`.
-->
<script module lang="ts">
  let seq = 0;
</script>

<script lang="ts">
  import type { Snippet } from 'svelte';
  import { follow } from './float';

  let { label, children }: { label: string; children: Snippet } = $props();

  const id = `help-${++seq}`;
  let ask: HTMLSpanElement;
  let pop: HTMLDivElement;
  let open = $state(false);

  // Light dismiss closes an open popover on the press that lands on the `?`, before its click,
  // so the click must not open it again.
  let openAtPress = false;

  let stop: (() => void) | undefined;
</script>

<span
  bind:this={ask}
  role="button"
  tabindex="0"
  class="ask"
  aria-label="About {label}"
  aria-controls={id}
  aria-expanded={open}
  onpointerdown={() => (openAtPress = open)}
  onclick={(e) => {
    // Inside a label or a summary, a click would also focus the field or fold the section.
    e.preventDefault();
    pop.togglePopover(!openAtPress);
    openAtPress = false;
  }}
  onkeydown={(e) => {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    e.preventDefault();
    pop.togglePopover();
  }}>?</span
>
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
        ? follow(pop, () => ask.getBoundingClientRect()).stop
        : undefined;
  }}
  ontoggle={(e) => (open = (e as ToggleEvent).newState === 'open')}
>
  {@render children()}
</div>

<style>
  .ask {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    box-sizing: border-box;
    flex: none;
    width: 1.15em;
    height: 1.15em;
    padding: 0;
    margin-left: 0.3rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: none;
    color: var(--dim);
    font: inherit;
    font-size: var(--text-note);
    font-weight: 400;
    line-height: 1;
    cursor: help;
  }

  .ask:hover {
    color: var(--ink);
    border-color: var(--ink);
  }

  /* No outline of its own, so the accent border is the focus ring. */
  .ask:focus-visible {
    color: var(--accent);
    border-color: var(--accent);
  }

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
    /* The popover is a child of whatever holds its button -- a caps
       heading, a nowrap table header -- and inherits text from it. */
    text-transform: none;
    letter-spacing: normal;
    white-space: normal;
    text-align: start;
    /* A detached overlay, the exception STYLE_GUIDE's "Flat" rule names. */
    box-shadow: 0 4px 14px rgb(0 0 0 / 0.18);
  }
</style>
