<!--
  The explainer register (STYLE_GUIDE.md, "Text"): a `?` beside a label
  that opens the long form on demand, in a `Popover`.

  The `?` is a span with a button's role, not a `<button>`: a button is
  labelable, so inside a `<label>` it would become the label's control,
  taking the field's name and its clicks. A span leaves the label to the
  field. Without `popovertarget` the platform no longer toggles the
  popover from it, so the handlers below do.
-->
<script module lang="ts">
  let seq = 0;
</script>

<script lang="ts">
  import type { Snippet } from 'svelte';
  import Popover from './Popover.svelte';

  let { label, children }: { label: string; children: Snippet } = $props();

  const id = `help-${++seq}`;
  let ask: HTMLSpanElement;
  let pop: Popover;
  let open = $state(false);

  // Light dismiss closes an open popover on the press that lands on the `?`, before its click,
  // so the click must not open it again.
  let openAtPress = false;
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
    pop.toggle(!openAtPress);
    openAtPress = false;
  }}
  onkeydown={(e) => {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    e.preventDefault();
    pop.toggle();
  }}>?</span
>
<Popover bind:this={pop} bind:open {id} anchor={() => ask}>
  {@render children()}
</Popover>

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
</style>
