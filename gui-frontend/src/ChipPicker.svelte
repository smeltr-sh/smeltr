<!--
  A chip that opens a panel of choices: the branch, the identity and the scope are all one of
  these, so the chrome is written once and each caller supplies only what is its own -- the
  chip's face, and the choices.

  The panel is dismissed by clicking away from it or pressing Escape, the two ways anyone expects
  one to close. Capture-phase, so a click lands on whatever it was aimed at rather than being
  spent shutting this; and pointer-down rather than click, so it closes as the press begins.
  Clicking another chip closes this one and opens that one, since each sits in its own wrapper
  and is therefore outside the other's.

  Choosing is the caller's: it owns `open`, so the panel closes when the choice has been acted on
  rather than whenever one was pressed.

  The panel floats from its chip (`float.ts`), so a scrolling table around a row's chip cannot clip
  it. A compact panel -- a row's own actions -- takes keyboard focus as it opens and gives it back to
  the chip as it closes, as a menu does.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import { float } from './float';

  let {
    open = $bindable(false),
    kind,
    align = 'right',
    compact = false,
    title,
    ariaLabel,
    onopen,
    face,
    children,
  }: {
    open?: boolean;
    /** Which chip this is. Styles the face, nothing else. */
    kind: 'branch' | 'identity' | 'scope' | 'columns' | 'row' | 'step' | 'model' | 'error';
    /** The edge the panel hangs from. `left` where the chip sits in a column narrower than the
     * panel, which would otherwise open off the side of the window. */
    align?: 'left' | 'right';
    /** A narrower panel, for a handful of short choices -- a row's own actions -- not the wide
     * list `branch`, `identity` and `scope` hold. It takes focus as it opens. */
    compact?: boolean;
    title?: string;
    ariaLabel?: string;
    /** Run when the panel opens, for a caller that reads its choices fresh. */
    onopen?: () => void;
    face: Snippet;
    children: Snippet;
  } = $props();

  let wrapper = $state<HTMLElement | undefined>();
  let chip = $state<HTMLButtonElement | undefined>();
  let picker = $state<HTMLElement | undefined>();

  function toggle() {
    open = !open;
    if (open) onopen?.();
  }

  $effect(() => {
    if (!open) return;
    if (compact) picker?.querySelector<HTMLElement>('.pick')?.focus({ preventScroll: true });
    const away = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (wrapper?.contains(target)) return;
      open = false;
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      open = false;
      // The key is spent: the page behind does not also leave.
      event.preventDefault();
    };
    // It closes, too, when its chip stops being drawn -- a narrow step's head widening until it
    // unfolds -- since the panel would stand with nothing to hand focus back to.
    const undrawn = new ResizeObserver(([entry]) => {
      if (entry.contentRect.width === 0) open = false;
    });
    // A panel belongs to the page it was opened on, and would cover the next one's controls.
    const left = () => (open = false);
    document.addEventListener('pointerdown', away, true);
    document.addEventListener('keydown', escape);
    window.addEventListener('hashchange', left);
    if (compact && chip) undrawn.observe(chip);
    return () => {
      undrawn.disconnect();
      document.removeEventListener('pointerdown', away, true);
      document.removeEventListener('keydown', escape);
      window.removeEventListener('hashchange', left);
      if (compact) chip?.focus();
    };
  });
</script>

<div class="chip-picker" bind:this={wrapper}>
  <button
    type="button"
    class={['chip', kind, kind === 'error' && 'error-button']}
    bind:this={chip}
    onclick={toggle}
    aria-expanded={open}
    {title}
    aria-label={ariaLabel}
  >
    {@render face()}
  </button>
  {#if open}
    <div
      class="picker"
      class:compact
      popover="manual"
      bind:this={picker}
      {@attach float(() => chip?.getBoundingClientRect(), { edge: align })}
    >
      {@render children()}
    </div>
  {/if}
</div>

<style>
  .chip {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.2rem 0.6rem;
    border-width: 1px;
    border-style: solid;
    border-radius: 999px;
    font-family: inherit;
    font-size: var(--text-note);
    cursor: pointer;
  }

  /* An error's face takes its colors from app.css's `.error-button`. */
  .chip:where(:not(.error)) {
    border-color: var(--border);
    background: var(--bg);
    color: var(--dim);
  }

  .chip.branch {
    color: var(--ink);
    max-width: 12rem;
    overflow: hidden;
  }

  .chip.identity,
  .chip.model {
    color: var(--ink);
  }

  .chip.scope {
    color: oklch(var(--scope-l) var(--scope-c) var(--scope-h));
    border-color: color-mix(
      in oklch,
      oklch(var(--scope-l) var(--scope-c) var(--scope-h)) 40%,
      var(--border)
    );
  }

  /* Sits in a row of ordinary buttons rather than among the header's chips,
     so it takes their shape instead of a pill's. */
  .chip.columns {
    border-radius: var(--radius);
    padding: 0.3rem 0.7rem;
    color: var(--ink);
    font-size: var(--text-label);
  }

  /* What failed, in a row of ordinary buttons: their shape. */
  .chip.error {
    border-radius: var(--radius);
    padding: 0.3rem 0.7rem;
    font-size: var(--text-label);
  }

  /* A table row's own actions. An icon control, not a pill. It carries no
     value of its own, unlike the others: they each name a branch, an
     identity, or a scope. */
  .chip.row {
    border: none;
    border-radius: var(--radius);
    padding: 0.15rem 0.3rem;
  }

  /* The controls a narrow step folds away. It wears the bordered box of the
     step's controls beside it, sized by the card's `--control-width` and
     `--control-height`, so it reads as one more of them. Top-aligned, or the
     line it sits on adds room for descenders and the head grows. */
  .chip.step {
    justify-content: center;
    width: var(--control-width);
    height: var(--control-height);
    padding: 0;
    border-radius: var(--radius);
    background: none;
    vertical-align: top;
  }

  .chip:hover {
    border-color: var(--ink);
  }

  .chip.row:hover {
    color: var(--ink);
  }

  /* Detached overlay, so it may cast a shadow where a surface may not. */
  .picker {
    padding: 0;
    min-width: 20rem;
    max-width: calc(100vw - 3rem);
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    color: var(--ink);
    box-shadow: 0 6px 18px oklch(0% 0 0 / 0.18);
  }

  .picker.compact {
    min-width: 9rem;
  }

  /* The rows come from the caller, but how a row of this panel looks is the panel's, so they are
     reached through it rather than restated at each call. */
  .picker :global(.pick) {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    width: 100%;
    padding: 0.4rem 0.7rem;
    border: 0;
    border-radius: 0;
    background: none;
    color: var(--ink);
    font: inherit;
    font-size: var(--text-note);
    text-align: left;
    cursor: pointer;
  }

  .picker :global(.pick:hover) {
    background: var(--bg);
  }

  .picker :global(.pick.on) {
    color: var(--ink);
    font-weight: 600;
  }

  .picker :global(.pick.remove) {
    color: var(--error);
  }

  .picker :global(.pick .mono) {
    font-family: var(--mono);
  }

  .picker :global(.pick .dim) {
    color: var(--dim);
  }

  .picker :global(.pick-note) {
    display: block;
    padding: 0.4rem 0.7rem;
    background: var(--bg);
    color: var(--dim);
    font-size: var(--text-note);
  }
</style>
