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

  A compact panel -- a row's own actions -- portals to `document.body` and positions itself off
  the trigger's own rect, fixed to the viewport. A row lives inside a scrolling table, and
  `overflow` on that wrapper clips any descendant regardless of its own `position`; only actually
  leaving the DOM subtree escapes it. It closes on scroll rather than trying to track the trigger,
  since a fixed panel does not move with the page under it. It opens above the trigger instead of
  below when below would run off the viewport, and moves keyboard focus in on open and back to the
  trigger on close, since the portal would otherwise put it after the rest of the page in tab
  order.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

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
     * list `branch`, `identity` and `scope` hold. Also what turns on the portal: those wide
     * panels live in the page header, never inside a scrolling wrapper, and stay as they are. */
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
  let fixedStyle = $state('');

  function toggle() {
    open = !open;
    if (open) onopen?.();
  }

  /** Below the trigger, unless the panel would run off the bottom of the viewport and there is
   * more room above it than below -- then above instead. Either way, clamped so the panel never
   * itself runs past the top or bottom edge. */
  function position() {
    if (!chip) return;
    const margin = 6;
    const rect = chip.getBoundingClientRect();
    const panelHeight = picker?.offsetHeight ?? 0;
    const spaceBelow = window.innerHeight - rect.bottom - margin;
    const spaceAbove = rect.top - margin;
    const above = panelHeight > spaceBelow && spaceAbove > spaceBelow;
    const top = above
      ? Math.max(margin, rect.top - margin - panelHeight)
      : Math.min(rect.bottom + margin, window.innerHeight - margin - panelHeight);
    fixedStyle =
      align === 'left'
        ? `position: fixed; top: ${top}px; left: ${rect.left}px;`
        : `position: fixed; top: ${top}px; right: ${window.innerWidth - rect.right}px;`;
  }

  /** Moves the compact panel to `document.body` once it exists; a plain function rather than an
   * action with an `update`, since `compact` is a prop this component's caller never changes
   * after mounting one. */
  function portal(node: HTMLElement) {
    if (!compact) return {};
    document.body.appendChild(node);
    return {
      destroy() {
        node.remove();
      },
    };
  }

  $effect(() => {
    if (!open) return;
    if (compact) {
      position();
      // A portaled panel sits after the whole app in tab order, so opening it has to move focus
      // in by hand -- otherwise Tab from the trigger walks the rest of the page instead of
      // reaching the panel's own choices. Without scrolling: the position above has not been
      // drawn yet, so the panel still hangs below the viewport, and scrolling to reach it would
      // close it again.
      picker?.querySelector<HTMLElement>('.pick')?.focus({ preventScroll: true });
    }
    const away = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (wrapper?.contains(target)) return;
      if (compact && picker?.contains(target)) return;
      open = false;
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      open = false;
      // The key is spent: the page behind does not also leave.
      event.preventDefault();
    };
    // A fixed panel does not move with whatever scrolled -- closing is simpler and more honest
    // than trying to track the trigger across every ancestor that might scroll.
    const scrolled = () => {
      if (compact) open = false;
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
    if (compact) window.addEventListener('scroll', scrolled, { capture: true, passive: true });
    if (compact && chip) undrawn.observe(chip);
    // A panel whose choices change while open -- a menu stepping into a submenu -- changes height, and where it
    // fits with it.
    const resized = new ResizeObserver(() => position());
    if (compact && picker) resized.observe(picker);
    return () => {
      undrawn.disconnect();
      resized.disconnect();
      document.removeEventListener('pointerdown', away, true);
      document.removeEventListener('keydown', escape);
      window.removeEventListener('hashchange', left);
      if (compact) {
        window.removeEventListener('scroll', scrolled, true);
        chip?.focus();
      }
    };
  });
</script>

<div class="chip-picker" bind:this={wrapper}>
  <button
    type="button"
    class="chip {kind}"
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
      class:left={align === 'left'}
      class:compact
      style={compact ? fixedStyle : undefined}
      bind:this={picker}
      use:portal
    >
      {@render children()}
    </div>
  {/if}
</div>

<style>
  .chip-picker {
    position: relative;
  }

  .chip {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.2rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--bg);
    color: var(--dim);
    font-family: inherit;
    font-size: var(--text-note);
    cursor: pointer;
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

  /* What failed, in a row of ordinary buttons: their shape, in the error's color. */
  .chip.error {
    border-radius: var(--radius);
    padding: 0.3rem 0.7rem;
    border-color: color-mix(in srgb, var(--error) 50%, var(--border));
    background: color-mix(in srgb, var(--error) 10%, transparent);
    color: var(--error);
    font-size: var(--text-label);
    font-weight: 600;
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
    position: absolute;
    right: 0;
    top: calc(100% + 0.35rem);
    z-index: 20;
    min-width: 20rem;
    max-width: calc(100vw - 3rem);
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    box-shadow: 0 6px 18px oklch(0% 0 0 / 0.18);
    overflow: hidden;
  }

  .picker.left {
    right: auto;
    left: 0;
  }

  .picker.compact {
    min-width: 9rem;
  }

  /* Below this, the chip sits too close to the edge for a panel anchored to it to stay on
     screen -- anchor to the wider row instead. */
  @media (max-width: 480px) {
    .chip-picker {
      position: static;
    }
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
