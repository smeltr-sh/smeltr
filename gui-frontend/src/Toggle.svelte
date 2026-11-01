<!--
  An on/off setting, drawn as a switch rather than a checkbox: a persistent
  yes/no a page's own Save commits, not one choice among several in a list.
  STYLE_GUIDE.md, "Toggle switch".

  A real `<input type="checkbox">` under the visible track, not a `<button>`
  standing in for one: wrapped in the caller's own `<label>`, a click
  anywhere on the caption reaches it the same way it always reached a plain
  checkbox, with no click handler of this component's own to keep in sync.
-->
<script lang="ts">
  let {
    checked = $bindable(false),
    disabled = false,
    ariaLabel,
    onchange,
  }: {
    checked?: boolean;
    disabled?: boolean;
    ariaLabel?: string;
    /** After a person flips it, with `checked` already the new value. */
    onchange?: () => void;
  } = $props();
</script>

<span class="toggle">
  <input type="checkbox" role="switch" bind:checked {disabled} aria-label={ariaLabel} {onchange} />
  <span class="track" aria-hidden="true"><span class="thumb"></span></span>
</span>

<style>
  .toggle {
    --w: 2.3rem;
    --h: 1.3rem;
    position: relative;
    display: inline-flex;
    flex-shrink: 0;
    width: var(--w);
    height: var(--h);
  }

  .toggle input {
    position: absolute;
    inset: 0;
    margin: 0;
    opacity: 0;
    cursor: pointer;
  }

  .toggle input:disabled {
    cursor: default;
  }

  .track {
    position: absolute;
    inset: 0;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--bg);
    pointer-events: none;
  }

  input:checked + .track {
    background: var(--accent);
    border-color: var(--accent);
  }

  input:disabled + .track {
    opacity: 0.5;
  }

  input:focus-visible + .track {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }

  .thumb {
    position: absolute;
    top: 1px;
    left: 1px;
    width: calc(var(--h) - 4px);
    height: calc(var(--h) - 4px);
    border-radius: 50%;
    background: var(--panel);
    border: 1px solid var(--border);
  }

  input:checked + .track .thumb {
    transform: translateX(calc(var(--w) - var(--h)));
    background: var(--on-accent);
    border-color: transparent;
  }
</style>
