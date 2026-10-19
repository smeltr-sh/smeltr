<!--
  A group's hue, picked off the wheel (STYLE_GUIDE.md, "Group hues").

  Hue alone: the lightness and chroma come from the theme, the same ones
  `--accent` is drawn at, so a group is that color turned rather than a
  color of its own. Any hue is allowed -- an admin changing one has something
  specific in mind -- but the three that already mean something are marked, so
  choosing one of them is a choice rather than an accident.
-->
<script lang="ts">
  import Field from './Field.svelte';

  let { value = $bindable(0), label }: { value: number; label: string } = $props();

  /** Where `--error`, `--accent` and `--warn` sit, so a group is not given one by mistake.
   * The accent's is the palette's, so it is read from the theme. */
  const TAKEN = [
    { hue: '25', what: 'error' },
    { hue: 'var(--accent-h)', what: 'accent' },
    { hue: '85', what: 'warn' },
  ];

  const WHEEL = Array.from(
    { length: 13 },
    (_, i) => `oklch(var(--scope-l) var(--scope-c) ${i * 30})`,
  ).join(', ');

  let wheel: HTMLDivElement | undefined = $state();
  let dragging = $state(false);

  /** The angle of a point about the wheel's center, with 0 at the top and
   * running clockwise -- the way `conic-gradient` lays the colors out, so
   * what is clicked is what is under the pointer. */
  function angleAt(event: PointerEvent) {
    if (!wheel) return;
    const box = wheel.getBoundingClientRect();
    const x = event.clientX - (box.left + box.width / 2);
    const y = event.clientY - (box.top + box.height / 2);
    value = Math.round(((Math.atan2(x, -y) * 180) / Math.PI + 360) % 360);
  }
</script>

<div class="picker">
  <div
    bind:this={wheel}
    class="wheel"
    role="slider"
    tabindex="-1"
    aria-label="{label} hue"
    aria-valuenow={value}
    aria-valuemin={0}
    aria-valuemax={359}
    style="background: conic-gradient(from 0deg, {WHEEL});"
    onpointerdown={(e) => {
      dragging = true;
      (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
      angleAt(e);
    }}
    onpointermove={(e) => dragging && angleAt(e)}
    onpointerup={() => (dragging = false)}
    onpointercancel={() => (dragging = false)}
  >
    {#each TAKEN as t (t.hue)}
      <span
        class="taken"
        style="transform: rotate(calc({t.hue} * 1deg));"
        title="{t.what} is this hue"
      ></span>
    {/each}
    <span class="hand" style="transform: rotate({value}deg);"></span>
    <span class="eye" style="background: oklch(var(--scope-l) var(--scope-c) {value});"></span>
  </div>
  <div class="degrees">
    <Field label="degrees" note="Any hue. The three the app already uses are marked on the wheel.">
      <input type="number" min="0" max="359" bind:value aria-label="{label} hue in degrees" />
    </Field>
  </div>
</div>

<style>
  .picker {
    display: flex;
    align-items: center;
    gap: 0.9rem;
  }

  .wheel {
    position: relative;
    flex: none;
    width: 84px;
    height: 84px;
    border-radius: 999px;
    border: 1px solid var(--border);
    cursor: crosshair;
    touch-action: none;
  }

  /* Each marker is a spoke rotated about the center, so its own length places it. */
  .taken,
  .hand {
    position: absolute;
    left: 50%;
    top: 0;
    transform-origin: 50% 42px;
    border-radius: 999px;
  }

  .taken {
    width: 2px;
    height: 9px;
    margin-left: -1px;
    background: var(--bg);
  }

  .hand {
    width: 3px;
    height: 42px;
    margin-left: -1.5px;
    background: var(--ink);
  }

  .eye {
    position: absolute;
    inset: 26px;
    border-radius: 999px;
    border: 2px solid var(--bg);
  }

  .degrees {
    max-width: 26ch;
  }

  input {
    width: 5rem;
    font-family: var(--mono);
  }
</style>
