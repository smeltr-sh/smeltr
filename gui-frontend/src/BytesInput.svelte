<!--
  A byte figure as a size: `64 MiB`, `4 GB`, or bare bytes. The field shows the exact form the bundle will carry and
  takes whatever `parseBytes` reads. Text the parser refuses stays in the field and is named under it; the stored
  value does not move until the text reads again. Emptied, the field puts the value back to absent, which is the
  default. An absent value shows as an empty field with a hint in it: the default, or `auto` for a figure the worker
  sizes to its memory. The hint is never text, so a field left alone is never written.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import { type ByteCount, byteCount, exactBytes, parseBytes } from './bytes';

  let {
    value,
    fallback,
    auto = false,
    onchange,
  }: {
    /** What the bundle holds: absent means `fallback`, or the worker's sizing when `auto`. */
    value: ByteCount | undefined;
    /** The default, in bytes. */
    fallback: number;
    /** The worker sizes an absent value to its memory, so the hint reads `auto` rather than `fallback`. */
    auto?: boolean;
    /** `undefined` clears the field back to absent, which is the default. */
    onchange: (bytes: number | undefined) => void;
  } = $props();

  /** The text `value` reads as: nothing when absent. */
  const shown = (v: ByteCount | undefined) =>
    v === undefined ? '' : exactBytes(byteCount(v, fallback));

  // Seeded once: the field is the reader's while it is being typed in, and a store that rewrote it to `64 MiB` under
  // a half-typed `6` would fight the keyboard.
  let text = $state(untrack(() => shown(value)));
  /** An emptied field is the default, not a refusal. */
  let cleared = $derived(text.trim() === '');
  let bad = $derived(!cleared && parseBytes(text) === undefined);

  // The store moved under the field -- a list keyed by index slid another item in, or a reload put the saved value
  // back -- and the text does not say what it holds. Text that already reads as the value is left alone, so typing
  // is never fought.
  $effect(() => {
    const held = value === undefined ? undefined : byteCount(value, fallback);
    const typed = untrack(() => text);
    if (typed.trim() === '' ? held !== undefined : parseBytes(typed) !== held) text = shown(value);
  });
</script>

<input
  type="text"
  inputmode="decimal"
  spellcheck="false"
  aria-invalid={bad}
  class:bad
  value={text}
  placeholder={auto ? 'auto' : exactBytes(fallback)}
  oninput={(e) => {
    text = e.currentTarget.value;
    if (text.trim() === '') {
      onchange(undefined);
      return;
    }
    const parsed = parseBytes(text);
    if (parsed !== undefined) onchange(parsed);
  }}
/>
{#if bad}
  <small class="note refused"
    >Not a size. Bytes, or a number with B, KB, MB, GB, TB, KiB, MiB, GiB or TiB. Empty is the
    default.</small
  >
{/if}

<style>
  input {
    font-family: var(--mono);
  }

  .bad {
    border-color: var(--error);
  }

  .refused {
    color: var(--error);
    font-size: var(--text-note);
    font-weight: 400;
    line-height: 1.4;
  }
</style>
