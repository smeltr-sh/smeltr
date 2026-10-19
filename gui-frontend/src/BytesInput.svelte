<!--
  A byte figure as a size: `64 MiB`, `4 GB`, or bare bytes. The field shows the exact form the bundle will carry and
  takes whatever `parseBytes` reads. Text the parser refuses stays in the field and is named under it; the stored
  value does not move until the text reads again. Emptied, the field puts the value back to absent, which is the
  default.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import { type ByteCount, byteCount, exactBytes, parseBytes } from './bytes';

  let {
    value,
    fallback,
    onchange,
  }: {
    /** What the bundle holds: absent means `fallback`. */
    value: ByteCount | undefined;
    /** The default, in bytes. */
    fallback: number;
    /** `undefined` clears the field back to absent, which is the default. */
    onchange: (bytes: number | undefined) => void;
  } = $props();

  // Seeded once: the field is the reader's while it is being typed in, and a store that rewrote it to `64 MiB` under
  // a half-typed `6` would fight the keyboard.
  let text = $state(untrack(() => exactBytes(byteCount(value, fallback))));
  /** An emptied field is the default, not a refusal. */
  let cleared = $derived(text.trim() === '');
  let bad = $derived(!cleared && parseBytes(text) === undefined);

  // The store moved under the field -- a list keyed by index slid another item in, or a reload put the saved value
  // back -- and the text does not say what it holds. Text that already reads as the value is left alone, so typing
  // is never fought.
  $effect(() => {
    const held = byteCount(value, fallback);
    if (parseBytes(untrack(() => text)) !== held) text = exactBytes(held);
  });
</script>

<input
  type="text"
  inputmode="decimal"
  spellcheck="false"
  aria-invalid={bad}
  class:bad
  value={text}
  placeholder={exactBytes(fallback)}
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
