<!--
  A single-line value masked behind a visibility toggle -- the one thing a
  masked field never lets you confirm on its own: that what you typed or
  pasted is actually what you meant to. A value with a line break in it, a
  PEM private key among them, is shown in the clear instead: HTML has no
  masked `textarea`, on screen or to a screen reader, and pretending
  otherwise with a CSS overlay only ever hid the value from sighted eyes.

  The line break is also why a value like that needs a `textarea` at all --
  an `input` strips carriage returns and newlines from its value, so a key
  pasted into one is stored a line long, with no error, and the failure
  arrives as a handshake nobody can explain. This grows into one on its own
  when a paste or a bound value turns out to have a line break, asking
  nothing of the person pasting and nothing of the pages that hold this.
-->
<script lang="ts">
  let {
    value = $bindable(''),
    placeholder = '',
    ariaLabel,
    rows = 6,
    required = false,
  }: {
    value: string;
    placeholder?: string;
    ariaLabel?: string;
    /** How tall it grows once it has to. */
    rows?: number;
    /** Said to a screen reader only, as `aria-required`: the browser does not refuse it empty. */
    required?: boolean;
  } = $props();

  let revealed = $state(false);

  /** A line break in what is held is the whole test: the value came in with one, or a paste brought
   * one. It never goes back to one line within an edit -- a field that collapsed under the cursor
   * would take the rest of the value with it. */
  let grown = $state(false);
  const tall = $derived(grown || value.includes('\n'));

  /** An `input` would drop the line breaks before `bind:value` ever saw them, so the paste is taken
   * here instead, whole. */
  function paste(e: ClipboardEvent) {
    const text = e.clipboardData?.getData('text') ?? '';
    if (!text.includes('\n')) return;
    e.preventDefault();
    const input = e.currentTarget as HTMLInputElement;
    const before = value.slice(0, input.selectionStart ?? value.length);
    const after = value.slice(input.selectionEnd ?? value.length);
    value = before + text + after;
    grown = true;
  }
</script>

<div class="pw" class:tall>
  {#if tall}
    <textarea
      bind:value
      {placeholder}
      {rows}
      spellcheck="false"
      autocomplete="off"
      aria-label={ariaLabel}
      data-bwignore="true"
      data-lpignore="true"
      data-1p-ignore
      data-form-type="other"></textarea>
  {:else}
    <input
      type={revealed ? 'text' : 'password'}
      bind:value
      {placeholder}
      autocomplete="off"
      aria-label={ariaLabel}
      aria-required={required || undefined}
      onpaste={paste}
      data-bwignore="true"
      data-lpignore="true"
      data-1p-ignore
      data-form-type="other"
    />
    <button
      type="button"
      class="reveal"
      onclick={() => (revealed = !revealed)}
      aria-label={revealed ? 'hide the value' : 'show the value'}
      aria-pressed={revealed}
    >
      {#if revealed}
        <svg
          width="15"
          height="15"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        >
          <path d="M2 12c2.5-4.5 6.5-7 10-7s7.5 2.5 10 7c-2.5 4.5-6.5 7-10 7s-7.5-2.5-10-7Z"></path>
          <circle cx="12" cy="12" r="3"></circle>
          <line x1="3" y1="21" x2="21" y2="3"></line>
        </svg>
      {:else}
        <svg
          width="15"
          height="15"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        >
          <path d="M2 12c2.5-4.5 6.5-7 10-7s7.5 2.5 10 7c-2.5 4.5-6.5 7-10 7s-7.5-2.5-10-7Z"></path>
          <circle cx="12" cy="12" r="3"></circle>
        </svg>
      {/if}
    </button>
  {/if}
</div>

<style>
  .pw {
    position: relative;
    display: flex;
    flex: 1 1 auto;
  }

  .pw input {
    width: 100%;
    /* Room for the reveal toggle over the field's end. */
    padding-right: 2.1rem;
  }

  .pw textarea {
    width: 100%;
    font-family: var(--mono);
    font-size: 0.78rem;
    line-height: 1.45;
    resize: vertical;
  }

  .reveal {
    position: absolute;
    right: 0.3rem;
    top: 50%;
    transform: translateY(-50%);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 1.6rem;
    height: 1.6rem;
    padding: 0;
    border: none;
    border-radius: var(--radius);
    background: none;
    color: var(--dim);
    cursor: pointer;
  }

  .reveal:hover {
    color: var(--ink);
  }
</style>
