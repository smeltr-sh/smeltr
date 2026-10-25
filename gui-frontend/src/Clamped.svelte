<!--
  A value shown at most three lines tall. Three lines is enough to recognize a value by; past that it is opened
  deliberately, so one long value cannot push the rest of what is around it out of view. Only a value that runs past
  its clamp offers to open.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    text,
    open,
    ontoggle,
    label,
    gone = false,
    children,
  }: {
    /** The value, whose length the link to open it gives. */
    text: string;
    /** Whether the reader has opened it. The caller keeps this, so it outlives a redraw. */
    open: boolean;
    ontoggle: () => void;
    /** The name the value goes by where it is shown, for the button that opens it. */
    label: string;
    /** Struck through: a value that is no longer there. */
    gone?: boolean;
    /** The value drawn with markup of its own, in place of `text`. */
    children?: Snippet;
  } = $props();

  let overflowing = $state(false);
  let shown = $derived(
    open ? 'Show less' : `Show all ${text.length.toLocaleString('en-US')} characters`,
  );

  /** Says whether the clamped value is cut short, as its width or its text changes. */
  function watch(node: HTMLElement, _text: string) {
    const check = () => {
      if (node.classList.contains('clamped'))
        overflowing = node.scrollHeight > node.clientHeight + 1;
    };
    const observer = new ResizeObserver(check);
    observer.observe(node);
    return { update: check, destroy: () => observer.disconnect() };
  }
</script>

<span class="clamp">
  <span class="value" class:gone class:clamped={!open} use:watch={text}
    >{#if children}{@render children()}{:else}{text}{/if}</span
  >
  {#if open || overflowing}
    <button
      type="button"
      class="more"
      aria-label={`${shown} of ${label}`}
      onclick={(e) => {
        // Opening the value is all the click does, inside a row or an option that a click would choose.
        e.stopPropagation();
        ontoggle();
      }}>{shown}</button
    >
  {/if}
</span>

<style>
  .clamp {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    min-width: 0;
  }

  .value {
    min-width: 0;
    overflow-wrap: anywhere;
    white-space: pre-wrap;
  }

  .clamped {
    display: -webkit-box;
    -webkit-line-clamp: 3;
    line-clamp: 3;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }

  .gone {
    text-decoration: line-through;
    color: var(--dim);
  }

  .more {
    flex-shrink: 0;
    border: none;
    background: none;
    padding: 0;
    font: inherit;
    color: var(--accent);
    cursor: pointer;
  }

  .more:hover {
    text-decoration: underline;
    text-underline-offset: 2px;
  }
</style>
