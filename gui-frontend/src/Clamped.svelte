<!--
  A value shown at most three lines tall. Three lines is enough to recognize a value by; past that it is opened
  deliberately, so one long value cannot push the rest of what is around it out of view. Only a value that runs past
  its clamp offers to open.
-->
<script lang="ts">
  let {
    text,
    open,
    ontoggle,
    label,
    gone = false,
  }: {
    text: string;
    /** Whether the reader has opened it. The caller keeps this, so it outlives a redraw. */
    open: boolean;
    ontoggle: () => void;
    /** What the value is, for the button that opens it. */
    label: string;
    /** Struck through: a value that is no longer there. */
    gone?: boolean;
  } = $props();

  let overflowing = $state(false);

  /** Says whether the clamped value is cut short, as its width changes. */
  function watch(node: HTMLElement) {
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
  <span class="value" class:gone class:clamped={!open} use:watch={text}>{text}</span>
  {#if open || overflowing}
    <button
      type="button"
      class="more"
      aria-expanded={open}
      aria-label={`${open ? 'collapse' : 'expand'} the value of ${label}`}
      onclick={ontoggle}>{open ? 'less' : `Show all ${text.length.toLocaleString('en-US')} characters`}</button
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
