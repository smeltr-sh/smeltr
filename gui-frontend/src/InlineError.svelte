<!--
  An error beside the control it belongs to: a rename refused under its box, a probe that failed under its grid. The
  call behind it is `quiet`, so the error bar does not show it too. A save or a load is not this: its error is the
  bar's (`ErrorBar.svelte`).
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    message = '',
    inline = false,
    children,
  }: {
    /** Nothing is drawn while it is empty. */
    message?: string;
    /** A `<span>` in a line of text, rather than a paragraph of its own. */
    inline?: boolean;
    /** In place of `message`, for an error that holds more than text. */
    children?: Snippet;
  } = $props();
</script>

{#if children || message}
  <svelte:element this={inline ? 'span' : 'p'} class="error" role="alert">
    {#if children}{@render children()}{:else}{message}{/if}
  </svelte:element>
{/if}

<style>
  /* It sits in its control's own layout, which spaces it. */
  p {
    margin: 0;
  }

  /* A link in an error is part of what it says. */
  .error :global(a) {
    color: inherit;
  }
</style>
