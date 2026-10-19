<!--
  The title row every page opens with: the page's name, an optional
  explainer on it, and a page's own buttons as children, landing at the
  row's right edge. Which pages a section holds -- and the rail that
  answers it -- is `SectionShell`'s concern, not this row's.

  The title names the page. `help` is optional context when the task is not
  clear from that title. `beside` is for something that acts on the title
  itself, which belongs next to it rather than among the page's own buttons.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import Help from './Help.svelte';

  let {
    title,
    help,
    beside,
    children,
  }: { title: string; help?: Snippet; beside?: Snippet; children?: Snippet } = $props();
</script>

<header class="head">
  <h1>{title}</h1>
  {#if help}<Help label={title}>{@render help()}</Help>{/if}
  {@render beside?.()}
  <span class="spacer"></span>
  {@render children?.()}
</header>

<style>
  .head {
    display: flex;
    align-items: baseline;
    flex-wrap: wrap;
    row-gap: 0.5rem;
    gap: 0.75rem;
    margin-bottom: 1rem;
  }

  h1 {
    margin: 0;
    font-family: var(--font-display);
    font-size: var(--text-title);
    line-height: 1.2;
  }

  .spacer {
    flex-grow: 1;
  }
</style>
