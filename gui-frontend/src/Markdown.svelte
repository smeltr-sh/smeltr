<!--
  Markdown the agent wrote, rendered: its paragraphs, lists, code, tables and links, in the face around it.
  `renderMarkdown` decides what is safe to show. With `charts`, a link to a Metrics view that stands alone in its
  paragraph shows that chart above it, read with the viewer's own access.
-->
<script lang="ts">
  import { mount, unmount } from 'svelte';
  import { renderMarkdown } from './markdown';
  import { viewFromParams } from './metricsView';

  let { text, charts = false }: { text: string; charts?: boolean } = $props();
  let body = $state<HTMLElement>();
  let html = $derived(renderMarkdown(text));

  $effect(() => {
    void html;
    if (!charts || !body) return;
    const slots = [...body.querySelectorAll<HTMLElement>('[data-chart]')];
    if (!slots.length) return;
    let gone = false;
    const mounted: Record<string, unknown>[] = [];
    void import('./MetricsPage.svelte').then(({ default: MetricsPage }) => {
      if (gone) return;
      for (const slot of slots) {
        const query = (slot.dataset.chart ?? '').split('?')[1] ?? '';
        mounted.push(
          mount(MetricsPage, {
            target: slot,
            anchor: slot.firstChild ?? undefined,
            props: { view: viewFromParams(new URLSearchParams(query)), inline: true },
          }),
        );
      }
    });
    return () => {
      gone = true;
      for (const m of mounted) void unmount(m);
    };
  });
</script>

<div class="markdown" bind:this={body}>{@html html}</div>

<style>
  /* Wherever it sits, it fills the room it is given, and keeps its own white space: the HTML's line breaks are
     not the text's. */
  .markdown {
    flex: 1 1 auto;
    min-width: 0;
    white-space: normal;
    overflow-wrap: anywhere;
    color: var(--ink);
    line-height: 1.5;
  }
  .markdown > :global(:first-child) {
    margin-top: 0;
  }
  .markdown > :global(:last-child) {
    margin-bottom: 0;
  }
  .markdown :global(:is(p, ul, ol, pre, blockquote, table, .chart-slot)) {
    margin: 0 0 0.6em;
  }
  .markdown :global(:is(ul, ol)) {
    padding-left: 1.4em;
  }
  .markdown :global(li > :is(ul, ol)) {
    margin: 0;
  }
  /* A heading in a reply is a label within it, never the page's title. */
  .markdown :global(:is(h1, h2, h3, h4, h5, h6)) {
    margin: 0.8em 0 0.3em;
    font-family: inherit;
    font-size: 1em;
    font-weight: 600;
    letter-spacing: normal;
  }
  .markdown :global(:is(h1, h2)) {
    font-size: 1.1em;
  }
  .markdown :global(code) {
    font-family: var(--mono);
    font-size: 0.92em;
  }
  .markdown :global(pre) {
    overflow-x: auto;
    padding: 0.5em 0.7em;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
    overflow-wrap: normal;
  }
  .markdown :global(pre code) {
    font-size: var(--text-note);
  }
  .markdown :global(blockquote) {
    padding-left: 0.8em;
    border-left: 3px solid var(--border);
  }
  /* A wide table scrolls by itself rather than widening the conversation. */
  .markdown :global(table) {
    display: block;
    max-width: 100%;
    overflow-x: auto;
    border-collapse: collapse;
    overflow-wrap: normal;
  }
  .markdown :global(:is(th, td)) {
    padding: 0.25em 0.6em;
    border: 1px solid var(--border);
    text-align: left;
  }
  .markdown :global(th) {
    font-weight: 600;
  }
  .markdown :global(hr) {
    border: 0;
    border-top: 1px solid var(--border);
    margin: 0.8em 0;
  }
</style>
