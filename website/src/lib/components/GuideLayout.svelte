<script lang="ts">
  import type { Snippet } from 'svelte';
  import DocsLayout from '#lib/components/DocsLayout.svelte';

  let {
    title,
    description,
    sections,
    children,
  }: {
    title: string;
    description: string;
    sections: { id: string; title: string }[];
    children: Snippet;
  } = $props();
</script>

<svelte:head>
  <title>{title} — smeltr</title>
  <meta name="description" content={description} />
</svelte:head>

<DocsLayout {sections}>
  <article>
    {@render children()}
  </article>
</DocsLayout>

<style>
  article {
    max-width: 60rem;
    padding-bottom: 2rem;
  }
  :global(article h1) {
    margin: 0.35rem 0 1rem;
    font-size: clamp(2.1rem, 4vw, 3.25rem);
    letter-spacing: -0.03em;
    line-height: 1.08;
  }
  :global(article h2) {
    margin-top: 3rem;
    font-size: clamp(1.45rem, 2.4vw, 1.9rem);
    letter-spacing: -0.02em;
  }
  :global(article h3) {
    margin-top: 2.4rem;
    font-size: 1.15rem;
  }
  :global(article h4) {
    margin-top: 2rem;
  }
  :global(article p),
  :global(article li) {
    color: var(--ink);
    line-height: 1.65;
  }
  :global(article a) {
    color: var(--ink);
    text-decoration: underline;
    text-underline-offset: 2px;
  }
  :global(article code) {
    padding: 0.12em 0.32em;
    border-radius: var(--radius);
    background: var(--panel);
    color: var(--ink);
    font-size: 0.9em;
  }
  :global(article pre) {
    overflow-x: auto;
    margin: 1.25rem 0;
    padding: 1rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  :global(article pre code) {
    padding: 0;
    background: none;
  }
  :global(.eyebrow) {
    margin: 0;
    color: var(--dim);
    font-family: var(--mono);
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  :global(.lede) {
    font-size: 1.14rem;
  }
  :global(.callout) {
    margin: 1.5rem 0;
    padding: 1rem 1.1rem;
    border-left: 3px solid var(--border);
    background: var(--panel);
  }
  :global(.callout p) {
    margin: 0;
  }
  :global(.steps) {
    padding-left: 1.35rem;
  }
  :global(article .checklist) {
    list-style: none;
    padding-left: 0;
  }
  :global(article .checklist li) {
    position: relative;
    padding-left: 1.6rem;
    margin: 0.6rem 0;
  }
  :global(article .checklist li::before) {
    content: '';
    position: absolute;
    left: 0;
    top: 0.4em;
    width: 0.8rem;
    height: 0.8rem;
    border: 1px solid var(--dim);
    border-radius: 2px;
  }
</style>
