<script lang="ts">
  import { onMount } from 'svelte';
  import DocsLayout from './DocsLayout.svelte';
  let {
    title,
    destination,
    links,
    preserveHash = false,
  }: {
    title: string;
    destination: string;
    links: { id: string; title: string; href: string }[];
    preserveHash?: boolean;
  } = $props();
  onMount(() => {
    const section = window.location.hash.slice(1);
    const target = preserveHash
      ? destination + window.location.search + window.location.hash
      : (links.find((link) => link.id === section)?.href ?? destination);
    window.location.replace(target);
  });
</script>

<svelte:head>
  <title>{title} — smeltr</title>
  <meta name="robots" content="noindex" />
</svelte:head>

<DocsLayout sections={[]} searchable={false}>
  <h1>{title}</h1>
  <p>
    Continue to <a href={destination}>the guide</a>{links.length
      ? ', or go directly to a topic:'
      : '.'}
  </p>
  <ul>
    {#each links as link (link.id)}
      <li id={link.id}><a href={link.href}>{link.title}</a></li>
    {/each}
  </ul>
</DocsLayout>
