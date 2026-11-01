<script lang="ts">
  import { page } from '$app/state';
  import type { Group, Link } from '#lib/nav.ts';

  /** `label` names the sidebar, `links` come before its groups. */
  let { label, links, groups }: { label: string; links: Link[]; groups: Group[] } = $props();

  function holds(group: Group, path: string): boolean {
    return group.links.some((e) => ('href' in e ? [e] : e.links).some((l) => l.href === path));
  }
  /** The groups a reader folded or opened, by title. */
  let flipped = $state(new Set<string>());
  function isOpen(group: Group): boolean {
    const starts = !group.closed || holds(group, page.url.pathname);
    return starts !== flipped.has(group.title);
  }
  function flip(group: Group) {
    const next = new Set(flipped);
    if (next.has(group.title)) next.delete(group.title);
    else next.add(group.title);
    flipped = next;
  }
</script>

{#snippet navLink(link: Link)}
  <a href={link.href} aria-current={page.url.pathname === link.href ? 'page' : undefined}
    >{link.title}</a
  >
{/snippet}

<nav aria-label={label}>
  <p class="nav-label">{label}</p>
  {#each links as link (link.href)}
    {@render navLink(link)}
  {/each}
  {#each groups as group (group.title)}
    <button class="nav-label fold" aria-expanded={isOpen(group)} onclick={() => flip(group)}
      >{group.title} <span aria-hidden="true">{isOpen(group) ? '▾' : '▸'}</span></button
    >
    {#each isOpen(group) ? group.links : [] as entry ('href' in entry ? entry.href : entry.title)}
      {#if 'href' in entry}
        {@render navLink(entry)}
      {:else}
        <p class="sub-label">{entry.title}</p>
        <div class="sub-links">
          {#each entry.links as link (link.href)}
            {@render navLink(link)}
          {/each}
        </div>
      {/if}
    {/each}
  {/each}
</nav>

<style>
  nav {
    display: grid;
    gap: 0.2rem;
  }
  .nav-label {
    margin: 0.8rem 0 0.35rem;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
  }
  nav > .nav-label:first-child {
    margin-top: 0;
  }
  .sub-label {
    margin: 0.35rem 0 0;
    padding: 0.2rem 0.65rem;
    font-size: 0.87rem;
    font-weight: 600;
    color: var(--ink);
  }
  /* A heading's pages hang off a guide line; the current page's marker covers it. */
  .sub-links {
    display: grid;
    gap: 0.1rem;
    margin-left: 0.9rem;
    border-left: 1px solid var(--border);
  }
  .sub-links a {
    margin-left: -1px;
    padding-left: 0.75rem;
  }
  .fold {
    display: block;
    width: auto;
    border: 0;
    border-top: 1px solid var(--border);
    border-radius: 0;
    padding: 0.9rem 0 0;
    background: none;
    font: inherit;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
    text-align: left;
    cursor: pointer;
  }
  .fold:hover {
    color: var(--ink);
  }
  a {
    padding: 0.32rem 0.65rem;
    border-left: 2px solid transparent;
    color: var(--dim);
    font-size: 0.87rem;
    text-decoration: none;
  }
  a:hover {
    color: var(--ink);
    background: var(--panel);
  }
  a[aria-current] {
    color: var(--ink);
    border-left-color: var(--ink);
    background: var(--panel);
    font-weight: 600;
  }
</style>
