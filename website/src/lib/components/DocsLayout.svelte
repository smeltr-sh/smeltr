<script module lang="ts">
  // The sidebar is shared by every page, but each route mounts a new instance.
  // Navigation hooks update this only in the browser.
  const navigation = { scrollTop: 0 };
</script>

<script lang="ts">
  import { onMount, type Snippet } from 'svelte';
  import { afterNavigate, beforeNavigate } from '$app/navigation';
  import { page } from '$app/state';
  import DocsSearchForm from './DocsSearchForm.svelte';
  import { stepPages } from '#lib/step-pages.ts';
  import {
    byCategory,
    destinations,
    sourceGroups,
    sources,
    type Connection,
  } from '#lib/connections.ts';

  type Link = { href: string; title: string };
  /** A sidebar entry: a page, or a category heading over its pages. */
  type Entry = Link | { title: string; links: Link[] };
  /** A sidebar group. It starts folded, except on its own pages, and folds or opens on a click. */
  type Group = { title: string; links: Entry[] };
  const groups: Group[] = [
    {
      title: 'Getting started',
      links: [
        { href: '/docs/getting-started', title: 'Set up a deployment' },
        {
          title: 'Install a leader',
          links: [
            { href: '/docs/install-leader/binary', title: 'Binary' },
            { href: '/docs/install-leader/container', title: 'Container' },
            { href: '/docs/install-leader/kubernetes', title: 'Kubernetes' },
          ],
        },
        { href: '/docs/first-login', title: 'First login' },
        { href: '/docs/https', title: 'HTTPS' },
        {
          title: 'Enroll a worker',
          links: [
            { href: '/docs/enroll-worker/host', title: 'On a host' },
            { href: '/docs/enroll-worker/kubernetes', title: 'On Kubernetes' },
            { href: '/docs/enroll-worker/sizing', title: 'Size a worker' },
          ],
        },
      ],
    },
    {
      title: 'Smeltr language',
      links: [{ href: '/docs/code-reference', title: 'Language reference' }],
    },
    {
      title: 'Administration',
      links: [
        { href: '/docs/certificates', title: 'Certificates & TLS' },
        { href: '/docs/accounts', title: 'Accounts & access' },
        { href: '/docs/secrets', title: 'Secrets & credentials' },
        { href: '/docs/reference', title: 'Administrator settings' },
      ],
    },
    {
      title: 'Operations',
      links: [
        { href: '/docs/operations', title: 'Operate the fleet' },
        { href: '/docs/upgrade', title: 'Upgrade' },
        { href: '/docs/release-extras', title: 'Release extras' },
        { href: '/docs/logs-metrics', title: 'Logs & metrics' },
        { href: '/docs/delivery', title: 'Delivery' },
        { href: '/docs/backup-recovery', title: 'Backup & recovery' },
        { href: '/docs/troubleshooting', title: 'Troubleshooting' },
      ],
    },
    {
      title: 'Pipeline workflow',
      links: [
        { href: '/docs/first-pipeline', title: 'First pipeline tutorial' },
        { href: '/docs/review', title: 'Review & deploy' },
        {
          title: 'Git & configuration',
          links: [
            { href: '/docs/git/configuration', title: 'How Git is used' },
            { href: '/docs/git/external-host', title: 'External Git host' },
          ],
        },
        { href: '/docs/capture-sample', title: 'Capture a sample' },
        { href: '/docs/schemas', title: 'Schemas' },
        {
          title: 'Routing',
          links: [
            { href: '/docs/routing/records', title: 'Routing records' },
            { href: '/docs/routing/exceptions', title: 'Exceptions' },
          ],
        },
      ],
    },
  ];
  function categoryLinks(kinds: Connection[], order?: string[]): Entry[] {
    return byCategory(kinds, order).map(({ category, kinds }) => ({
      title: category,
      links: kinds.map(({ href, name }) => ({ href, title: name })),
    }));
  }
  groups.push(
    {
      title: 'Pipeline steps',
      links: [{ href: '/docs/steps', title: 'Overview' }, ...stepPages],
    },
    {
      title: 'Sources',
      links: [
        { href: '/docs/sources', title: 'Overview' },
        ...categoryLinks(
          sources,
          sourceGroups.map((g) => g.category),
        ),
      ],
    },
    {
      title: 'Destinations',
      links: [{ href: '/docs/destinations', title: 'Overview' }, ...categoryLinks(destinations)],
    },
  );

  function holds(group: Group, path: string): boolean {
    return group.links.some((e) => ('href' in e ? [e] : e.links).some((l) => l.href === path));
  }
  /** The groups a reader folded or opened, by title. */
  let flipped = $state(new Set<string>());
  function isOpen(group: Group): boolean {
    return holds(group, page.url.pathname) !== flipped.has(group.title);
  }
  function flip(group: Group) {
    const next = new Set(flipped);
    if (next.has(group.title)) next.delete(group.title);
    else next.add(group.title);
    flipped = next;
  }

  let {
    sections,
    entries = [],
    searchable = true,
    children,
  }: {
    sections: { id: string; title: string }[];
    entries?: { id: string; title: string; summary: string }[];
    searchable?: boolean;
    children: Snippet;
  } = $props();
  let query = $state('');
  const matches = $derived(
    entries.filter((entry) =>
      `${entry.title} ${entry.summary}`.toLowerCase().includes(query.trim().toLowerCase()),
    ),
  );
  let toggle: HTMLButtonElement;
  let toolsToggle = $state<HTMLButtonElement>();
  let toolsExpanded = $state(false);
  let expanded = $state(false);
  let active = $state('');
  let manualNavigation: HTMLElement;

  beforeNavigate(() => {
    // On mobile the menu is part of the document, not a separate scroll area.
    if (manualNavigation && getComputedStyle(manualNavigation).overflowY === 'auto') {
      navigation.scrollTop = manualNavigation.scrollTop;
    }
  });

  afterNavigate(() => {
    if (manualNavigation && getComputedStyle(manualNavigation).overflowY === 'auto') {
      manualNavigation.scrollTop = navigation.scrollTop;
    }
  });

  function closeNavigation(id: string) {
    toolsExpanded = false;
    requestAnimationFrame(() => {
      const heading = document.getElementById(id);
      heading?.setAttribute('tabindex', '-1');
      heading?.focus({ preventScroll: true });
    });
  }

  onMount(() => {
    let frame = 0;
    function update() {
      frame = 0;
      active = sections[0]?.id ?? '';
      for (const { id } of sections) {
        const heading = document.getElementById(id);
        if (
          heading &&
          heading.getBoundingClientRect().top <=
            (parseFloat(
              getComputedStyle(document.documentElement).getPropertyValue('--site-header-height'),
            ) || 80) +
              80
        )
          active = id;
      }
      // A last section too short to scroll its heading that far up is the one in view once the page ends.
      if (window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 1)
        active = sections.at(-1)?.id ?? active;
    }
    function schedule() {
      if (!frame) frame = requestAnimationFrame(update);
    }
    update();
    window.addEventListener('scroll', schedule, { passive: true });
    window.addEventListener('resize', schedule);
    return () => {
      cancelAnimationFrame(frame);
      window.removeEventListener('scroll', schedule);
      window.removeEventListener('resize', schedule);
    };
  });
</script>

<svelte:window
  onkeydown={(event) => {
    if (event.key === 'Escape' && toolsExpanded) {
      toolsExpanded = false;
      toolsToggle?.focus();
    } else if (event.key === 'Escape' && expanded) {
      expanded = false;
      toggle.focus();
    }
  }}
/>

{#snippet navLink(link: Link)}
  <a href={link.href} aria-current={page.url.pathname === link.href ? 'page' : undefined}
    >{link.title}</a
  >
{/snippet}

<div class="docs-layout" class:no-tools={sections.length === 0 && entries.length === 0}>
  <aside class="manual-nav" bind:this={manualNavigation}>
    <DocsSearchForm />
    <button
      bind:this={toggle}
      aria-expanded={expanded}
      aria-controls="docs-navigation"
      onclick={() => (expanded = !expanded)}
    >
      Documentation <span aria-hidden="true">{expanded ? '−' : '+'}</span>
    </button>
    <div id="docs-navigation" class:expanded>
      <nav aria-label="Documentation">
        <p class="nav-label">Documentation</p>
        <a href="/docs" aria-current={page.url.pathname === '/docs' ? 'page' : undefined}
          >Overview</a
        >
        {#each groups as group (group.title)}
          <button
            class="nav-label group-label fold"
            aria-expanded={isOpen(group)}
            onclick={() => flip(group)}
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
    </div>
  </aside>
  <div class="docs-content" data-pagefind-body={searchable ? '' : undefined}>
    {@render children()}
  </div>
  {#if sections.length || entries.length}
    <aside class="page-tools" aria-label="Page tools">
      <button
        bind:this={toolsToggle}
        aria-expanded={toolsExpanded}
        aria-controls="page-tools-content"
        onclick={() => (toolsExpanded = !toolsExpanded)}
      >
        {entries.length ? 'Find a function · On this page' : 'On this page'}
        <span aria-hidden="true">{toolsExpanded ? '−' : '+'}</span>
      </button>
      <div id="page-tools-content" class:expanded={toolsExpanded}>
        {#if searchable}
          <!-- The page's Markdown twin, which the build writes beside it after prerendering: `external`, so
               neither the router nor the prerender's crawl takes it for a route. -->
          <nav class="formats" aria-label="Formats">
            <a href={`${page.url.pathname.replace(/\/$/, '')}.md`} rel="external"
              >View as Markdown</a
            >
          </nav>
        {/if}
        {#if entries.length}
          <div class="finder">
            <label for="function-search">Find a function</label>
            <input
              id="function-search"
              type="search"
              bind:value={query}
              placeholder="Name or keyword…"
            />
            {#if query.trim()}
              <p class="search-status" role="status">
                {matches.length}
                {matches.length === 1 ? 'function' : 'functions'} found
              </p>
              <nav class="results" aria-label="Function search results">
                {#each matches as entry (entry.id)}
                  <a href={`#${entry.id}`} onclick={() => closeNavigation(entry.id)}
                    >{entry.title}</a
                  >
                {/each}
              </nav>
              {#if matches.length === 0}<p class="search-hint">
                  Try a function name or a keyword such as “time”.
                </p>{/if}
            {/if}
          </div>
        {/if}
        <nav class="sections" aria-label="On this page">
          <p class="nav-label">On this page</p>
          {#each sections as section (section.id)}
            <a
              href={`#${section.id}`}
              aria-current={active === section.id ? 'location' : undefined}
              onclick={() => closeNavigation(section.id)}>{section.title}</a
            >
          {/each}
        </nav>
      </div>
    </aside>
  {/if}
</div>

<style>
  .docs-layout {
    max-width: 100rem;
    margin: 0 auto;
    padding: 2.5rem 1.5rem;
    display: grid;
    grid-template-columns: 14rem minmax(0, 1fr) 13rem;
    gap: 2rem;
  }
  aside {
    position: sticky;
    top: calc(var(--site-header-height, 5rem) + 1rem);
    align-self: start;
    max-height: calc(100dvh - var(--site-header-height, 5rem) - 2rem);
    overflow-y: auto;
    scrollbar-gutter: stable;
    /* Room inside the scroll box for a focus outline (2px, offset 2px), which it would otherwise clip. */
    margin: -4px 0 0 -4px;
    padding: 4px 0.75rem 4px 4px;
  }
  nav {
    display: grid;
    gap: 0.2rem;
  }
  .sections {
    border-top: 1px solid var(--border);
    margin-top: 1.5rem;
    padding-top: 1rem;
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
  /* A category's pages hang off a guide line; the current page's marker covers it. */
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
  .group-label {
    border-top: 1px solid var(--border);
    padding-top: 0.9rem;
  }
  nav .fold {
    display: block;
    width: auto;
    border: 0;
    border-top: 1px solid var(--border);
    border-radius: 0;
    padding: 0.9rem 0 0;
    background: none;
    color: var(--dim);
    font: inherit;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    text-align: left;
    cursor: pointer;
  }
  nav .fold:hover {
    color: var(--ink);
  }
  nav a {
    padding: 0.32rem 0.65rem;
    border-left: 2px solid transparent;
    color: var(--dim);
    font-size: 0.87rem;
    text-decoration: none;
  }
  nav a:hover {
    color: var(--ink);
    background: var(--panel);
  }
  nav a[aria-current] {
    color: var(--ink);
    border-left-color: var(--ink);
    background: var(--panel);
    font-weight: 600;
  }
  .docs-content {
    min-width: 0;
  }
  .no-tools {
    grid-template-columns: 14rem minmax(0, 1fr);
  }
  button {
    display: none;
  }
  .finder {
    margin-top: 0;
  }
  .formats + .finder {
    border-top: 1px solid var(--border);
    margin-top: 1.5rem;
    padding-top: 1rem;
  }
  .sections:first-child {
    border-top: 0;
    margin-top: 0;
    padding-top: 0;
  }
  .finder label {
    display: block;
    font-size: 0.85rem;
    font-weight: 600;
    margin-bottom: 0.5rem;
  }
  .finder input {
    width: 100%;
    min-height: 44px;
    padding: 0.6rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--bg);
    color: var(--ink);
    font: inherit;
    font-size: 1rem;
  }
  .search-status,
  .search-hint {
    color: var(--dim);
    font-size: 0.8rem;
  }
  .results {
    max-height: 16rem;
    overflow-y: auto;
  }
  .results a {
    overflow-wrap: anywhere;
  }

  @media (max-width: 80rem) {
    .docs-layout {
      grid-template-columns: 13rem minmax(0, 1fr);
      gap: 1.5rem;
    }
    .manual-nav {
      grid-column: 1;
      grid-row: 1 / 3;
    }
    .page-tools {
      grid-column: 2;
      grid-row: 1;
      position: static;
      max-height: none;
      overflow: visible;
      margin: 0;
      padding: 0 0.75rem 0 0;
    }
    .docs-content {
      grid-column: 2;
      grid-row: 2;
    }
    .no-tools .docs-content {
      grid-row: 1;
    }
    .page-tools button {
      display: flex;
    }
    #page-tools-content {
      display: none;
      padding: 1rem;
      border: 1px solid var(--border);
    }
    #page-tools-content.expanded {
      display: block;
    }
  }
  button {
    align-items: center;
    justify-content: space-between;
    width: 100%;
    gap: 1rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.75rem;
    background: var(--panel);
    color: var(--ink);
    font: inherit;
    cursor: pointer;
  }
  @media (max-width: 60rem) {
    .docs-layout {
      grid-template-columns: minmax(0, 1fr);
      gap: 1rem;
      padding-top: 0.75rem;
    }
    .manual-nav,
    .page-tools,
    .docs-content {
      grid-column: 1;
      grid-row: auto;
    }
    .manual-nav {
      grid-row: 1;
      position: static;
      max-height: none;
      overflow: visible;
      margin: 0;
      padding: 0 0.75rem 0 0;
    }
    .page-tools {
      grid-row: 2;
    }
    .docs-content {
      grid-row: 3;
    }
    .no-tools .docs-content {
      grid-row: 2;
    }
    button {
      display: flex;
    }
    #docs-navigation {
      display: none;
      padding: 1rem;
      border: 1px solid var(--border);
    }
    #docs-navigation.expanded {
      display: block;
    }
  }
  @media (max-width: 32rem) {
    .docs-layout {
      padding-inline: 1rem;
    }
  }
</style>
