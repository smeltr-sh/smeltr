<script lang="ts">
  import '#lib/foundry/palette.css';
  import '#lib/foundry/controls.css';
  import '../app.css';
  import '#lib/marketing.css';
  import { page } from '$app/state';
  import Logo from '#lib/components/Logo.svelte';
  import ThemeToggle from '#lib/components/ThemeToggle.svelte';
  import { source } from '#lib/release.ts';

  let { children } = $props();

  function measureHeader(node: HTMLElement) {
    const update = () =>
      document.documentElement.style.setProperty(
        '--site-header-height',
        node.getBoundingClientRect().height + 'px',
      );
    const observer = new ResizeObserver(update);
    observer.observe(node);
    update();
    return { destroy: () => observer.disconnect() };
  }
</script>

<svelte:head>
  <!-- One address per page for search engines, whichever host, scheme or slash a link used. -->
  <link rel="canonical" href={`https://smeltr.sh${page.url.pathname}`} />
</svelte:head>

<a class="skip-link" href="#main">Skip to content</a>

<header use:measureHeader>
  <nav aria-label="Main navigation">
    <a class="brand" href="/">
      <Logo size={22} />
      <span>smeltr</span>
    </a>

    <div class="links">
      {#each [['/siem-costs', 'Cut SIEM costs'], ['/one-schema', 'One schema'], ['/feed-health', 'Feed health'], ['/how-it-works', 'How it works']] as [href, label] (href)}
        <a {href} aria-current={page.url.pathname === href ? 'page' : undefined}>{label}</a>
      {/each}
      <a
        href="/docs"
        aria-current={page.url.pathname === '/docs' || page.url.pathname.startsWith('/docs/')
          ? 'page'
          : undefined}>Docs</a
      >
      <a href="/get" aria-current={page.url.pathname === '/get' ? 'page' : undefined}>Get smeltr</a>
    </div>
    <div class="theme-control"><ThemeToggle /></div>
  </nav>
</header>

<main id="main">
  {@render children()}
</main>

<footer>
  <div>
    <a class="brand" href="/">
      <Logo size={20} />
      <span>smeltr</span>
    </a>
  </div>
  <p>
    <a href="/releases">Releases</a> · <a href={source}>Source code</a> ·
    <a href="mailto:support@smeltr.sh">support@smeltr.sh</a>
  </p>
</footer>

<style>
  header {
    position: sticky;
    top: 0;
    z-index: 10;
    background: var(--band-bg);
    border-bottom: 1px solid var(--band-line);
  }
  /* The band is dark in both themes, so its own pieces read the band's
     tokens. Set on the pieces, not the header, so a menu one opens keeps
     the page's. The logo's cutout takes --bg and so matches the band. */
  header .brand,
  .links,
  .theme-control {
    --bg: var(--band-bg);
    --ink: var(--band-ink);
    --dim: var(--band-dim);
    --border: var(--band-line);
  }
  nav,
  footer {
    max-width: 72rem;
    margin: 0 auto;
    padding: 0.85rem 1.5rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
  }
  nav {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto;
  }
  .theme-control {
    display: flex;
  }
  .brand {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    color: var(--ink);
    text-decoration: none;
    font-family: var(--font-display);
    font-weight: 700;
    font-size: 1.15rem;
  }
  .links {
    justify-content: flex-end;
    display: flex;
    align-items: center;
    gap: 1.15rem;
    font-size: 0.9rem;
  }
  .links a {
    color: var(--dim);
    text-decoration: none;
  }
  /* The one accent on a page with no primary action. */
  .links a[aria-current='page'] {
    color: var(--ink);
    font-weight: 600;
    text-decoration: underline 3px var(--accent);
    text-underline-offset: 0.4rem;
  }
  .links a:hover {
    color: var(--ink);
  }
  main {
    min-height: 70vh;
  }
  footer {
    border-top: 1px solid var(--border);
    margin-top: 4rem;
    padding-top: 2rem;
    padding-bottom: 2rem;
    color: var(--dim);
    font-size: 0.9rem;
  }
  footer .brand {
    font-size: 1rem;
  }
  footer p {
    margin: 0.35rem 0 0;
  }
  @media (max-width: 55rem) {
    nav {
      align-items: center;
    }
    .links {
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 0.6rem 0.9rem;
    }
  }
  @media (max-width: 32rem) {
    nav,
    footer {
      padding-inline: 1rem;
    }
    nav {
      grid-template-columns: 1fr auto;
      gap: 0.5rem 1rem;
    }
    .theme-control {
      grid-column: 2;
      grid-row: 1;
    }
    .links {
      grid-column: 1 / -1;
      grid-row: 2;
      width: 100%;
      justify-content: space-between;
    }
    footer {
      display: block;
    }
  }
</style>
