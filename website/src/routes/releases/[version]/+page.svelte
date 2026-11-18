<script lang="ts">
  import NavGroups from '#lib/components/NavGroups.svelte';
  import { anchor, categorized, codeRuns, downloadHref, shippedOn } from '#lib/releases.ts';

  let { data } = $props();
  const r = $derived(data.release);
  const categories = $derived(categorized(r));
  const sections = $derived(
    categories.map((c) => ({
      title: `${c.title} ${c.groups.reduce((n, g) => n + g.items.length, 0)}`,
      links: c.groups.map((g) => ({ href: `#${anchor(c.title, g.title)}`, title: g.title })),
    })),
  );
</script>

<svelte:head><title>Smeltr {r.version} — smeltr</title></svelte:head>

<div class="release">
  <div class="marketing">
    <section class="hero">
      <p class="eyebrow"><a href="/releases">Releases</a></p>
      <h1>Smeltr {r.version}</h1>
      {#if r.date}<p class="date">{shippedOn(r)}</p>{/if}
      <p class="lede">{r.summary}</p>
      {#if r.date}
        <p class="actions">
          <a class="btn large primary" href={downloadHref(r)}>Download {r.version}</a>
          <a class="btn large" href="/docs/upgrade">How to upgrade</a>
        </p>
      {/if}
    </section>
    {#if r.highlights?.length}
      <section class="highlights" aria-label="Highlights">
        {#each r.highlights as h (h.title)}
          <a href={h.href}>
            <h2>{h.title}</h2>
            <p>{h.text}</p>
          </a>
        {/each}
      </section>
    {/if}
    {#each categories as c (c.title)}
      <section class="category" id={anchor(c.title)}>
        <h2>{c.title}</h2>
        {#each c.groups as g (g.title)}
          <div class="area" id={anchor(c.title, g.title)}>
            <h3>{g.title}</h3>
            <ul>
              {#each g.items as item (item)}
                <li>
                  {#each codeRuns(item) as run, i (i)}{#if run.code}<code>{run.text}</code
                      >{:else}{run.text}{/if}{/each}
                </li>
              {/each}
            </ul>
          </div>
        {/each}
      </section>
    {/each}
  </div>
  <aside>
    <NavGroups label="On this page" id="release-sections" links={[]} groups={sections} open />
  </aside>
</div>

<style>
  .release {
    display: grid;
    grid-template-columns: minmax(0, 46rem) 13rem;
    gap: 4rem;
  }
  aside {
    position: sticky;
    top: calc(var(--site-header-height, 5rem) + 1rem);
    align-self: start;
    margin-top: 3rem;
  }
  .hero {
    padding: 3rem 0 2.5rem;
  }
  .hero h1 {
    margin: 0;
  }
  .date {
    margin: 0.5rem 0 0;
    color: var(--dim);
  }
  .lede {
    margin: 1.25rem 0 0;
    font-size: 1.2rem;
  }
  .actions {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin: 1.75rem 0 0;
  }
  /* One rule between neighbours: each card draws its right and bottom edges, the grid its top and left. */
  .highlights {
    display: grid;
    grid-template-columns: 1fr 1fr;
    margin: 0 0 3.5rem;
    padding: 0;
    border-top: 1px solid var(--border);
    border-left: 1px solid var(--border);
  }
  .highlights a {
    padding: 1.5rem 1.5rem 1.6rem;
    border-right: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    color: var(--ink);
    text-decoration: none;
  }
  .highlights a:hover {
    background: var(--panel);
  }
  .highlights h2 {
    margin: 0 0 0.5rem;
    font-size: 1.15rem;
  }
  .highlights p {
    margin: 0;
  }
  .category {
    margin: 0 0 3rem;
    padding: 0;
  }
  .category > h2 {
    padding-bottom: 0.6rem;
    border-bottom: 1px solid var(--border);
  }
  .area {
    display: grid;
    grid-template-columns: 9rem minmax(0, 1fr);
    gap: 1.5rem;
    padding: 1rem 0;
    scroll-margin-top: calc(var(--site-header-height, 5rem) + 1rem);
  }
  .area + .area {
    border-top: 1px solid var(--border);
  }
  .area h3 {
    margin: 0.1rem 0 0;
    font-size: 0.95rem;
    color: var(--dim);
  }
  .area ul {
    margin: 0;
    padding-left: 1.1rem;
  }
  .area li + li {
    margin-top: 0.5rem;
  }
  @media (max-width: 75rem) {
    .release {
      grid-template-columns: minmax(0, 1fr);
    }
    aside {
      display: none;
    }
  }
  @media (max-width: 40rem) {
    .highlights {
      grid-template-columns: 1fr;
    }
    .area {
      grid-template-columns: 1fr;
      gap: 0.5rem;
    }
  }
</style>
