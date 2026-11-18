<script lang="ts">
  import { downloadHref, latest, releases, shippedOn } from '#lib/releases.ts';
</script>

<svelte:head><title>Releases — smeltr</title></svelte:head>

<div class="marketing">
  <section class="page-hero">
    <h1>Releases</h1>
    <p class="lede">What each release of Smeltr changed.</p>
  </section>
  <ol class="timeline">
    {#each releases as r (r.version)}
      <li>
        <p class="date">{shippedOn(r)}</p>
        <div>
          <h2>
            <a href={`/releases/${r.version}`}>Smeltr {r.version}</a>
            {#if r === latest}<span class="latest">Latest</span>{/if}
          </h2>
          <p class="summary">{r.summary}</p>
          {#if r.highlights?.length}
            <ul>
              {#each r.highlights as h (h.title)}
                <li><a href={h.href}>{h.title}</a></li>
              {/each}
            </ul>
          {/if}
          <p class="links">
            <a href={`/releases/${r.version}`}>Everything in {r.version} →</a>
            {#if r === latest}<a class="btn primary" href={downloadHref(r)}>Download {r.version}</a
              >{/if}
          </p>
        </div>
      </li>
    {/each}
  </ol>
</div>

<style>
  .timeline {
    list-style: none;
    margin: 0;
    padding: 0;
    max-width: 60rem;
  }
  .timeline > li {
    display: grid;
    grid-template-columns: 10rem minmax(0, 1fr);
    gap: 2rem;
    padding: 2rem 0;
    border-top: 1px solid var(--border);
  }
  .date {
    margin: 0.55rem 0 0;
    color: var(--dim);
    font-size: 0.9rem;
  }
  h2 {
    margin: 0;
  }
  h2 a {
    color: var(--ink);
    text-decoration: none;
  }
  .latest {
    margin-left: 0.75rem;
    padding: 0.15rem 0.5rem;
    border: 1px solid var(--accent);
    border-radius: var(--radius);
    color: var(--accent);
    font-family: var(--mono);
    font-size: 0.7rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    vertical-align: middle;
  }
  .summary {
    margin: 0.5rem 0 0.75rem;
  }
  ul {
    margin: 0;
    padding-left: 1.1rem;
  }
  .links {
    display: flex;
    flex-wrap: wrap;
    gap: 1.25rem;
    align-items: center;
    margin: 1rem 0 0;
  }
  @media (max-width: 40rem) {
    .timeline > li {
      grid-template-columns: minmax(0, 1fr);
      gap: 0.25rem;
    }
  }
</style>
