<script lang="ts">
  import { downloadHref } from '#lib/releases.ts';

  let { data } = $props();
  const r = $derived(data.release);
</script>

<svelte:head><title>Smeltr {r.version} — smeltr</title></svelte:head>

<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow"><a href="/releases">Releases</a></p>
    <h1>Smeltr {r.version}</h1>
    <p class="date">{r.date ?? 'Not released yet'}</p>
    <p class="lede">{r.summary}</p>
  </section>
  {#if r.highlights?.length}
    <section class="highlights" aria-labelledby="highlights-heading">
      <h2 id="highlights-heading">Highlights</h2>
      <div class="cards">
        {#each r.highlights as h (h.title)}
          <a class="card" href={h.href}>
            <h3>{h.title}</h3>
            <p>{h.text}</p>
          </a>
        {/each}
      </div>
    </section>
  {/if}
  <article>
    {#if r.highlights?.length}<h2 class="all">In detail</h2>{/if}
    {#each r.sections as s (s.title)}
      <h3>{s.title}</h3>
      <ul>
        {#each s.items as item (item)}
          <li>{item}</li>
        {/each}
      </ul>
    {/each}
    {#if r.date}
      <h2>Upgrade</h2>
      <p>
        <a href={downloadHref(r)}>Download {r.version}</a> ·
        <a href="/docs/upgrade">How to upgrade</a>
      </p>
    {/if}
  </article>
</div>

<style>
  .highlights {
    margin: 0 0 1rem;
  }
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(18rem, 1fr));
    gap: 1rem;
  }
  .card {
    display: block;
    padding: 1.1rem 1.2rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--panel);
    color: var(--ink);
    text-decoration: none;
  }
  .card:hover {
    border-color: var(--ink);
  }
  .card h3 {
    margin: 0 0 0.4rem;
    font-size: 1.05rem;
  }
  .card p {
    margin: 0;
    color: var(--dim);
    font-size: 0.92rem;
  }
  .all {
    margin: 3rem 0 1rem;
  }
  .date {
    color: var(--dim);
  }
</style>
