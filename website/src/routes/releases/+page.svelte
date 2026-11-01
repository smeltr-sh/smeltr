<script lang="ts">
  import { releases } from '#lib/releases.ts';
</script>

<svelte:head><title>Releases — smeltr</title></svelte:head>

<div class="marketing">
  <section class="page-hero">
    <h1>Releases</h1>
    <p class="lede">What each release of Smeltr changed.</p>
  </section>
  <article>
    {#each releases as r (r.version)}
      <section class="release">
        <h2><a href={`/releases/${r.version}`}>Smeltr {r.version}</a></h2>
        <p class="date">{r.date ?? 'Not released yet'}</p>
        <p>{r.summary}</p>
        <p><a href={`/releases/${r.version}`}>What's in {r.version} →</a></p>
      </section>
    {/each}
  </article>
</div>

<style>
  .date {
    color: var(--dim);
    margin-top: -0.5rem;
  }
  .release + .release {
    border-top: 1px solid var(--border);
  }
</style>
