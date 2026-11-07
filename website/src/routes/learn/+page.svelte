<script lang="ts">
  import VideoPlayer from '#lib/components/VideoPlayer.svelte';
  import { topicNames, videos, type Topic } from '#lib/videos.ts';

  const filters: [Topic | 'all', string][] = [['all', 'All'], ...Object.entries(topicNames)] as [
    Topic | 'all',
    string,
  ][];

  let shown: Topic | 'all' = $state('all');
  const visible = $derived(videos.filter((v) => shown === 'all' || v.topic === shown));
</script>

<svelte:head><title>Learn — smeltr</title></svelte:head>

<div class="marketing">
  <section class="page-hero">
    <h1>Learn</h1>
  </section>

  <div class="filters" role="group" aria-label="Topic">
    {#each filters as [id, label] (id)}
      <button
        class="compact"
        class:primary={shown === id}
        aria-pressed={shown === id}
        onclick={() => (shown = id)}>{label}</button
      >
    {/each}
  </div>

  <div class="library">
    {#each visible as video (video.title)}
      <article class="video">
        <VideoPlayer {video} href={video.youtube ? `/learn/${video.slug}` : undefined} />
        <div class="copy">
          <p class="eyebrow">
            {topicNames[video.topic]}{#if video.minutes}&nbsp;· {video.minutes} min{/if}
          </p>
          <h2>
            {#if video.youtube}<a href="/learn/{video.slug}">{video.title}</a
              >{:else}{video.title}{/if}
          </h2>
          {#if video.youtube}
            <a href="/demo/try/" rel="external">Try it yourself in the demo →</a>
          {:else}
            <a href={video.guide}>Read the guide →</a>
          {/if}
        </div>
      </article>
    {/each}
  </div>
</div>

<style>
  .marketing .page-hero {
    padding-bottom: 1.5rem;
  }
  .filters {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-bottom: 1.5rem;
  }
  .library {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 1rem;
  }
  .video {
    display: flex;
    flex-direction: column;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    overflow: hidden;
  }
  .copy {
    padding: 1.3rem 1.4rem 1.5rem;
  }
  .copy .eyebrow {
    margin-bottom: 0.5rem;
  }
  .copy h2 {
    font-size: 1.3rem;
    margin: 0 0 0.9rem;
  }
  .copy h2 a {
    color: inherit;
    text-decoration: none;
  }
  .copy a {
    font-weight: 600;
    font-size: 0.92rem;
  }
  @media (max-width: 46rem) {
    .library {
      grid-template-columns: 1fr;
    }
  }
</style>
