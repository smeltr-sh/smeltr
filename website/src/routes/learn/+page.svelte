<script lang="ts">
  type Topic = 'start' | 'build' | 'operate';
  type Video = {
    topic: Topic;
    title: string;
    minutes?: number;
    src?: string;
    poster?: string;
    guide: string;
  };

  const topics: [Topic | 'all', string][] = [
    ['all', 'All'],
    ['start', 'Get started'],
    ['build', 'Build & test'],
    ['operate', 'Deploy & operate'],
  ];
  const topicName = Object.fromEntries(topics) as Record<Topic, string>;

  // A video with no `src` is planned: its card says so and links the guide that covers it today.
  const items: Video[] = [
    { topic: 'start', title: 'Install smeltr', guide: '/docs/getting-started' },
    {
      topic: 'build',
      title: 'Fixing a feed in smeltr',
      minutes: 6,
      src: '/videos/walkthrough.mp4',
      poster: '/videos/walkthrough-poster.png',
      guide: '/docs/first-pipeline',
    },
    { topic: 'operate', title: 'Review and deploy a change', guide: '/docs/review' },
  ];

  let shown: Topic | 'all' = $state('all');
  const visible = $derived(items.filter((i) => shown === 'all' || i.topic === shown));
</script>

<svelte:head><title>Learn smeltr</title></svelte:head>

<div class="marketing">
  <section class="page-hero">
    <h1>Learn</h1>
  </section>

  <div class="filters" role="group" aria-label="Topic">
    {#each topics as [id, label] (id)}
      <button
        class="compact"
        class:primary={shown === id}
        aria-pressed={shown === id}
        onclick={() => (shown = id)}>{label}</button
      >
    {/each}
  </div>

  <div class="library">
    {#each visible as item (item.title)}
      <article class="video">
        {#if item.src}
          <!-- svelte-ignore a11y_media_has_caption -->
          <video controls preload="none" poster={item.poster}>
            <source src={item.src} type="video/mp4" />
          </video>
        {:else}
          <div class="thumb"><span>Coming soon</span></div>
        {/if}
        <div class="copy">
          <p class="eyebrow">
            {topicName[item.topic]}{#if item.minutes}&nbsp;· {item.minutes} min{/if}
          </p>
          <h2>{item.title}</h2>
          {#if item.src}
            <a href="/demo/try/" rel="external">Try it yourself in the demo →</a>
          {:else}
            <a href={item.guide}>Read the guide →</a>
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
  video,
  .thumb {
    display: block;
    width: 100%;
    aspect-ratio: 16 / 9;
    background: #131313;
  }
  .thumb {
    display: grid;
    place-items: center;
    background: var(--band-bg);
    color: var(--band-dim);
    font-family: var(--mono);
    font-size: 0.75rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
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
