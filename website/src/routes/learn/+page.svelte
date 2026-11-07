<script lang="ts">
  type Topic = 'start' | 'build' | 'operate';
  type Item =
    | { kind: 'video'; topic: Topic; title: string; minutes: number; src: string; poster: string }
    | { kind: 'guide'; topic: Topic; title: string; href: string };

  const topics: [Topic | 'all', string][] = [
    ['all', 'All'],
    ['start', 'Get started'],
    ['build', 'Build & test'],
    ['operate', 'Deploy & operate'],
  ];
  const topicName = Object.fromEntries(topics) as Record<Topic, string>;

  const items: Item[] = [
    {
      kind: 'video',
      topic: 'build',
      title: 'Fixing a feed in smeltr',
      minutes: 6,
      src: '/videos/walkthrough.mp4',
      poster: '/videos/walkthrough-poster.png',
    },
    { kind: 'guide', topic: 'start', title: 'Set up a deployment', href: '/docs/getting-started' },
    { kind: 'guide', topic: 'start', title: 'Install a leader', href: '/docs/install-leader/container' },
    { kind: 'guide', topic: 'start', title: 'First login', href: '/docs/first-login' },
    { kind: 'guide', topic: 'start', title: 'Enroll a worker', href: '/docs/enroll-worker/host' },
    { kind: 'guide', topic: 'build', title: 'First pipeline tutorial', href: '/docs/first-pipeline' },
    { kind: 'guide', topic: 'build', title: 'Capture a sample', href: '/docs/capture-sample' },
    { kind: 'guide', topic: 'build', title: 'Schemas', href: '/docs/schemas' },
    { kind: 'guide', topic: 'build', title: 'Pipeline steps', href: '/docs/steps' },
    { kind: 'guide', topic: 'operate', title: 'Review & deploy', href: '/docs/review' },
    { kind: 'guide', topic: 'operate', title: 'Operate the fleet', href: '/docs/operations' },
    { kind: 'guide', topic: 'operate', title: 'Upgrade', href: '/docs/upgrade' },
    { kind: 'guide', topic: 'operate', title: 'Troubleshooting', href: '/docs/troubleshooting' },
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
      {#if item.kind === 'video'}
        <article class="video">
          <!-- svelte-ignore a11y_media_has_caption -->
          <video controls preload="none" poster={item.poster}>
            <source src={item.src} type="video/mp4" />
          </video>
          <div class="copy">
            <p class="eyebrow">Video · {item.minutes} min · {topicName[item.topic]}</p>
            <h2>{item.title}</h2>
            <a href="/demo/try/" rel="external">Try it yourself in the demo →</a>
          </div>
        </article>
      {:else}
        <a class="guide" href={item.href}>
          <p class="eyebrow">Guide · {topicName[item.topic]}</p>
          <h3>{item.title} →</h3>
        </a>
      {/if}
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
  .video,
  .guide {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  .video {
    grid-column: 1 / -1;
    display: grid;
    grid-template-columns: minmax(0, 2fr) minmax(0, 1fr);
    overflow: hidden;
  }
  video {
    display: block;
    width: 100%;
    background: #131313;
  }
  .copy {
    padding: 1.6rem;
    align-self: center;
  }
  .copy h2 {
    font-size: 1.6rem;
    margin: 0 0 1rem;
  }
  .copy a {
    font-weight: 600;
  }
  .guide {
    display: block;
    padding: 1.2rem 1.4rem;
    color: var(--ink);
    text-decoration: none;
  }
  .guide:hover {
    border-color: var(--accent);
  }
  .guide .eyebrow {
    margin-bottom: 0.5rem;
  }
  .guide h3 {
    margin: 0;
    font-size: 1.05rem;
  }
  @media (max-width: 46rem) {
    .library,
    .video {
      grid-template-columns: 1fr;
    }
  }
</style>
