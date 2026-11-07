<script lang="ts">
  import VideoPlayer from '#lib/components/VideoPlayer.svelte';
  import { topicNames } from '#lib/videos.ts';

  let { data } = $props();
  const video = $derived(data.video);
</script>

<svelte:head><title>{video.title} — smeltr</title></svelte:head>

<div class="marketing">
  <section class="watch">
    <p class="eyebrow">
      <a href="/learn">Learn</a> · {topicNames[video.topic]}
      {#if video.minutes}· {video.minutes} min{/if}
    </p>
    <h1>{video.title}</h1>
    <div class="player"><VideoPlayer {video} /></div>
    <p><a href="/demo/try/" rel="external">Try it yourself in the demo →</a></p>
  </section>
</div>

<style>
  .marketing .watch {
    padding-top: 2.5rem;
  }
  .marketing h1 {
    max-width: none;
    font-size: clamp(2rem, 4vw, 3rem);
    margin-bottom: 1.5rem;
  }
  .eyebrow a {
    color: inherit;
  }
  /* As wide as the page, but never taller than the window leaves room for. */
  .player {
    max-width: calc((100vh - 15rem) * 16 / 9);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    overflow: hidden;
  }
  .player + p a {
    font-weight: 600;
  }
</style>
