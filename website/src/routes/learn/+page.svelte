<script lang="ts">
  import VideoPlayer from '#lib/components/VideoPlayer.svelte';
  import { courseLength, courses, released } from '#lib/videos.ts';
</script>

<svelte:head><title>Learn — smeltr</title></svelte:head>

<div class="marketing">
  <section class="page-hero">
    <h1>Learn</h1>
  </section>

  <div class="library">
    {#each courses as course (course.slug)}
      <article class="course">
        <VideoPlayer video={released(course)[0] ?? course.videos[0]} href="/learn/{course.slug}" />
        <div class="copy">
          <p class="eyebrow">{courseLength(course)}</p>
          <h2><a href="/learn/{course.slug}">{course.title}</a></h2>
          {#if course.demo}
            <a href="/demo/try/" rel="external">Try it yourself in the demo →</a>
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
  .library {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 1rem;
  }
  .course {
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
