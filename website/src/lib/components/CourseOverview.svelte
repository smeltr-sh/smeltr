<script lang="ts">
  import VideoPlayer from '#lib/components/VideoPlayer.svelte';
  import { courseLength, courses, released, type About, type Course } from '#lib/videos.ts';

  let { course, about }: { course: Course; about: About } = $props();
  const video = $derived(released(course)[0]);
  const watch = $derived(`/learn/${course.slug}/${video.slug}`);
  const next = $derived(
    (about.next ?? [])
      .map((slug) => courses.find((c) => c.slug === slug))
      .filter((c): c is Course => c !== undefined),
  );
  const time = (s: number) => `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
</script>

<section class="overview">
  <p class="eyebrow"><a href="/learn">Learn</a> · {courseLength(course)}</p>
  <h1>{course.title}</h1>
  <p class="lede">{about.goal}</p>
  <div class="top">
    <div class="player"><VideoPlayer {video} href={watch} /></div>
    <div class="needs">
      <h2>What you need</h2>
      <ul>
        {#each about.needs as need (need)}<li>{need}</li>{/each}
      </ul>
      <a class="btn primary large" href={watch}>Watch the course</a>
    </div>
  </div>

  {#if video.chapters}
    <h2>Chapters</h2>
    <ol class="rows">
      {#each video.chapters as chapter, i (chapter.title)}
        <li>
          <a href={chapter.start === undefined ? watch : `${watch}#t=${chapter.start}`}>
            <span class="n">{i + 1}</span>
            <span class="t">{chapter.title}</span>
            {#if chapter.start !== undefined}<span class="m">{time(chapter.start)}</span>{/if}
          </a>
        </li>
      {/each}
      {#if course.selfTest}
        <li>
          <a href="/learn/{course.slug}/self-test">
            <span class="n">✓</span>
            <span class="t">Self-test</span>
            <span class="m">{course.selfTest.questions.length} questions</span>
          </a>
        </li>
      {/if}
    </ol>
  {/if}

  {#if next.length}
    <h2>Next</h2>
    <ul class="next">
      {#each next as c (c.slug)}
        <li><a href="/learn/{c.slug}">{c.title}</a></li>
      {/each}
    </ul>
  {/if}
</section>

<style>
  .overview {
    padding-top: 2.5rem;
  }
  .eyebrow a {
    color: inherit;
  }
  .overview h1 {
    max-width: none;
    font-size: clamp(2rem, 4vw, 3rem);
  }
  .lede {
    margin: 0.8rem 0 1.8rem;
    font-size: 1.15rem;
  }
  .overview h2 {
    font-size: 1.3rem;
    margin: 2.5rem 0 1rem;
  }
  .top {
    display: grid;
    grid-template-columns: minmax(0, 3fr) minmax(0, 2fr);
    gap: 2rem;
    align-items: start;
  }
  .player {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    overflow: hidden;
  }
  .needs h2 {
    margin-top: 0;
  }
  .needs ul {
    padding-left: 1.2rem;
    margin: 0 0 1.5rem;
  }
  .needs li + li {
    margin-top: 0.5rem;
  }
  .rows {
    list-style: none;
    margin: 0;
    padding: 0;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    overflow: hidden;
  }
  .rows li + li {
    border-top: 1px solid var(--border);
  }
  .rows a {
    display: grid;
    grid-template-columns: 2rem 1fr auto;
    gap: 0.75rem;
    padding: 0.9rem 1.2rem;
    color: inherit;
    text-decoration: none;
  }
  .rows a:hover {
    background: var(--bg);
  }
  .n,
  .m {
    font-family: var(--mono);
  }
  .t {
    font-weight: 600;
  }
  .m {
    font-size: 0.8rem;
    color: var(--dim);
  }
  @media (max-width: 46rem) {
    .top {
      grid-template-columns: 1fr;
    }
  }
</style>
