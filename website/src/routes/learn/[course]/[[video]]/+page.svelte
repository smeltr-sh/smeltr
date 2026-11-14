<script lang="ts">
  import { page } from '$app/state';
  import VideoPlayer from '#lib/components/VideoPlayer.svelte';
  import { courseLength } from '#lib/videos.ts';

  let { data } = $props();
  const course = $derived(data.course);
  const video = $derived(data.video);
  const at = $derived(course.videos.findIndex((v) => v.slug === video.slug));
  const next = $derived(course.videos.slice(at + 1).find((v) => v.youtube));

  // The list shows titles until the visitor plays a video, then folds to numbers to give the player
  // room. Once the visitor opens or folds it themselves, it stays as they left it. On a phone the list sits
  // under the player and never folds.
  let open = $state(true);
  let touched = false;
</script>

<svelte:head><title>{video.title} — smeltr</title></svelte:head>

<div class="marketing">
  <section class="watch">
    <p class="eyebrow"><a href="/learn">Learn</a> · {courseLength(course)}</p>
    <h1>{course.title}</h1>
    <div class="layout" class:open>
      <div>
        <div class="player">
          {#key video}
            <VideoPlayer
              {video}
              onplay={() => {
                if (!touched) open = false;
              }}
            />
          {/key}
        </div>
        <!-- A course's own page plays its first video. Learn search finds that video at its own page,
          `/learn/{course}/{video}.html`. -->
        <div
          data-pagefind-body={page.params.video ? '' : undefined}
          data-pagefind-filter={page.params.video ? 'kind:video' : undefined}
        >
          <div class="under">
            <h2>{at + 1}. {video.title}</h2>
            {#if next}
              <a href="/learn/{course.slug}/{next.slug}" data-pagefind-ignore
                >Next: {course.videos.indexOf(next) + 1}. {next.title} →</a
              >
            {/if}
            {#if course.demo}
              <a href="/demo/try/" rel="external" data-pagefind-ignore
                >Try it yourself in the demo →</a
              >
            {/if}
          </div>
          <details class="transcript">
            <summary data-pagefind-ignore>Transcript</summary>
            {#each data.transcript as paragraph, i (i)}<p>{paragraph}</p>{/each}
          </details>
        </div>
      </div>
      <nav aria-label="Videos">
        <button
          class="toggle"
          aria-expanded={open}
          aria-controls="course-videos"
          aria-label="Show titles"
          onclick={() => {
            touched = true;
            open = !open;
          }}><span aria-hidden="true">{open ? '›' : '‹'}</span></button
        >
        <ol id="course-videos">
          {#each course.videos as v, i (v.slug)}
            <li>
              {#if v.youtube}
                <a
                  href="/learn/{course.slug}/{v.slug}"
                  aria-current={v.slug === video.slug ? 'page' : undefined}
                  title={open ? undefined : v.title}
                >
                  <span class="n">{i + 1}</span>
                  <span class="t">{v.title}</span>
                  {#if v.minutes}<span class="m">{v.minutes} min</span>{/if}
                </a>
              {:else}
                <span class="planned" title={open ? undefined : v.title}>
                  <span class="n">{i + 1}</span>
                  <span class="t">{v.title}</span>
                  <span class="m">Coming soon</span>
                </span>
              {/if}
            </li>
          {/each}
        </ol>
      </nav>
    </div>
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
  .layout {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 3.25rem;
    gap: 1rem;
    align-items: start;
  }
  .layout.open {
    grid-template-columns: minmax(0, 1fr) 22rem;
  }
  /* As wide as the column, but never so tall that it does not fit in the window. */
  .player {
    max-width: calc((100vh - 15rem) * 16 / 9);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    overflow: hidden;
  }
  .under {
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: baseline;
    gap: 1rem;
    margin-top: 1rem;
  }
  .transcript {
    margin-top: 1rem;
    max-width: 48rem;
  }
  .transcript summary {
    cursor: pointer;
    font-weight: 600;
    padding-block: 0.5rem;
  }
  .under h2 {
    font-size: 1.3rem;
    margin: 0;
  }
  .under a {
    font-weight: 600;
    font-size: 0.92rem;
  }
  nav {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    overflow: hidden;
  }
  .toggle {
    width: 100%;
    display: flex;
    justify-content: flex-end;
    padding: 0.5rem 1rem;
    border: 0;
    border-bottom: 1px solid var(--border);
    border-radius: 0;
    background: none;
    color: inherit;
    font-size: 1.3rem;
    line-height: 1;
    cursor: pointer;
  }
  ol {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  li + li {
    border-top: 1px solid var(--border);
  }
  li > * {
    display: grid;
    grid-template-columns: 1.5rem 1fr;
    gap: 0.2rem 0.75rem;
    padding: 0.9rem 1rem;
    color: inherit;
    text-decoration: none;
  }
  @media (min-width: 46rem) {
    .layout:not(.open) .toggle,
    .layout:not(.open) li > * {
      grid-template-columns: 1fr;
      justify-content: center;
      text-align: center;
      padding-inline: 0;
    }
    /* Folded, a row shows its number; its title stays for screen readers. */
    .layout:not(.open) :is(.t, .m) {
      position: absolute;
      width: 1px;
      height: 1px;
      overflow: hidden;
      clip-path: inset(50%);
      white-space: nowrap;
    }
  }
  [aria-current='page'] {
    background: var(--band-bg);
    color: var(--band-ink);
  }
  .planned {
    opacity: 0.55;
  }
  .n {
    font-family: var(--mono);
    grid-row: span 2;
  }
  .t {
    font-weight: 600;
  }
  .m {
    font-family: var(--mono);
    font-size: 0.72rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }
  @media (max-width: 45.99rem) {
    .layout,
    .layout.open {
      grid-template-columns: 1fr;
    }
    .toggle {
      display: none;
    }
  }
</style>
