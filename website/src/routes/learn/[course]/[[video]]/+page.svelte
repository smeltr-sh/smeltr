<script lang="ts">
  import VideoPlayer from '#lib/components/VideoPlayer.svelte';
  import { courseLength } from '#lib/videos.ts';

  let { data } = $props();
  const course = $derived(data.course);
  const video = $derived(data.video);
  const at = $derived(course.videos.indexOf(video));
  const next = $derived(course.videos[at + 1]);

  // The list shows titles until the visitor plays a video, then folds to numbers to give the player
  // room. Once the visitor opens or folds it themselves, it stays as they left it.
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
        <div class="under">
          <h2>{at + 1}. {video.title}</h2>
          {#if next?.youtube}
            <a href="/learn/{course.slug}/{next.slug}">Next: {at + 2}. {next.title} →</a>
          {/if}
        </div>
      </div>
      <nav aria-label="Videos">
        <button
          class="toggle"
          aria-expanded={open}
          aria-label={open ? 'Hide titles' : 'Show titles'}
          onclick={() => {
            touched = true;
            open = !open;
          }}><span aria-hidden="true">{open ? '›' : '‹'}</span></button
        >
        <ol>
          {#each course.videos as v, i (v.slug)}
            <li>
              {#if v.youtube}
                <a
                  href="/learn/{course.slug}/{v.slug}"
                  aria-current={v === video ? 'page' : undefined}
                  title={open ? undefined : v.title}
                >
                  <span class="n">{i + 1}</span>
                  {#if open}<span class="t">{v.title}</span><span class="m">{v.minutes} min</span
                    >{/if}
                </a>
              {:else}
                <span class="planned" title={open ? undefined : v.title}>
                  <span class="n">{i + 1}</span>
                  {#if open}<span class="t">{v.title}</span><span class="m">Coming soon</span>{/if}
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
  .player {
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
  .layout:not(.open) .toggle,
  .layout:not(.open) li > * {
    grid-template-columns: 1fr;
    justify-content: center;
    text-align: center;
    padding-inline: 0;
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
  @media (max-width: 46rem) {
    .layout,
    .layout.open {
      grid-template-columns: 1fr;
    }
    .toggle {
      display: none;
    }
  }
</style>
