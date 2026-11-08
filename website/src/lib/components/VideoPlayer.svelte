<script lang="ts">
  import type { Video } from '#lib/videos.ts';

  // With `href`, the poster links there instead of playing in place. `onplay` is called when the visitor presses play.
  let { video, href, onplay }: { video: Video; href?: string; onplay?: () => void } = $props();

  // YouTube's player loads only once the visitor presses play, so a visitor who
  // does not watch never contacts YouTube.
  let playing = $state(false);
</script>

{#if !video.youtube}
  <div class="frame planned"><span>Coming soon</span></div>
{:else if href}
  <a class="frame poster" {href} aria-label="Watch {video.title}">
    <img src={video.poster} alt="" loading="lazy" />
    <span class="play" aria-hidden="true"></span>
  </a>
{:else if playing}
  <iframe
    class="frame"
    src="https://www.youtube-nocookie.com/embed/{video.youtube}?autoplay=1"
    title={video.title}
    allow="autoplay; encrypted-media; picture-in-picture; fullscreen"
    allowfullscreen
  ></iframe>
{:else}
  <button
    class="frame poster"
    aria-label="Play {video.title}"
    onclick={() => {
      playing = true;
      onplay?.();
    }}
  >
    <img src={video.poster} alt="" loading="lazy" />
    <span class="play" aria-hidden="true"></span>
  </button>
{/if}

<style>
  .frame {
    display: block;
    width: 100%;
    aspect-ratio: 16 / 9;
    border: 0;
    background: var(--band-bg);
  }
  .planned {
    display: grid;
    place-items: center;
    color: var(--band-dim);
    font-family: var(--mono);
    font-size: 0.75rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  .poster {
    position: relative;
    padding: 0;
    border-radius: 0;
    cursor: pointer;
  }
  .poster img {
    display: block;
    width: 100%;
    height: 100%;
    object-fit: cover;
  }
  .play {
    position: absolute;
    inset: 50% auto auto 50%;
    width: 4.5rem;
    height: 4.5rem;
    translate: -50% -50%;
    border-radius: 50%;
    background: var(--band-accent);
    box-shadow: 0 0.25rem 1rem rgb(0 0 0 / 0.3);
  }
  /* The triangle, drawn rather than typed so no font changes its shape. */
  .play::after {
    content: '';
    position: absolute;
    inset: 50% auto auto 54%;
    translate: -50% -50%;
    border-style: solid;
    border-width: 0.85rem 0 0.85rem 1.4rem;
    border-color: transparent transparent transparent var(--band-on-accent);
  }
  .poster:hover .play,
  .poster:focus-visible .play {
    scale: 1.08;
  }
</style>
