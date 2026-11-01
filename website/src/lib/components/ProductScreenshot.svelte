<script lang="ts">
  type Crop = { x: number; y: number; width: number; height: number };
  /** One screenshot, by its file's stem, its size and the part of it shown. */
  type Shot = { source: string; height: number; width?: number; crop?: Crop };

  let {
    source,
    alt,
    caption,
    height,
    width = 2880,
    crop,
    phone,
  }: {
    source: string;
    alt: string;
    caption: string;
    height: number;
    width?: number;
    crop?: Crop;
    /** What a phone shows instead: the same page taken at a narrow width, where a wide shot's text
     * would be too small to read. */
    phone?: Shot;
  } = $props();

  let views = $derived.by(() => {
    const main = { source, height, width, crop };
    return phone
      ? [
          { shot: main, view: 'wide' },
          { shot: phone, view: 'narrow' },
        ]
      : [{ shot: main, view: '' }];
  });
</script>

{#snippet image(shot: Shot, theme: string)}
  {@const w = shot.width ?? 2880}
  {#if shot.crop}
    <div
      class="crop"
      style:aspect-ratio={`${shot.crop.width} / ${shot.crop.height}`}
      style:max-width={`${shot.crop.width / 2}px`}
    >
      <img
        src={`/${shot.source}-${theme}.png`}
        width={w}
        height={shot.height}
        {alt}
        loading="lazy"
        decoding="async"
        style:width={`${(w / shot.crop.width) * 100}%`}
        style:left={`${(-shot.crop.x / shot.crop.width) * 100}%`}
        style:top={`${(-shot.crop.y / shot.crop.height) * 100}%`}
      />
    </div>
  {:else}
    <img
      src={`/${shot.source}-${theme}.png`}
      width={w}
      height={shot.height}
      {alt}
      loading="lazy"
      decoding="async"
    />
  {/if}
{/snippet}

<figure>
  {#each ['light', 'dark'] as theme}
    {#each views as { shot, view }}
      <!-- Each view opens its own shot: the phone's stays readable at full size. The view picks
           the screen and the link the theme, each by its own `display`. -->
      <div class={view}>
        <a
          class="shot"
          class:shot-light={theme === 'light'}
          class:shot-dark={theme === 'dark'}
          href={`/${shot.source}-${theme}.png`}
          aria-label={`Open full-size screenshot: ${alt}`}
        >
          {@render image(shot, theme)}
          <span>Open complete screenshot ↗</span>
        </a>
      </div>
    {/each}
  {/each}
  <figcaption>{caption}</figcaption>
</figure>

<style>
  figure {
    margin: 0;
  }
  .shot {
    text-decoration: none;
  }
  img {
    display: block;
    width: 100%;
    height: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
  }
  /* A 2x screenshot shown no larger than the screen it was taken on. */
  .crop {
    margin: 0 auto;
    position: relative;
    overflow: hidden;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
  }
  .crop img {
    position: absolute;
    max-width: none;
    border: 0;
    border-radius: 0;
  }
  .narrow {
    display: none;
  }
  @media (max-width: 640px) {
    .wide {
      display: none;
    }
    .narrow {
      display: block;
    }
  }
  .shot:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 5px;
    border-radius: var(--radius-card);
  }
  span {
    display: block;
    text-align: center;
    margin-top: 0.85rem;
    font-size: 0.8rem;
    text-decoration: underline;
    text-underline-offset: 0.2em;
  }
  figcaption {
    max-width: 46rem;
    margin: 1rem auto 0;
    text-align: center;
    font-size: 0.85rem;
    color: var(--ink);
  }
</style>
