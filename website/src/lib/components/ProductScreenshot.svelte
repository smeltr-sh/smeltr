<script lang="ts">
  let {
    source,
    alt,
    caption,
    height,
    width = 2880,
    crop,
  }: {
    source: string;
    alt: string;
    caption: string;
    height: number;
    width?: number;
    crop?: { x: number; y: number; width: number; height: number };
  } = $props();
</script>

<figure>
  {#each ['light', 'dark'] as theme}
    <a
      class="shot"
      class:shot-light={theme === 'light'}
      class:shot-dark={theme === 'dark'}
      href={`/${source}-${theme}.png`}
      aria-label={`Open full-size screenshot: ${alt}`}
    >
      {#if crop}
        <div
          class="crop"
          style:aspect-ratio={`${crop.width} / ${crop.height}`}
          style:max-width={`${crop.width / 2}px`}
        >
          <img
            src={`/${source}-${theme}.png`}
            {width}
            {height}
            {alt}
            loading="lazy"
            decoding="async"
            style:width={`${(width / crop.width) * 100}%`}
            style:left={`${(-crop.x / crop.width) * 100}%`}
            style:top={`${(-crop.y / crop.height) * 100}%`}
          />
        </div>
      {:else}
        <img
          src={`/${source}-${theme}.png`}
          {width}
          {height}
          {alt}
          loading="lazy"
          decoding="async"
        />
      {/if}
      <span>Open complete screenshot ↗</span>
    </a>
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
