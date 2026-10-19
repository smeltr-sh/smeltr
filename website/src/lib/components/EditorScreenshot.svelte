<script lang="ts">
  let { focused = false }: { focused?: boolean } = $props();
  // Windows into the unchanged 2880 × 2000 screenshot, measured in source pixels.
  const details = [
    {
      label: 'Configure the step',
      x: 470,
      y: 908,
      width: 810,
      height: 490,
      alt: 'Open directory lookup step, with source_user as the input and directory (csv) as the lookup table.',
    },
    {
      label: 'See what changes',
      x: 1350,
      y: 778,
      width: 1440,
      height: 515,
      alt: 'Two expanded sample records show the added user.department and user.full_name fields.',
    },
  ];
</script>

<figure>
  {#each ['light', 'dark'] as theme}
    <a
      class="shot"
      class:shot-light={theme === 'light'}
      class:shot-dark={theme === 'dark'}
      href={`/pipeline-editor-${theme}.png`}
      aria-label="Open the complete pipeline editor screenshot"
    >
      {#if focused}
        <div class="details">
          {#each details as detail}
            <div class="detail">
              <p class="label">{detail.label}</p>
              <div class="crop" style:aspect-ratio={`${detail.width} / ${detail.height}`}>
                <img
                  src={`/pipeline-editor-${theme}.png`}
                  width="2880"
                  height="2000"
                  loading="lazy"
                  decoding="async"
                  alt={detail.alt}
                  style:width={`${(2880 / detail.width) * 100}%`}
                  style:left={`${(-detail.x / detail.width) * 100}%`}
                  style:top={`${(-detail.y / detail.height) * 100}%`}
                />
              </div>
            </div>
          {/each}
        </div>
      {:else}
        <img
          class="full"
          src={`/pipeline-editor-${theme}.png`}
          width="2880"
          height="2000"
          loading="lazy"
          decoding="async"
          alt="Smeltr's pipeline editor with an open directory lookup on the left and added department and full-name fields highlighted in the sample preview on the right."
        />
      {/if}
      <span class="expand">Open complete screenshot ↗</span>
    </a>
  {/each}
  <figcaption>
    {focused
      ? 'Details from the running editor, using sample data. This lookup adds a department and full name to each matching record.'
      : 'Running editor with sample data. Inspect each step’s effect, then lock the expected output as a regression test.'}
  </figcaption>
</figure>

<style>
  figure {
    margin: 0;
  }
  .shot {
    text-decoration: none;
  }
  .details {
    display: grid;
    grid-template-columns: 0.9fr 1.6fr;
    gap: 1.25rem;
    align-items: start;
  }
  .detail {
    min-width: 0;
  }
  .label {
    margin: 0 0 0.75rem;
    color: var(--ink);
    font-weight: 600;
    font-size: 1rem;
  }
  .crop {
    position: relative;
    overflow: hidden;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  .crop img {
    position: absolute;
    max-width: none;
    height: auto;
    display: block;
  }
  .full {
    display: block;
    width: 100%;
    height: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
  }
  .shot:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 5px;
    border-radius: var(--radius-card);
  }
  .expand {
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
  }
  @media (max-width: 48rem) {
    .details {
      grid-template-columns: minmax(0, 1fr);
      gap: 1.5rem;
    }
  }
</style>
