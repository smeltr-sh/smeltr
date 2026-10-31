<script lang="ts">
  // Each step's card from the pipeline editor, opened, at 2x.
  const steps = [
    {
      id: 'parser',
      label: 'Parser',
      what: 'names the 68 columns and leaves the 48 it skips empty',
      height: 6416,
    },
    { id: 'drop', label: 'Drop', what: 'drops allowed DNS lookups', height: 220 },
  ];
</script>

<div class="steps">
  {#each steps as step (step.id)}
    <details>
      <summary><strong>{step.label}:</strong> {step.what}</summary>
      <div class="card">
        {#each ['light', 'dark'] as theme (theme)}
          <img
            class:shot-light={theme === 'light'}
            class:shot-dark={theme === 'dark'}
            src={`/siem-step-${step.id}-${theme}.png`}
            width={1496}
            height={step.height}
            alt={`The ${step.label} step's settings in the pipeline editor.`}
            loading="lazy"
            decoding="async"
          />
        {/each}
      </div>
    </details>
  {/each}
</div>

<style>
  .steps {
    display: grid;
    gap: 0.5rem;
    margin-block: 1rem 1.5rem;
  }
  details {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }
  summary {
    padding: 0.7rem 0.9rem;
    cursor: pointer;
    color: var(--dim);
  }
  summary strong {
    color: var(--ink);
  }
  /* A long step scrolls inside its card rather than stretching the page. */
  .card {
    max-height: 32rem;
    overflow-y: auto;
    padding: 0 0.9rem 0.9rem;
  }
  img {
    width: 100%;
    max-width: 748px;
    height: auto;
  }
</style>
