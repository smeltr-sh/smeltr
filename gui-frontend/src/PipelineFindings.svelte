<!--
  What a pipeline's rows break that no one step did, in a box under its steps, and after it the fields its schemas
  recommend and the rows lack. The pipeline editor and review's pipeline card both show them here.
-->
<script lang="ts">
  import WarningLine from './WarningLine.svelte';
  import Recommends from './Recommends.svelte';

  let {
    warnings,
    recommended,
  }: {
    warnings: string[];
    /** By schema title, the fields it recommends and the rows lack. */
    recommended: { schema: string; fields: string[] }[];
  } = $props();
</script>

{#if warnings.length > 0}
  <div class="warnings" role="status">
    {#each warnings as message, n (n)}
      <WarningLine {message} />
    {/each}
  </div>
{/if}

{#if recommended.length > 0}
  <div class="recommended" role="status">
    {#each recommended as r (r.schema)}
      <div class="recommended-note">
        <span aria-hidden="true">ⓘ</span>
        <span><Recommends schema={r.schema} fields={r.fields} /></span>
      </div>
    {/each}
  </div>
{/if}

<style>
  .warnings {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    margin-top: 0.6rem;
    padding: 0.55rem 0.7rem;
    background: color-mix(in oklab, var(--warn) 12%, var(--panel));
    border-radius: var(--radius);
  }

  /* What the schemas recommend and the rows lack: told under the warnings, and no warning itself. */
  .recommended {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    margin-top: 0.4rem;
    padding: 0.55rem 0.7rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }

  .recommended-note {
    display: flex;
    gap: 0.5rem;
    align-items: flex-start;
    font-size: var(--text-note);
    color: var(--dim);
    line-height: 1.45;
  }
</style>
