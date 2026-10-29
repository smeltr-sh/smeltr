<!--
  Review's Schemas tab: where the rows of the change's pipelines do not fit the schemas they produce, at the
  branch's tip, by group and pipeline. Each finding is said, never held against the deploy.
-->
<script lang="ts">
  import type { SchemaFindingDto } from './api';

  let {
    findings,
    loading,
    error,
    pipelineName,
  }: {
    findings: SchemaFindingDto[];
    loading: boolean;
    error: string;
    pipelineName: (scope: string, id: string) => string;
  } = $props();

  /** The findings by group and pipeline, in the order found. */
  let grouped = $derived.by(() => {
    const out = new Map<string, { group: string; pipeline: string; found: SchemaFindingDto[] }>();
    for (const f of findings) {
      const key = JSON.stringify([f.group, f.pipeline]);
      const at = out.get(key);
      if (at) at.found.push(f);
      else out.set(key, { group: f.group, pipeline: f.pipeline, found: [f] });
    }
    return [...out.values()];
  });
</script>

{#if error}
  <p class="problem">{error}</p>
{:else if loading}
  <p class="dim">Loading&hellip;</p>
{:else if findings.length === 0}
  <p class="dim">The rows fit every schema their pipelines produce.</p>
{:else}
  <p class="dim">
    Where a pipeline's rows do not fit a schema it produces. None of these block the deploy.
  </p>
  {#each grouped as g (JSON.stringify([g.group, g.pipeline]))}
    <section class="pipeline">
      <h3>
        <a href="#/build/pipelines/{encodeURIComponent(g.pipeline)}"
          >{pipelineName(g.group, g.pipeline)}</a
        >
        <span class="dim">in {g.group}</span>
      </h3>
      <ul>
        {#each g.found as f, i (i)}
          <li>
            <span class="step mono">{f.step ?? 'no step'}</span>
            <span>{f.message}</span>
          </li>
        {/each}
      </ul>
    </section>
  {/each}
{/if}

<style>
  .pipeline + .pipeline {
    margin-top: 1rem;
  }

  h3 {
    margin: 0 0 0.4rem;
    font-size: var(--text-label);
  }

  ul {
    margin: 0;
    padding: 0;
    list-style: none;
  }

  li {
    display: flex;
    gap: 0.8rem;
    padding: 0.35rem 0;
    border-bottom: 1px solid var(--border);
    font-size: var(--text-note);
  }

  .step {
    flex: 0 0 9rem;
    color: var(--dim);
    overflow-wrap: anywhere;
  }

  .problem {
    color: var(--error);
  }

  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
