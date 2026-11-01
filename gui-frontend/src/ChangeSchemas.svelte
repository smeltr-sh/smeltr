<!--
  Review's Schemas tab: where the rows of the change's pipelines do not fit the schemas they produce, at the
  branch's tip, by group and pipeline. Each finding is said, never held against the deploy. After a pipeline's
  findings come the recommended fields its rows lack, which are not findings.
-->
<script lang="ts">
  import Ticked from './Ticked.svelte';
  import Recommends from './Recommends.svelte';
  import type { SchemaFindingDto } from './api';
  import { openOnBranch } from './unsavedGuard';

  let {
    branch,
    findings,
    loading,
    error,
    pipelineName,
  }: {
    /** The branch the change is on, where its pipelines open. */
    branch: string;
    findings: SchemaFindingDto[];
    loading: boolean;
    error: string;
    pipelineName: (scope: string, id: string) => string;
  } = $props();

  /** Whether any of them is a finding, and not only recommended fields the rows lack. */
  let breaking = $derived(findings.some((f) => f.breaks));

  /** The findings by group and pipeline, in the order found, each pipeline's recommended fields after them. */
  let grouped = $derived.by(() => {
    const out = new Map<string, { group: string; pipeline: string; found: SchemaFindingDto[] }>();
    for (const f of [...findings.filter((f) => f.breaks), ...findings.filter((f) => !f.breaks)]) {
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
  {#if breaking}
    <p class="dim">
      Where a pipeline's rows do not fit a schema it produces. None of these block the deploy.
    </p>
  {:else}
    <p class="dim">
      The rows fit every schema their pipelines produce. They lack fields a schema recommends:
    </p>
  {/if}
  {#each grouped as g (JSON.stringify([g.group, g.pipeline]))}
    <section class="pipeline">
      <h3>
        <a
          href="#/build/pipelines/{encodeURIComponent(g.pipeline)}"
          onclick={(e) => {
            e.preventDefault();
            void openOnBranch(branch, g.group, e.currentTarget.hash);
          }}>{pipelineName(g.group, g.pipeline)}</a
        >
        <span class="dim">in {g.group}</span>
      </h3>
      <ul>
        {#each g.found as f, i (i)}
          <li>
            <span class="step mono">{f.step ?? ''}</span>
            {#if f.breaks}<span><Ticked text={f.message} /></span>{:else}<span class="dim"
                ><span aria-hidden="true">ⓘ&nbsp;</span>{#if f.recommended}<Recommends
                    {...f.recommended}
                  />{:else}<Ticked text={f.message} />{/if}</span
              >{/if}
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
