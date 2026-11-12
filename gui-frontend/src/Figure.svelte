<!--
  One reading set apart to be read at a glance: its name over its value, and a note under it,
  marked up as a term with its definition. Laid out in rows by `Figures.svelte`. `tone` colors the
  value when the reading needs a look.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import Help from './Help.svelte';

  let {
    label,
    help,
    note,
    tone,
    children,
    after,
  }: {
    label: string;
    /** What the label leaves out, behind its `?`. */
    help?: string;
    note?: string;
    tone?: 'warn' | 'alarm';
    /** The value. */
    children: Snippet;
    /** Drawn under the value, such as a meter. */
    after?: Snippet;
  } = $props();
</script>

<dl class="figure">
  <dt class="uppercase-label">
    {label}{#if help}<Help {label}>{help}</Help>{/if}
  </dt>
  <dd class="value" class:warn={tone === 'warn'} class:alarm={tone === 'alarm'}>
    {@render children()}
  </dd>
  {#if note}<dd class="note">{note}</dd>{/if}
  {#if after}<dd>{@render after()}</dd>{/if}
</dl>

<style>
  .figure {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    margin: 0;
    padding: 0.6rem 0.75rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
  }

  dd {
    margin: 0;
  }

  .value {
    font-variant-numeric: tabular-nums;
    font-size: 1.4rem;
    font-weight: 600;
    line-height: 1.1;
  }

  /* Two figures to a line in a narrow panel; the value is sized so a long reading still fits on its
     line. */
  @container (max-width: 32rem) {
    .value {
      font-size: 1.1rem;
    }
  }

  .warn {
    color: var(--warn);
  }

  .alarm {
    color: var(--error);
  }

  .note {
    font-size: var(--text-label);
    color: var(--dim);
  }
</style>
