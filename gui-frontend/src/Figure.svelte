<!--
  One reading set apart to be read at a glance: its name over its value, and a note under it. Laid
  out in rows by `Figures.svelte`. `tone` colors the value when the reading needs a look.
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

<div class="figure">
  <span class="uppercase-label"
    >{label}{#if help}<Help {label}>{help}</Help>{/if}</span
  >
  <span class="value" class:warn={tone === 'warn'} class:alarm={tone === 'alarm'}
    >{@render children()}</span
  >
  {#if note}<span class="note">{note}</span>{/if}
  {@render after?.()}
</div>

<style>
  .figure {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    padding: 0.6rem 0.75rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
  }

  .value {
    font-variant-numeric: tabular-nums;
    font-size: 1.4rem;
    font-weight: 600;
    line-height: 1.1;
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
