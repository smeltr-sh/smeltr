<!--
  A disclosure for settings with a working default (STYLE_GUIDE.md,
  "Advanced settings"). A `Collapsible` whose header says when something
  inside is not at its default: a collapsed section that hid a live
  setting would make the visible config a lie. Starts open when
  `modified`, unless the caller binds `open`, and opens whenever a field
  inside has an error.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import type { Snippet } from 'svelte';
  import Collapsible from './Collapsible.svelte';
  import Help from './Help.svelte';
  import ErrorCount from './ErrorCount.svelte';

  let {
    label = 'Advanced',
    values,
    modified,
    errors = 0,
    // Read once: a `$derived` would collapse the block the moment a field
    // was edited back to its default.
    open = $bindable(untrack(() => (modified ?? (values?.length ?? 0) > 0) || errors > 0)),
    help,
    children,
  }: {
    label?: string;
    /** Each setting inside that is not at its default, by its label, as the
     * header names them. Without these the header only says whether any is. */
    values?: { label: string; value: string }[];
    /** True when any setting inside differs from its default. Drives both
     * the header marker and, unless `open` is bound, whether this starts open.
     * Given `values`, it is whether there are any. */
    modified?: boolean;
    /** How many fields inside have an error. */
    errors?: number;
    open?: boolean;
    /** The label's `?`. */
    help?: Snippet;
    children: Snippet;
  } = $props();

  let changed = $derived(modified ?? (values?.length ?? 0) > 0);

  // An error inside is never left folded away. Opening is all this does: the
  // author may fold it again.
  $effect(() => {
    if (errors > 0) open = true;
  });
</script>

<!-- Outside the toggle: a `?` inside it would open the block too. -->
{#snippet ask()}
  {#if help}<Help {label}>{@render help()}</Help>{/if}
{/snippet}

<div class="advanced" class:has-error={errors > 0}>
  <Collapsible bind:open actions={help ? ask : undefined}>
    {#snippet summary()}
      <span class="label">{label}</span>
      <ErrorCount {errors} />
      {#if values && values.length > 0}
        <span class="values">
          {#each values as v, i (v.label)}
            {#if i > 0}<span class="sep" aria-hidden="true">·</span>{/if}
            <span>{v.label} <code>{v.value}</code></span>
          {/each}
        </span>
      {:else if changed}
        <span class="modified">changed from defaults</span>
      {:else}
        <span class="dim">defaults</span>
      {/if}
    {/snippet}
    {@render children()}
  </Collapsible>
</div>

<style>
  .advanced {
    margin-top: 0.6rem;
  }

  .label {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  /* Ink and bold against the dim "defaults", not warn: a changed default
     is a choice, not a problem, and the accent belongs to the primary action. */
  .modified,
  .values {
    margin-left: 0.5rem;
    font-size: var(--text-micro);
    font-weight: 600;
    color: var(--ink);
  }

  .sep {
    margin: 0 0.3rem;
    color: var(--dim);
  }

  .values code {
    font-family: var(--mono);
    font-weight: 500;
  }

  .dim {
    margin-left: 0.5rem;
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .has-error :global(.collapsible) {
    border-color: var(--error);
  }
</style>
