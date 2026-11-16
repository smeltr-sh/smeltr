<!--
  A pipeline's Produces row: the schemas its rows are held to (`SchemaChips`), and how many places the last run found
  its rows do not fit them. The editor and review say where; nothing refuses a save.
-->
<script lang="ts">
  import type { ComponentProps } from 'svelte';
  import SchemaChips from './SchemaChips.svelte';
  import { count } from './time';

  let {
    produces = $bindable(),
    findings,
    readOnly = false,
    ...chips
  }: {
    /** The schema ids, in the order named. */
    produces: string[];
    /** What the last run found, absent before one. */
    findings: number | undefined;
    readOnly?: boolean;
  } & Omit<ComponentProps<typeof SchemaChips>, 'ids' | 'adding' | 'readOnly'> = $props();
</script>

<div class="produces">
  <span class="label">Produces</span>
  <SchemaChips bind:ids={produces} adding="Add a schema it produces" {readOnly} {...chips} />
  {#if readOnly && produces.length === 0}<span class="dim">no schema</span>{/if}
  <span class="grow"></span>
  {#if produces.length > 0 && findings !== undefined}
    <span class:warn={findings > 0} class="dim" role="status"
      >{findings > 0 ? count(findings, 'schema finding') : 'the rows fit'}</span
    >
  {/if}
</div>

<style>
  .produces {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    align-items: center;
    padding: 0.45rem 0.8rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--panel);
    font-size: var(--text-note);
  }

  .label {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-right: 0.3rem;
  }

  .grow {
    flex: 1;
  }

  .dim {
    color: var(--dim);
  }

  .warn {
    color: var(--warn);
  }
</style>
