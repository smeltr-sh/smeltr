<!--
  A list of schemas by id, as chips, each a link to it on the Schemas page, and a choice of the rest to add: what a
  pipeline produces, and what a destination accepts.
-->
<script lang="ts">
  import type { SchemaDto } from './api';

  let {
    ids = $bindable(),
    declared,
    adding,
    readOnly = false,
  }: {
    ids: string[];
    /** The bundle's schemas, `shared/`'s and the scope's, offered to add. */
    declared: SchemaDto[];
    /** The add choice's name, read out: "Add a schema it produces". */
    adding: string;
    readOnly?: boolean;
  } = $props();

  let picking = $state('');

  function add(id: string) {
    if (id && !ids.includes(id)) ids = [...ids, id];
    picking = '';
  }
</script>

{#each ids as id (id)}
  <span class="chip">
    <a class="mono" href="#/build/schemas?open={encodeURIComponent(id)}">{id}</a>
    {#if !readOnly}
      <button
        type="button"
        aria-label="Remove {id}"
        onclick={() => (ids = ids.filter((p) => p !== id))}>×</button
      >
    {/if}
  </span>
{/each}
{#if !readOnly}
  <select aria-label={adding} bind:value={picking} onchange={() => add(picking)}>
    <option value="">+ add schema</option>
    {#each declared.filter((d) => !ids.includes(d.id)) as d (d.id)}
      <option value={d.id}>{d.id}</option>
    {/each}
  </select>
{/if}

<style>
  .chip {
    display: inline-flex;
    gap: 0.25rem;
    align-items: center;
    padding: 0.05rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--field);
    font-size: var(--text-note);
  }

  .chip button {
    padding: 0;
    border: none;
    background: none;
    color: var(--dim);
  }

  select {
    width: auto;
    border-color: transparent;
    background: none;
    color: var(--accent);
    font-size: var(--text-note);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
