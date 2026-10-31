<!--
  A list of schemas by id, as chips, each a link to it on the Schemas page, and a choice of the rest to add: what a
  pipeline produces, and what a destination accepts.
-->
<script lang="ts">
  import type { SchemaDto } from './api';
  import RemovableChip from './RemovableChip.svelte';

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
  const offered = $derived(declared.filter((d) => !ids.includes(d.id)));

  function add(id: string) {
    if (id && !ids.includes(id)) ids = [...ids, id];
    picking = '';
  }
</script>

{#each ids as id (id)}
  <RemovableChip
    removeLabel="remove {id}"
    onremove={readOnly ? undefined : () => (ids = ids.filter((p) => p !== id))}
  >
    <a class="mono" href="#/build/schemas?open={encodeURIComponent(id)}">{id}</a>
  </RemovableChip>
{/each}
{#if !readOnly && offered.length === 0}
  <!-- Nothing left to choose: a schema is made on the Schemas page. -->
  <a class="add" href="#/build/schemas">Add a schema on the Schemas page</a>
{:else if !readOnly}
  <select aria-label={adding} bind:value={picking} onchange={() => add(picking)}>
    <option value="">+ add schema</option>
    {#each offered as d (d.id)}
      <option value={d.id}>{d.id}</option>
    {/each}
  </select>
{/if}

<style>
  .add {
    color: var(--accent);
    font-size: var(--text-note);
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
