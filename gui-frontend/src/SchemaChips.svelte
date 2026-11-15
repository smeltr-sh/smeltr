<!--
  A list of schemas by id, as chips, each a link to it on the Schemas page, and a choice of the rest to add: what a
  pipeline produces, and what a destination accepts.
-->
<script lang="ts">
  import type { SchemaDto } from './api';
  import RemovableChip from './RemovableChip.svelte';
  import PickOrNew from './PickOrNew.svelte';

  let {
    ids = $bindable(),
    declared,
    adding,
    onnew,
    readOnly = false,
  }: {
    ids: string[];
    /** The bundle's schemas, `shared/`'s and the scope's, offered to add. */
    declared: SchemaDto[];
    /** The add choice's name, read out: "Add a schema it produces". */
    adding: string;
    /** Opens New schema, so a schema is made there and then. Without it, a schema is made on the Schemas page. */
    onnew?: () => void;
    readOnly?: boolean;
  } = $props();

  const offered = $derived(declared.filter((d) => !ids.includes(d.id)));
</script>

{#each ids as id (id)}
  <RemovableChip
    removeLabel="remove {id}"
    onremove={readOnly ? undefined : () => (ids = ids.filter((p) => p !== id))}
  >
    <a class="mono" href="#/build/schemas?open={encodeURIComponent(id)}">{id}</a>
  </RemovableChip>
{/each}
{#if !readOnly && offered.length === 0 && onnew}
  <button type="button" class="add link-button" onclick={onnew}>+ New schema…</button>
{:else if !readOnly && offered.length === 0}
  <!-- Nothing left to choose: a schema is made on the Schemas page. -->
  <a class="add" href="#/build/schemas">Add a schema on the Schemas page</a>
{:else if !readOnly}
  <PickOrNew
    class="schema-add"
    label={adding}
    value=""
    placeholder="+ add schema"
    choices={offered.map((d) => d.id)}
    newLabel="+ New schema…"
    {onnew}
    onpick={(id) => (ids = [...ids, id])}
  />
{/if}

<style>
  .add {
    color: var(--accent);
    font-size: var(--text-note);
  }

  :global(select.schema-add) {
    width: auto;
    border-color: transparent;
    background: none;
    color: var(--accent);
    font-size: var(--text-note);
  }
</style>
