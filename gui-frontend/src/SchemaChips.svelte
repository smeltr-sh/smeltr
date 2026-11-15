<!--
  The schemas a pipeline produces or a destination accepts, listed by id as chips that link to the Schemas page, with
  a choice of the other schemas to add. New schema makes one there and then, saves it with the scope's schemas at
  once, and names it in the list.
-->
<script lang="ts">
  import { putSchemas, type FileConfigDto, type SchemaDto } from './api';
  import RemovableChip from './RemovableChip.svelte';
  import PickOrNew from './PickOrNew.svelte';
  import NewSchema, { schemaOf, type SchemaDraft } from './NewSchema.svelte';

  let {
    ids = $bindable(),
    shared,
    own,
    config,
    importers,
    adding,
    onmade,
    readOnly = false,
  }: {
    ids: string[];
    /** `shared/`'s schemas, offered to add. */
    shared: SchemaDto[];
    /** The scope's schemas, offered to add; a new schema is saved with them. */
    own: SchemaDto[];
    /** The scope's bundle as the leader reads it, which a schema import's pipeline runs in. */
    config: FileConfigDto | null;
    /** The pipelines an import may run. */
    importers: { id: string; name: string }[];
    /** The add choice's name, read out: "Add a schema it produces". */
    adding: string;
    /** Called with the schema New schema made and the commit its save made. */
    onmade: (schema: SchemaDto, commit: string | null) => void;
    readOnly?: boolean;
  } = $props();

  const declared = $derived([...shared, ...own]);
  const offered = $derived(declared.filter((d) => !ids.includes(d.id)));
  let making = $state(false);

  async function make(draft: SchemaDraft) {
    const schema = await schemaOf(draft, config);
    const saved = await putSchemas([...$state.snapshot(own), schema]);
    if (!saved.ok) throw new Error(saved.errors.map((e) => e.message).join(' '));
    onmade(schema, saved.commit ?? null);
    ids = [...ids, schema.id];
    making = false;
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
  <button type="button" class="add link-button" aria-label={adding} onclick={() => (making = true)}
    >+ New schema…</button
  >
{:else if !readOnly}
  <PickOrNew
    class="schema-add"
    label={adding}
    value=""
    placeholder="+ add schema"
    choices={offered.map((d) => d.id)}
    newLabel="+ New schema…"
    onnew={() => (making = true)}
    onpick={(id) => (ids = [...ids, id])}
  />
{/if}
{#if !readOnly}
  <NewSchema
    open={making}
    unavailable={declared.map((d) => d.id)}
    {importers}
    onadd={make}
    oncancel={() => (making = false)}
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
