<!--
  Adding a schema (`NewObject`): imported by a pipeline of the bundle run over an uploaded file, from a JSON Schema
  pasted or uploaded, or empty. What it starts from is the draft's `type`; the schema itself is made on Add
  (`SchemasPage`).
-->
<script lang="ts" module>
  import type { CatalogType } from './TypeCatalog.svelte';

  export type SchemaStart = 'import' | 'jsonSchema' | 'empty';

  /** A schema being added, before it is one. */
  export type SchemaDraft = {
    id: string;
    type: SchemaStart;
    /** The JSON Schema, as text, for `jsonSchema`. */
    text: string;
    /** The importer, by pipeline id, for `import`. */
    pipeline: string;
    /** The uploaded file, for `import`, cut into records on the leader. */
    file: string;
    /** Where a record of the file ends, for `import`; absent, each line is one. */
    lineBreaker: string | undefined;
  };

  const LABELS: Record<SchemaStart, string> = {
    import: 'imported',
    jsonSchema: 'schema from JSON Schema',
    empty: 'empty',
  };

  export const SCHEMA_STARTS: CatalogType<SchemaStart>[] = [
    {
      type: 'import',
      label: 'Import',
      category: 'From a file',
      help: 'A pipeline of the bundle reads a file and writes one row per field: ECS, OCSF, a spreadsheet.',
      // An arrow into a tray.
      glyph: { d: 'M32 8 V40 M20 28 L32 40 L44 28 M12 46 V56 H52 V46' },
    },
    {
      type: 'jsonSchema',
      label: 'JSON Schema',
      category: 'From a file',
      help: 'Upload or paste a JSON Schema: your own, or one exported from here.',
      // A page with its corner folded.
      glyph: { d: 'M14 6 H40 L52 18 V58 H14 Z M40 6 V18 H52' },
    },
    {
      type: 'empty',
      label: 'Empty schema',
      category: 'Empty',
      help: 'No fields yet.',
      glyph: { d: 'M12 12 H52 V52 H12 Z M18 18 V46 H46 V18 Z', evenodd: true },
    },
  ];
</script>

<script lang="ts">
  import NewObject from './NewObject.svelte';
  import Field from './Field.svelte';
  import { suggestId } from './ids';
  import LineBreakerField from './LineBreakerField.svelte';

  let {
    open,
    unavailable,
    importers,
    onadd,
    oncancel,
  }: {
    open: boolean;
    /** Every id the new one may not take. */
    unavailable: string[];
    /** The pipelines an import may run, by id, with their names. */
    importers: { id: string; name: string }[];
    /** Takes the draft, and makes the schema. The dialog stays open until its owner closes it. */
    onadd: (draft: SchemaDraft) => void | Promise<void>;
    oncancel: () => void;
  } = $props();

  function blank(type: SchemaStart, taken: string[]): SchemaDraft {
    return {
      id: suggestId('schema', taken),
      type,
      text: '',
      pipeline: '',
      file: '',
      lineBreaker: undefined,
    };
  }

  async function upload(draft: SchemaDraft, input: HTMLInputElement) {
    const file = input.files?.[0];
    if (file) draft.text = await file.text();
  }

  async function uploadFile(draft: SchemaDraft, input: HTMLInputElement) {
    const file = input.files?.[0];
    if (!file) return;
    draft.file = await file.text();
  }

  function ready(d: SchemaDraft): boolean {
    switch (d.type) {
      case 'jsonSchema':
        return d.text.trim() !== '';
      case 'import':
        return d.pipeline !== '' && d.file !== '';
      default:
        return true;
    }
  }
</script>

<NewObject
  {open}
  noun="schema"
  {unavailable}
  types={SCHEMA_STARTS}
  {blank}
  typeLabel={(t) => LABELS[t]}
  {ready}
  {onadd}
  {oncancel}
>
  {#snippet fields(draft)}
    {#if draft.type === 'import'}
      <Field
        required
        label="Importer"
        help="A pipeline that writes one row per field: field, type, required, description."
      >
        <select bind:value={draft.pipeline}>
          <option value="">Choose a pipeline…</option>
          {#each importers as p (p.id)}
            <option value={p.id}>{p.name || p.id}</option>
          {/each}
        </select>
      </Field>
      <Field required label="File">
        <input type="file" onchange={(e) => uploadFile(draft, e.currentTarget)} />
      </Field>
      <LineBreakerField value={draft.lineBreaker} onchange={(p) => (draft.lineBreaker = p)} />
    {:else if draft.type === 'jsonSchema'}
      <Field required label="JSON Schema">
        <textarea
          class="mono"
          rows="12"
          bind:value={draft.text}
          placeholder={'{ "type": "object", "properties": { … } }'}></textarea>
      </Field>
      <label class="upload">
        or upload a file
        <input
          type="file"
          accept=".json,application/json"
          onchange={(e) => upload(draft, e.currentTarget)}
        />
      </label>
    {/if}
  {/snippet}
</NewObject>

<style>
  textarea {
    width: 100%;
    box-sizing: border-box;
  }

  .upload {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    color: var(--dim);
    font-size: var(--text-note);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
