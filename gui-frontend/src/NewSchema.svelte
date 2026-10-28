<!--
  Adding a schema (`NewObject`): from a JSON Schema pasted or uploaded, or empty. What it starts from is the draft's `type`; the schema itself is made on Add (`SchemasPage`).
-->
<script lang="ts" module>
  import type { CatalogType } from './TypeCatalog.svelte';

  export type SchemaStart = 'jsonSchema' | 'empty';

  /** A schema being added, before it is one. */
  export type SchemaDraft = {
    id: string;
    type: SchemaStart;
    /** The JSON Schema, as text, for `jsonSchema`. */
    text: string;
  };

  const LABELS: Record<SchemaStart, string> = {
    jsonSchema: 'JSON Schema',
    empty: 'empty',
  };

  export const SCHEMA_STARTS: CatalogType<SchemaStart>[] = [
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

  let {
    open,
    unavailable,
    onadd,
    oncancel,
  }: {
    open: boolean;
    /** Every id the new one may not take. */
    unavailable: string[];
    /** Takes the draft, and makes the schema. The dialog stays open until its owner closes it. */
    onadd: (draft: SchemaDraft) => void | Promise<void>;
    oncancel: () => void;
  } = $props();

  function blank(type: SchemaStart, taken: string[]): SchemaDraft {
    return { id: suggestId('schema', taken), type, text: '' };
  }

  async function upload(draft: SchemaDraft, input: HTMLInputElement) {
    const file = input.files?.[0];
    if (file) draft.text = await file.text();
  }
</script>

<NewObject
  {open}
  noun="schema"
  {unavailable}
  types={SCHEMA_STARTS}
  {blank}
  typeLabel={(t) => LABELS[t]}
  ready={(d) => d.type !== 'jsonSchema' || d.text.trim() !== ''}
  {onadd}
  {oncancel}
>
  {#snippet fields(draft)}
    {#if draft.type === 'jsonSchema'}
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
