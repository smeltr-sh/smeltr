<!--
  Adding a schema (`NewObject`): read from a file a standard publishes (ECS, an OCSF class, a CSV of fields, JSON
  Schema), imported by a pipeline of the bundle run over an uploaded file of any other format, or empty. What it starts
  from is the draft's `type`; the schema itself is made on Add (`SchemasPage`).
-->
<script lang="ts" module>
  import type { CatalogType } from './TypeCatalog.svelte';
  import type { OcsfLevel, SchemaReadFormat } from './api';

  export type SchemaStart = 'ecs' | 'ocsf' | 'csv' | 'jsonSchema' | 'import' | 'empty';

  /** The format each start that reads a file reads it as. */
  export const READ_FORMATS: Partial<Record<SchemaStart, SchemaReadFormat>> = {
    ecs: 'ecs',
    ocsf: 'ocsf',
    csv: 'csv',
    jsonSchema: 'json-schema',
  };

  /** OCSF's requirement levels, in order. */
  export const OCSF_LEVELS: OcsfLevel[] = ['required', 'recommended', 'optional'];

  /** A schema being added, before it is one. */
  export type SchemaDraft = {
    id: string;
    type: SchemaStart;
    /** The file, as text, for a start that reads one; JSON Schema may be pasted. */
    text: string;
    /** The importer, by pipeline id, for `import`. */
    pipeline: string;
    /** The uploaded file, for `import`, cut into records on the leader. */
    file: string;
    /** Where a record of the file ends, for `import`; absent, each line is one. */
    lineBreaker: string | undefined;
    /** The OCSF file's classes, and the one read. */
    classes: string[];
    class: string;
    /** How many keys deep an OCSF class is read. */
    depth: number;
    /** Which OCSF requirement levels are read. */
    levels: OcsfLevel[];
  };

  const LABELS: Record<SchemaStart, string> = {
    ecs: 'schema from ECS',
    ocsf: 'schema from an OCSF class',
    csv: 'schema from a CSV of fields',
    jsonSchema: 'schema from JSON Schema',
    import: 'schema imported from a file',
    empty: 'empty',
  };

  export const SCHEMA_STARTS: CatalogType<SchemaStart>[] = [
    {
      type: 'ecs',
      label: 'ECS',
      category: 'From a standard',
      help: "The Elastic Common Schema, from the fields.csv or ecs_flat.yml in ECS's generated/ folder.",
      // Three layers.
      glyph: { d: 'M32 8 L56 20 L32 32 L8 20 Z M8 32 L32 44 L56 32 M8 44 L32 56 L56 44' },
    },
    {
      type: 'ocsf',
      label: 'OCSF',
      category: 'From a standard',
      help: "One class of the Open Cybersecurity Schema Framework, from ocsf-schema-compiler's output.",
      // A shield.
      glyph: { d: 'M32 6 L52 14 V30 C52 44 42 54 32 58 C22 54 12 44 12 30 V14 Z' },
    },
    {
      type: 'csv',
      label: 'CSV of fields',
      category: 'From a file',
      help: 'A spreadsheet with a row per field, under a header naming field and type, and optionally required, description, example and elasticsearch.',
      // A grid.
      glyph: { d: 'M10 14 H54 V50 H10 Z M10 26 H54 M10 38 H54 M26 14 V50 M40 14 V50' },
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
      type: 'import',
      label: 'Import',
      category: 'From a pipeline',
      help: 'A pipeline of the bundle reads a file of any other format and writes one row per field.',
      // An arrow into a tray.
      glyph: { d: 'M32 8 V40 M20 28 L32 40 L44 28 M12 46 V56 H52 V46' },
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
      id: suggestId(type === 'ecs' ? 'ecs' : 'schema', taken),
      type,
      text: '',
      pipeline: '',
      file: '',
      lineBreaker: undefined,
      classes: [],
      class: '',
      depth: 2,
      levels: ['required', 'recommended'],
    };
  }

  async function upload(draft: SchemaDraft, input: HTMLInputElement) {
    const file = input.files?.[0];
    if (!file) return;
    draft.text = await file.text();
    if (draft.type === 'ocsf') {
      // The classes to pick from. A file that is not JSON lists none, and the leader says why on Add.
      try {
        draft.classes = Object.keys(JSON.parse(draft.text).classes ?? {}).sort();
      } catch {
        draft.classes = [];
      }
      draft.class = draft.classes.includes(draft.class) ? draft.class : (draft.classes[0] ?? '');
    }
  }

  async function uploadFile(draft: SchemaDraft, input: HTMLInputElement) {
    const file = input.files?.[0];
    if (!file) return;
    draft.file = await file.text();
  }

  function level(draft: SchemaDraft, l: OcsfLevel, on: boolean) {
    draft.levels = OCSF_LEVELS.filter((x) => (x === l ? on : draft.levels.includes(x)));
  }

  function ready(d: SchemaDraft): boolean {
    switch (d.type) {
      case 'ecs':
      case 'csv':
      case 'jsonSchema':
        return d.text.trim() !== '';
      case 'ocsf':
        return d.text !== '' && d.class !== '' && d.levels.length > 0 && d.depth >= 1;
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
    {:else if draft.type === 'ecs' || draft.type === 'csv'}
      <Field required label="File">
        <input
          type="file"
          accept={draft.type === 'ecs' ? '.csv,.yml,.yaml' : '.csv,text/csv'}
          onchange={(e) => upload(draft, e.currentTarget)}
        />
      </Field>
    {:else if draft.type === 'ocsf'}
      <Field required label="File">
        <input
          type="file"
          accept=".json,application/json"
          onchange={(e) => upload(draft, e.currentTarget)}
        />
      </Field>
      {#if draft.text !== ''}
        <Field required label="Class">
          <select bind:value={draft.class}>
            {#if draft.classes.length === 0}<option value="">No classes in the file</option>{/if}
            {#each draft.classes as c (c)}<option value={c}>{c}</option>{/each}
          </select>
        </Field>
        <Field
          required
          label="Depth"
          help="How many keys deep fields are read: 1 is the class's own attributes, 2 adds those of the objects they hold."
        >
          <input type="number" min="1" max="8" bind:value={draft.depth} />
        </Field>
        <fieldset class="levels">
          <legend>Requirements read</legend>
          {#each OCSF_LEVELS as l (l)}
            <label
              ><input
                type="checkbox"
                checked={draft.levels.includes(l)}
                onchange={(e) => level(draft, l, e.currentTarget.checked)}
              />
              {l}</label
            >
          {/each}
        </fieldset>
      {/if}
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

  .levels {
    display: flex;
    gap: 1rem;
    border: 0;
    padding: 0;
    margin: 0;
  }

  .levels legend {
    font-weight: 600;
    font-size: var(--text-label);
    margin-bottom: 0.3rem;
  }
</style>
