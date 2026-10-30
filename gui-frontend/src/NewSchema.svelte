<!--
  Adding a schema (`NewObject`): read from a file a standard publishes (ECS, an OCSF class, a CSV of fields, JSON
  Schema), imported by a pipeline of the bundle run over an uploaded file of any other format, or empty. What it starts
  from is the draft's `type`; the schema itself is made on Add (`SchemasPage`).
-->
<script lang="ts" module>
  import type { CatalogType } from './TypeCatalog.svelte';
  import type { CsvColumns, CsvPreview, OcsfLevel, SchemaReadFormat } from './api';
  import { FIELD_TYPES } from './api';

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
    /** A CSV as the leader read it before its columns were chosen, or why it could not. */
    preview: CsvPreview | null;
    previewError: string;
    /** Which CSV column holds what. */
    columns: CsvColumns;
    /** The type each value of the CSV's type column reads as. */
    types: Record<string, string>;
  };

  /** What a CSV column may hold. */
  export const CSV_ROLES = [
    'field',
    'type',
    'required',
    'description',
    'example',
    'elasticsearch',
  ] as const;

  /** The types a value of a CSV's type column may be read as. */
  export const SCHEMA_TYPES = [...FIELD_TYPES, '{...}'];

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
      help: "A CSV file with a row per field and a header row. You choose which column holds each field's name, which holds its type, and optionally which hold required, description, example and elasticsearch.",
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
      help: 'A pipeline in this area reads a file of any other format and writes one row per field.',
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
  import { readSchema } from './api';
  import { count } from './time';

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
      preview: null,
      previewError: '',
      columns: {},
      types: {},
    };
  }

  /** Reads the CSV for its preview, with the columns chosen, or as its header names them. A type already chosen
   * stays; one the leader does not know is read as `str` until another is chosen. */
  let previews = 0;
  async function previewCsv(draft: SchemaDraft, columns?: CsvColumns) {
    // Only the latest answers: a column picked while an earlier preview is out must not be undone by it.
    const asked = ++previews;
    const read = await readSchema({
      id: draft.id,
      format: 'csv',
      text: draft.text,
      preview: true,
      ...(columns ? { columns } : {}),
    }).catch((e: unknown) => ({ ok: false, errors: [{ message: String(e) }], preview: undefined }));
    if (asked !== previews) return;
    if (!read.ok || !read.preview) {
      draft.preview = null;
      draft.previewError = read.errors.map((e) => e.message).join(' ');
      return;
    }
    draft.preview = read.preview;
    draft.previewError = '';
    draft.columns = read.preview.columns;
    draft.types = Object.fromEntries(
      read.preview.types.map((t) => [t.value, draft.types[t.value] ?? t.readsAs ?? 'str']),
    );
  }

  /** Column `at` holds `role`, and no other column does; no role, it holds nothing the schema reads. */
  function hold(draft: SchemaDraft, at: number, role: string) {
    const columns: CsvColumns = {};
    for (const r of CSV_ROLES) {
      const i = draft.columns[r];
      if (i !== undefined && i !== null && i !== at && r !== role) columns[r] = i;
    }
    if (role) columns[role as (typeof CSV_ROLES)[number]] = at;
    draft.columns = columns;
    void previewCsv(draft, columns);
  }

  /** A row's cell in the column that holds `role`, trimmed. */
  function cellOf(draft: SchemaDraft, row: string[], role: (typeof CSV_ROLES)[number]): string {
    const at = draft.columns[role];
    return at == null ? '' : (row[at] ?? '').trim();
  }

  /** The role column `at` holds, or '' for none. */
  function roleOf(draft: SchemaDraft, at: number): string {
    return CSV_ROLES.find((r) => draft.columns[r] === at) ?? '';
  }

  async function upload(draft: SchemaDraft, input: HTMLInputElement) {
    const file = input.files?.[0];
    if (!file) return;
    draft.text = await file.text();
    if (draft.type === 'csv') {
      draft.types = {};
      await previewCsv(draft);
    }
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
      case 'csv':
        return d.preview !== null && d.columns.field != null && d.columns.type != null;
      case 'ecs':
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
      {#if draft.type === 'csv' && draft.previewError}
        <p class="problem" role="alert">{draft.previewError}</p>
      {:else if draft.type === 'csv' && draft.preview}
        {@const preview = draft.preview}
        <fieldset class="picker">
          <legend>What each column holds</legend>
          {#if draft.columns.field == null || draft.columns.type == null}
            <p class="note">
              Choose the column that holds each field's name, and the one that holds its type.
            </p>
          {/if}
          <div class="scroll">
            <table class="preview">
              <thead>
                <tr>
                  {#each preview.header as h, i (i)}
                    <th>
                      <select
                        aria-label={`Column ${h || i + 1} holds`}
                        value={roleOf(draft, i)}
                        onchange={(e) => hold(draft, i, e.currentTarget.value)}
                      >
                        <option value="">ignore</option>
                        {#each CSV_ROLES as r (r)}<option value={r}>{r}</option>{/each}
                      </select>
                    </th>
                  {/each}
                </tr>
                <tr>
                  {#each preview.header as h, i (i)}<th class="name mono">{h}</th>{/each}
                </tr>
              </thead>
              <tbody>
                {#each preview.rows as row, r (r)}
                  {@const value = cellOf(draft, row, 'type')}
                  <!-- A type value's choice is in the first row that holds it. -->
                  {@const first =
                    preview.rows.findIndex((o) => cellOf(draft, o, 'type') === value) === r}
                  {@const said = preview.types.find((t) => t.value === value)}
                  {#if r === preview.first}
                    <tr><td class="dim" colspan={preview.header.length}>…</td></tr>
                  {/if}
                  <tr>
                    {#each preview.header as _, i (i)}
                      {#if i === draft.columns.type && said}
                        <td class="typed">
                          <div class="mono">{row[i]}</div>
                          {#if first}
                            <div class="reads">
                              as <select
                                aria-label={`Type of ${value}`}
                                bind:value={draft.types[value]}
                              >
                                {#each said.readsAs && !SCHEMA_TYPES.includes(said.readsAs) ? [said.readsAs, ...SCHEMA_TYPES] : SCHEMA_TYPES as t (t)}
                                  <option value={t}>{t}</option>
                                {/each}
                              </select>
                              <span class="dim">{count(said.fields, 'field')}</span>
                              {#if said.readsAs === null}<span class="dim"
                                  >· not a type smeltr knows: choose how to read it</span
                                >{/if}
                            </div>
                          {:else}
                            <div class="reads dim">
                              as <span class="mono">{draft.types[value]}</span>
                            </div>
                          {/if}
                        </td>
                      {:else}
                        <td class="mono">{row[i] ?? ''}</td>
                      {/if}
                    {/each}
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
          {#if draft.columns.field != null && draft.columns.type != null}
            {@const reads = preview.rows
              .slice(0, preview.first)
              .map(
                (row) =>
                  `${cellOf(draft, row, 'field')}: ${draft.types[cellOf(draft, row, 'type')] ?? ''}`,
              )}
            <p class="note">
              Reads as {#each reads as said, r (r)}{r > 0 ? ', ' : ''}<span class="mono"
                  >{said}</span
                >{/each}{preview.types.reduce((n, t) => n + t.fields, 0) > reads.length
                ? ', …'
                : ''}
            </p>
          {/if}
        </fieldset>
      {/if}
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
          help="How many levels of nested objects to read: 1 is the class's own attributes, 2 adds those of the objects they hold."
        >
          <input type="number" min="1" max="8" bind:value={draft.depth} />
        </Field>
        <fieldset class="levels">
          <legend>Include fields that are</legend>
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

  .picker {
    border: 0;
    padding: 0;
    margin: 0;
    min-width: 0;
  }

  .picker legend {
    font-weight: 600;
    margin-bottom: 0.3rem;
  }

  .scroll {
    overflow-x: auto;
    max-width: 100%;
  }

  .preview {
    border-collapse: collapse;
    font-size: var(--text-note);
  }

  .preview th,
  .preview td {
    padding: 0.2rem 0.5rem 0.2rem 0;
    text-align: left;
    vertical-align: top;
    white-space: nowrap;
    max-width: 16rem;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  /* A type cell holds its choice, which wraps rather than being cut short. */
  .preview .typed {
    white-space: normal;
    max-width: 20rem;
  }

  .reads {
    margin-top: 0.15rem;
  }

  .preview .name {
    color: var(--dim);
    font-weight: 400;
  }

  .problem {
    color: var(--error);
    margin: 0;
  }

  .note {
    margin: 0;
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
