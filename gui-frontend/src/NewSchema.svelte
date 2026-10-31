<!--
  Adding a schema (`NewObject`): read from a file a standard publishes (ECS, an OCSF class, a CSV of fields, JSON
  Schema), imported by a pipeline of the bundle run over an uploaded file of any other format, or empty. What it starts
  from is the draft's `type`; the schema itself is made on Add (`SchemasPage`).
-->
<script lang="ts" module>
  import type { CatalogType } from './TypeCatalog.svelte';
  import type {
    CsvColumns,
    CsvPreview,
    EcsRequirements,
    OcsfClass,
    Requirement,
    SchemaReadFormat,
  } from './api';
  import { FIELD_TYPES } from './api';
  import { REQUIREMENTS } from './schemas';

  export type SchemaStart = 'ecs' | 'ocsf' | 'csv' | 'jsonSchema' | 'import' | 'empty';

  /** The format each start that reads a file reads it as. */
  export const READ_FORMATS: Partial<Record<SchemaStart, SchemaReadFormat>> = {
    ecs: 'ecs',
    ocsf: 'ocsf',
    csv: 'csv',
    jsonSchema: 'json-schema',
  };

  /** A schema being added, before it is one. */
  export type SchemaDraft = {
    id: string;
    type: SchemaStart;
    /** The file, as text, for a start that reads one; JSON Schema may be pasted. */
    text: string;
    /** For ECS and OCSF, read the release the leader carries rather than `text`. Until the leader says which it
     * carries, a file is uploaded, and an upload turns this off. */
    builtIn: boolean;
    /** The importer, by pipeline id, for `import`. */
    pipeline: string;
    /** The uploaded file, for `import`, cut into records on the leader. */
    file: string;
    /** Where a record of the file ends, for `import`; absent, each line is one. */
    lineBreaker: string | undefined;
    /** The uploaded OCSF file's classes, and the one read, by name. */
    classes: OcsfClass[];
    class: string;
    /** The id the dialog last suggested, which follows the class until the id is typed over. */
    suggested: string;
    /** How many keys deep an OCSF class is read. */
    depth: number;
    /** Which OCSF requirement levels are read. */
    levels: Requirement[];
    /** Whose requirements an ECS schema's fields take: ECS's own, or Elastic Security's added to them. */
    requirements: EcsRequirements;
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
    'requirement',
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
      help: "The Elastic Common Schema: the release built in, or the fields.csv or ecs_flat.yml under ECS's generated/ folder.",
      // Three layers.
      glyph: { d: 'M32 8 L56 20 L32 32 L8 20 Z M8 32 L32 44 L56 32 M8 44 L32 56 L56 44' },
    },
    {
      type: 'ocsf',
      label: 'OCSF',
      category: 'From a standard',
      help: "One class of the Open Cybersecurity Schema Framework: from the release built in, or from ocsf-schema-compiler's output.",
      // A shield.
      glyph: { d: 'M32 6 L52 14 V30 C52 44 42 54 32 58 C22 54 12 44 12 30 V14 Z' },
    },
    {
      type: 'csv',
      label: 'CSV of fields',
      category: 'From a file',
      help: "A CSV file with a header row and a row per field. You choose the columns for each field's name and type. Columns for its requirement (required, recommended or optional), description, example value and Elasticsearch type are optional.",
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
  import { getSchemaStandards, readSchema, type SchemaStandards } from './api';
  import Segmented from './Segmented.svelte';
  import { count } from './time';
  import Help from './Help.svelte';

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

  /** The releases the leader carries; null until it says, and where it cannot, so a file is uploaded. Asked as the
   * page opens rather than the dialog, so it is here by the time a tile is chosen. */
  let standards = $state<SchemaStandards | null>(null);
  $effect(() => {
    getSchemaStandards().then(
      (s) => (standards = s),
      () => {},
    );
  });

  /** Whether `d` reads the release the leader carries. */
  function fromRelease(d: SchemaDraft): boolean {
    return d.builtIn && standards !== null;
  }

  /** The OCSF classes `d` may be read from. */
  function classesOf(d: SchemaDraft): OcsfClass[] {
    return fromRelease(d) ? standards!.ocsf.classes : d.classes;
  }

  /** `classes` by category, each by caption; a class in no category comes last, under Other. */
  function byCategory(classes: OcsfClass[]): [string, OcsfClass[]][] {
    const groups = new Map<string, OcsfClass[]>();
    for (const c of [...classes].sort((a, b) => a.caption.localeCompare(b.caption))) {
      const key = c.category || 'Other';
      groups.set(key, [...(groups.get(key) ?? []), c]);
    }
    return [...groups].sort(([a], [b]) =>
      a === 'Other' ? 1 : b === 'Other' ? -1 : a.localeCompare(b),
    );
  }

  /** The class a draft starts on, where `classes` holds it. */
  function firstClass(classes: OcsfClass[]): string {
    return classes.some((c) => c.name === 'authentication')
      ? 'authentication'
      : (classes[0]?.name ?? '');
  }

  /** The class `d` reads: the one chosen, or, where the releases arrived after the tile was chosen, the first. */
  function classOf(d: SchemaDraft): string {
    return d.class || firstClass(classesOf(d));
  }

  /** `d` reads `name`, and its id follows, unless it was typed over. */
  function setClass(d: SchemaDraft, name: string) {
    d.class = name;
    if (d.type !== 'ocsf' || !name || d.id !== d.suggested) return;
    d.id = d.suggested = suggestId(name, unavailable);
  }

  /** `d` reads the release, or an upload; its class stays where the other offers it. */
  function setBuiltIn(d: SchemaDraft, on: boolean) {
    d.builtIn = on;
    const classes = classesOf(d);
    if (d.type === 'ocsf' && !classes.some((c) => c.name === d.class))
      setClass(d, firstClass(classes));
  }

  function blank(type: SchemaStart, taken: string[]): SchemaDraft {
    const first = type === 'ocsf' ? firstClass(standards?.ocsf.classes ?? []) : '';
    const id = suggestId(type === 'ecs' ? 'ecs' : first || 'schema', taken);
    return {
      id,
      suggested: id,
      type,
      text: '',
      builtIn: true,
      pipeline: '',
      file: '',
      lineBreaker: undefined,
      classes: [],
      class: first,
      depth: 2,
      levels: ['required', 'recommended'],
      requirements: 'ecs',
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
    draft.builtIn = false;
    if (draft.type === 'csv') {
      draft.types = {};
      await previewCsv(draft);
    }
    if (draft.type === 'ocsf') {
      // The classes to pick from. A file that is not JSON lists none, and the leader says why on Add.
      try {
        const listed: Record<string, { caption?: string; category_name?: string }> =
          JSON.parse(draft.text).classes ?? {};
        draft.classes = Object.entries(listed).map(([name, c]) => ({
          name,
          caption: c.caption || name,
          category: c.category_name ?? '',
        }));
      } catch {
        draft.classes = [];
      }
      if (!draft.classes.some((c) => c.name === draft.class)) {
        setClass(draft, firstClass(draft.classes));
      }
    }
  }

  async function uploadFile(draft: SchemaDraft, input: HTMLInputElement) {
    const file = input.files?.[0];
    if (!file) return;
    draft.file = await file.text();
  }

  function level(draft: SchemaDraft, l: Requirement, on: boolean) {
    draft.levels = REQUIREMENTS.filter((x) => (x === l ? on : draft.levels.includes(x)));
  }

  function ready(d: SchemaDraft): boolean {
    switch (d.type) {
      case 'csv':
        return d.preview !== null && d.columns.field != null && d.columns.type != null;
      case 'ecs':
        return fromRelease(d) || d.text.trim() !== '';
      case 'jsonSchema':
        return d.text.trim() !== '';
      case 'ocsf':
        return (
          (fromRelease(d) || d.text !== '') &&
          classOf(d) !== '' &&
          d.levels.length > 0 &&
          d.depth >= 1
        );
      case 'import':
        return d.pipeline !== '' && d.file !== '';
      default:
        return true;
    }
  }
</script>

{#snippet ecsRequirementsHelp()}
  ECS requires @timestamp and ecs.version, and recommends its core fields. Elastic Security requires five fields:
  @timestamp, ecs.version, event.kind, event.category and event.type. It recommends the other fields its <a
    href="https://www.elastic.co/guide/en/security/current/siem-field-reference.html"
    target="_blank"
    rel="noreferrer">field reference</a
  > lists.
{/snippet}

{#snippet link(href: string)}
  <a {href} target="_blank" rel="noreferrer">{href.replace(/^https:\/\//, '').replace(/\/$/, '')}</a
  >
{/snippet}

<NewObject
  {open}
  noun="schema"
  {unavailable}
  types={SCHEMA_STARTS}
  {blank}
  typeLabel={(t) => LABELS[t]}
  {ready}
  onadd={(d) => {
    if (d.type === 'ocsf') d.class = classOf(d);
    return onadd(d);
  }}
  {oncancel}
>
  {#snippet fields(draft)}
    {#if (draft.type === 'ecs' || draft.type === 'ocsf') && standards}
      {@const release = draft.type === 'ecs' ? standards.ecs : standards.ocsf}
      {@const name = draft.type === 'ecs' ? 'ECS' : 'OCSF'}
      <div class="source">
        <Segmented
          label="Read from"
          options={[
            { value: 'built-in', label: `${name} ${release.version}, built in` },
            { value: 'upload', label: 'Upload a file' },
          ]}
          bind:value={
            () => (draft.builtIn ? 'built-in' : 'upload'),
            (v) => setBuiltIn(draft, v === 'built-in')
          }
        />
      </div>
      {#if draft.builtIn}
        <p class="note">
          As published at {@render link(release.source)}{draft.type === 'ocsf'
            ? ', compiled by ocsf-schema-compiler'
            : ''}.
        </p>
      {:else if draft.type === 'ecs'}
        <p class="note">
          The fields.csv or ecs_flat.yml under the generated/ folder of {@render link(
            'https://github.com/elastic/ecs',
          )}, at the release you use.
        </p>
      {:else}
        <p class="note">
          The output of {@render link('https://pypi.org/project/ocsf-schema-compiler/')} run over
          {@render link('https://github.com/ocsf/ocsf-schema')}, at the release you use.
        </p>
      {/if}
    {/if}
    {#if draft.type === 'ecs'}
      <Field label="Requirements" help={ecsRequirementsHelp}>
        <select bind:value={draft.requirements}>
          <option value="ecs">ECS</option>
          <option value="elastic-security">Elastic Security</option>
        </select>
      </Field>
    {/if}
    {#if draft.type === 'import'}
      <Field
        required
        label="Importer"
        help="A pipeline that writes one row per field: field, type, requirement, description."
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
    {:else if (draft.type === 'ecs' && !fromRelease(draft)) || draft.type === 'csv'}
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
        {@const typed = draft.columns.type != null && preview.types.length > 0}
        <fieldset class="picker">
          <legend
            >What each column holds <Help label="What each column holds"
              >Rows with the same type value share one choice. A value tagged unknown is not a type
              smeltr knows: choose how to read it.</Help
            ></legend
          >
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
                    {#if typed && i === draft.columns.type}<th></th>{/if}
                  {/each}
                </tr>
                <tr>
                  {#each preview.header as h, i (i)}
                    <th class="name mono">{h}</th>
                    {#if typed && i === draft.columns.type}<th class="name">reads as</th>{/if}
                  {/each}
                </tr>
              </thead>
              <tbody>
                {#each preview.rows as row, r (r)}
                  {@const value = cellOf(draft, row, 'type')}
                  {@const said = preview.types.find((t) => t.value === value)}
                  <!-- A type value's count and its name for a screen reader are at the first row that holds it. -->
                  {@const first =
                    preview.rows.findIndex((o) => cellOf(draft, o, 'type') === value) === r}
                  {#if r === preview.first}
                    <tr>
                      <td class="dim gap" colspan={preview.header.length + (typed ? 1 : 0)}
                        >… and a row for each other type value</td
                      >
                    </tr>
                  {/if}
                  <tr>
                    {#each preview.header as _, i (i)}
                      <td class="mono">{row[i] ?? ''}</td>
                      {#if typed && i === draft.columns.type}
                        <td class="reads">
                          {#if said}
                            <select
                              aria-label={first
                                ? `Type of ${value}`
                                : `Type of ${value}, row ${r + 1}`}
                              bind:value={draft.types[value]}
                            >
                              {#each said.readsAs && !SCHEMA_TYPES.includes(said.readsAs) ? [said.readsAs, ...SCHEMA_TYPES] : SCHEMA_TYPES as t (t)}
                                <option value={t}>{t}</option>
                              {/each}
                            </select>
                            {#if said.readsAs === null}<span class="unknown">unknown</span>{/if}
                            {#if first}<span class="dim">{count(said.fields, 'field')}</span>{/if}
                          {/if}
                        </td>
                      {/if}
                    {/each}
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        </fieldset>
      {/if}
    {:else if draft.type === 'ocsf'}
      {#if !fromRelease(draft)}
        <Field required label="File">
          <input
            type="file"
            accept=".json,application/json"
            onchange={(e) => upload(draft, e.currentTarget)}
          />
        </Field>
      {/if}
      {#if fromRelease(draft) || draft.text !== ''}
        {@const classes = classesOf(draft)}
        <Field required label="Class">
          <select bind:value={() => classOf(draft), (v) => setClass(draft, v)}>
            {#if classes.length === 0}<option value="">No classes in the file</option>
            {:else if !classes.some((c) => c.name === classOf(draft))}<option value=""
                >Choose a class…</option
              >{/if}
            {#each byCategory(classes) as [category, members] (category)}
              <optgroup label={category}>
                {#each members as c (c.name)}<option value={c.name}>{c.caption}</option>{/each}
              </optgroup>
            {/each}
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
          {#each REQUIREMENTS as l (l)}
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

  /* A file with many type values has a row for each; the table scrolls under its choices of column. */
  .scroll {
    overflow: auto;
    max-width: 100%;
    max-height: 24rem;
  }

  .preview thead {
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--panel);
  }

  .preview {
    border-collapse: collapse;
    font-size: var(--text-note);
  }

  .preview th,
  .preview td {
    padding: 0.2rem 0.5rem 0.2rem 0;
    text-align: left;
    vertical-align: middle;
    white-space: nowrap;
    max-width: 16rem;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .preview .reads {
    max-width: none;
    padding-right: 1.5rem;
  }

  .preview .reads select {
    padding-block: 0;
  }

  .preview .reads span {
    margin-left: 0.4rem;
  }

  .unknown {
    color: var(--warn);
  }

  .preview .gap {
    font-style: italic;
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

  .source {
    display: flex;
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
