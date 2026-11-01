<!--
  Schemas page: the bundle's `schemas/`, read through `GET /api/areas/:area` and written with `PUT /api/areas/:area/schemas`, plus the
  raw-JSON page. A schema is a contract a pipeline's rows are held to: a table of fields, each a dotted path and a
  type. The pipeline names it under Produces, and a destination may name it under accepts.

  One list, searched as one, by a schema's id, its title and its fields' paths: the group's own, editable, and
  `shared/`'s, changed in `shared/`. A row opens to the schema's fields, narrowed to the search.
-->
<script lang="ts">
  import EditInShared from './EditInShared.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import SaveBar from './SaveBar.svelte';
  import ValidationItem from './ValidationItem.svelte';
  import { provideValidation } from './validation.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import HeldEdit from './HeldEdit.svelte';
  import { SectionEditor } from './sectionEditor.svelte';
  import Field from './Field.svelte';
  import JsonEditPage from './JsonEditPage.svelte';
  import SchemaFields from './SchemaFields.svelte';
  import NewSchema, { READ_FORMATS, type SchemaDraft } from './NewSchema.svelte';
  import CrossButton from './CrossButton.svelte';
  import { resourcesTabs } from './nav';
  import {
    readSchema,
    importSchema,
    exportSchema,
    type ExportFormat,
    sharedConfig,
    putSchemas,
    type DestinationDto,
    type FileConfigDto,
    type PipelineDto,
    type SchemaDto,
    type Requirement,
    type SchemaFieldDto,
  } from './api';
  import { downloadJson, fileName, matchesOf, userHash, usersOf, type User } from './schemas';
  import { idProblem } from './ids';
  import { count } from './time';
  import { editable } from './access';
  import { reported } from './errors.svelte';

  let { showJson = false, open }: { showJson?: boolean; open?: string } = $props();

  const readOnly = !editable();

  let schemas: SchemaDto[] = $state([]);
  /** What `shared/` declares, which this group's pipelines may name as well. Never saved from here. */
  let sharedSchemas: SchemaDto[] = $state([]);
  /** What names a schema, here and in `shared/`. */
  let pipelines: PipelineDto[] = $state([]);
  let destinations: DestinationDto[] = $state([]);
  /** The group's own bundle as loaded, which an import runs a pipeline of. */
  let config: FileConfigDto | null = $state(null);
  const validation = provideValidation('schemas');
  const editor = new SectionEditor<SchemaDto[]>('schemas', {
    get: () => schemas,
    set: (value) => (schemas = value),
    pick: (config) => config.schemas ?? [],
    put: putSchemas,
    what: 'schemas',
    validation,
    onLoaded: async (loaded) => {
      config = loaded;
      const shared = await sharedConfig();
      sharedSchemas = shared?.schemas ?? [];
      pipelines = [...(shared?.pipelines ?? []), ...loaded.pipelines];
      destinations = [...(shared?.destinations ?? []), ...loaded.destinations];
    },
  });
  void editor.load();

  let users = $derived(usersOf(pipelines, destinations));

  /** The rows opened to their fields, by schema id. */
  let opened = $state(new Set<string>());

  // Land on the schema the hash names, opened.
  $effect(() => {
    if (open) opened = new Set([open]);
  });

  function toggle<T>(set: Set<T>, key: T): Set<T> {
    const next = new Set(set);
    if (!next.delete(key)) next.add(key);
    return next;
  }

  // -- search -------------------------------------------------------------

  let q = $state('');
  /** What the search found, by schema id, with the fields that hold the words. Null while there is no search. */
  let matches = $derived(matchesOf([...sharedSchemas, ...schemas], q));
  function shown(id: string): boolean {
    return matches === null || matches.has(id);
  }
  let total = $derived(schemas.length + sharedSchemas.length);

  // -- adding -------------------------------------------------------------

  let adding = $state(false);
  let taken = $derived([...schemas, ...sharedSchemas].map((s) => s.id));

  async function add(draft: SchemaDraft) {
    let schema: SchemaDto = { id: draft.id, fields: [] };
    if (draft.type === 'import') {
      if (!config) throw new Error('the bundle has not loaded');
      const read = await importSchema({
        id: draft.id,
        config: $state.snapshot(config),
        pipeline: draft.pipeline,
        text: draft.file,
        lineBreaker: draft.lineBreaker,
      });
      if (!read.ok) throw new Error(read.errors.map((e) => e.message).join(' '));
      schema = read.schema;
    } else if (READ_FORMATS[draft.type]) {
      const read = await readSchema({
        id: draft.id,
        format: READ_FORMATS[draft.type]!,
        text: draft.text,
        ...((draft.type === 'ecs' || draft.type === 'ocsf') && draft.builtIn
          ? { builtIn: true }
          : {}),
        ...(draft.type === 'ocsf'
          ? { class: draft.class, cut: { depth: draft.depth, levels: draft.levels } }
          : {}),
        ...(draft.type === 'ecs' ? { requirements: draft.requirements } : {}),
        ...(draft.type === 'csv' ? { columns: draft.columns, types: draft.types } : {}),
      });
      if (!read.ok) throw new Error(read.errors.map((e) => e.message).join(' '));
      schema = read.schema;
    }
    schemas.push(schema);
    opened = new Set(opened).add(draft.id);
    adding = false;
  }

  function require(of: SchemaDto, field: string, requirement: Requirement) {
    const f = of.fields.find((f) => f.field === field);
    if (!f) return;
    if (requirement === 'optional') delete f.requirement;
    else f.requirement = requirement;
  }

  function map(of: SchemaDto, field: string, mapping: SchemaFieldDto['elasticsearch']) {
    const f = of.fields.find((f) => f.field === field);
    if (!f) return;
    if (mapping) f.elasticsearch = mapping;
    else delete f.elasticsearch;
  }

  async function exported(id: string, format: ExportFormat) {
    const own = [...schemas, ...sharedSchemas].find((s) => s.id === id);
    if (!own) return;
    try {
      const written = await exportSchema($state.snapshot(own), format);
      if (written.ok) downloadJson(fileName(id, format), written.document);
      else reported(new Error(written.errors.map((e) => e.message).join(' ')));
    } catch (e) {
      reported(e);
    }
  }

  /** A declared schema's description: its own, or its title. */
  function about(s: SchemaDto): string {
    return s.description || s.title || '';
  }
</script>

{#if editor.loading}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else if editor.loadFailed}
  <SectionShell />
{:else if showJson}
  <JsonEditPage
    title="Edit JSON — Schemas"
    initialText={JSON.stringify(schemas, null, 2)}
    backHash="#/build/schemas"
    onSave={(text) => editor.saveFromJson(text)}
  />
{:else}
  <SectionShell>
    <PageHeader title="Schemas">
      {#snippet help()}
        A schema is what a pipeline's rows should hold: each field's type, and the fields they
        should have. A pipeline names the schemas it produces, and the editor and review say where
        its rows do not fit them. A destination can name the schemas it accepts.
      {/snippet}
    </PageHeader>
    <SecondaryTabs tabs={resourcesTabs()} active="#/build/schemas" ariaLabel="Resources" />

    <div>
      <UnreadableFiles problems={editor.bundleProblems} onchanged={() => editor.load()} />

      {#if editor.moved}
        <HeldEdit
          what="schemas"
          rows={editor.moved.rows}
          onrestore={() => editor.restoreHeld()}
          ondiscard={() => editor.dropHeld()}
        />
      {/if}

      {#if editor.conflict}
        <SaveConflictPanel
          conflict={editor.conflict}
          onDiscard={() => location.reload()}
          onRetry={() => editor.save()}
        />
      {/if}

      <div class="toolbar">
        <input
          type="search"
          placeholder="Search {count(total, 'schema')} and their fields"
          aria-label="Search schemas"
          bind:value={q}
        />
        <span class="grow"></span>
        {#if !readOnly}
          <button type="button" class="primary" onclick={() => (adding = true)}>+ Add schema</button
          >
          <button type="button" onclick={() => (location.hash = '#/build/schemas/json')}>
            Edit JSON&hellip;
          </button>
        {/if}
      </div>

      <table class="list">
        <thead>
          <tr>
            <th class="name">Name</th>
            <th class="kind">Fields</th>
            <th>Description</th>
            <th class="used">Used by</th>
          </tr>
        </thead>
        <tbody>
          {#each schemas as s, n (n)}
            <ValidationItem id={s.id} open={() => (q = '')}>
              <tr hidden={!shown(s.id)}>
                <td class="name">
                  <Field
                    required
                    bare
                    field="id"
                    label="id"
                    error={idProblem(
                      s.id,
                      taken.filter((_, i) => i !== n),
                    )}
                  >
                    <input type="text" class="mono" bind:value={s.id} disabled={readOnly} />
                  </Field>
                </td>
                <td class="kind">
                  <button
                    type="button"
                    class="opener"
                    aria-label="{s.id || 'unnamed'} fields"
                    aria-expanded={opened.has(s.id)}
                    onclick={() => (opened = toggle(opened, s.id))}
                    >{opened.has(s.id) ? '▾' : '▸'} {count(s.fields.length, 'field')}</button
                  >
                </td>
                <td class="about">
                  {about(s)}
                  {@render found(s.id)}
                </td>
                <td class="used">
                  {@render usedBy(users.get(s.id))}
                  {#if !readOnly}
                    <CrossButton
                      removes
                      label="Remove {s.id || 'unnamed'}"
                      onclick={() => schemas.splice(n, 1)}
                    />
                  {/if}
                </td>
              </tr>
              {#if opened.has(s.id) && shown(s.id)}
                {@render detail(s.id, s)}
              {/if}
            </ValidationItem>
          {/each}
          {#if schemas.length === 0 && matches === null}
            <tr><td colspan="4" class="dim">No schemas declared here yet.</td></tr>
          {/if}

          {#if sharedSchemas.length > 0}
            <tr class="band"
              ><th colspan="4">from shared/ &mdash; every worker group may name these</th></tr
            >
            {#each sharedSchemas as s (s.id)}
              {#if shown(s.id)}
                {@render row(s.id, `${count(s.fields.length, 'field')} · shared`, about(s), true)}
                {#if opened.has(s.id)}{@render detail(s.id, s, true)}{/if}
              {/if}
            {/each}
          {/if}
        </tbody>
      </table>
      {#if matches !== null && matches.size === 0}
        <p class="dim">No schema or field matches “{q.trim()}”.</p>
      {/if}
      <SaveBar
        dirty={editor.isDirty}
        saving={editor.saving}
        onsave={() => void editor.save()}
        ondiscard={() => editor.discard()}
      />
    </div>
  </SectionShell>

  <NewSchema
    open={adding}
    unavailable={taken}
    importers={pipelines.map((p) => ({ id: p.id, name: p.name }))}
    onadd={add}
    oncancel={() => (adding = false)}
  />
{/if}

{#snippet row(id: string, kind: string, description: string, fromShared = false)}
  <tr class="fixed" class:fromshared={fromShared}>
    <td class="name">
      <button
        type="button"
        class="opener mono"
        aria-expanded={opened.has(id)}
        onclick={() => (opened = toggle(opened, id))}>{opened.has(id) ? '▾' : '▸'} {id}</button
      >
    </td>
    <td class="kind">{kind}</td>
    <td class="about">
      {description}
      {@render found(id)}
    </td>
    <td class="used">
      {@render usedBy(users.get(id))}
      {#if fromShared}<EditInShared hash="#/build/schemas" />{/if}
    </td>
  </tr>
{/snippet}

{#snippet found(id: string)}
  {@const fields = matches?.get(id) ?? []}
  {#if fields.length > 0}
    <div class="found">
      fields: {#each fields as f, i (f)}{i > 0 ? ', ' : ''}<code>{f}</code>{/each}
    </div>
  {/if}
{/snippet}

{#snippet usedBy(list: User[] | undefined)}
  {#if !list || list.length === 0}
    <span class="dim">—</span>
  {:else}
    {#each list as u, i (`${u.kind} ${u.id}`)}{i > 0 ? ', ' : ''}<a href={userHash(u)}
        >{u.kind} {u.id}</a
      >{/each}
  {/if}
{/snippet}

{#snippet detail(id: string, of: SchemaDto, fromShared = false)}
  <tr class="detail" class:fromshared={fromShared}>
    <td colspan="4">
      <div class="actions">
        <span class="dim"
          >{#if fromShared}<EditInShared
              hash="#/build/schemas"
              label="Edit in shared/"
            />.{:else if readOnly}Edit it with Edit JSON.{:else}Choose each field’s requirement
            here, or edit it with Edit JSON.{/if}</span
        >
        <span class="grow"></span>
        <button type="button" onclick={() => void exported(id, 'json-schema')}
          >Export JSON Schema</button
        >
        <button type="button" onclick={() => void exported(id, 'elasticsearch')}
          >Export Elasticsearch mapping</button
        >
      </div>
      <SchemaFields
        {of}
        {q}
        onmap={fromShared || readOnly ? undefined : (field, m) => map(of, field, m)}
        onrequire={fromShared || readOnly ? undefined : (field, r) => require(of, field, r)}
      />
    </td>
  </tr>
{/snippet}

<style>
  .list {
    width: 100%;
    border-collapse: collapse;
  }

  th,
  td {
    padding: 0.25rem 0.5rem;
    border-bottom: 1px solid var(--border);
    text-align: left;
    vertical-align: middle;
  }

  thead th,
  .band th {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  .band th {
    padding-top: 1rem;
  }

  .name {
    width: 16rem;
  }

  .kind {
    width: 11rem;
    font-size: var(--text-note);
  }

  .used {
    width: 14rem;
    font-size: var(--text-note);
  }

  .fixed td {
    padding: 0.35rem 0.5rem;
    font-size: var(--text-note);
  }

  .about {
    overflow-wrap: anywhere;
  }

  /* `shared/`'s own: shown, because a group's pipelines may name them, and not editable here. */
  .fromshared td:first-child {
    border-left: 3px solid oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  .opener {
    padding: 0;
    border: none;
    background: none;
    color: inherit;
    text-align: left;
    font: inherit;
  }

  .found {
    color: var(--dim);
  }

  .detail > td {
    padding: 0.6rem 0.8rem 1rem 1.6rem;
    border-left: 3px solid var(--accent);
  }

  .actions {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin-bottom: 0.5rem;
    font-size: var(--text-note);
  }

  input {
    width: 100%;
  }

  .toolbar {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin: 1.2rem 0;
  }

  .toolbar input {
    width: 18rem;
  }

  .grow {
    flex: 1;
  }

  .used :global(.removes) {
    float: right;
  }

  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
