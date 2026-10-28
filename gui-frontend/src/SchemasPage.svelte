<!--
  Schemas page: `schemas.json`, read through `GET /api/config` and written with `PUT /api/schemas`, plus the
  raw-JSON page. A schema is a contract a pipeline's rows are held to: the pipeline names it under Produces, and a
  destination may name it under accepts.

  One table, searched as one, by a schema's id, its title and its fields' paths: the group's own, editable;
  `shared/`'s, changed in `shared/`; ECS; and OCSF's classes under its categories, each category opened on its own and
  paged. A row opens to the schema's fields.
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
  import NewSchema, { type SchemaDraft } from './NewSchema.svelte';
  import TrashIcon from './TrashIcon.svelte';
  import { resourcesTabs } from './nav';
  import {
    builtinSchemas,
    exportSchema,
    schemaObject,
    searchSchemas,
    sharedConfig,
    putSchemas,
    type DestinationDto,
    type PipelineDto,
    type SchemaCatalog,
    type SchemaDto,
    type SchemaSummary,
  } from './api';
  import {
    byCategory,
    downloadJson,
    fileName,
    PAGE,
    paged,
    userHash,
    usersOf,
    type User,
  } from './schemas';
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
  const validation = provideValidation('schemas');
  const editor = new SectionEditor<SchemaDto[]>('schemas', {
    get: () => schemas,
    set: (value) => (schemas = value),
    pick: (config) => config.schemas ?? [],
    put: putSchemas,
    what: 'schemas',
    validation,
    onLoaded: async (config) => {
      const shared = await sharedConfig();
      sharedSchemas = shared?.schemas ?? [];
      pipelines = [...(shared?.pipelines ?? []), ...config.pipelines];
      destinations = [...(shared?.destinations ?? []), ...config.destinations];
    },
  });
  void editor.load();

  let catalog = $state<SchemaCatalog | null>(null);
  builtinSchemas()
    .then((c) => (catalog = c))
    .catch(reported);
  let ecs = $derived(catalog?.schemas.filter((s) => s.origin === 'ecs') ?? []);
  let categories = $derived(catalog ? byCategory(catalog.categories, catalog.schemas) : []);
  let classCount = $derived(categories.reduce((n, c) => n + c.classes.length, 0));
  let users = $derived(usersOf(pipelines, destinations));

  /** The rows opened to their fields, by schema id. */
  let opened = $state(new Set<string>());
  /** The categories opened, by uid, and the page each shows. */
  let openCategories = $state(new Set<number>());
  let pages = $state(new Map<number, number>());

  // Land on the schema the hash names, opened, inside its category.
  $effect(() => {
    if (!open || !catalog) return;
    opened = new Set([open]);
    const c = categories.find((c) => c.classes.some((s) => s.id === open));
    if (c) {
      openCategories = new Set(openCategories).add(c.uid);
      pages = new Map(pages).set(
        c.uid,
        Math.floor(c.classes.findIndex((s) => s.id === open) / PAGE),
      );
    }
  });

  function toggle<T>(set: Set<T>, key: T): Set<T> {
    const next = new Set(set);
    if (!next.delete(key)) next.add(key);
    return next;
  }

  // -- search -------------------------------------------------------------

  let q = $state('');
  /** What the search found, by schema id, with the fields that hold the words. Null while there is no search. */
  let matches = $state<Map<string, string[]> | null>(null);
  let searches = 0;
  $effect(() => {
    const needle = q.trim();
    const mine = ++searches;
    if (!needle) {
      matches = null;
      return;
    }
    const declared = [...sharedSchemas, ...$state.snapshot(schemas)];
    const later = setTimeout(() => {
      searchSchemas(needle, declared)
        .then((res) => {
          if (mine === searches) matches = new Map(res.matches.map((m) => [m.id, m.fields]));
        })
        .catch(reported);
    }, 200);
    return () => clearTimeout(later);
  });
  function shown(id: string): boolean {
    return matches === null || matches.has(id);
  }
  let total = $derived(schemas.length + sharedSchemas.length + ecs.length + classCount);

  // -- adding -------------------------------------------------------------

  let adding = $state(false);
  let taken = $derived([...schemas, ...sharedSchemas].map((s) => s.id));

  async function add(draft: SchemaDraft) {
    let schema: Record<string, unknown>;
    if (draft.type === 'jsonSchema') {
      try {
        schema = JSON.parse(draft.text);
      } catch (e) {
        throw new Error(`the JSON Schema is not JSON: ${e instanceof Error ? e.message : e}`);
      }
      const read = await schemaObject({ schema });
      if (!read.ok) throw new Error(read.errors.map((e) => e.message).join(' '));
    } else if (draft.type === 'builtin') {
      const res = await exportSchema(draft.from);
      if (!res.ok) return;
      schema = res.schema;
    } else {
      schema = {
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        type: 'object',
        properties: {},
      };
    }
    schemas.push({ id: draft.id, schema });
    opened = new Set(opened).add(draft.id);
    adding = false;
  }

  async function exported(id: string) {
    const own = [...schemas, ...sharedSchemas].find((s) => s.id === id);
    if (own) return downloadJson(fileName(id), own.schema);
    const res = await exportSchema(id);
    if (res.ok) downloadJson(fileName(id), res.schema);
  }

  function plural(n: number, one: string, many: string): string {
    return `${n} ${n === 1 ? one : many}`;
  }

  /** A declared schema's description: its own, or its title. */
  function about(s: SchemaDto): string {
    const { description, title } = s.schema as { description?: unknown; title?: unknown };
    return typeof description === 'string' ? description : typeof title === 'string' ? title : '';
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
            <th class="kind">Kind</th>
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
                <td class="kind">JSON Schema</td>
                <td>
                  <button
                    type="button"
                    class="opener"
                    onclick={() => (opened = toggle(opened, s.id))}
                    >{opened.has(s.id) ? '▾' : '▸'} {about(s) || 'fields'}</button
                  >
                  {@render found(s.id)}
                </td>
                <td class="used">
                  {@render usedBy(users.get(s.id))}
                  {#if !readOnly}
                    <button
                      type="button"
                      class="icon"
                      aria-label="Remove {s.id || 'unnamed'}"
                      onclick={() => schemas.splice(n, 1)}><TrashIcon /></button
                    >
                  {/if}
                </td>
              </tr>
              {#if opened.has(s.id) && shown(s.id)}
                {@render detail(s.id, { schema: s.schema })}
              {/if}
            </ValidationItem>
          {/each}
          {#if schemas.length === 0 && matches === null}
            <tr><td colspan="4" class="dim">No schemas declared here yet.</td></tr>
          {/if}

          {#if sharedSchemas.length > 0}
            <tr class="band"
              ><th colspan="4">from shared/ &mdash; every group may name these</th></tr
            >
            {#each sharedSchemas as s (s.id)}
              {#if shown(s.id)}
                {@render row(s.id, 'JSON Schema · shared', about(s), true)}
                {#if opened.has(s.id)}{@render detail(s.id, { schema: s.schema }, true)}{/if}
              {/if}
            {/each}
          {/if}

          {#if catalog}
            {#if ecs.some((s) => shown(s.id))}
              <tr class="band"><th colspan="4">built-in · ECS</th></tr>
              {#each ecs as s (s.id)}
                {#if shown(s.id)}
                  {@render row(s.id, `ECS ${catalog.ecsVersion}`, s.description)}
                  {#if opened.has(s.id)}{@render detail(s.id, { id: s.id })}{/if}
                {/if}
              {/each}
            {/if}

            <tr class="band">
              <th colspan="3">
                built-in · OCSF {catalog.ocsfVersion} · {plural(classCount, 'class', 'classes')} in
                {plural(categories.length, 'category', 'categories')}
              </th>
              <th class="all">
                {#if matches === null}
                  <button
                    type="button"
                    class="link"
                    onclick={() => (openCategories = new Set(categories.map((c) => c.uid)))}
                    >Expand all</button
                  >
                  ·
                  <button type="button" class="link" onclick={() => (openCategories = new Set())}
                    >Collapse all</button
                  >
                {/if}
              </th>
            </tr>
            {#each categories as c (c.uid)}
              {@const classes = c.classes.filter((s) => shown(s.id))}
              {@const isOpen = matches !== null || openCategories.has(c.uid)}
              {@const page = paged(classes, pages.get(c.uid) ?? 0)}
              {@const using = [
                ...new Map(
                  c.classes
                    .flatMap((s) => users.get(s.id) ?? [])
                    .map((u) => [`${u.kind} ${u.id}`, u]),
                ).values(),
              ]}
              {#if classes.length > 0}
                <tr class="category">
                  <td class="name">
                    <button
                      type="button"
                      class="opener"
                      aria-expanded={isOpen}
                      onclick={() => (openCategories = toggle(openCategories, c.uid))}
                      >{isOpen ? '▾' : '▸'} {c.caption}</button
                    >
                  </td>
                  <td class="kind"
                    >category {c.uid} · {plural(classes.length, 'class', 'classes')}</td
                  >
                  <td></td>
                  <td class="used">{@render usedBy(using)}</td>
                </tr>
                {#if isOpen}
                  {#each page.shown as s (s.id)}
                    {@render row(s.id, `class ${s.classUid}`, s.description, false, true)}
                    {#if opened.has(s.id)}{@render detail(s.id, { id: s.id })}{/if}
                  {/each}
                  {#if page.pages > 1}
                    <tr class="pager">
                      <td colspan="4">
                        <span class="dim"
                          >{page.first + 1}–{page.first + page.shown.length} of {classes.length} in {c.caption}</span
                        >
                        {#each Array.from({ length: page.pages }, (_, i) => i) as i (i)}
                          <button
                            type="button"
                            aria-current={i === (pages.get(c.uid) ?? 0) ? 'page' : undefined}
                            onclick={() => (pages = new Map(pages).set(c.uid, i))}>{i + 1}</button
                          >
                        {/each}
                      </td>
                    </tr>
                  {/if}
                {/if}
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
    builtins={catalog?.schemas ?? []}
    onadd={add}
    oncancel={() => (adding = false)}
  />
{/if}

{#snippet row(id: string, kind: string, description: string, fromShared = false, indent = false)}
  <tr class="fixed" class:fromshared={fromShared}>
    <td class="name" class:indent>
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

{#snippet detail(
  id: string,
  of: { id: string } | { schema: Record<string, unknown> },
  fromShared = false,
)}
  <tr class="detail" class:fromshared={fromShared}>
    <td colspan="4">
      <div class="actions">
        <span class="dim"
          >{'id' in of
            ? 'Built-in, read-only.'
            : fromShared
              ? 'Declared in shared/.'
              : 'Edit it with Edit JSON.'}
          <span class="required">*</span> required.</span
        >
        <span class="grow"></span>
        <button type="button" onclick={() => void exported(id)}>Export JSON Schema</button>
      </div>
      <SchemaFields {of} />
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

  .band .all {
    text-align: right;
    text-transform: none;
    letter-spacing: 0;
  }

  .name {
    width: 16rem;
  }

  .indent {
    padding-left: 1.6rem;
  }

  .kind {
    width: 11rem;
    font-size: var(--text-note);
  }

  .used {
    width: 14rem;
    font-size: var(--text-note);
  }

  .fixed td,
  .category td {
    padding: 0.35rem 0.5rem;
    font-size: var(--text-note);
  }

  .about {
    overflow-wrap: anywhere;
  }

  .category {
    background: var(--panel);
  }

  .category .opener {
    font-weight: 600;
  }

  /* `shared/`'s own: shown, because a group's pipelines may name them, and not editable here. */
  .fromshared td:first-child {
    border-left: 3px solid oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  .opener,
  .link {
    padding: 0;
    border: none;
    background: none;
    color: inherit;
    text-align: left;
    font: inherit;
  }

  .link {
    color: var(--accent);
    text-decoration: underline;
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

  .pager td {
    padding-left: 1.6rem;
    font-size: var(--text-note);
  }

  .pager button {
    min-width: 1.8rem;
    margin-left: 0.3rem;
    padding: 0.1rem 0.4rem;
  }

  .pager button[aria-current='page'] {
    background: var(--ink);
    color: var(--bg);
  }

  .required {
    color: var(--error);
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

  .icon {
    display: inline-grid;
    float: right;
    padding: 0;
    border-color: transparent;
    background: none;
    color: var(--dim);
  }

  .icon:hover {
    color: var(--error);
  }

  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
