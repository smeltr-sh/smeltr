<!--
  Lookup tables page: `lookup-tables.json`, read through `GET /api/config`
  and written with `PUT /api/lookup-tables`, plus the raw-JSON page. A
  table is a CSV or MMDB upload, or a small table authored by hand, shared
  by id across every `lookup` step. Every table sits in the list; the one
  chosen opens beside it.

  A CSV's rows are read from its file when it is opened. Edited, they go
  back in the same Save as the file's new text (`contents`), which the
  leader writes as a new file and points the table at.
-->
<script lang="ts">
  import { unreported } from './errors.svelte';
  import Connections from './Connections.svelte';
  import EditInShared from './EditInShared.svelte';
  import Locked from './Locked.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import { count } from './time';
  import { SvelteMap } from 'svelte/reactivity';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Help from './Help.svelte';
  import SaveBar from './SaveBar.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import HeldEdit from './HeldEdit.svelte';
  import LookupGrid from './LookupGrid.svelte';
  import SortButton from './SortButton.svelte';
  import { TableSort, type SortValue } from './sort.svelte';
  import { drop } from './edits';
  import { SectionEditor } from './sectionEditor.svelte';
  import { provideValidation } from './validation.svelte';
  import InlineError from './InlineError.svelte';
  import JsonEditPage from './JsonEditPage.svelte';
  import { resourcesTabs } from './nav';
  import { columnStats, parseCsv, serializeCsv, stepsReading } from './lookups';
  import {
    sharedConfig,
    getLookupFile,
    getLookupMmdb,
    putLookupTables,
    uploadLookupFile,
    errorText,
    errorMessage,
    type LookupTableDto,
    type LookupMmdbResult,
    type PipelineDto,
  } from './api';
  import { editable } from './access';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import TypeCatalog, { type CatalogType } from './TypeCatalog.svelte';
  import { STEP_GLYPHS } from './stepGlyphs';
  import { idProblem, suggestId } from './ids';

  let { showJson = false }: { showJson?: boolean } = $props();

  const readOnly = !editable();

  let tables: LookupTableDto[] = $state([]);
  /** `shared/`'s own, which this group looks up too. A table's file is named relative to the bundle that
   * declares it, so a shared one's file lives in `shared/` -- which is where it is changed. */
  let sharedTables: LookupTableDto[] = $state([]);
  /** This bundle's pipelines, for which steps read a table. */
  let pipelines: PipelineDto[] = $state([]);
  /** A CSV table's rows, read from its file: keyed by the table itself, since its id is editable
   * and its index shifts when a table above it is removed. `edited` is what sends `contents`. */
  type CsvGrid = {
    read: string;
    headers: string[];
    rows: string[][];
    edited: boolean;
    error: string;
  };
  const grids = new SvelteMap<LookupTableDto, CsvGrid>();

  /** What Save sends: the tables, each edited CSV carrying its rows as text. Built when it is sent
   * rather than derived, since a derivation reads every cell and typing in a large file would rebuild
   * the whole file on each keystroke. */
  function payload(): LookupTableDto[] {
    return tables.map((t) => {
      const g = t.type === 'csv' ? grids.get(t) : undefined;
      if (t.type !== 'csv' || !g?.edited) return t;
      return { ...t, contents: serializeCsv([g.headers, ...g.rows], t.delimiter) };
    });
  }
  /** Counts cell edits, so the held edit hears of one without reading every cell. */
  let cellEdits = $state(0);
  const rowsEdited = (t: LookupTableDto) => t.type === 'csv' && !!grids.get(t)?.edited;
  const validation = provideValidation('lookups');
  const editor = new SectionEditor<LookupTableDto[]>('lookups', {
    get: () => tables,
    set: (value) => (tables = value),
    pick: (config) => config.lookupTables,
    put: putLookupTables,
    what: 'lookup tables',
    validation,
    // Holding the edit builds every edited file, so it is held a moment after the last edit.
    held: { value: () => JSON.stringify(payload()), afterMs: 300, reads: () => void cellEdits },
    onLoaded: async (config) => {
      pipelines = config.pipelines;
      sharedTables = (await sharedConfig())?.lookupTables ?? [];
    },
    onDiscard: () => grids.clear(),
  });
  let isDirty = $derived(editor.isDirty || tables.some(rowsEdited));
  let savedTables = $derived(editor.saved ? (JSON.parse(editor.saved) as LookupTableDto[]) : []);

  let sel = $state(0);
  let table = $derived(tables[sel] as LookupTableDto | undefined);
  let grid = $derived(table?.type === 'csv' ? grids.get(table) : undefined);
  let tab = $state('Rows');
  let tabs = $derived(
    table?.type === 'mmdb'
      ? ['Overview', 'Used by', 'File']
      : ['Rows', 'Columns', 'Used by', ...(table?.type === 'csv' ? ['File'] : [])],
  );
  let shownTab = $derived(tabs.includes(tab) ? tab : tabs[0]);

  const KIND_LABEL: Record<string, string> = { csv: 'CSV', inline: 'inline', mmdb: 'MMDB' };

  const fileNameOf = (path: string) => path.split(/[\\/]/).pop() || path;

  function summarizeTable(t: LookupTableDto): string {
    if (t.type === 'inline') return `${t.headers.length} col · ${count(t.rows.length, 'row')}`;
    return t.path ? fileNameOf(t.path) : 'no file uploaded';
  }

  /** A grid is keyed by its table, and a read replaces every table. */
  function load() {
    grids.clear();
    return editor.load();
  }
  void load();

  // Reads an opened CSV's rows, again when its file or delimiter changes, unless they are edited.
  // Rows held from an earlier visit come back as `contents` and are read from that.
  $effect(() => {
    const t = table;
    if (t?.type !== 'csv') return;
    const [path, delimiter, contents] = [t.path, t.delimiter, t.contents];
    const read = `${delimiter}${path}`;
    const g = grids.get(t);
    if (g && (g.edited || g.read === read)) return;
    const fill = (text: string, error = '') => {
      // A late read never replaces rows edited, or read for another file, since it was asked for.
      const now = grids.get(t);
      if (now !== g && (now?.edited || now?.read !== read)) return;
      const [headers = [], ...rows] = parseCsv(text, delimiter);
      const fresh = $state({ read, headers, rows, edited: contents !== undefined, error });
      grids.set(t, fresh);
    };
    if (contents !== undefined || !path) fill(contents ?? '');
    else
      getLookupFile(path).then(
        (res) =>
          fill(res.text ?? '', res.ok ? '' : errorText(res.errors) || 'the file could not be read'),
        (e) => fill('', errorMessage(e)),
      );
  });

  // An MMDB table's facts, read when it is opened, and the record for one address.
  let mmdb: { path: string; facts?: LookupMmdbResult; error?: string } | null = $state(null);
  let ip = $state('');
  let probe: { path: string; ip: string; record?: unknown; error?: string } | null = $state(null);
  const MMDB_FACTS = ['Database type', 'Built', 'IP version', 'Networks', 'Languages'];
  // Keyed by column position, since the heads are the snippet's data.
  const columnsSort = new TableSort<string>();
  const readersSort = new TableSort<string>();

  /** `body` in `sort`'s order, every column sorting by the value in its cell. */
  function sorted(sort: TableSort<string>, head: string[], body: unknown[][]): unknown[][] {
    const by = Object.fromEntries(
      head.map((_, j) => [String(j), (r: unknown[]) => r[j] as SortValue]),
    );
    return sort.apply(body, by);
  }
  let mmdbFacts = $derived.by(() => {
    const f = mmdb?.facts;
    if (!f) return [];
    const built = new Date(f.buildEpoch * 1000).toISOString().slice(0, 10);
    return [f.databaseType, built, `IPv${f.ipVersion}`, f.networks, f.languages.join(' ')];
  });

  $effect(() => {
    const path = table?.type === 'mmdb' ? table.path : '';
    if (!path || mmdb?.path === path) return;
    mmdb = { path };
    const done = (r: { facts?: LookupMmdbResult; error?: string }) => {
      if (mmdb?.path === path) mmdb = { path, ...r };
    };
    getLookupMmdb(path).then(
      (res) =>
        done(
          res.ok
            ? { facts: res }
            : { error: errorText(res.errors) || 'the file could not be read' },
        ),
      (e) => done({ error: errorMessage(e) }),
    );
  });

  async function lookUp(e: SubmitEvent, path: string) {
    e.preventDefault();
    const at = ip.trim();
    try {
      const res = await getLookupMmdb(path, at);
      probe = {
        path,
        ip: at,
        record: res.record ?? null,
        error: res.ok ? '' : errorText(res.errors),
      };
    } catch (err) {
      probe = { path, ip: at, error: errorMessage(err) };
    }
  }

  type Kind = LookupTableDto['type'];

  // No kind has a sign of its own yet, so each carries the Lookup step's.
  const KINDS: CatalogType<Kind>[] = (
    [
      ['inline', 'Built by hand'],
      ['csv', 'CSV file'],
      ['mmdb', 'MaxMind DB (MMDB) file'],
    ] as const
  ).map(([type, label]) => ({ type, label, category: 'Lookup table', glyph: STEP_GLYPHS.lookup }));

  /** The add dialog: `draft` is null while the kind is being chosen. */
  let adding: { draft: LookupTableDto | null } | null = $state(null);

  const takenIds = () => [...tables, ...sharedTables].map((t) => t.id);

  /** A new table of `kind`, named after it. A file is uploaded once the table is on the page, since
   * an upload commits at once and the dialog's Cancel must leave nothing behind. */
  function blank(kind: Kind): LookupTableDto {
    return blankOf(kind, suggestId('table', takenIds()));
  }
  function blankOf(kind: Kind, id: string): LookupTableDto {
    return kind === 'csv'
      ? { id, type: 'csv', path: '', delimiter: ',', headers: [] }
      : kind === 'inline'
        ? { id, type: 'inline', headers: ['key', 'value'], rows: [] }
        : { id, type: 'mmdb', path: '' };
  }

  function add() {
    const draft = adding?.draft;
    if (!draft || idProblem(draft.id, takenIds())) return;
    sel = tables.length;
    tables.push(draft);
    adding = null;
  }
  function removeTable(i: number) {
    grids.delete(tables[i]);
    tables.splice(i, 1);
    sel = Math.min(sel, tables.length - 1);
  }
  function setType(i: number, type: 'csv' | 'inline' | 'mmdb') {
    grids.delete(tables[i]);
    tables[i] = blankOf(type, tables[i].id);
  }

  // -- CSV/MMDB upload ------------------------------------------------------
  //
  // `headers` from a CSV upload is persisted on the table so a step's
  // column pickers work after a reload.
  let uploading = $state(false);
  let uploadError = $state('');

  async function uploadFile(t: LookupTableDto, file: File) {
    if (t.type === 'inline') return;
    uploading = true;
    uploadError = '';
    try {
      const data = await uploadLookupFile(
        file,
        t.type === 'csv' ? t.delimiter : undefined,
        editor.readAtCommit,
      );
      if (!data.ok || !data.path) {
        uploadError = errorText(data.errors) || 'the upload was refused';
        return;
      }
      // The upload is this page's own commit, made against what it read, so the next save is too.
      editor.readAtCommit = data.commit ?? editor.readAtCommit;
      t.path = data.path;
      if (t.type === 'csv') {
        t.headers = data.headers ?? [];
        // The file replaces any edited rows, so they are read again from it.
        delete t.contents;
        grids.delete(t);
      }
    } catch (e) {
      uploadError = unreported(e);
    } finally {
      uploading = false;
    }
  }

  async function save(): Promise<boolean> {
    const sent = payload();
    const submitted = JSON.stringify(sent);
    const ok = await editor.save(sent);
    // A CSV written from its rows has a new file the leader named, so the tables are read again,
    // unless that would lose an edit made while the save was in flight.
    if (
      ok &&
      sent.some((t) => t.type === 'csv' && t.contents !== undefined) &&
      JSON.stringify(payload()) === submitted
    ) {
      drop(editor.file);
      await load();
    }
    return ok;
  }
</script>

{#snippet item(t: LookupTableDto, dirty: boolean)}
  <span class="item-id">{t.id || '(unnamed)'}</span>
  <span>{KIND_LABEL[t.type]} · {summarizeTable(t)}</span>
  {#if dirty}<span class="dot" aria-label="unsaved">●</span>{/if}
{/snippet}

<!-- An MMDB file's facts are one row, which no sort orders: it comes without one. -->
{#snippet facts(head: string[], body: unknown[][], sort?: TableSort<string>)}
  <table class="facts">
    <thead>
      <tr>
        {#each head as h, j (h)}
          {#if sort}<th aria-sort={sort.aria(String(j))}
              ><SortButton {sort} key={String(j)}>{h}</SortButton></th
            >{:else}<th>{h}</th>{/if}
        {/each}
      </tr>
    </thead>
    <tbody>
      {#each sort ? sorted(sort, head, body) : body as row (row)}
        <tr>
          {#each row as v, j (j)}<td>{v === '' ? '—' : v}</td>{/each}
        </tr>
      {/each}
    </tbody>
  </table>
{/snippet}

{#if editor.loading}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else if editor.loadFailed}
  <SectionShell />
{:else if showJson}
  <JsonEditPage
    title="Edit JSON — Lookup Tables"
    initialText={JSON.stringify(tables, null, 2)}
    backHash="#/build/lookups"
    onSave={(text) => editor.saveFromJson(text)}
  />
{:else}
  <SectionShell>
    <PageHeader title="Lookup Tables">
      {#snippet help()}
        A pipeline's Lookup step names a table by its id and reads values from the row that matches
        a record's key.
      {/snippet}
    </PageHeader>
    <SecondaryTabs tabs={resourcesTabs()} active="#/build/lookups" ariaLabel="Resources" />

    <UnreadableFiles problems={editor.bundleProblems} onchanged={load} />

    {#if editor.moved}
      <HeldEdit
        what="lookup tables"
        rows={editor.moved.rows}
        onrestore={() => editor.restoreHeld()}
        ondiscard={() => editor.dropHeld()}
      />
    {/if}

    {#if editor.conflict}
      <SaveConflictPanel
        conflict={editor.conflict}
        onDiscard={() => location.reload()}
        onRetry={() => save()}
      />
    {/if}

    <div class="layout lookups">
      <aside class="list">
        <div class="band">this group</div>
        {#each tables as t, i (i)}
          <button type="button" class="item" class:on={i === sel} onclick={() => (sel = i)}>
            {@render item(t, JSON.stringify(t) !== JSON.stringify(savedTables[i]) || rowsEdited(t))}
          </button>
        {/each}
        {#if tables.length === 0}<p class="dim item">No tables of this group's own yet.</p>{/if}
        {#if sharedTables.length > 0}
          <div class="band">from shared/</div>
          {#each sharedTables as t (t.id)}
            <div class="item shared">
              {@render item(t, false)}
              <EditInShared hash="#/build/lookups" />
            </div>
          {/each}
        {/if}
        {#if !readOnly}
          <div class="list-foot">
            <button type="button" class="primary" onclick={() => (adding = { draft: null })}
              >+ Add table</button
            >
            <button type="button" onclick={() => (location.hash = '#/build/lookups/json')}>
              Edit JSON&hellip;
            </button>
          </div>
        {/if}
      </aside>

      <section class="detail">
        {#if table}
          {@const t = table}
          <Connections kind="lookupTable" id={t.id} />
          <div class="bar">
            <input class="id" aria-label="table id" bind:value={t.id} disabled={readOnly} />
            <select
              disabled={readOnly}
              aria-label="kind"
              value={t.type}
              onchange={(e) => setType(sel, e.currentTarget.value as 'csv' | 'inline' | 'mmdb')}
            >
              <option value="inline">Built by hand</option>
              <option value="csv">CSV file</option>
              <option value="mmdb">MaxMind DB (MMDB) file</option>
            </select>
            <span class="grow"></span>
            {#if !readOnly}
              <button type="button" onclick={() => removeTable(sel)}>Remove</button>
            {/if}
          </div>
          <div class="tabs" role="tablist">
            {#each tabs as name (name)}
              <button
                type="button"
                role="tab"
                aria-selected={name === shownTab}
                onclick={() => (tab = name)}>{name}</button
              >
            {/each}
          </div>

          {@const cells =
            t.type === 'inline' ? t : grid && !grid.error ? grid : { headers: [], rows: [] }}
          {#if shownTab === 'Overview' && t.type === 'mmdb'}
            {#if !t.path}
              <p class="dim">No file uploaded yet. Upload one under File.</p>
            {:else if mmdb?.error}
              <InlineError message={mmdb.error} />
            {:else if !mmdb?.facts}
              <p class="dim">Reading the file&hellip;</p>
            {:else}
              {@render facts(MMDB_FACTS, [mmdbFacts])}
              <form class="bar" onsubmit={(e) => lookUp(e, t.path)}>
                <input aria-label="IP address" placeholder="Try an IP" bind:value={ip} />
                <button type="submit" disabled={!ip.trim()}>Look up</button>
              </form>
              {#if probe && probe.path === t.path}
                {#if probe.error}
                  <InlineError message={probe.error} />
                {:else if probe.record === null}
                  <p>No record for <code>{probe.ip}</code>.</p>
                {:else}
                  <pre>{JSON.stringify(probe.record, null, 2)}</pre>
                {/if}
              {/if}
            {/if}
          {:else if shownTab === 'Rows' || shownTab === 'Columns'}
            {#if t.type === 'csv' && !grid}
              <p class="dim">Reading the file&hellip;</p>
            {:else if grid?.error}
              <InlineError message={grid.error} />
            {:else if shownTab === 'Rows'}
              {#key t}
                <LookupGrid
                  bind:headers={cells.headers}
                  bind:rows={cells.rows}
                  onedit={() => {
                    if (grid) grid.edited = true;
                    cellEdits++;
                  }}
                />
              {/key}
            {:else}
              {@render facts(
                ['column', 'distinct', 'empty'],
                columnStats(cells.headers, cells.rows),
                columnsSort,
              )}
            {/if}
          {:else if shownTab === 'Used by'}
            {@const readers = stepsReading(pipelines, t.id)}
            {#if readers.length === 0}
              <p class="dim">No step in this group's pipelines reads this table.</p>
            {:else}
              {@render facts(
                ['pipeline › step', 'input', 'key', 'reads → writes'],
                readers,
                readersSort,
              )}
            {/if}
          {:else if t.type !== 'inline'}
            {#if !readOnly}
              <Locked refusedInDemo
                ><label class="bar">
                  {t.type === 'csv' ? 'CSV file' : 'MMDB file'}
                  <Help label={t.type === 'csv' ? 'CSV file' : 'MMDB file'}>
                    Choosing a file uploads it and commits it to the branch at once; Discard does
                    not undo it.
                  </Help>
                  <input
                    type="file"
                    accept={t.type === 'csv' ? '.csv,.tsv,text/csv' : '.mmdb'}
                    disabled={uploading}
                    onchange={(e) => {
                      const f = e.currentTarget.files?.[0];
                      if (f) void uploadFile(t, f);
                    }}
                  />
                </label></Locked
              >
            {/if}
            {#if uploading}<p class="dim">uploading&hellip;</p>{/if}
            <InlineError message={uploadError} />
            <!-- The upload's path is the server's storage layout, not an
                 operator's choice, so it is not shown or editable. -->
            <p class="dim">
              {#if t.path}
                Uploaded &mdash; <strong>{fileNameOf(t.path)}</strong>, already committed to the
                branch.
              {:else}
                No file uploaded yet.
              {/if}
            </p>
            {#if t.type === 'csv'}
              <label class="bar">
                delimiter
                <input
                  type="text"
                  maxlength="1"
                  size="2"
                  bind:value={t.delimiter}
                  disabled={readOnly}
                />
              </label>
            {/if}
          {/if}
        {/if}
      </section>
    </div>

    <Dialog
      open={adding !== null}
      title={adding?.draft ? `new ${KIND_LABEL[adding.draft.type]} table` : 'add table'}
      wide
      oncancel={() => (adding = null)}
      onback={adding?.draft ? () => adding && (adding.draft = null) : undefined}
      onsubmit={add}
    >
      {#if adding?.draft}
        {@const draft = adding.draft}
        <div class="form-body">
          <Field required label="id" error={idProblem(draft.id, takenIds())}>
            <input type="text" bind:value={draft.id} />
          </Field>
          {#if draft.type === 'csv'}
            <Field optional label="delimiter">
              <input type="text" maxlength="1" size="2" bind:value={draft.delimiter} />
            </Field>
          {/if}
        </div>
      {:else}
        <TypeCatalog types={KINDS} onpick={(kind) => adding && (adding.draft = blank(kind))} />
      {/if}
      {#snippet actions()}
        <button type="button" onclick={() => (adding = null)}>Cancel</button>
        {#if adding?.draft}
          <button
            type="submit"
            class="primary"
            disabled={idProblem(adding.draft.id, takenIds()) !== null}>Add</button
          >
        {/if}
      {/snippet}
    </Dialog>
    <SaveBar
      dirty={isDirty}
      saving={editor.saving}
      onsave={() => void save()}
      ondiscard={() => editor.discard()}
    />
  </SectionShell>
{/if}

<style>
  .layout {
    display: grid;
    grid-template-columns: 16rem minmax(0, 1fr);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    min-height: 32rem;
  }

  @media (max-width: 720px) {
    .layout {
      grid-template-columns: minmax(0, 1fr);
    }
  }

  .list {
    display: flex;
    flex-direction: column;
    background: var(--panel);
    border-right: 1px solid var(--border);
    min-width: 0;
  }

  .item {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    padding: 0.45rem 0.7rem;
    border: 0;
    border-left: 3px solid transparent;
    border-radius: 0;
    text-align: left;
  }

  /* A menu row, not a button: none of the shared button's frame. */
  button.item {
    background: none;
    font-size: var(--text-note);
  }

  button.item:hover {
    background: color-mix(in oklab, var(--ink) 6%, var(--panel));
  }

  .item.on {
    background: color-mix(in oklab, var(--ink) 10%, var(--panel));
    border-left-color: var(--ink);
  }

  .item-id {
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .item > :not(.item-id, .dot) {
    grid-column: 1;
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .dot {
    grid-area: 1 / 2;
    color: var(--changed);
  }

  /* `shared/`'s own: shown, because the group runs them, and not editable here -- the scope chip at
     the rail head names what this page writes, and it does not name `shared/`. */
  .item.shared {
    border-left-color: oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  /* An uppercase label: a band in the list, a fact's name, a column head. */
  .band,
  .facts th {
    padding: 0.7rem 0.7rem 0.3rem;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    text-align: left;
  }

  .list-foot {
    margin-top: auto;
    padding: 0.6rem;
    border-top: 1px solid var(--border);
  }

  .form-body {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }

  .detail {
    min-width: 0;
    padding: 1rem 1.1rem;
  }

  /* Shared with `LookupGrid`, which sits inside the detail. */
  :global(:where(.lookups) .bar),
  .list-foot {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    margin: 0.5rem 0;
  }

  :global(:where(.lookups) table) {
    border-spacing: 0;
  }

  :global(:where(.lookups) .grow) {
    flex: 1;
  }

  pre {
    background: var(--field);
    color: var(--ink);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.3rem 0.55rem;
    font: inherit;
    font-size: var(--text-note);
  }

  input.id {
    font-size: var(--text-section);
    font-weight: 600;
  }

  /* A secondary tab row (STYLE_GUIDE.md), held in the page rather than the hash. */
  .tabs {
    display: flex;
    gap: 1rem;
    margin-bottom: 0.9rem;
    border-bottom: 1px solid var(--border);
  }

  .tabs button {
    background: none;
    padding: 0.35rem 0;
    border-width: 0 0 3px;
    border-radius: 0;
    border-color: transparent;
    color: var(--dim);
    font-size: var(--text-body);
  }

  .tabs button[aria-selected='true'] {
    color: var(--ink);
    font-weight: 600;
    border-color: var(--accent);
  }

  .facts :is(th, td) {
    padding: 0.3rem 1.2rem 0.3rem 0;
    border-bottom: 1px solid var(--border);
  }

  .facts td,
  .item-id,
  pre,
  :global(:where(.lookups) input) {
    font-family: var(--mono);
  }

  pre {
    overflow-x: auto;
  }

  :global(:where(.lookups) .dim) {
    color: var(--dim);
  }
</style>
