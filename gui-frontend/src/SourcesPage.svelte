<!--
  Sources page: `sources.json`, read through `GET /api/areas/:area` and written
  with `PUT /api/areas/:area/sources`, plus the raw-JSON page.
-->
<script lang="ts">
  import Connections from './Connections.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import SaveBar from './SaveBar.svelte';
  import { provideValidation } from './validation.svelte';
  import Field from './Field.svelte';
  import SourceFields from './SourceFields.svelte';
  import JsonEditPage from './JsonEditPage.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import HeldEdit from './HeldEdit.svelte';
  import { SectionEditor } from './sectionEditor.svelte';
  import {
    sharedConfig,
    currentScope,
    SHARED_SCOPE,
    setScope,
    putSources,
    type SourceDto,
    type SecretDto,
    type NodeReadingDto,
    fieldCatalogs,
    getMapMeasurements,
  } from './api';
  import { holdings, sourceReadings } from './mapTerms';
  import NewSource from './NewSource.svelte';
  import ObjectList from './ObjectList.svelte';
  import { SOURCE_KIND } from './sourceKinds';
  import { isSql, isStream, reads, typeLabel, type StreamSource } from './sources';

  // `focus` is a source's own way in from the map (design doc "The map"): which card to land
  // on and highlight, set from the route rather than chosen on this page.
  let {
    showJson = false,
    focus,
    only,
  }: { showJson?: boolean; focus?: string; only?: string[] } = $props();

  let sources: SourceDto[] = $state([]);
  /** What `shared/` declares, which this group runs too. Never saved from here: the page writes its own scope,
   * and these belong to the bundle every group draws on (design C -- read here, edited in `shared/`). */
  let sharedSources: SourceDto[] = $state([]);
  /** A source being added (`NewSource`). It joins the list on Add, and the file on the page's Save. */
  let adding = $state(false);
  /** The row to show and mark: the one the map landed on, then the one just added. */
  // Follows the route, which can land on another row without remounting the page.
  let highlight = $derived(focus);
  const validation = provideValidation('sources');
  const editor = new SectionEditor<SourceDto[]>('sources', {
    get: () => sources,
    set: (value) => (sources = value),
    pick: (config) => config.sources,
    put: putSources,
    validation,
    onLoaded: async () => {
      await loadShared();
    },
  });
  /** Each saved stream source's start, by id. A start that differs from it moves the source when deployed. */
  let savedStarts = $derived(
    new Map(
      (editor.saved ? (JSON.parse(editor.saved) as SourceDto[]) : [])
        .filter(isStream)
        .map((s) => [s.id, JSON.stringify(s.start ?? null)]),
    ),
  );
  const startMoves = (s: StreamSource): boolean =>
    savedStarts.has(s.id) && savedStarts.get(s.id) !== JSON.stringify(s.start ?? null);

  /** `shared/`'s own, read once the group's are in. A group runs the two merged, so a page that showed only
   * its own would be hiding half of what runs. `shared/` draws on nothing, so there is nothing to add there. */
  async function loadShared() {
    const shared = await sharedConfig();
    sharedSources = shared ? shared.sources : [];
  }

  void editor.load();

  /** What this leader's build can select, for the security section, and the secret catalog. */
  let mechanisms: string[] = $state([]);
  let secrets: SecretDto[] = $state([]);
  void fieldCatalogs().then((c) => ({ secrets, mechanisms } = c));

  /** Each source's current readings, by id, for the worker holding a listing source: this scope's group's, or under
   * `shared/`, which runs in every group, each group's. Best-effort: a page without readings still edits. */
  let readings = $state(new Map<string, NodeReadingDto[]>());
  /** How often they are read again, as the map does: once per bucket the leader reports in. */
  let readEveryMs = $state(10_000);
  function readHolders() {
    getMapMeasurements()
      .then((m) => {
        const by = new Map<string, NodeReadingDto[]>();
        for (const n of sourceReadings(m.nodes, currentScope()))
          by.set(n.id, [...(by.get(n.id) ?? []), n]);
        readings = by;
        readEveryMs = m.bucketMillis;
      })
      .catch(() => {});
  }
  readHolders();
  $effect(() => {
    const id = setInterval(readHolders, readEveryMs);
    return () => clearInterval(id);
  });

  /** Every id a new one may not take: this scope's and `shared`'s. */
  const unavailable = () => [...sources, ...sharedSources].map((s) => s.id);

  function add(made: SourceDto) {
    sources.push(made);
    highlight = made.id;
    adding = false;
  }

  function removeSource(i: number) {
    sources.splice(i, 1);
  }
</script>

{#if editor.loading}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else if editor.loadFailed}
  <SectionShell />
{:else if showJson}
  <JsonEditPage
    title="Edit JSON — Sources"
    initialText={JSON.stringify(sources, null, 2)}
    backHash="#/build/sources"
    onSave={(text) => editor.saveFromJson(text)}
  />
{:else}
  <SectionShell>
    <PageHeader title="Sources" />

    <div class="form">
      <UnreadableFiles problems={editor.bundleProblems} onchanged={() => editor.load()} />

      {#if editor.moved}
        <HeldEdit
          what="sources"
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

      <ObjectList
        items={sources}
        shared={sharedSources}
        noun="source"
        {validation}
        idOf={(s) => s.id}
        typeOf={(s) => s.type}
        typeLabel={(t) => typeLabel(t as SourceDto['type'])}
        columns={[
          { key: 'reads', label: 'reads', value: reads },
          {
            key: 'from',
            label: 'from',
            value: (s) =>
              s.type === 'kafka'
                ? s.brokers
                : s.type === 'eventHubs'
                  ? s.namespace
                  : s.type === 'sftp'
                    ? s.host
                    : s.type === 'oktaLogs'
                      ? s.org
                      : isSql(s)
                        ? s.host
                        : '',
          },
          {
            key: 'worker',
            label: 'worker',
            // Readings carry no source type, so a source that took a single-worker source's id is shown none of its
            // holder.
            value: (s) =>
              SOURCE_KIND[s.type] === 'pullOne'
                ? holdings(readings.get(s.id) ?? [], currentScope() === SHARED_SCOPE)
                : '',
          },
        ]}
        anchor={(s) => `source-${encodeURIComponent(s.id)}`}
        {highlight}
        {only}
        empty="No sources yet."
      >
        {#snippet toolbar()}
          <button type="button" class="primary" onclick={() => (adding = true)}>+ Add source</button
          >
        {/snippet}
        {#snippet extra()}
          <button type="button" onclick={() => (location.hash = '#/build/sources/json')}
            >Edit JSON&hellip;</button
          >
        {/snippet}
        {#snippet body(source)}
          <Connections kind="source" id={source.id} />
          <div class="pair">
            <Field required field="id" label="id">
              <input type="text" bind:value={source.id} />
            </Field>
            <Field label="type"><span class="fixed">{typeLabel(source.type)}</span></Field>
          </div>
          <SourceFields
            {source}
            {secrets}
            {mechanisms}
            {startMoves}
            readAt={editor.readAtCommit}
            onsaved={(commit) => (editor.readAtCommit = commit)}
          />
        {/snippet}
        {#snippet rowActions(_, i)}
          <button type="button" class="pick remove" onclick={() => removeSource(i)}>Remove</button>
        {/snippet}
        {#snippet sharedActions()}
          <!-- Reloads: the rows and the saved baseline were read under the old scope, and every
               write from here carries the new one. -->
          <button type="button" class="pick" onclick={() => setScope(SHARED_SCOPE)}
            >Edit in shared/</button
          >
        {/snippet}
      </ObjectList>

      <NewSource
        open={adding}
        unavailable={unavailable()}
        {secrets}
        {mechanisms}
        readAt={editor.readAtCommit}
        onsaved={(commit) => (editor.readAtCommit = commit)}
        onadd={add}
        oncancel={() => (adding = false)}
      />
      <SaveBar
        dirty={editor.isDirty}
        saving={editor.saving}
        onsave={() => void editor.save()}
        ondiscard={() => editor.discard()}
      />
    </div>
  </SectionShell>
{/if}

<style>
  .fixed {
    display: block;
    padding: 0.3rem 0;
    font-family: var(--mono);
    font-size: var(--text-label);
  }

  /* A choice and the value it asks for, side by side: the value has no
     meaning without the choice above it. */
  .pair {
    display: flex;
    gap: 0.8rem;
    align-items: flex-start;
  }

  .pair > :global(*) {
    flex: 1;
  }

  .form {
    max-width: var(--width-prose);
  }

  input {
    font-family: var(--mono);
  }
</style>
