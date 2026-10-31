<!--
  Destinations page: `destinations.json`, read through `GET /api/areas/:area`
  and written with `PUT /api/areas/:area/destinations`, plus the raw-JSON page.
-->
<script lang="ts">
  import Connections from './Connections.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import SaveBar from './SaveBar.svelte';
  import { provideValidation } from './validation.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import HeldEdit from './HeldEdit.svelte';
  import { SectionEditor } from './sectionEditor.svelte';
  import { sharedConfig, fieldCatalogs, NO_KAFKA_FACTS, type KafkaFacts } from './api';
  import ObjectList from './ObjectList.svelte';
  import Field from './Field.svelte';
  import ExceptionsFold from './ExceptionsFold.svelte';
  import { BUILT_IN, lay, overrideCount, problems } from './exceptions';
  import DestinationFields from './DestinationFields.svelte';
  import SchemaChips from './SchemaChips.svelte';
  import JsonEditPage from './JsonEditPage.svelte';
  import CaptureFlow from './CaptureFlow.svelte';
  import {
    currentScope,
    setScope,
    SHARED_SCOPE,
    putDestinations,
    type DestinationDto,
    type ExceptionsDto,
    type FileConfigDto,
    type PipelineDto,
    type RouteDto,
    type SchemaDto,
    type SecretDto,
  } from './api';
  import { mayCapture } from './access';
  import { bundleRoutes } from './routeFlow';
  import { at, typeLabel, writes } from './destinations';
  import NewDestination from './NewDestination.svelte';

  // `focus` is a destination's own way in from the map (design doc "The map"): which card to
  // land on and highlight, set from the route rather than chosen on this page.
  let {
    showJson = false,
    focus,
    only,
  }: { showJson?: boolean; focus?: string; only?: string[] } = $props();

  let destinations: DestinationDto[] = $state([]);
  /** What `shared/` declares, which this group delivers to as well. Never saved from here: the page writes its
   * own scope, and these belong to the bundle every group draws on. */
  let sharedDestinations: DestinationDto[] = $state([]);
  /** The group's exception answers and routes, as read: what a destination's own answer to Refused lays over, and
   * what the load checks beside it. */
  let group = $state<ExceptionsDto | undefined>(undefined);
  let routes: RouteDto[] = $state([]);
  let pipelines: PipelineDto[] = $state([]);
  let shared = $state<FileConfigDto | null>(null);
  /** What an accepts list may name beside the built-ins. */
  let ownSchemas: SchemaDto[] = $state([]);
  let sharedSchemas: SchemaDto[] = $state([]);
  let schemas = $derived([...sharedSchemas, ...ownSchemas]);
  let all = $derived([...destinations, ...sharedDestinations]);
  let refusals = $derived(
    problems(
      group,
      bundleRoutes({ routes, pipelines }, shared).slice(shared?.routes.length ?? 0),
      all,
    ),
  );
  /** The destination being captured at, by id, and the scope that declares it. Only a group runs a
   * destination, so `shared/`'s own page offers none. */
  let capturing: { id: string; scope: string } | null = $state(null);
  const captureGroup = currentScope() === SHARED_SCOPE ? null : currentScope();
  /** A destination being added (`NewDestination`). It joins the list on Add, and the file on the page's Save. */
  let adding = $state(false);
  /** The row to show and mark: the one the map landed on, then the one just added. */
  // Follows the route, which can land on another row without remounting the page.
  let highlight = $derived(focus);
  const validation = provideValidation('destinations');
  const editor = new SectionEditor<DestinationDto[]>('destinations', {
    get: () => destinations,
    set: (value) => (destinations = value),
    pick: (config) => config.destinations,
    put: putDestinations,
    validation,
    onLoaded: async (config) => {
      group = config.exceptions;
      routes = config.routes;
      pipelines = config.pipelines;
      ownSchemas = config.schemas ?? [];
      await loadShared();
    },
  });

  /** `shared/`'s own. A group runs the two merged, so a page showing only its own hides half of what runs. */
  async function loadShared() {
    shared = await sharedConfig();
    sharedDestinations = shared ? shared.destinations : [];
    sharedSchemas = shared?.schemas ?? [];
  }

  void editor.load();

  /** What this leader's build can select, for the security section, and the secret catalog. */
  let kafka: KafkaFacts = $state(NO_KAFKA_FACTS);
  let secrets: SecretDto[] = $state([]);
  void fieldCatalogs().then((c) => ({ secrets, kafka } = c));

  /** Ids the suggestion must not walk into. This scope's alone: the leader refuses a collision with the other
   * subtree, which this page cannot see. */
  function taken(): string[] {
    return destinations.map((d) => d.id);
  }

  /** Every id a new one may not take: this scope's and `shared`'s. */
  const unavailable = () => [...taken(), ...sharedDestinations.map((d) => d.id)];

  function add(draft: DestinationDto) {
    destinations.push(draft);
    highlight = draft.id;
    adding = false;
  }

  function removeDestination(i: number) {
    destinations.splice(i, 1);
  }
</script>

{#if editor.loading}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else if editor.loadFailed}
  <SectionShell />
{:else if showJson}
  <JsonEditPage
    title="Edit JSON — Destinations"
    initialText={JSON.stringify(destinations, null, 2)}
    backHash="#/build/destinations"
    onSave={(text) => editor.saveFromJson(text)}
  />
{:else}
  <SectionShell>
    <PageHeader title="Destinations" />

    <div class="form">
      <UnreadableFiles problems={editor.bundleProblems} onchanged={() => editor.load()} />

      {#if editor.moved}
        <HeldEdit
          what="destinations"
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
        items={destinations}
        shared={sharedDestinations}
        noun="destination"
        {validation}
        idOf={(d) => d.id}
        typeOf={(d) => d.type}
        typeLabel={(t) => typeLabel(t as DestinationDto['type'])}
        columns={[
          { key: 'writes', label: 'writes to', value: writes },
          { key: 'at', label: 'at', value: at },
          // Only where some row has one: a column empty all the way down is room the others need.
          ...(destinations.some((d) => overrideCount(d.exceptions) > 0)
            ? [
                {
                  key: 'exceptions',
                  label: 'exceptions',
                  value: (d: DestinationDto) => overrideCount(d.exceptions) || null,
                  width: '7rem',
                },
              ]
            : []),
        ]}
        anchor={(d) => `destination-${encodeURIComponent(d.id)}`}
        {highlight}
        {only}
        empty="No destinations yet."
      >
        {#snippet toolbar()}
          <button type="button" class="primary" onclick={() => (adding = true)}
            >+ Add destination</button
          >
        {/snippet}
        {#snippet extra()}
          <button type="button" onclick={() => (location.hash = '#/build/destinations/json')}
            >Edit JSON&hellip;</button
          >
        {/snippet}
        {#snippet body(destination)}
          <Connections kind="destination" id={destination.id} />
          <div class="pair">
            <Field required field="id" label="id">
              <input type="text" bind:value={destination.id} />
            </Field>
            <Field label="type"><span class="fixed">{typeLabel(destination.type)}</span></Field>
          </div>
          <DestinationFields {destination} {secrets} {kafka} />
          <Field field="accepts" label="accepts">
            {#snippet help()}
              A pipeline sending here should produce one of these schemas. Review says where one
              does not; nothing blocks it. Left empty, any pipeline may send here.
            {/snippet}
            <div class="accepts">
              <SchemaChips
                bind:ids={
                  () => destination.accepts ?? [],
                  (ids) => (destination.accepts = ids.length > 0 ? ids : undefined)
                }
                declared={schemas}
                adding="Add a schema it accepts"
              />
            </div>
          </Field>
          <ExceptionsFold
            bind:value={destination.exceptions}
            name={destination.id}
            rows={['Refused']}
            beneath={lay(BUILT_IN, group)}
            targets={all.map((x) => x.id).filter((x) => x !== destination.id)}
          >
            {#snippet help()}What happens to a record this destination refuses.{/snippet}
          </ExceptionsFold>
        {/snippet}
        {#snippet rowActions(destination, i)}
          {#if captureGroup}{@render captureButton(destination.id, captureGroup)}{/if}
          <button type="button" class="pick remove" onclick={() => removeDestination(i)}
            >Remove</button
          >
        {/snippet}
        {#snippet sharedActions(destination)}
          {@render captureButton(destination.id, SHARED_SCOPE)}
          <!-- Reloads: the rows and the saved baseline were read under the old scope, and every
               write from here carries the new one. -->
          <button type="button" class="pick" onclick={() => setScope(SHARED_SCOPE)}
            >Edit in shared/</button
          >
        {/snippet}
      </ObjectList>

      <NewDestination
        open={adding}
        unavailable={unavailable()}
        {secrets}
        {kafka}
        onadd={add}
        oncancel={() => (adding = false)}
      />

      {#if capturing !== null && captureGroup}
        <CaptureFlow
          target={{ kind: 'destination', ...capturing, group: captureGroup }}
          oncancel={() => (capturing = null)}
        />
      {/if}
      <SaveBar
        dirty={editor.isDirty}
        saving={editor.saving}
        {refusals}
        onsave={() => void editor.save()}
        ondiscard={() => editor.discard()}
      />
    </div>
  </SectionShell>
{/if}

{#snippet captureButton(id: string, scope: string)}
  {#if captureGroup && mayCapture({ kind: 'destination', scope, group: captureGroup })}
    <button type="button" class="pick" onclick={() => (capturing = { id, scope })}>Capture</button>
  {/if}
{/snippet}

<style>
  .fixed {
    display: block;
    padding: 0.3rem 0;
    font-family: var(--mono);
    font-size: var(--text-label);
  }

  /* The id beside the type it was added as. */
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

  .accepts {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    align-items: center;
  }
</style>
