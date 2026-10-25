<!--
  Routes page: `routes.json`, read through `GET /api/config` and written
  with `PUT /api/routes`, plus the raw-JSON page. Routes are unordered: each
  sees every row of its source (`plan::topology::route_masks`), so a row two
  routes match runs both.

  The Exceptions at the foot are the group's `exceptions`, in its manifest, so saving them writes
  `system.json` rather than this page's own file. Each route's own Exceptions lay over them and save with the
  routes. A destination made from a route card is saved to `destinations.json` on Add.
-->
<script lang="ts">
  // A link can open a new route (`make`, from the map), and a pipeline made for one comes back to it (`chose`):
  //
  // ```mermaid
  // flowchart LR
  //   M[Map: + route] -- make --> R[Routes: the new route alone]
  //   R -- + New pipeline… --> P[Pipeline editor] -- Save, chose --> R
  //   R -- Save --> B[back]
  // ```
  //
  // The new route is held unsaved (`edits.ts`) while the pipeline is made, as any edit here is.
  import EditInShared from './EditInShared.svelte';
  import OnlyNotice from './OnlyNotice.svelte';
  import { Narrowing } from './narrowing.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import { SvelteMap } from 'svelte/reactivity';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import SaveBar from './SaveBar.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import HeldEdit from './HeldEdit.svelte';
  import Advanced from './Advanced.svelte';
  import Collapsible from './Collapsible.svelte';
  import RouteFlow from './RouteFlow.svelte';
  import { cards, type Card } from './routeFlow';
  import JsonEditPage from './JsonEditPage.svelte';
  import RoutePreview from './RoutePreview.svelte';
  import ExceptionMatrix from './ExceptionMatrix.svelte';
  import Locked from './Locked.svelte';
  import { editable } from './access';
  import NewDestination from './NewDestination.svelte';
  import NewSource from './NewSource.svelte';
  import ExceptionsFold from './ExceptionsFold.svelte';
  import TagField from './TagField.svelte';
  import {
    answerText,
    BUILT_IN,
    CATEGORIES,
    kindsOf,
    lay,
    problems,
    withCategories,
    type ExceptionsDto,
  } from './exceptions';
  import Help from './Help.svelte';
  import {
    sharedConfig,
    currentScope,
    SHARED_SCOPE,
    putRoutes,
    putManifest,
    putDestinations,
    putSources,
    fieldCatalogs,
    type RouteDto,
    type DestinationDto,
    type SourceDto,
    type MutationResult,
    type IdentityDto,
    type SecretDto,
    type FileConfigDto,
    type AskAt,
    afterDot,
    TAG_FIELD,
  } from './api';
  import { mintRouteId } from './ids';
  import { newPipelineHash, routeBackHash, ROUTES_TABS, type Making } from './nav';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import { SectionEditor } from './sectionEditor.svelte';
  import { provideValidation } from './validation.svelte';
  import ErrorCount from './ErrorCount.svelte';

  let {
    showJson = false,
    sampling = false,
    only,
    make,
    chose,
    group,
    back,
  }: {
    showJson?: boolean;
    /** The Route a sample tab (`ROUTES_TABS`). */
    sampling?: boolean;
    only?: string[];
    /** A new route to open, taking this. */
    make?: Making;
    /** A pipeline just made for route `route`, to put on it. */
    chose?: { route: string; pipeline: string };
    /** Where the map showed the source of the route being made (`Making`). */
    group?: string;
    /** Where Save returns to. */
    back?: string;
  } = $props();
  const narrowing = new Narrowing(() => only);
  const kept = (id: string) => narrowing.keeps(id);

  let routes: RouteDto[] = $state([]);
  // The bundle as read, kept whole so a route preview can be run against the
  // routes on screen rather than against what was last saved.
  let loadedConfig = $state<FileConfigDto | null>(null);
  let sourceIds: string[] = $state([]);
  /** What each exception does to its row, under each route's own: the group's manifest, not `routes.json`. Every
   * category answered (`withCategories`). */
  let exceptions = $state<ExceptionsDto | undefined>(undefined);
  let pipelineIds: string[] = $state([]);
  let pipelineNames: Record<string, string> = $state({});
  let sharedPipelineIds: Set<string> = $state(new Set());
  let destinationIds: string[] = $state([]);
  /** Every destination this group runs, its own and `shared/`'s. */
  let destinations: DestinationDto[] = $state([]);
  /** Each kind's answer under the group's. */
  let groupAnswers = $derived(lay(BUILT_IN, exceptions));
  /** True when an answer, or the tag field, is not the built-in. */
  let answersModified = $derived(
    (exceptions?.tagField ?? TAG_FIELD) !== TAG_FIELD ||
      JSON.stringify(groupAnswers) !== JSON.stringify(BUILT_IN),
  );
  /** Closed even when modified: the answers are a whole matrix, and its header says when they are not the built-in. Open
   * while Save holds back one of them. */
  let answersOpen = $state(false);
  /** What the load would refuse because of the group's answers: its own deliveries, and refusal loops through it. */
  let groupRefusals = $derived.by(() => {
    const without = problems(undefined, [], destinations);
    return problems(exceptions, [], destinations).filter((p) => !without.includes(p));
  });
  // Save holds back what the answers would have refused, so the answers stay in view until it is fixed.
  $effect(() => {
    if (groupRefusals.length > 0) answersOpen = true;
  });
  /** Puts a destination being made from a route card where the card asked for it (`RouteFlow`). */
  let placing = $state<((dest: string) => void) | null>(null);
  /** Makes a source being made from a route card that card's (`RouteFlow`). */
  let sourcing = $state<((source: string) => void) | null>(null);
  /** What a destination made here may name: this scope's identities and `shared/`'s, and the catalogs. */
  let identities: IdentityDto[] = $state([]);
  let secrets: SecretDto[] = $state([]);
  let mechanisms: string[] = $state([]);
  const GROUP_ROWS = CATEGORIES.flatMap((c) => [c, ...kindsOf(c)]);
  // Of the delivery kinds a route answers only `Unmatched`, for the rows its delivery rules decline.
  const ROUTE_ROWS = GROUP_ROWS.filter(
    (r) => r === 'Unmatched' || (!kindsOf('delivery').includes(r) && r !== 'delivery'),
  );
  /** Ids `shared/` declares. A group runs `shared/` merged with its own subtree, so a route of its own may name
   * one of these; marking them is what keeps an author from reading a fleet-wide object as their group's. */
  let fromShared: Set<string> = $state(new Set());
  /** `shared/`'s own routes, which this group runs too. Never saved from here: the page writes its own scope. */
  let sharedRoutes: RouteDto[] = $state([]);
  let refusals = $derived(problems(exceptions, [...routes, ...sharedRoutes], destinations));
  // This page writes two files (`edits.ts`): the routes, and the manifest's
  // answers. The manifest write lands first and can land alone, so a
  // half-landed save keeps what is still unsaved rather than dropping all.
  /** The group's answers: a file of its own, read with the routes (`load`) and saved before them (`save`). */
  const manifestEditor = new SectionEditor<ExceptionsDto | undefined>('manifest', {
    get: () => exceptions,
    set: (value) => (exceptions = value),
    pick: (config) => withCategories(config.exceptions),
    put: (value, readAtCommit) => putManifest({ exceptions: value ?? null }, readAtCommit),
    text: (value) => JSON.stringify(value ?? null),
    parse: (text) => JSON.parse(text) ?? undefined,
  });
  /** A save's errors on a route's condition and its rules' `when`, drawn on them by `RouteFlow`. */
  const validation = provideValidation('routes');
  /** Asks the engine what may follow a dot in a route's expression, over the routes as they are being written. */
  const askAt: AskAt = (at) => (text, dot) =>
    afterDot({ config: JSON.stringify({ ...loadedConfig, routes }), at, text, dot });
  const routesEditor = new SectionEditor<RouteDto[]>('routes', {
    get: () => routes,
    set: (value) => (routes = value.map(withRules)),
    pick: (config) => config.routes,
    put: putRoutes,
    validation,
    onLoaded: async (config, res) => {
      loadedConfig = config;
      await manifestEditor.adopt(res);
      sourceIds = config.sources.map((s) => s.id);
      pipelineIds = config.pipelines.map((p) => p.id);
      pipelineNames = Object.fromEntries(config.pipelines.map((p) => [p.id, p.name]));
      sharedPipelineIds = new Set();
      destinationIds = config.destinations.map((d) => d.id);
      destinations = config.destinations;
      identities = config.identities;
      await addShared();
    },
    onDiscard: () => {
      expanded.clear();
      unwired.clear();
    },
  });
  let isDirty = $derived(routesEditor.isDirty || manifestEditor.isDirty);
  let saving = $derived(routesEditor.saving || manifestEditor.saving);
  let conflict = $derived(manifestEditor.conflict ?? routesEditor.conflict);

  /** One card per source, each holding every route that reads it (`routeFlow.ts`). */
  let sourceCards = $derived(cards(routes));
  // Keyed by source, not index: removing a card shifts every index after it.
  // Maps, not objects: a hand-written id may be `__proto__` or `constructor`, which an object answers
  // from its prototype.
  const expanded = new SvelteMap<string, boolean>();
  // A link to one route opens its card.
  $effect(() => {
    if (only?.length !== 1) return;
    const r = routes.find((r) => r.id === only[0]);
    if (r) expanded.set(r.source, true);
  });
  // A save error's link in the error bar opens the card that holds its route.
  $effect(() => {
    const off = sourceCards.flatMap((card) =>
      card.routes.map((r) => validation.opener(r.id, () => expanded.set(card.source, true))),
    );
    return () => off.forEach((f) => f());
  });
  const readOnly = !editable();
  /** Destinations brought onto a card and not yet wired, per source (`RouteFlow`). Kept here because collapsing a
   * card unmounts its body. */
  const unwired = new SvelteMap<string, string[]>();
  /** What a route preview runs: the bundle as read, with the routes on
   * screen. Unsaved edits are the point -- a preview of what is already
   * saved answers a question nobody is asking. */
  let previewConfig = $derived<FileConfigDto | null>(
    loadedConfig
      ? {
          ...loadedConfig,
          routes,
          exceptions,
        }
      : null,
  );

  /** One read fills both editors, and the manifest's waits on the whole of it. */
  async function load() {
    manifestEditor.loading = true;
    await routesEditor.load();
    manifestEditor.loadFailed = routesEditor.loadFailed;
    manifestEditor.loading = false;
  }
  void load().then(arrive);
  void fieldCatalogs().then((c) => ({ secrets, mechanisms } = c));

  /** A minted id, never a counted one: it is what the route's durable history is keyed by (`mintRouteId`). A new
   * route starts on a source no route reads yet, so it opens a card of its own; once every source has one, it starts
   * with no source. */
  function addRoute() {
    narrowing.widen();
    const id = mintRouteId();
    const source = sourceIds.find((s) => !routes.some((r) => r.source === s)) ?? '';
    expanded.set(source, true);
    routes.push({
      id,
      source,
      condition: null,
      pipeline: pipelineIds[0] ?? '',
      deliver: destinationIds.length > 0 ? [{ to: [destinationIds[0]] }] : [],
    });
  }
  /** Acts on what the link asked, then takes it off the link, so a reload or Back does not do it twice. */
  function arrive() {
    if (routesEditor.loadFailed || readOnly) return;
    let id: string | undefined;
    if (make) {
      id = mintRouteId();
      expanded.set(make.source, true);
      routes.push({
        id,
        source: make.source,
        condition: make.condition ?? null,
        // Chosen by hand: the first pipeline would read as a choice nobody made.
        pipeline: '',
        deliver: destinationIds.length > 0 ? [{ to: [destinationIds[0]] }] : [],
      });
    } else if (chose) {
      const r = routes.find((r) => r.id === chose.route);
      if (r) {
        r.pipeline = chose.pipeline;
        id = r.id;
      }
    }
    if (id) location.replace(routeBackHash(id, back, make ? make.group : group));
  }

  /** The pipeline editor, making a pipeline for `route`, and back to it. */
  function newPipeline(route: RouteDto) {
    const making = { source: route.source, condition: route.condition ?? undefined, group };
    location.hash = newPipelineHash(making, routeBackHash(route.id, back, group));
  }

  /** Save, and back to where the link came from. */
  async function saveAndReturn(): Promise<boolean> {
    const saved = await save();
    if (saved && back) location.hash = back;
    return saved;
  }

  /** Trades `route`'s place with the next route of its source `by` places along: the card's pipelines, and their
   * destinations, follow this order. */
  function shiftRoute(route: RouteDto, by: number) {
    const mine = routes.filter((r) => r.source === route.source);
    const other = mine[mine.indexOf(route) + by];
    if (!other) return;
    const a = routes.indexOf(route);
    const b = routes.indexOf(other);
    [routes[a], routes[b]] = [routes[b], routes[a]];
  }
  function removeRoute(route: RouteDto) {
    const i = routes.indexOf(route);
    if (i >= 0) routes.splice(i, 1);
  }
  /** What was brought onto a card goes with it, so a new route on the same source starts clean. */
  function removeCard(card: Card) {
    card.routes.forEach(removeRoute);
    unwired.delete(card.source);
    expanded.delete(card.source);
  }
  /** A card whose routes moved from source `from` to `to` is `to`'s now, or part of it: it stays open there, with
   * what was brought onto it. */
  function moveCard(from: string, to: string) {
    expanded.set(to, true);
    const brought = unwired.get(from) ?? [];
    unwired.set(to, [...new Set([...(unwired.get(to) ?? []), ...brought])]);
    unwired.delete(from);
  }
  /** A new route on `source`'s card, its pipeline still to choose. */
  function addBranch(source: string) {
    routes.push({
      id: mintRouteId(),
      source,
      condition: null,
      pipeline: '',
      deliver: [{ to: [] }],
    });
  }
  /** `deliver` is optional on the wire; the editor works against an array. */
  /** What `shared/` adds to the choices. A route lives in one subtree and may name an object from either, since
   * what runs is the two merged -- so a group's page has to offer both. `shared/` itself draws on nothing, so
   * there is nothing to add there. A failed read leaves the group's own choices rather than the page. */
  async function addShared() {
    const shared = await sharedConfig();
    if (!shared) return;
    fromShared = new Set([
      ...shared.sources.map((x) => x.id),
      ...shared.pipelines.map((x) => x.id),
      ...shared.destinations.map((x) => x.id),
    ]);
    // The group's own first: these lists are also where a new route takes its defaults from, and a route should
    // start on something the group owns rather than on a fleet-wide object it merely draws on.
    sharedRoutes = shared.routes;
    sourceIds = [...sourceIds, ...shared.sources.map((x) => x.id)];
    pipelineIds = [...pipelineIds, ...shared.pipelines.map((x) => x.id)];
    pipelineNames = {
      ...Object.fromEntries(shared.pipelines.map((p) => [p.id, p.name])),
      ...pipelineNames,
    };
    sharedPipelineIds = new Set(shared.pipelines.map((p) => p.id));
    destinationIds = [...destinationIds, ...shared.destinations.map((x) => x.id)];
    destinations = [...destinations, ...shared.destinations];
    identities = [...shared.identities, ...identities];
  }

  /** Saves `made` with the rest of `list` on its own, as its own page would, against the commit this page read: the
   * routes that will name it are saved after it. The list saved, or null when it was refused: the dialog's error
   * bar says why, and the dialog stays open. */
  async function saveAlone<O>(
    list: O[],
    made: O,
    put: (all: O[], base: string | null) => Promise<MutationResult>,
  ): Promise<O[] | null> {
    const own = [...($state.snapshot(list) as O[]), made];
    const res = await put(own, routesEditor.readAtCommit);
    if (!res.ok) return null;
    routesEditor.readAtCommit = manifestEditor.readAtCommit = res.commit ?? null;
    return own;
  }

  /** A destination made from a route card, saved on its own and placed where the card asked. */
  async function addDestination(made: DestinationDto): Promise<void> {
    if (!loadedConfig) return;
    // The card that asked, as it was when Add was pressed: the dialog can close and open on another card meanwhile.
    const place = placing;
    const own = await saveAlone(loadedConfig.destinations, made, putDestinations);
    if (own === null) return;
    loadedConfig.destinations = own;
    destinations = [...destinations, made];
    destinationIds = [...destinationIds, made.id];
    place?.(made.id);
    if (placing === place) placing = null;
  }

  /** A source made from a route card, saved on its own and made that card's. */
  async function addSource(made: SourceDto): Promise<void> {
    if (!loadedConfig) return;
    const place = sourcing;
    const own = await saveAlone(loadedConfig.sources, made, putSources);
    if (own === null) return;
    loadedConfig.sources = own;
    sourceIds = [...sourceIds, made.id];
    place?.(made.id);
    if (sourcing === place) sourcing = null;
  }

  /** Where the rows of `rs` go when no rule sends them anywhere: Unmatched, as each route answers it. */
  function toUnmatched(rs: RouteDto[]): string {
    const answers = rs.map((r) => answerText(lay(groupAnswers, r.exceptions).Unmatched));
    return `Unmatched (${[...new Set(answers)].join(', ')})`;
  }

  /** A shared route in one line, in the direction it runs. */
  function flow(r: RouteDto): string {
    const to = (r.deliver ?? []).flatMap((d) => d.to);
    return `${r.source || '(no source)'} \u2192 ${r.pipeline ? pipelineLabel(r.pipeline) : '(no pipeline)'} \u2192 ${
      to.length > 0 ? [...new Set(to)].join(', ') : toUnmatched([r])
    }`;
  }

  /** `shared/` has no manifest, so its page offers no group answers. */
  const inShared = currentScope() === SHARED_SCOPE;

  /** An option's text. A native `<option>` holds text, so where a chip would go elsewhere this says it in a word. */
  function label(id: string): string {
    return fromShared.has(id) ? `${id} \u2014 shared` : id;
  }

  function pipelineLabel(id: string): string {
    const name = pipelineNames[id] || id;
    return sharedPipelineIds.has(id) ? `${name} \u2014 shared` : name;
  }

  function withRules(r: RouteDto): RouteDto {
    return { ...r, deliver: r.deliver ?? [] };
  }
  /** The collapsed header's destinations. "conditional" when any rule is
   * gated, so a header never implies a row always goes somewhere it might
   * not. */
  function summarizeDeliver(card: RouteDto[]): string {
    const rs = card.flatMap((r) => r.deliver ?? []);
    if (rs.length === 0) return toUnmatched(card);
    const dests = [...new Set(rs.flatMap((rule) => rule.to))];
    if (dests.length === 0) return toUnmatched(card);
    const gated = rs.some((rule) => (rule.when ?? '').trim() !== '');
    return dests.join(', ') + (gated ? ' (conditional)' : '');
  }

  function discard() {
    routesEditor.discard();
    manifestEditor.discard();
  }

  async function save(): Promise<boolean> {
    // A refusal from the write not made this time would otherwise stay on screen.
    for (const editor of [routesEditor, manifestEditor]) {
      editor.conflict = null;
    }
    // The manifest is not `routes.json`, so editing it is a commit of its own. It goes first: a refusal there
    // leaves the routes as they were, which is what this page still shows.
    if (manifestEditor.isDirty) {
      if (!(await manifestEditor.save())) return false;
      // This write moved the branch on, so the routes write is against what it
      // produced rather than what this page loaded.
      routesEditor.readAtCommit = manifestEditor.readAtCommit ?? routesEditor.readAtCommit;
    }
    if (!(await routesEditor.save())) return false;
    manifestEditor.readAtCommit = routesEditor.readAtCommit;
    return true;
  }

  async function saveFromJson(text: string) {
    const res = await routesEditor.saveFromJson(text);
    if (res.ok) {
      manifestEditor.readAtCommit = routesEditor.readAtCommit;
      // The JSON is now the whole truth: a destination left unwired on a card is not in it.
      unwired.clear();
      if (back) location.hash = back;
    }
    return res;
  }

  /** This page's hash under `path`, keeping what the link carried, so raw JSON and back still return to `back`. */
  function here(path: string): string {
    const at = location.hash.indexOf('?');
    return path + (at < 0 ? '' : location.hash.slice(at));
  }
</script>

{#if routesEditor.loading}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else if routesEditor.loadFailed}
  <SectionShell />
{:else if showJson}
  <JsonEditPage
    title="Edit JSON — Routes"
    initialText={JSON.stringify(routes, null, 2)}
    backHash={here('#/build/routes')}
    onSave={saveFromJson}
  />
{:else}
  <SectionShell>
    <PageHeader title="Routes">
      {#snippet help()}
        Routes have no order: a record two routes match runs through both. Drag from a socket to
        create a path in the route. Click on the path to remove it.
      {/snippet}
    </PageHeader>
    <UnreadableFiles problems={routesEditor.bundleProblems} onchanged={load} />
    <SecondaryTabs
      tabs={ROUTES_TABS}
      active={sampling ? ROUTES_TABS[1].hash : ROUTES_TABS[0].hash}
      ariaLabel="Routes"
    />

    {#if routesEditor.moved}
      <HeldEdit
        what="routes"
        rows={routesEditor.moved.rows}
        onrestore={() => routesEditor.restoreHeld()}
        ondiscard={() => routesEditor.dropHeld()}
      />
    {/if}

    {#if manifestEditor.moved}
      <HeldEdit
        what="the exception answers"
        rows={[]}
        onrestore={() => manifestEditor.restoreHeld()}
        ondiscard={() => manifestEditor.dropHeld()}
      />
    {/if}

    {#if conflict}
      <SaveConflictPanel
        {conflict}
        onDiscard={() => location.reload()}
        onRetry={() => saveAndReturn()}
      />
    {/if}

    {#if sampling}
      <RoutePreview config={previewConfig} />
      <div class="preview-link">
        <a href="#/build/routes/preview">Where these records would go</a>
        <Help label="Where these records would go">
          Draws a sample's trip from source to destination through the routes saved on this branch.
        </Help>
      </div>
    {:else}
      {#if !readOnly}
        <div class="toolbar">
          <button type="button" class="primary" data-guide="add-route" onclick={addRoute}>
            + Add route
          </button>
          <button type="button" onclick={() => (location.hash = here('#/build/routes/json'))}
            >Edit JSON&hellip;</button
          >
        </div>
      {/if}

      <OnlyNotice {narrowing} ids={[...sharedRoutes, ...routes].map((r) => r.id)} />

      {#if sharedRoutes.some((r) => kept(r.id))}
        <div class="band">from shared/ &mdash; every group runs these</div>
        {#each sharedRoutes.filter((r) => kept(r.id)) as route (route.id)}
          <div class="rest fromshared">
            <span class="rest-meta">{flow(route)}</span>
            <span class="rest-gap"></span>
            <span class="shared-chip"><span class="shared-dot"></span>shared</span>
            <EditInShared hash="#/build/routes" />
          </div>
        {/each}
        <div class="band">this group</div>
      {/if}

      {#each sourceCards as card (card.source)}
        {#if card.routes.some((r) => kept(r.id))}
          <Collapsible
            bind:open={
              () => expanded.get(card.source), (v) => expanded.set(card.source, v ?? false)
            }
          >
            {#snippet summary()}
              <span class="sum-flow">
                <span class="sum-part">{card.source || '(no source)'}</span>
                <span class="sum-arrow">&rarr;</span>
                <span class="sum-part"
                  >{card.routes
                    .map((r) => (r.pipeline ? pipelineLabel(r.pipeline) : '(no pipeline)'))
                    .join(', ')}</span
                >
                <span class="sum-arrow">&rarr;</span>
                <span class="sum-part">{summarizeDeliver(card.routes)}</span>
              </span>
              {#if card.routes.length === 1 && card.routes[0].condition}
                <span class="sum-cond">when <code>{card.routes[0].condition}</code></span>
              {/if}
            {/snippet}
            {#snippet actions()}
              <!-- Here, not in the summary, which a narrow window cuts short. -->
              <ErrorCount
                errors={validation.placed.filter((e) => card.routes.some((r) => r.id === e.item.id))
                  .length}
              />
              {#if !readOnly}
                <button
                  type="button"
                  class="compact"
                  aria-label="Remove"
                  title="Remove"
                  onclick={() => removeCard(card)}>&#10005;</button
                >
              {/if}
            {/snippet}
            <RouteFlow
              routes={card.routes}
              {askAt}
              {sourceIds}
              {pipelineIds}
              {destinationIds}
              {label}
              {pipelineLabel}
              unmatched={(route) => lay(groupAnswers, route.exceptions).Unmatched}
              bind:extras={() => unwired.get(card.source) ?? [], (v) => unwired.set(card.source, v)}
              onaddroute={readOnly ? undefined : () => addBranch(card.source)}
              onremoveroute={removeRoute}
              onorderroute={readOnly ? undefined : shiftRoute}
              onmoveroute={(to) => expanded.set(to, true)}
              onsource={(to) => moveCard(card.source, to)}
              onnewdest={readOnly ? undefined : (place) => (placing = place)}
              onnewpipeline={readOnly ? undefined : newPipeline}
              onnewsource={readOnly ? undefined : (place) => (sourcing = place)}
            />
            {#each card.routes as route (route.id)}
              {#if card.routes.length > 1}
                <span class="branch-name"
                  >{route.pipeline ? pipelineLabel(route.pipeline) : '(no pipeline)'}</span
                >
              {/if}
              <ExceptionsFold
                bind:value={route.exceptions}
                name={route.id}
                rows={ROUTE_ROWS}
                beneath={groupAnswers}
                targets={destinationIds}
                tagField={exceptions?.tagField ?? TAG_FIELD}
              />
            {/each}
          </Collapsible>
        {/if}
      {/each}
      {#if routes.length === 0}
        <p class="dim">No routes yet.</p>
      {/if}

      {#if !inShared}
        <Advanced
          label="Exceptions"
          modified={answersModified}
          bind:open={() => answersOpen, (v) => (answersOpen = !!v || groupRefusals.length > 0)}
        >
          {#snippet help()}The answer every route in this group gives each exception, unless the
            route overrides it.{/snippet}
          <ExceptionMatrix
            bind:value={exceptions}
            rows={GROUP_ROWS}
            beneath={BUILT_IN}
            fixed
            targets={destinationIds}
            name="group"
          />
          <TagField bind:value={exceptions} placeholder={TAG_FIELD} />
        </Advanced>
      {/if}
    {/if}
    <NewDestination
      open={placing !== null}
      unavailable={destinationIds}
      {identities}
      {secrets}
      {mechanisms}
      onadd={addDestination}
      oncancel={() => (placing = null)}
    />
    <NewSource
      open={sourcing !== null}
      unavailable={sourceIds}
      {identities}
      {secrets}
      {mechanisms}
      readAt={routesEditor.readAtCommit}
      onsaved={(commit) => (routesEditor.readAtCommit = manifestEditor.readAtCommit = commit)}
      onadd={addSource}
      oncancel={() => (sourcing = null)}
    />

    <SaveBar
      dirty={isDirty}
      {saving}
      {refusals}
      onsave={() => void saveAndReturn()}
      ondiscard={discard}
    />
  </SectionShell>
{/if}

<style>
  /* A read-only row: what `shared/` gave this group, or the `everything else` answer. */
  .rest {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.6rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--panel);
    padding: 0.5rem 0.7rem;
    margin-bottom: 0.5rem;
  }

  .rest-meta {
    font-family: var(--mono);
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .rest-gap {
    flex: 1;
  }

  /* `shared/`'s own: shown, because the group runs them, and not editable here -- the scope chip at the rail
     head names what this page writes, and it does not name `shared/`. */
  .rest.fromshared {
    background: none;
    border-style: dashed;
    border-left: 3px solid oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  .rest.fromshared .rest-meta {
    color: var(--dim);
  }

  .shared-chip {
    display: inline-flex;
    align-items: center;
    gap: 0.28rem;
    border: 1px solid
      color-mix(
        in oklch,
        oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260)) 45%,
        var(--border)
      );
    border-radius: 999px;
    padding: 0.02rem 0.45rem;
    font-size: var(--text-micro);
    color: oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
    white-space: nowrap;
  }

  .shared-dot {
    width: 7px;
    height: 7px;
    border-radius: 999px;
    background: oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  .band {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin: 1rem 0 0.5rem;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  .band::after {
    content: '';
    flex: 1;
    height: 1px;
    background: var(--border);
  }

  .preview-link {
    margin: 0.5rem 0 1rem;
    font-size: var(--text-note);
  }

  .dim {
    color: var(--dim);
  }

  .toolbar {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin: 1.2rem 0;
  }

  .sum-flow {
    display: inline-flex;
    align-items: baseline;
    gap: 0.4rem;
    min-width: 0;
  }

  .sum-part,
  .branch-name {
    font-family: var(--mono);
    font-size: var(--text-label);
  }

  .sum-arrow {
    color: var(--dim);
  }

  .sum-cond {
    margin-left: 0.5rem;
    font-size: var(--text-label);
    color: var(--dim);
  }

  .sum-cond code {
    font-size: var(--text-label);
  }

  code {
    font-family: var(--mono);
    font-size: var(--text-label);
  }
</style>
