<!--
  The topology map (design doc "The map"): the whole fleet's compiled topology, one band per
  worker group, its lines connections only, and searchable. One map, no mode -- a node's own card
  selects it: its directed path draws in front of the rest, every copy of a shared object lights
  with it, and its details open in a panel docked beside the map (`FocusView.svelte`), whose
  pencil is the one step further into the editor. The selection is the route's, so it is a link and the back
  button undoes it. The map has its own group filter, defaulting to all groups: unrelated to the
  scope chip in the nav, which stays the authoring surface's -- "a group is a filter on that view
  rather than a mode that hides the rest" (`docs/user-stories.md` G2). A group that does not
  compile shows its problems where its band would be; the groups that do still draw.

  Geometry, search, lines and the path are `map.ts`'s, pure and tested there; this wires them to
  a fetch, a search box, the group filter and a click.
-->
<script lang="ts">
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import { store, stored } from './stored';
  import PageHeader from './PageHeader.svelte';
  import {
    getConfig,
    getMap,
    getMapMeasurements,
    getPipelineCallers,
    listWorkerGroups,
    setScope,
    SHARED_SCOPE,
    streamsIn,
    type MapGroupError,
    type MapNode,
    type NodeReadingDto,
    type MapResponse,
    type MeasurementsResponse,
    type UnmatchedStreamsDto,
    type WorkerGroupDto,
  } from './api';
  import FocusView, { FOCUS_TABS, type FocusTab } from './FocusView.svelte';
  import MapRows from './MapRows.svelte';
  import StackView from './StackView.svelte';
  import MapSearch from './MapSearch.svelte';
  import CaptureFlow from './CaptureFlow.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import {
    attention,
    cardLine,
    groupSummary,
    suggestions,
    unmatchedRate,
    type Attention,
  } from './mapTerms';
  import { mapGlyph, SYSTEM_LABELS } from './systemGlyphs';
  import {
    HEAD_H,
    NODE_H,
    NODE_W,
    cardWidth,
    PAD,
    edgePaths,
    edgeDimmed,
    edgeKey,
    gaps,
    groupOfKey,
    isFallback,
    layout,
    linkForError,
    mapEdges,
    nodeKey,
    presentGroups,
    routesThrough,
    search,
    selectedPath,
    sharedCopies,
    statsHash,
    chartRange,
    DEFAULT_CHART_RANGE,
    survivors,
    cardKey,
    objectCount,
    stackHash,
    stackNodes,
    stackReading,
    type MapCard,
    usedNodes,
    visibleEdges,
    visibleNodes,
    type CallersIndex,
    type EdgeKind,
    type KnownIds,
    type MapEdge,
    type Layout,
    type SourceGap,
    type Positioned,
  } from './map';
  import { ago, count } from './time';
  import { canEdit, mayCapture } from './access';
  import { newRouteHash } from './nav';

  /** The selected node and which tab of its panel shows. Read from the route (`App.svelte`)
   * rather than held here, so selecting one is a link and the back button undoes it. */
  let {
    stats,
    q,
  }: {
    stats?: {
      group: string;
      kind: MapNode['kind'];
      id: string;
      panel?: string;
      stack?: boolean;
      range?: string;
    };
    /** What the search starts with: a link that picks out several nodes at once. */
    q?: string;
  } = $props();
  // A band is a region named by its group's heading.
  const uid = $props.id();

  let loading = $state(true);
  let loadError = $state('');
  let mapResult = $state<MapResponse | null>(null);
  let measurements = $state<MeasurementsResponse | null>(null);
  let callers = $state<CallersIndex>({});
  let known = $state<KnownIds>({ pipelines: [], sources: [], destinations: [], routes: [] });
  let workerGroups = $state<WorkerGroupDto[]>([]);
  /** What the fleet saw arrive that no route took, in every group. */
  let unmatched = $state<UnmatchedStreamsDto[]>([]);
  let query = $state('');
  // Follows the route, so a second such link replaces what the first searched.
  $effect(() => {
    if (q) query = q;
  });
  let groupFilter = $state('all');
  /** When the readings on screen arrived, by this browser's clock. */
  let readAt = $state<number | null>(null);
  let now = $state(Date.now());
  /** The branch moved since the layout was drawn. The readings keep coming; the layout waits for
   * the person to ask, so nothing moves under a pointer. */
  let topologyChanged = $state(false);
  let updating = $state(false);
  let layoutError = $state('');

  /** Bumped by every layout fetch, so a poll that left before one cannot report its commit as
   * a change. */
  let layoutRound = 0;

  /** `quiet` redraws in place, for Update layout: the page stays, and a failure is said beside the
   * button rather than in the page's place. */
  async function load(quiet = false) {
    layoutRound++;
    if (quiet) layoutError = '';
    else {
      loading = true;
      loadError = '';
    }
    try {
      const [res, groupsRes, readings, streams] = await Promise.all([
        getMap(),
        listWorkerGroups(),
        // Best-effort: a map without readings still draws.
        getMapMeasurements().catch(() => null),
        // Best-effort as well: without them a gap is only a source no route takes.
        streamsIn(SHARED_SCOPE).catch(() => []),
      ]);
      mapResult = res;
      unmatched = streams;
      measurements = readings?.ok ? readings : null;
      readAt = measurements ? Date.now() : null;
      layoutWarned = new Set(
        res.ok
          ? res.nodes.filter((n) => drawn(n)).map((n) => nodeKey(n.group, n.kind, n.authorId))
          : [],
      );
      layoutListed = listedKeys();
      // The two requests race a push: readings from another commit mean the layout is behind.
      topologyChanged = !!(
        measurements?.commit &&
        res.commit &&
        measurements.commit !== res.commit
      );
      workerGroups = groupsRes.ok ? groupsRes.groups : [];
      if (res.ok) {
        // Only the map needs this: `calls:` has nowhere else to read a call from, since a
        // call is never on the map itself (design doc: "A called pipeline is not a node").
        const c = await getPipelineCallers();
        if (c.ok) callers = c.callers;
      } else {
        // Best-effort only, to link a problem to its object: `getConfig` reads leniently, so
        // this can still name every id that did parse even though the bundle as a whole did
        // not compile.
        const cfg = await getConfig();
        if (cfg.config) {
          known = {
            pipelines: cfg.config.pipelines.map((p) => ({ id: p.id, name: p.name })),
            sources: cfg.config.sources.map((s) => s.id),
            destinations: cfg.config.destinations.map((d) => d.id),
            routes: cfg.config.routes.map((r) => r.id),
          };
        }
      }
    } catch (e) {
      if (quiet) layoutError = unreported(e);
      else loadError = unreported(e);
    } finally {
      loading = false;
    }
  }
  load();

  /** One bucket of the series the readings come from, until the leader says how long one is:
   * polling faster reads the same numbers. */
  let pollMs = $derived(measurements?.bucketMillis ?? 10_000);

  /** The nodes a warning or a gap named when the layout was drawn: which shared objects it shows. */
  let layoutWarned = $state(new Set<string>());

  /** The objects Needs attention listed when the layout was drawn: which stand out of a stack. */
  let layoutListed = $state(new Set<string>());

  function listedKeys(): Set<string> {
    return new Set(
      attention(nodes, readingOf).flatMap((a) =>
        a.node ? [nodeKey(a.group, a.node.kind, a.node.authorId)] : [],
      ),
    );
  }

  /** Bumped by every poll, so a slow one cannot land over a newer one's readings. */
  let pollSeq = 0;
  async function poll() {
    const round = layoutRound;
    const seq = ++pollSeq;
    try {
      // A stream is reported as workers beat, so the gaps refresh with the readings.
      const [m, streams] = await Promise.all([
        getMapMeasurements(),
        streamsIn(SHARED_SCOPE).catch(() => null),
      ]);
      if (!m.ok || round !== layoutRound || seq !== pollSeq) return;
      measurements = m;
      if (streams) unmatched = streams;
      readAt = Date.now();
      if (m.commit && mapResult?.commit && m.commit !== mapResult.commit) topologyChanged = true;
      // A warning that comes or goes on a shared object no line touches adds or drops a card.
      const now = mapResult?.ok ? usedNodes(mapResult.nodes, allEdges, drawn) : nodes;
      if (now.length !== nodes.length || now.some((n, i) => n !== nodes[i])) topologyChanged = true;
      // A problem that comes or goes moves its object into or out of a stack.
      const listed = listedKeys();
      if (listed.size !== layoutListed.size || [...listed].some((k) => !layoutListed.has(k)))
        topologyChanged = true;
    } catch {
      // The last readings stay on screen, and their age says how old they are.
    }
  }

  /** Redraws at the commit the branch is at now, keeping the selection, search and fold. */
  async function updateLayout() {
    if (updating) return;
    updating = true;
    await load(true);
    updating = false;
  }

  $effect(() => {
    const id = setInterval(poll, pollMs);
    const tick = setInterval(() => (now = Date.now()), 1000);
    return () => {
      clearInterval(id);
      clearInterval(tick);
    };
  });

  /** Where a node's own editor is -- `kind` and its author id are all it takes, so this also
   * builds the panel's own edit link straight from `stats`, without a whole `MapNode` at hand. */
  function hashFor(node: Pick<MapNode, 'kind' | 'authorId'>): string {
    if (node.kind === 'pipeline') return `#/build/pipelines/${encodeURIComponent(node.authorId)}`;
    if (node.kind === 'source') return `#/build/sources/${encodeURIComponent(node.authorId)}`;
    return `#/build/destinations/${encodeURIComponent(node.authorId)}`;
  }

  /** The node's glyph: the system behind a source or destination, a block for a pipeline. `px` is
   * 30 on a card, where it is the mark the eye lands on, and 16 in the legend beside running text. */
  function shapeGlyph(node: Pick<MapNode, 'kind' | 'system'>, px = 24): string {
    const g = mapGlyph(node);
    const rule = g.evenodd ? 'evenodd' : 'nonzero';
    return `<svg viewBox="0 0 64 64" width="${px}" height="${px}" aria-hidden="true"><path d="${g.d}" fill="currentColor" fill-rule="${rule}"/></svg>`;
  }

  /** A card's second line: the system behind a source or destination, or how many routes run
   * through a pipeline. */
  function cardMeta(n: MapNode): string {
    if (n.kind === 'pipeline')
      return count(routesThrough(allEdges, nodeKey(n.group, n.kind, n.authorId)).size, 'route');
    if (!n.system) return '';
    // `MapSystem` mirrors the leader's enum by hand: a system this build does not know reads as
    // the leader named it.
    return (SYSTEM_LABELS as Partial<Record<string, string>>)[n.system] ?? n.system;
  }

  /** A group's display name and whether it is deployed yet -- the same two facts the nav's scope
   * picker shows, so the two read as one vocabulary. A group with no name of its own is served
   * its id as one, so the id is what shows when it is all there is; falling back to it again here
   * covers a name set to the empty string, which nothing refuses yet. */
  function groupLabel(id: string): string {
    const g = workerGroups.find((g) => g.id === id);
    return g ? `${g.name || id}${g.deployed ? '' : ' (pending)'}` : id;
  }

  let ok = $derived(mapResult?.ok === true);
  let allEdges = $derived<MapEdge[]>(mapResult?.ok ? mapEdges(mapResult) : []);
  let nodes = $derived<MapNode[]>(
    mapResult?.ok
      ? usedNodes(mapResult.nodes, allEdges, (n) =>
          layoutWarned.has(nodeKey(n.group, n.kind, n.authorId)),
        )
      : [],
  );
  let groupErrors = $derived<MapGroupError[]>(mapResult?.ok ? mapResult.groupErrors : []);

  let allGroups = $derived(presentGroups(nodes, groupErrors));
  let scopedToOneGroup = $derived(groupFilter !== 'all');
  let filteredNodes = $derived(
    scopedToOneGroup ? nodes.filter((n) => n.group === groupFilter) : nodes,
  );
  let filteredEdges = $derived(
    scopedToOneGroup ? allEdges.filter((e) => groupOfKey(e.from) === groupFilter) : allEdges,
  );
  let filteredGroupErrors = $derived(
    scopedToOneGroup ? groupErrors.filter((e) => e.group === groupFilter) : groupErrors,
  );

  /** Each node's current reading, by `nodeKey`. */
  let readings = $derived(
    new Map((measurements?.nodes ?? []).map((r) => [nodeKey(r.group, r.kind, r.id), r])),
  );

  function readingOf(n: MapNode): NodeReadingDto | undefined {
    return readings.get(nodeKey(n.group, n.kind, n.authorId));
  }

  /** A card's reading: its object's, or its stack's members' together. */
  function cardReading(c: MapCard): NodeReadingDto | undefined {
    return c.members ? stackReading(c, readingOf) : readingOf(c);
  }

  /** A stack's card title. */
  function stackTitle(count: number, kind: MapNode['kind']): string {
    return `${count} ${kind === 'source' ? 'sources' : 'destinations'}`;
  }

  /** Whether the warning catalog names a node now: its polled warnings once there are any
   * readings, the layout's own flag before then. */
  function warned(n: MapNode): boolean {
    return measurements ? (readingOf(n)?.warnings.length ?? 0) > 0 : n.warning;
  }

  /** What arrives at each source that no route takes, by `nodeKey`. */
  let nodeGaps = $derived(
    mapResult?.ok ? gaps(mapResult.nodes, unmatched) : new Map<string, SourceGap>(),
  );

  /** Whether a shared node is drawn where no line touches it: a warning names it, or rows arrive
   * there that no route takes. */
  function drawn(n: MapNode): boolean {
    return warned(n) || nodeGaps.has(nodeKey(n.group, n.kind, n.authorId));
  }

  function gapOf(n: MapNode): SourceGap | undefined {
    return nodeGaps.get(nodeKey(n.group, n.kind, n.authorId));
  }

  /** Opens a new route on the Routes page of the scope that declares `node`, taking `condition` of it, or all of
   * it. Save there comes back here. */
  function routeFrom(node: MapNode, condition?: string) {
    setScope(
      node.shared ? SHARED_SCOPE : node.group,
      newRouteHash({ source: node.authorId, condition, group: node.group }, location.hash),
    );
  }

  let result = $derived(search(filteredNodes, filteredEdges, query, callers, readingOf));
  let offered = $derived(suggestions(query, filteredNodes, readingOf));
  let shownIds = $derived(survivors(result));

  /** The selected node, if the map has it. */
  let selected = $derived(
    stats && !stats.stack
      ? nodes.find(
          (n) => n.group === stats.group && n.kind === stats.kind && n.authorId === stats.id,
        )
      : undefined,
  );
  let selectedKey = $derived(
    selected ? nodeKey(selected.group, selected.kind, selected.authorId) : null,
  );
  let copies = $derived(selected ? sharedCopies(nodes, selected) : new Set<string>());

  let shownNodes = $derived(visibleNodes(filteredNodes, shownIds));
  let shownEdges = $derived(visibleEdges(filteredEdges, shownIds));

  let shownGroups = $derived(presentGroups(filteredNodes, filteredGroupErrors));
  let needs = $derived(
    attention(filteredNodes, readingOf, measurements?.groupWarnings ?? [], shownGroups),
  );

  /** What stands out of a stack: what is selected, a copy of a selected shared object, a search's
   * match, and everything Needs attention listed when the layout was drawn -- the same things that
   * open a folded group. A problem that comes or goes waits for Update layout, like any other
   * change to what the map draws, so no card moves under a pointer. */
  let apart = $derived(
    new Set([
      ...(selectedKey ? [selectedKey] : []),
      ...copies,
      ...(result.active ? result.matched : []),
      ...layoutListed,
    ]),
  );
  let stacked = $derived(stackNodes(shownNodes, shownEdges, (key) => apart.has(key), stackTitle));

  /** The selected stack, found among the stacks of the whole fleet, so a search or the group filter
   * that hides its card leaves its panel open, as it does an object's. A stack whose members
   * changed is another. */
  let selectedStack = $derived(
    stats?.stack
      ? stackNodes(nodes, allEdges, (key) => layoutListed.has(key), stackTitle).cards.find(
          (c) =>
            c.members &&
            c.group === stats.group &&
            c.kind === stats.kind &&
            c.authorId === stats.id,
        )
      : undefined,
  );
  let selectedCardKey = $derived(selectedStack ? cardKey(selectedStack) : selectedKey);

  /** No path while search or the group filter hides the selection: one drawn against a card
   * that is not there would dim the results for no reason on screen. */
  let path = $derived(
    selectedStack
      ? stacked.cards.some((c) => cardKey(c) === cardKey(selectedStack))
        ? selectedPath(stacked.edges, cardKey(selectedStack), selectedStack.kind)
        : null
      : selected && selectedKey && shownIds.has(selectedKey)
        ? selectedPath(stacked.edges, selectedKey, selected.kind)
        : null,
  );

  /** Shows only the selection's path, and the other copies of a shared selection. */
  let onlyPath = $state(false);
  /** What a capture opened from the selection starts on: a source or destination, the scope that
   * declares it and the group that runs it. */
  let selectedTarget = $derived(
    selected?.kind === 'source' || selected?.kind === 'destination'
      ? {
          kind: selected.kind,
          id: selected.authorId,
          scope: selected.shared ? SHARED_SCOPE : selected.group,
          group: selected.group,
        }
      : null,
  );
  /** The target being captured at. Kept apart from the selection, so the capture stays open while
   * the map moves under it. */
  let capturing = $state<typeof selectedTarget>(null);
  $effect(() => {
    if (!selectedCardKey) onlyPath = false;
  });

  let drawnCards = $derived<MapCard[]>(
    onlyPath && path
      ? stacked.cards.filter((c) => {
          const key = cardKey(c);
          return path.nodes.has(key) || copies.has(key);
        })
      : stacked.cards,
  );

  /** A fallback line must not read as a route, so each kind has its own color: the unmatched
   * destination is a disposition someone configured and takes the warning color; the poison DLQ
   * and an exception answer are failures and take the error one. Dashes mean fallback. */
  const EDGE_STROKE: Record<EdgeKind, string> = {
    route: 'var(--dim)',
    deliver: 'var(--dim)',
    unmatched: 'var(--warn)',
    poison: 'var(--error)',
    exception: 'var(--error)',
  };

  /** One arrowhead per stroke a layer draws, so a head always matches its line: the back layer
   * strokes by kind, the path layer strokes a route or delivery in ink. */
  const ARROWS: Record<'back' | 'front', [string, string][]> = {
    back: Object.entries(EDGE_STROKE),
    front: [
      ['path', 'var(--ink)'],
      ...Object.entries(EDGE_STROKE).filter(([kind]) => isFallback(kind as EdgeKind)),
    ],
  };

  /** A marker id unique on the page: every band draws its own two layers. */
  function arrowId(band: number, front: boolean, name: string): string {
    return `map-arrow-${band}-${front ? 'f' : 'b'}-${name}`;
  }

  type DrawnEdge = {
    key: string;
    kind: EdgeKind;
    d: string;
    onPath: boolean;
    dimmed: boolean;
  };

  /** One group's band: either its problems, or its diagram. A group with no surviving node --
   * filtered out entirely, or emptied by search -- has no band at all, the same "the rest closes
   * up" search already gives a single diagram. */
  type Band = {
    group: string;
    error?: MapGroupError;
    diagram: Layout;
    edges: DrawnEdge[];
  };

  /** The width a group's diagram has, the same for every group: the cards narrow to fit it. */
  let diagramRoom = $state(0);
  let nodeW = $derived(diagramRoom ? cardWidth(diagramRoom) : NODE_W);

  /** A fallback line draws only on the selected path that can take it. */
  function bandEdges(diagram: Layout, edges: MapEdge[]): DrawnEdge[] {
    const posByKey = new Map(diagram.positions.map((p) => [keyOf(p), p]));
    const drawn = [];
    for (const e of edges) {
      const key = edgeKey(e);
      const onPath = path?.edges.has(key) ?? false;
      if (isFallback(e.kind) && !onPath) continue;
      const from = posByKey.get(e.from);
      const to = posByKey.get(e.to);
      if (!from || !to) continue;
      drawn.push({ e, key, onPath, from, to });
    }
    const ds = edgePaths(drawn, diagram.nodeW);
    return drawn.map(({ e, key, onPath }, i) => ({
      key,
      kind: e.kind,
      d: ds[i],
      onPath,
      dimmed: (path !== null && !onPath) || edgeDimmed(result.matched, e),
    }));
  }

  /** Past this many objects a fleet opens on its problems: a group with nothing wrong starts
   * folded. */
  const LARGE_FLEET = 60;
  /** Groups the person folded or opened, by group id. */
  let folded = $state<Record<string, boolean>>({});

  function isOpen(group: string): boolean {
    // A selection is always on screen, whatever was folded.
    if (selected?.group === group || selectedStack?.group === group) return true;
    if (nodes.some((n) => n.group === group && copies.has(nodeKey(n.group, n.kind, n.authorId))))
      return true;
    if (group in folded) return !folded[group];
    // A search's answer is on screen until folded by hand.
    if (result.active) return true;
    if (filteredNodes.length <= LARGE_FLEET) return true;
    return summaries.get(group)?.level !== 'ok';
  }

  let summaries = $derived(
    new Map(
      // A group drawn as its errors alone gets a summary only when a warning names it.
      shownGroups
        .filter((g) => filteredNodes.some((n) => n.group === g) || needs.some((a) => a.group === g))
        .map((g) => [
          g,
          groupSummary(
            filteredNodes.filter((n) => n.group === g),
            readingOf,
            needs.filter((a) => a.group === g),
          ),
        ]),
    ),
  );

  /** A problem opens where it is shown: a catalog warning on its node's overview, which carries
   * the warning's sentence, or on the Issues page when no node is its subject; a reading on the
   * tab that shows it. */
  function attentionHash(a: Attention): string {
    if (!a.node) return '#/status/issues';
    if (a.message !== undefined) return hashOf(a.node);
    const n = a.node;
    if (n.kind === 'destination' && readingOf(n)?.queue) return hashOf(n, 'buffers');
    if (n.kind === 'source' && unmatchedRate(readingOf(n)) !== null) return hashOf(n, 'routes');
    if (n.kind === 'source' && readingOf(n)?.lag !== undefined) return hashOf(n, 'backlog');
    return hashOf(n);
  }

  /** The fallback kinds the selected path draws in one band, for that band's legend. */
  const FALLBACK_LABEL: Partial<Record<EdgeKind, string>> = {
    unmatched: 'no route matched it',
    poison: 'a batch nothing could process',
    exception: 'an exception answer or a refusal',
  };

  function pathFallbacks(band: Band): EdgeKind[] {
    return [
      ...new Set(band.edges.filter((e) => e.onPath && isFallback(e.kind)).map((e) => e.kind)),
    ];
  }

  let bands = $derived.by<Band[]>(() => {
    const out: Band[] = [];
    const ids = new Set<string>();
    for (const n of drawnCards) ids.add(n.group);
    // Only the path: another group's problems are off it.
    if (!(onlyPath && path)) for (const e of filteredGroupErrors) ids.add(e.group);

    for (const group of [...ids].sort((a, b) => a.localeCompare(b))) {
      const error = filteredGroupErrors.find((e) => e.group === group);
      if (error) {
        out.push({
          group,
          error,
          diagram: { positions: [], columns: [], nodeW, width: 0, height: 0 },
          edges: [],
        });
        continue;
      }
      const diagram = layout(
        drawnCards.filter((n) => n.group === group),
        nodeW,
      );
      out.push({
        group,
        diagram,
        edges: bandEdges(
          diagram,
          stacked.edges.filter((e) => groupOfKey(e.from) === group),
        ),
      });
    }
    return out;
  });

  let panelTab = $derived<FocusTab>(FOCUS_TABS.find((t) => t === stats?.panel) ?? 'overview');

  /** The charts' range the last panel showed, so the next one opens on it too. Kept in this
   * browser between visits; a link that names a range still wins, and one that names none, from
   * another page, keeps it. */
  const RANGE_KEY = 'smeltr.map.chartRange';
  let lastRange = $state(storedRange());
  function storedRange(): string {
    try {
      return chartRange(stored(RANGE_KEY) ?? undefined).range;
    } catch {
      return DEFAULT_CHART_RANGE;
    }
  }
  $effect(() => {
    if (!stats || stats.stack || stats.range === undefined) return;
    const shown = chartRange(stats.range).range;
    lastRange = shown;
    store(RANGE_KEY, shown);
  });

  function hashOf(n: MapNode, panel?: FocusTab, range: string = lastRange): string {
    return statsHash(n.group, n.kind, n.authorId, panel, range);
  }

  /** What selecting a card opens: its object's panel or its stack's. */
  function cardHash(c: MapCard): string {
    return c.members ? stackHash(c.group, c.kind, c.authorId) : hashOf(c);
  }

  /** What the panel lists as connected: the path, in drawing order, without the selection. */
  let connected = $derived(
    path && selected
      ? layout(stacked.cards.filter((c) => path.nodes.has(cardKey(c))))
          .positions.map((p) => p.node)
          .filter((c) => cardKey(c) !== selectedKey)
          .map((c) => ({ node: c, hash: cardHash(c) }))
      : [],
  );

  /** The selection's routes in its own group: the ones it is the source, pipeline or a
   * destination of. */
  let selectedRoutes = $derived.by(() => {
    if (!selected || !mapResult?.ok) return [];
    const s = selected;
    const deliveringTo = new Set(
      mapResult.delivers
        .filter((d) => d.group === s.group && d.destination === s.authorId)
        .map((d) => d.route),
    );
    return mapResult.routes.filter(
      (r) =>
        r.group === s.group &&
        ((s.kind === 'source' && r.source === s.authorId) ||
          (s.kind === 'pipeline' && r.pipeline === s.authorId) ||
          (s.kind === 'destination' && deliveringTo.has(r.route))),
    );
  });
  /** The selection's group's pipelines, by id, for the routes shown beside it. */
  let pipelineNames = $derived(
    new Map(
      (mapResult?.ok ? mapResult.nodes : [])
        .filter((n) => n.kind === 'pipeline' && n.group === selected?.group)
        .map((n) => [n.authorId, n.name]),
    ),
  );
  function pipelineName(id: string): string {
    return pipelineNames.get(id) ?? id;
  }
  let selectedDelivers = $derived.by(() => {
    if (!selected || !mapResult?.ok) return [];
    const ids = new Set(selectedRoutes.map((r) => r.route));
    const g = selected.group;
    return mapResult.delivers.filter((d) => d.group === g && ids.has(d.route));
  });

  /** How a card reads: the selection itself, another copy of a selected shared object, on the
   * selected path, or set back -- off the path, or a search's neighbor rather than its match. */
  function cardState(p: Positioned): {
    selected: boolean;
    copy: boolean;
    matched: boolean;
    dimmed: boolean;
  } {
    const key = keyOf(p);
    const isSelected = key === selectedCardKey;
    const copy = !isSelected && copies.has(key);
    const offPath = path !== null && !isSelected && !copy && !path.nodes.has(key);
    const matched = result.matched.has(key);
    return {
      selected: isSelected,
      copy,
      matched: result.active && matched,
      dimmed:
        offPath ||
        (!matched &&
          (p.node.members
            ? p.node.members.every((m) => result.dimmed.has(nodeKey(m.group, m.kind, m.authorId)))
            : result.dimmed.has(key))),
    };
  }

  /** The selection the panel last showed, so closing it can hand focus back to its card. */
  let lastSelected: string | null = null;
  $effect(() => {
    const now = stats
      ? stats.stack
        ? stackHash(stats.group, stats.kind, stats.id)
        : statsHash(stats.group, stats.kind, stats.id, undefined, lastRange)
      : null;
    const was = lastSelected;
    lastSelected = now;
    // Closing the panel took focus with it, which would drop a keyboard user at the page's top.
    if (now || !was || (document.activeElement && document.activeElement !== document.body)) return;
    const card = document.querySelector<HTMLElement>(`a.node[href="${CSS.escape(was)}"]`);
    (card ?? document.querySelector<HTMLElement>('input.search'))?.focus();
  });

  /** A click on the canvas itself, not a card, clears the selection. */
  function clearOnEmpty(e: MouseEvent) {
    if (stats && !(e.target as Element).closest('.node')) location.hash = '#/map';
  }

  /** Escape clears the selection, unless a field or the search box is taking the key. */
  function onWindowKey(e: KeyboardEvent) {
    if (e.key !== 'Escape' || !stats || e.defaultPrevented) return;
    // An open help popover takes this Escape for itself.
    if (document.querySelector(':popover-open')) return;
    if ((e.target as Element).closest('input, textarea, select, [contenteditable]')) return;
    location.hash = '#/map';
  }

  function keyOf(p: Positioned): string {
    return cardKey(p.node);
  }
</script>

<svelte:window onkeydown={onWindowKey} />

<main class="column">
  <PageHeader title="Map">
    {#snippet help()}
      <p>
        The sources, pipelines and destinations saved on the selected branch, one band per worker
        group. Select one to see its path; fallback lines show only on a selected path. Lines show
        what connects to what; every reading is on the cards and in the panel.
      </p>
      <p>
        Search takes names and terms, and combines them: <code>kind:</code>, <code>system:</code>,
        <code>shared</code>, <code>pressure:</code>, <code>pressure:near</code>,
        <code>warning:</code>, <code>pressure:held</code>, <code>unmatched:</code>,
        <code>stale:</code>,
        <code>nodata:</code>, <code>calls:</code>. Focus the box for what is worth trying now.
      </p>
      <div class="legend">
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'source', system: 'kafka' }, 16)} kafka</span
        >
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'destination', system: 'elasticsearch' }, 16)} elasticsearch</span
        >
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'destination', system: 'splunkHec' }, 16)} splunk hec</span
        >
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'destination', system: 'http' }, 16)} http</span
        >
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'destination', system: 's3' }, 16)} object store: s3, sqs/s3, gcs,
          azure blob</span
        >
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'destination', system: 'iceberg' }, 16)} iceberg</span
        >
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'destination', system: 'delta' }, 16)} delta</span
        >
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'source', system: 'metrics' }, 16)} smeltr's own metrics</span
        >
        <span class="legend-item"
          >{@html shapeGlyph({ kind: 'source', system: 'logs' }, 16)} smeltr's own logs</span
        >
        <span class="legend-item">{@html shapeGlyph({ kind: 'pipeline' }, 16)} pipeline</span>
        <span class="legend-item"><span class="swatch warn"></span> a warning names it</span>
        <span class="legend-item"><span class="swatch unmatched"></span> no route matched it</span>
        <span class="legend-item"
          ><span class="swatch poison"></span> a batch nothing could process, or an exception</span
        >
      </div>
      <p>
        A destination's card shows its busiest worker's buffer; a source's shows its Kafka lag, or
        the worker holding it and its ledger's entries. Readings refresh every bucket; the layout
        changes only when you ask.
      </p>
    {/snippet}
    <span class="dim age">
      {readAt === null ? 'No readings' : `Updated ${ago(readAt / 1000, now)}`}
    </span>
  </PageHeader>

  {#if loading}
    <p class="dim">Loading&hellip;</p>
  {:else if loadError}
    <!-- The map is its own list of what is wrong: its read is quiet. -->
    <InlineError message={loadError} />
  {:else if !mapResult}
    <InlineError message="no response" />
  {:else if !ok}
    <p class="subtitle">
      The fleet could not be read, so there is no topology to draw. Fix what is below and the map
      follows.
    </p>
    <UnreadableFiles
      problems={mapResult.errors.map((e) => e.message)}
      scope={SHARED_SCOPE}
      onchanged={() => load()}
    >
      {#snippet rest(left)}
        <ul class="problems">
          {#each left as message}
            {@const link = linkForError(message, known)}
            <li>
              {#if link}
                <a class="link" href={link.hash}>{link.label}</a>
                <span> &mdash; {message}</span>
              {:else}
                {message}
              {/if}
            </li>
          {/each}
        </ul>
      {/snippet}
    </UnreadableFiles>
  {:else}
    <div class="toolbar">
      <MapSearch bind:query items={offered} onselect={(n) => (location.hash = hashOf(n))} />
      <select bind:value={groupFilter} aria-label="filter by worker group">
        <option value="all">All groups</option>
        {#each allGroups as g (g)}
          <option value={g}>{groupLabel(g)}</option>
        {/each}
      </select>
      <span class="dim count">
        {#if result.active || (onlyPath && path)}
          {objectCount(drawnCards)} of {count(filteredNodes.length, 'object')} shown
        {:else}
          {count(filteredNodes.length, 'object')}
        {/if}
      </span>
    </div>

    {#each result.unsupported as u (u.field)}
      <p class="notice">the map cannot search by <code>{u.field}</code> &mdash; {u.note}</p>
    {/each}

    {#if topologyChanged || layoutError}
      <div class="topology-notice" role="status">
        <span>The layout is out of date</span>
        <button type="button" class="primary" disabled={updating} onclick={updateLayout}
          >Update layout</button
        >
        <InlineError message={layoutError} inline />
      </div>
    {/if}

    {#snippet selectionControls()}
      <span class="selection-controls">
        {#if path}
          <button type="button" aria-pressed={onlyPath} onclick={() => (onlyPath = !onlyPath)}>
            Show this path
          </button>
        {/if}
        {#if selectedTarget && mayCapture(selectedTarget)}
          <button type="button" data-guide="capture" onclick={() => (capturing = selectedTarget)}
            >Capture</button
          >
        {/if}
      </span>
    {/snippet}

    <div class="workspace">
      <div class="canvas">
        {#if bands.length === 0}
          <p class="dim empty">
            {result.active ? 'Nothing matches.' : 'Nothing is routed on this branch yet.'}
          </p>
        {:else}
          {#each bands as band, bandIndex (band.group)}
            {@const open = isOpen(band.group)}
            {@const summary = summaries.get(band.group)}
            <section class="band" aria-labelledby="{uid}-band-{bandIndex}">
              <div class="band-head">
                <button
                  type="button"
                  class="fold"
                  aria-expanded={open}
                  aria-label={groupLabel(band.group)}
                  onclick={() => (folded = { ...folded, [band.group]: open })}
                  >{open ? '▾' : '▸'}</button
                >
                <h2 class="band-title" id="{uid}-band-{bandIndex}">{groupLabel(band.group)}</h2>
                {#if summary?.first}
                  <a
                    class="summary {summary.level}"
                    href={attentionHash(summary.first)}
                    onclick={() => (query = '')}>{summary.text}</a
                  >
                {:else if summary}
                  <span class="summary {summary.level}">{summary.text}</span>
                {/if}
                {#each pathFallbacks(band) as kind (kind)}
                  <span class="legend-item path-legend"
                    ><span class="swatch {kind === 'unmatched' ? 'unmatched' : 'poison'}"></span>
                    {FALLBACK_LABEL[kind]}</span
                  >
                {/each}
              </div>
              {#if !open}
                <!-- Folded: the header's summary says what is in it. -->
              {:else if band.error}
                <UnreadableFiles
                  problems={band.error.errors}
                  scope={band.group}
                  onchanged={() => load()}
                >
                  {#snippet rest(left)}
                    <ul class="problems">
                      {#each left as message}
                        <li>{message}</li>
                      {/each}
                    </ul>
                  {/snippet}
                </UnreadableFiles>
              {:else}
                <div class="diagram-scroll" bind:clientWidth={diagramRoom}>
                  <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
                  <!-- Escape clears from the keyboard (`onWindowKey`). -->
                  <div
                    class="diagram"
                    style="width: {band.diagram.width}px; height: {band.diagram.height}px"
                    onclick={clearOnEmpty}
                  >
                    {#each band.diagram.columns as c (c.kind)}
                      <span
                        class="colhead mono"
                        style="left: {c.x}px; top: {PAD}px; width: {band.diagram
                          .nodeW}px; height: {HEAD_H}px"
                        ><span>{c.kind}</span><span>{c.count}</span></span
                      >
                    {/each}
                    <!-- Two layers: the selected path draws over every other line. -->
                    {#each [false, true] as front (front)}
                      <svg class="edges" width={band.diagram.width} height={band.diagram.height}>
                        <defs>
                          {#each ARROWS[front ? 'front' : 'back'] as [name, color] (name)}
                            <marker
                              id={arrowId(bandIndex, front, name)}
                              viewBox="0 0 10 10"
                              refX="9"
                              refY="5"
                              markerWidth={front ? 4 : 6}
                              markerHeight={front ? 4 : 6}
                              orient="auto"
                            >
                              <path d="M0 0 L10 5 L0 10 Z" fill={color} />
                            </marker>
                          {/each}
                        </defs>
                        {#each band.edges.filter((e) => e.onPath === front) as e (e.key)}
                          {@const arrow = front && !isFallback(e.kind) ? 'path' : e.kind}
                          {#if front}
                            <path d={e.d} fill="none" stroke="var(--panel)" stroke-width="6" />
                          {/if}
                          <path
                            d={e.d}
                            fill="none"
                            stroke={front && !isFallback(e.kind)
                              ? 'var(--ink)'
                              : EDGE_STROKE[e.kind]}
                            stroke-opacity={e.dimmed ? 0.2 : front ? 1 : 0.75}
                            stroke-width={front ? 2.5 : 1.25}
                            stroke-dasharray={isFallback(e.kind) ? '5 5' : undefined}
                            stroke-linecap="round"
                            marker-end="url(#{arrowId(bandIndex, front, arrow)})"
                          />
                        {/each}
                      </svg>
                    {/each}
                    {#each band.diagram.positions as p (keyOf(p))}
                      {@const state = cardState(p)}
                      {@const line = cardLine(p.node, cardReading(p.node))}
                      <!-- The whole card is the link: the click a plain card gives you is its own
                       biggest target. Editing lives one step further, from the panel this opens
                       (`FocusView.svelte`), so there is no second control here. -->
                      <a
                        class="node clickable"
                        class:terminal={p.node.kind !== 'pipeline'}
                        class:warn={warned(p.node)}
                        class:selected={state.selected}
                        class:copy={state.copy}
                        class:matched={state.matched}
                        class:dimmed={state.dimmed}
                        class:stack={!!p.node.members}
                        href={cardHash(p.node)}
                        aria-current={state.selected ? 'true' : undefined}
                        title={p.node.name}
                        style="left: {p.x}px; top: {p.y}px; width: {band.diagram
                          .nodeW}px; height: {NODE_H}px"
                      >
                        <span class="node-head">
                          <span class="node-shape">{@html shapeGlyph(p.node, 14)}</span>
                          <span class="node-label">{p.node.name}</span>
                        </span>
                        <span class="node-meta">
                          {cardMeta(p.node)}
                          {#if p.node.shared}<span class="tag">shared</span>{/if}
                        </span>
                        {#if line}
                          <span class="node-line">
                            <span>{line.rate ?? ''}</span>
                            {#if line.note}<span class="note {line.level}">{line.note}</span>{/if}
                          </span>
                        {/if}
                        {#if line?.fill !== undefined}
                          <span class="bar {line.level}" aria-hidden="true"
                            ><span style="width: {Math.min(100, line.fill * 100)}%"></span></span
                          >
                        {/if}
                      </a>
                    {/each}
                  </div>
                </div>
              {/if}
            </section>
          {/each}
        {/if}
      </div>
      {#if selected}
        <!-- Keyed on the node's identity: a page of workers, or an answer still loading, is the
           old node's and must not carry over to the next one. -->
        {#key selectedKey}
          <div class="side">
            <FocusView
              node={selected}
              tab={panelTab}
              reading={selectedKey ? readings.get(selectedKey) : undefined}
              nowMillis={measurements?.nowMillis}
              alsoIn={nodes
                .filter((n) => copies.has(nodeKey(n.group, n.kind, n.authorId)) && n !== selected)
                .map((n) => ({ group: n.group, hash: hashOf(n) }))}
              {connected}
              routes={selectedRoutes}
              delivers={selectedDelivers}
              {pipelineName}
              tabHash={(t) => hashOf(selected!, t)}
              range={stats?.range ?? lastRange}
              rangeHash={(r) => hashOf(selected!, panelTab, r)}
              editHash={hashFor(selected)}
              onedit={selected.shared ? () => setScope(SHARED_SCOPE) : undefined}
              onclose={() => (location.hash = '#/map')}
              footer={path || selected.kind === 'destination' ? selectionControls : undefined}
              gap={gapOf(selected)}
              onroute={canEdit(selected.shared ? SHARED_SCOPE : selected.group)
                ? (condition) => routeFrom(selected!, condition)
                : undefined}
            />
          </div>
        {/key}
      {:else if selectedStack}
        {#key selectedCardKey}
          <div class="side">
            <StackView
              stack={selectedStack}
              system={cardMeta(selectedStack)}
              rows={selectedStack.members!.map((m) => {
                const line = cardLine(m, readingOf(m));
                return {
                  key: nodeKey(m.group, m.kind, m.authorId),
                  href: hashOf(m),
                  subject: m.name,
                  note: line?.note,
                  level: line?.level,
                  under: line?.rate,
                };
              })}
              onclose={() => (location.hash = '#/map')}
              footer={path ? selectionControls : undefined}
            />
          </div>
        {/key}
      {:else}
        <aside class="side needs" aria-label="Needs attention">
          <h2>Needs attention</h2>
          {#if !measurements}
            <p class="dim">No readings yet.</p>
          {:else if needs.length === 0}
            <p class="dim">Nothing.</p>
          {:else}
            <MapRows
              rows={needs.map((a) => ({
                key: `${a.group}|${a.node ? nodeKey(a.group, a.node.kind, a.node.authorId) : a.subject}|${a.text}|${a.site ?? ''}`,
                href: attentionHash(a),
                subject: a.subject,
                note: a.text,
                level: a.rank === 0 ? 'alarm' : a.rank < 3 ? 'warn' : 'dim',
                under: groupLabel(a.group),
              }))}
              onpick={() => (query = '')}
            />
          {/if}
        </aside>
      {/if}
    </div>
  {/if}
  {#if capturing}
    <CaptureFlow target={capturing} oncancel={() => (capturing = null)} />
  {/if}
</main>

<style>
  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }

  .subtitle {
    margin: 0 0 1rem;
    font-size: var(--text-body);
  }

  .problems {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }

  /* A refusal the server makes -- never smaller than running prose. */
  .problems li {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.5rem 0.7rem;
    font-size: var(--text-body);
  }

  .toolbar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.6rem;
    margin: 1rem 0 0.6rem;
  }

  .selection-controls {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .count {
    font-size: var(--text-label);
    white-space: nowrap;
  }

  .notice {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--bg);
    color: var(--dim);
    font-size: var(--text-label);
    padding: 0.4rem 0.7rem;
    margin: 0 0 0.5rem;
  }

  .notice code {
    font-family: var(--mono);
  }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    margin: 0.6rem 0 1rem;
    font-size: var(--text-note);
    color: var(--dim);
  }

  .legend-item {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
  }

  .legend :global(svg) {
    color: var(--dim);
    flex-shrink: 0;
  }

  .swatch {
    display: inline-block;
    width: 14px;
  }

  .swatch.warn {
    width: 8px;
    height: 8px;
    border-radius: 999px;
    border: 2px solid var(--error);
  }

  /* A fallback line is dashed, wherever it is drawn. */
  .swatch.unmatched,
  .swatch.poison {
    border-top: 2px dashed var(--warn);
    height: 0;
  }

  .swatch.poison {
    border-top-color: var(--error);
  }

  .empty {
    padding: 1.5rem 0;
    margin: 0;
  }

  .workspace {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 21.25rem;
    gap: 1.4rem;
    align-items: start;
  }

  .side {
    position: sticky;
    top: 1rem;
    max-height: calc(100vh - 2rem);
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }

  /* Under this width the panel beside the map would narrow the cards to their floor and cut the
     destination column off (`cardWidth`): the panel moves out of its way. */
  @media (max-width: 78rem) {
    .workspace {
      grid-template-columns: minmax(0, 1fr);
    }

    .side {
      position: static;
      max-height: none;
    }

    /* What needs attention is read before the map; a selection's details are read after it. */
    .side.needs {
      order: -1;
    }
  }

  /* A group is a card: its header a strip across the top, its diagram below. */
  .band {
    margin: 0 0 1rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
    overflow: hidden;
  }

  .band-head {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.6rem;
    padding: 0.5rem 0.75rem;
    background: var(--panel);
  }

  .band-title {
    margin: 0;
    font-family: var(--mono);
    font-size: var(--text-body);
    font-weight: 400;
  }

  /* A stack is several cards, one behind the other. */
  .node.stack {
    --behind:
      4px 4px 0 -1px var(--bg), 4px 4px 0 0 var(--border), 8px 8px 0 -1px var(--bg),
      8px 8px 0 0 var(--border);
    box-shadow: var(--behind);
  }

  .fold {
    border: 0;
    background: none;
    padding: 0 0.2rem;
    color: var(--dim);
    cursor: pointer;
  }

  .summary {
    font-size: var(--text-note);
    text-decoration: none;
  }

  .path-legend {
    font-size: var(--text-note);
    color: var(--dim);
  }

  .note.alarm,
  .summary.alarm {
    color: var(--error);
  }

  .note.warn,
  .summary.warn {
    color: var(--warn);
  }

  .summary.ok,
  .summary.dim {
    color: var(--dim);
  }

  .age {
    font-size: var(--text-note);
  }

  .topology-notice {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin: 0 0 1rem;
    padding: 0.5rem 0.75rem;
    border-left: 3px solid var(--warn);
    background: color-mix(in srgb, var(--warn) 10%, var(--bg));
    font-size: var(--text-note);
  }

  .needs {
    padding: 1rem 1.05rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
  }

  .needs h2 {
    font-size: var(--text-section);
    margin: 0 0 0.75rem;
  }

  .needs p {
    margin: 0;
    font-size: var(--text-note);
  }

  .diagram-scroll {
    overflow-x: auto;
  }

  .diagram {
    position: relative;
  }

  .edges {
    position: absolute;
    inset: 0;
    pointer-events: none;
  }

  /* A column's kind, named once at its head. Chrome, so the body face overrides `.mono`. */
  .colhead {
    position: absolute;
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    box-sizing: border-box;
    border-bottom: 1px solid var(--border);
    color: var(--dim);
    font-family: var(--font-body);
    font-size: var(--text-micro);
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }

  .node {
    position: absolute;
    display: flex;
    flex-direction: column;
    box-sizing: border-box;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.55rem 0.75rem;
    color: var(--ink);
    text-decoration: none;
    overflow: hidden;
  }

  /* The flowchart's convention -- a terminal rounded, a process a block -- so the kind reads
     from the outline at any zoom. */
  .node.terminal {
    border-radius: 20px;
    padding-inline: 1rem;
  }

  .node:hover {
    border-color: var(--accent);
  }

  .node.clickable {
    cursor: pointer;
  }

  .node.matched {
    border-color: var(--accent);
    background: color-mix(in srgb, var(--accent) 8%, var(--bg));
  }

  .node.dimmed {
    opacity: 0.4;
  }

  /* A shadow rather than a wider border, so the card's content does not move when selected. */
  .node.selected {
    border-color: var(--ink);
    box-shadow: inset 0 0 0 1px var(--ink);
  }

  .node.stack.selected {
    box-shadow:
      inset 0 0 0 1px var(--ink),
      var(--behind);
  }

  /* Another running copy of a selected shared object: lit with it, so it reads as one thing. */
  .node.copy {
    border: 1px dashed var(--ink);
  }

  .node.clickable:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: -2px;
  }

  .node.warn {
    border-color: var(--error);
  }

  .node-shape {
    display: flex;
    flex-shrink: 0;
    color: var(--dim);
  }

  .node.warn .node-shape {
    color: var(--error);
  }

  .node-head {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    min-width: 0;
  }

  .node-meta {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    margin-top: 2px;
    color: var(--dim);
    font-size: var(--text-micro);
    white-space: nowrap;
  }

  .node-line {
    display: flex;
    justify-content: space-between;
    gap: 0.5rem;
    margin-top: 5px;
    font-size: var(--text-micro);
    white-space: nowrap;
  }

  .note.dim {
    color: var(--dim);
  }

  /* The busiest worker's buffer, along the card's foot. A figure can pass 100%; only the bar
     stops there. */
  .bar {
    margin-top: 6px;
    flex-shrink: 0;
    height: 4px;
    border-radius: 2px;
    background: color-mix(in srgb, var(--dim) 25%, transparent);
    overflow: hidden;
  }

  .bar > span {
    display: block;
    height: 100%;
    background: var(--dim);
  }

  .bar.warn > span {
    background: var(--warn);
  }

  .bar.alarm > span {
    background: var(--error);
  }

  .node-label {
    font-family: var(--mono);
    font-size: var(--text-note);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .node.warn .node-label {
    color: var(--error);
  }

  .tag {
    display: inline-flex;
    align-items: center;
    padding: 0 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    font-family: var(--font-body);
    font-size: var(--text-micro);
    color: var(--dim);
    white-space: nowrap;
  }

  .node.selected .tag,
  .node.copy .tag {
    border-color: var(--ink);
    color: var(--ink);
  }
</style>
