<!--
  The records a pipeline's preview made, beside its steps: the sample's rows as a grid, each
  opening on its record, with the find box and the Fields menu over them. The editor runs the
  preview and owns the pause; this pane only shows what came back, and asks the editor for an
  expression's rows (`preview`).

  The editor's own controls sit in it: `sampleMenu` beside the find box, `stage` (the failed
  stage and the pause) over the grid, and `head` above everything.
-->
<script module lang="ts">
  /** A chip in the find box: the rows it keeps. */
  export type FindChip =
    | { kind: 'failing' | 'dropped' | 'changed' }
    | { kind: 'has'; path: string }
    | { kind: 'droppedBy' | 'failedAt'; step: string };

  /** A pause's two runs, each side of its step. */
  export type PauseRun = {
    before: PreviewRow[];
    after: PreviewRow[];
    declared: { before: Declared | null; after: Declared | null };
  };
</script>

<script lang="ts">
  import { untrack, type Snippet } from 'svelte';
  import { SvelteMap, SvelteSet } from 'svelte/reactivity';
  import { store, storedJson } from './stored';
  import { stepName, type Step } from './pipeline';
  import RecordFields from './RecordFields.svelte';
  import FindBox, { type FindFilter } from './FindBox.svelte';
  import RemovableChip from './RemovableChip.svelte';
  import FieldsMenu from './FieldsMenu.svelte';
  import ChipPicker from './ChipPicker.svelte';
  import GearIcon from './GearIcon.svelte';
  import Toggle from './Toggle.svelte';
  import SortButton from './SortButton.svelte';
  import FoldAllButton from './FoldAllButton.svelte';
  import { TableSort } from './sort.svelte';
  import { formatDiffValue } from './diff';
  import InlineError from './InlineError.svelte';
  import {
    countStates,
    fieldAt,
    leafMap,
    pathsOf,
    splitPath,
    typeAt,
    type Declared,
  } from './recordDiff';
  import {
    isInternal,
    sampleSource,
    type PreviewResult,
    type PreviewRow,
    type SampleRecordDto,
  } from './api';

  let {
    pipelineId,
    steps,
    signature,
    sampleRows,
    selectedSample,
    lastSample,
    result,
    lastGoodResult,
    gridRows,
    attribution,
    stepNumbers,
    pause,
    pauseRun,
    declared,
    canLock,
    onlock,
    onjump,
    preview,
    head,
    sampleMenu,
    stage,
  }: {
    /** What the field choice and the recent expressions are remembered under. */
    pipelineId: string;
    steps: Step[];
    /** Changes whenever the steps a preview runs do, so an expression is run again. */
    signature: string;
    sampleRows: SampleRecordDto[];
    selectedSample: string;
    /** The sample the rows on screen came from. */
    lastSample: string;
    result: PreviewResult | null;
    lastGoodResult: PreviewResult | null;
    /** The rows the grid shows: the pause's, once run, or else the whole pipeline's. */
    gridRows: PreviewRow[] | null;
    /** Each row's attribution, by row. */
    attribution: Map<number, PreviewRow> | null;
    /** Each step by id, with its number in the step list. */
    stepNumbers: Map<string, { step: Step; number: number }>;
    /** Where the records stop: the pipeline up to and including that step. */
    pause: { after: Step[] } | null;
    pauseRun: PauseRun | null;
    /** The whole pipeline's types, each side. */
    declared: { before: Declared | null; after: Declared | null };
    canLock: boolean;
    onlock: () => void;
    onjump: (step: Step) => void;
    /** The rows `filter` keeps of `sample` run through `steps`. */
    preview: (steps: Step[], sample: string, filter: string) => Promise<PreviewResult>;
    head?: Snippet;
    sampleMenu: Snippet;
    stage: Snippet;
  } = $props();

  // How a record is drawn, the reader's own, kept in this browser for every pipeline. Only what
  // changed: a mapping step rewrites most of a record, so the filter is what makes a later step's
  // handful of changes findable without reading the whole thing.
  const VIEW_KEY = 'smeltr-records-view';
  type View = { changedOnly: boolean; fullPaths: boolean; showTypes: boolean };
  const VIEW: View = { changedOnly: false, fullPaths: false, showTypes: true };
  const savedView = storedJson<Record<string, unknown>>(VIEW_KEY, {});
  let view = $state<View>({ ...VIEW });
  for (const k of Object.keys(VIEW) as (keyof View)[]) {
    const saved = savedView?.[k];
    if (typeof saved === 'boolean') view[k] = saved;
  }
  $effect(() => store(VIEW_KEY, JSON.stringify(view)));
  let viewOpen = $state(false);

  // What the record's source set is shown on every row, read or not, so an
  // author sees what there is to read. It is never delivered, so the
  // pipeline's output does not carry it.
  function withSource(record: unknown, row: number): unknown {
    const source = sampleRows[row] && sampleSource(sampleRows[row]);
    if (!source || record === null || typeof record !== 'object') return record;
    return { __source: source, ...record };
  }

  /** The steps that touched each field of `row`, in the order they ran. A
   * list is compared whole, so a field inside one takes the list's. */
  function touchedOn(row: number) {
    const touched = attribution?.get(row)?.steps;
    return (path: string) => {
      let at = path;
      while (touched && !(at in touched)) {
        const up = at.replace(/(\.[^.[]*|\[[^\]]*\])$/, '');
        if (up === at || up === '') break;
        at = up;
      }
      return (touched?.[at] ?? []).flatMap((id) => {
        const t = stepNumbers.get(id);
        return t ? [t] : [];
      });
    };
  }

  // The pause's runs by row, so a row finds its sides without a search.
  let pausedByRow = $derived.by(() => {
    const d = pause ? pauseRun : null;
    if (!d) return null;
    const byRow = (rows: PreviewRow[]) => new Map(rows.map((r) => [r.row, r]));
    return { before: byRow(d.before), after: byRow(d.after) };
  });

  /** The types each side of the step shown: the pause's, or the whole pipeline's. */
  let shownDeclared = $derived(pause && pauseRun ? pauseRun.declared : declared);

  /** The record on either side of the step shown: the selected step, or the
   * whole pipeline from its sample row. */
  function rowSides(row: PreviewRow): {
    before: unknown;
    after: unknown;
    declared: { before: Declared | null; after: Declared | null };
  } {
    if (pausedByRow) {
      return {
        before: withSource(pausedByRow.before.get(row.row)?.json ?? null, row.row),
        after: withSource(pausedByRow.after.get(row.row)?.json ?? null, row.row),
        declared: shownDeclared,
      };
    }
    return {
      before: withSource(sampleRows[row.row] ?? {}, row.row),
      after: withSource(row.json, row.row),
      declared: shownDeclared,
    };
  }

  /** Each column's type, from the side the grid's cells show: after the step, or before where
   * the step removed the field. The head shows it as a declaration does, `status: i64`, so
   * a field nothing declares shows none. */
  let columnTypes = $derived.by(() => {
    return new Map(
      columns.map((path) => {
        const after = typeAt(shownDeclared.after, path);
        const type = after === 'undeclared' ? typeAt(shownDeclared.before, path) : after;
        return [path, type === 'undeclared' ? null : type];
      }),
    );
  });

  // Each row's leaves, both sides, by path: what a cell, the search and the
  // sort read, without diffing a row nobody is looking at. The sample's side
  // is read once per sample rather than once per run.
  let sampleMaps = $derived(sampleRows.map((r, i) => leafMap(withSource(r, i))));
  type RowMaps = { before: Map<string, unknown>; after: Map<string, unknown> };
  let rowsMaps: RowMaps[] = $derived(
    (gridRows ?? []).map((row) => {
      if (pausedByRow) {
        const sides = rowSides(row);
        return { before: leafMap(sides.before), after: leafMap(sides.after) };
      }
      return {
        before: sampleMaps[row.row] ?? leafMap(withSource({}, row.row)),
        after: leafMap(withSource(row.json, row.row)),
      };
    }),
  );
  let allColumns = $derived(pathsOf(rowsMaps.flatMap((m) => [m.before, m.after])));

  /** What a cell shows: what the field became, or what it was if the step took it away. */
  function cellValue(m: RowMaps, path: string): unknown {
    return m.after.has(path) ? m.after.get(path) : m.before.get(path);
  }

  // How many records hold each field, after the step shown.
  let fieldCounts = $derived.by(() => {
    const counts = new Map<string, number>();
    for (const m of rowsMaps)
      for (const p of m.after.keys()) counts.set(p, (counts.get(p) ?? 0) + 1);
    return counts;
  });

  /** The record the steps write, from the last run that compiled: with no sample, a run that
   * read no records. */
  let dictionary = $derived(
    (result?.ok ? result.dictionary : undefined) ?? lastGoodResult?.dictionary,
  );
  let declaredTypes = $derived(new Map(dictionary?.compiled.fields ?? []));

  /** Every field the records hold, and every field the pipeline declares that none of them
   * do, which the Fields menu lists as held by none. A declared struct a record fills is its
   * fields, not one of its own. */
  let menuFields = $derived.by(() => {
    const paths = new Set(allColumns);
    for (const [path] of dictionary?.compiled.fields ?? []) {
      if (path.includes('*')) continue;
      if (allColumns.some((c) => c.startsWith(path + '.') || c.startsWith(path + '['))) continue;
      paths.add(path);
    }
    return [...paths].sort().map((path) => ({
      path,
      type: declaredTypes.get(path) ?? null,
      count: fieldCounts.get(path) ?? 0,
    }));
  });

  // What the reader chose for each field they touched, and the default for the
  // rest: a step added along the way brings its fields with it, instead of
  // arriving invisible behind a choice made before they existed. The default is
  // the fields most records hold, up to a grid's worth, so it moves with the
  // step and sample shown; a choice does not. A field under a `__`
  // key is internal, smeltr's own and never delivered, so it starts hidden,
  // and an unfolded row shows it only while its column is shown.
  const DEFAULT_FIELDS = 50;
  const startsHidden = isInternal;
  let shownByDefault = $derived(
    new Set(
      menuFields
        .filter((f) => !startsHidden(f.path))
        .sort((x, y) => y.count - x.count || (x.path < y.path ? -1 : 1))
        .slice(0, DEFAULT_FIELDS)
        .map((f) => f.path),
    ),
  );
  let chosenFields = $state(new SvelteMap<string, boolean>());
  const isShown = (path: string) => chosenFields.get(path) ?? shownByDefault.has(path);
  let columns = $derived(allColumns.filter(isShown));

  const unfoldedPath = (path: string) => !startsHidden(path) || isShown(path);

  function showFields(paths: string[], show: boolean) {
    for (const path of paths) chosenFields.set(path, show);
  }

  // The field choice is the reader's own, remembered per pipeline in this browser.
  const fieldsKey = (pipeline: string) => `smeltr-workspace-fields:${pipeline}`;
  $effect(() => {
    const key = fieldsKey(pipelineId);
    untrack(() => {
      chosenFields.clear();
      const saved = storedJson<unknown>(key, {});
      if (saved && typeof saved === 'object' && !Array.isArray(saved))
        for (const [path, shown] of Object.entries(saved))
          if (typeof shown === 'boolean') chosenFields.set(path, shown);
    });
  });
  $effect(() => {
    const saved = JSON.stringify(Object.fromEntries(chosenFields));
    store(
      untrack(() => fieldsKey(pipelineId)),
      saved === '{}' ? null : saved,
    );
  });

  // A record is a line until asked for, and any number can be open at once:
  // comparing two is reading down the page rather than remembering the last.
  let expanded = $state(new SvelteSet<number>());

  function toggleRow(row: number) {
    if (expanded.has(row)) expanded.delete(row);
    else expanded.add(row);
  }

  // Every current row, rather than a count: `expanded` keeps the rows an
  // author opened across a re-preview, so after a smaller sample its size can
  // exceed the rows on screen and a count would leave `Fold all` unreachable.
  let allOpen = $derived(
    gridRows !== null && gridRows.length > 0 && gridRows.every((r) => expanded.has(r.row)),
  );

  function toggleAll() {
    if (allOpen) expanded.clear();
    else for (const row of gridRows ?? []) expanded.add(row.row);
  }

  // A sample opens on a record rather than a wall of lines: the first that
  // failed, which is what needs reading, or else the first. Once per sample,
  // so a re-run as the steps are typed leaves the reader's choice alone.
  let openedFor = '';
  $effect(() => {
    const rows = gridRows;
    const sample = lastSample;
    if (!rows || !sample || sample === openedFor) return;
    openedFor = sample;
    const first = rows.find((r) => r.exceptions.length > 0) ?? rows[0];
    if (first) untrack(() => expanded.add(first.row));
  });

  /** What the step did to a row's shown fields, for the head of its record. */
  function rowMarks(m: RowMaps) {
    return countStates(
      pathsOf([m.before, m.after])
        .filter(unfoldedPath)
        .map((path) => fieldAt(m.before, m.after, path)!),
    );
  }

  // -- the window -------------------------------------------------------------
  //
  // A thousand rows of five hundred cells is more than a page draws and stays
  // responsive, so the grid draws the rows near the viewport and a spacer
  // stands for the rest. A folded row is one line, as tall as the first one
  // drawn; an opened row's record is measured once it is drawn.

  const OVERSCAN = 20;
  let tbody = $state<HTMLElement | undefined>();
  let rowHeight = $state(24);
  let openHeights: Record<number, number> = $state({});
  /** How wide the grid shows, which an opened record wraps to however wide its columns run. */
  let paneWidth = $state(0);
  /** The viewport, from the top of the grid's body. */
  let viewport = $state({ top: 0, bottom: 1080 });

  function measure() {
    if (!tbody) return;
    const top = tbody.getBoundingClientRect().top;
    // What shows of the grid: its own box where it scrolls itself, the window where the page does.
    const box = tbody.closest('.grid-wrap')!.getBoundingClientRect();
    viewport = {
      top: Math.max(0, box.top) - top,
      bottom: Math.min(window.innerHeight, box.bottom) - top,
    };
    const first = tbody.querySelector<HTMLElement>('tr.grid-row');
    if (first && first.offsetHeight > 0) rowHeight = first.offsetHeight;
  }

  $effect(() => {
    // Again whenever the rows change, since what sits above the grid may have too.
    void gridOrder;
    if (!tbody) return;
    measure();
    // Captured, so the grid hears whichever ancestor it is that scrolls.
    window.addEventListener('scroll', measure, { capture: true, passive: true });
    window.addEventListener('resize', measure);
    return () => {
      window.removeEventListener('scroll', measure, { capture: true });
      window.removeEventListener('resize', measure);
    };
  });

  // The records pane ends at the bottom of the window, wherever it starts, so only the steps make the
  // page taller than the window. What the page holds below the pane is left room for too.
  let rightCol = $state<HTMLElement | undefined>();
  let paneTop = $state(0);
  let paneAfter = $state(0);
  $effect(() => {
    const pane = rightCol;
    if (!pane) return;
    const place = () => {
      const top = pane.getBoundingClientRect().top;
      const columns = pane.parentElement!.getBoundingClientRect().bottom;
      paneTop = Math.max(0, top);
      // The page ends where its content does, short of the window or past it.
      paneAfter = Math.max(0, document.documentElement.getBoundingClientRect().bottom - columns);
    };
    place();
    // And as the page settles: what loads above the pane moves it down.
    const settled = new ResizeObserver(place);
    settled.observe(document.body);
    settled.observe(pane);
    window.addEventListener('scroll', place, { capture: true, passive: true });
    window.addEventListener('resize', place);
    return () => {
      settled.disconnect();
      window.removeEventListener('scroll', place, { capture: true });
      window.removeEventListener('resize', place);
    };
  });

  let drawn = $derived.by(() => {
    const rows = gridRows ?? [];
    const n = gridOrder.length;
    const tops = new Array<number>(n + 1);
    tops[0] = 0;
    for (let k = 0; k < n; k++) {
      const row = rows[gridOrder[k]].row;
      tops[k + 1] = tops[k] + rowHeight + (expanded.has(row) ? (openHeights[row] ?? 0) : 0);
    }
    let first = 0;
    while (first < n && tops[first + 1] <= viewport.top) first++;
    let last = first;
    while (last < n && tops[last] < viewport.bottom) last++;
    const start = Math.max(0, first - OVERSCAN);
    const end = Math.min(n, last + OVERSCAN);
    return { start, end, above: tops[start], below: tops[n] - tops[end] };
  });

  // Widths fixed per column, from its head and the values it holds, so a column
  // does not jump as rows scroll past. Read off the first rows of the run rather
  // than all of them: enough to size by, and the same whichever are drawn.
  const COLUMN_CH = 28;
  const COLUMN_MIN_CH = 6;
  const SIZED_ROWS = 200;
  let columnWidths = $derived.by(() => {
    const sized = rowsMaps.slice(0, SIZED_ROWS);
    return columns.map((path) => {
      const type = view.showTypes ? columnTypes.get(path) : null;
      // The name, `: ` and its type, and the sort arrow.
      let w = Math.max(COLUMN_MIN_CH, path.length + (type ? type.length + 2 : 0) + 2);
      for (const m of sized) {
        if (w >= COLUMN_CH) break;
        if (m.after.has(path) || m.before.has(path))
          w = Math.max(w, formatDiffValue(cellValue(m, path)).length);
      }
      return Math.min(w, COLUMN_CH);
    });
  });
  /** The row number, with room for a failing row's mark. */
  let indexWidth = $derived(String(gridRows?.length ?? 0).length + 2);
  let gridWidth = $derived(
    `max(100%, calc(${columnWidths.reduce((a, b) => a + b, 2 + indexWidth)}ch + ${columns.length + 2}rem))`,
  );

  // Keyed apart from the field paths: a record may carry a field named `row`, but
  // no field has an empty path.
  const ROW_KEY = '';
  const gridSort = new TableSort<string>();
  // -- the find box ----------------------------------------------------------
  //
  // A keyword keeps a row when one of its shown cells holds the text; an
  // expression runs over the pipeline's output (`FindBox.svelte`). Chips
  // beside the text keep the rows that are failing, dropped, changed by a step
  // or hold a field, each on top of the text and of each other.

  let chips = $state<FindChip[]>([]);
  const chipKey = (c: FindChip) =>
    c.kind === 'has'
      ? `has ${c.path}`
      : c.kind === 'droppedBy'
        ? `dropped by ${c.step}`
        : c.kind === 'failedAt'
          ? `failed at ${c.step}`
          : c.kind;
  function chipLabel(c: FindChip): string {
    if (c.kind === 'has') return `has ${c.path}`;
    if (c.kind === 'droppedBy' || c.kind === 'failedAt') {
      const number = stepNumbers.get(c.step)?.number ?? '?';
      return c.kind === 'droppedBy' ? `⌫ Dropped by step ${number}` : `⚠ Failed at step ${number}`;
    }
    return CHIP_LABELS[c.kind];
  }

  const hasChip = (c: FindChip) => chips.some((x) => chipKey(x) === chipKey(c));

  /** Keeps the rows `c` keeps, on top of what the find box already keeps. */
  export function addChip(c: FindChip) {
    if (!hasChip(c)) chips = [...chips, c];
  }

  /** Empties the find box, for records it was not typed over. */
  export function clearFind() {
    outputQuery = '';
    chips = [];
  }

  function toggleChip(c: FindChip) {
    if (hasChip(c)) chips = chips.filter((x) => chipKey(x) !== chipKey(c));
    else chips = [...chips, c];
  }

  // Whether a step changed a row, kept per run: it diffs every field of the
  // row, so it is asked only of rows a chip or a count needs.
  let changedRows = new Map<number, boolean>();
  let changedFor: typeof rowsMaps | undefined;
  function rowChanged(i: number): boolean {
    if (changedFor !== rowsMaps) {
      changedRows = new Map();
      changedFor = rowsMaps;
    }
    let known = changedRows.get(i);
    if (known === undefined) {
      const m = rowsMaps[i];
      known = pathsOf([m.before, m.after]).some(
        (p) => fieldAt(m.before, m.after, p)!.state !== 'unchanged',
      );
      changedRows.set(i, known);
    }
    return known;
  }

  function chipKeeps(c: FindChip, i: number): boolean {
    if (c.kind === 'has') return rowsMaps[i].after.has(c.path);
    const row = gridRows![i];
    if (c.kind === 'failing') return row.exceptions.length > 0;
    if (c.kind === 'dropped') return row.json === null;
    if (c.kind === 'droppedBy') return attribution?.get(row.row)?.droppedBy === c.step;
    if (c.kind === 'failedAt') return row.exceptions.some((e) => e.step === c.step);
    return rowChanged(i);
  }

  let failingCount = $derived((gridRows ?? []).filter((r) => r.exceptions.length > 0).length);

  const CHIP_LABELS = {
    failing: '⚠ Failing',
    dropped: '⌫ Dropped',
    changed: '~ Changed by a step',
  } as const;

  // What the find box offers: the kinds of row a chip keeps, with how many there are, narrowed to what is typed.
  type FindItem = { kind: keyof typeof CHIP_LABELS; count: number; section?: undefined };
  let findItems = $derived.by((): FindItem[] => {
    const rows = gridRows ?? [];
    const q = outputQuery.trim().toLowerCase();
    const counts = {
      failing: () => failingCount,
      dropped: () => rows.filter((r) => r.json === null).length,
      changed: () => rows.filter((_, i) => rowChanged(i)).length,
    };
    return (Object.keys(CHIP_LABELS) as (keyof typeof CHIP_LABELS)[])
      .filter((kind) => !chips.some((c) => c.kind === kind))
      .filter((kind) => CHIP_LABELS[kind].toLowerCase().includes(q))
      .map((kind) => ({ kind, count: counts[kind]() }))
      .filter((k) => k.count > 0);
  });

  function pickFind(item: FindItem): boolean {
    addChip({ kind: item.kind });
    return true;
  }

  let outputQuery = $state('');
  let find = $state<FindFilter>({ kind: 'none' });
  let findError = $state('');
  // Paused, the expression reads the records where they stand.
  let evaluate = $derived.by(() => {
    const [sample, cut] = [lastSample, pause?.after];
    void signature;
    return sample
      ? (expression: string) => preview(cut ?? untrack(() => steps), sample, expression)
      : null;
  });

  function matches(i: number): boolean {
    const row = gridRows?.[i];
    if (!row) return false;
    if (!chips.every((c) => chipKeeps(c, i))) return false;
    if (find.kind === 'expression') return find.rows.has(row.row);
    if (find.kind !== 'keyword') return true;
    const q = find.text.toLowerCase();
    const m = rowsMaps[i];
    return columns.some(
      (path) =>
        (m.after.has(path) || m.before.has(path)) &&
        formatDiffValue(cellValue(m, path)).toLowerCase().includes(q),
    );
  }

  // Indices, not rows: `rowsMaps` is position-parallel to the run's rows.
  let gridOrder = $derived.by(() => {
    const rows = gridRows ?? [];
    const by: Record<string, (i: number) => number | string | undefined> = {
      [ROW_KEY]: (i) => rows[i].row,
    };
    for (const path of columns)
      by[path] = (i) => {
        const m = rowsMaps[i];
        if (!m.after.has(path) && !m.before.has(path)) return undefined;
        const v = cellValue(m, path);
        return typeof v === 'number' ? v : formatDiffValue(v);
      };
    return gridSort.apply(rows.map((_, i) => i).filter(matches), by);
  });
</script>

<section
  class="right-col"
  bind:this={rightCol}
  style:--pane-top={`${paneTop}px`}
  style:--pane-after={`${paneAfter}px`}
>
  {@render head?.()}

  {#if !selectedSample}{@render stage()}{/if}

  {#snippet viewMenu()}
    <ChipPicker kind="columns" bind:open={viewOpen} title="View" ariaLabel="View">
      {#snippet face()}<GearIcon />{/snippet}
      {#snippet children()}
        <div class="view-menu">
          <label class="inline-checkbox"
            ><Toggle bind:checked={view.changedOnly} /> Only changed fields</label
          >
          <label class="inline-checkbox"><Toggle bind:checked={view.fullPaths} /> Full paths</label>
          <label class="inline-checkbox"><Toggle bind:checked={view.showTypes} /> Types</label>
        </div>
      {/snippet}
    </ChipPicker>
  {/snippet}

  {#snippet fieldsMenu(records: number | null)}
    <FieldsMenu
      fields={menuFields}
      {records}
      shown={isShown}
      {dictionary}
      onshow={showFields}
      ondefault={() => chosenFields.clear()}
      onhas={(path) => {
        showFields([path], true);
        addChip({ kind: 'has', path });
      }}
    />
  {/snippet}

  <div class="records">
    <div class="output-bar">
      {@render sampleMenu()}
      {#if gridRows && gridRows.length > 0}
        <span class="grow"></span>
        {@render fieldsMenu(gridRows.length)}
        {@render viewMenu()}
      {:else if !selectedSample && dictionary}
        <!-- No records to count, but the steps compiled: what they declare. -->
        <span class="grow"></span>
        {@render fieldsMenu(null)}
      {/if}
    </div>
    {#if gridRows && gridRows.length > 0}
      <div class="output-bar">
        <FindBox
          bind:query={outputQuery}
          bind:filter={find}
          bind:error={findError}
          {evaluate}
          remember={`smeltr-workspace-expressions:${pipelineId}`}
          items={findItems}
          onpick={pickFind}
          id="find"
          label="Search the output"
          placeholder="Search the output, or filter with an expression such as status >= 500"
        >
          {#snippet before()}<span aria-hidden="true">⌕</span
            >{#each chips as c (chipKey(c))}<RemovableChip
                removeLabel={`remove ${chipKey(c)}`}
                onremove={() => toggleChip(c)}
                ><span class="find-chip">{chipLabel(c)}</span></RemovableChip
              >{/each}{/snippet}
          {#snippet option(item)}
            <span>{CHIP_LABELS[item.kind]}</span>
            <span></span>
            <span class="dim">{item.count}</span>
          {/snippet}
        </FindBox>
      </div>
    {/if}
    <InlineError message={findError} />
    {#if selectedSample}
      {@render stage()}
      <div class="output-actions">
        {#if gridRows && (outputQuery.trim() || chips.length > 0)}
          <span class="dim count">{gridOrder.length} of {gridRows.length} rows</span>
        {/if}
        {#if failingCount > 0}
          <button
            type="button"
            class="failing error-button"
            aria-pressed={chips.some((c) => c.kind === 'failing')}
            onclick={() => toggleChip({ kind: 'failing' })}>⚠ {failingCount} failing</button
          >
        {/if}
        <span class="grow"></span>
        <button
          type="button"
          class="primary"
          data-guide="lock-test"
          onclick={onlock}
          disabled={!canLock}
        >
          Lock this as a test
        </button>
      </div>

      <div class="results">
        {#if !gridRows}
          {#if !result}
            <p class="dim">Run the pipeline to see output rows here.</p>
          {/if}
        {:else if gridRows.length === 0}
          <p class="dim">No sample rows.</p>
        {:else}
          <!-- Fixed widths, so a column holds still as rows scroll past
           (`columnWidths`); a value wider than its column ends in an
           ellipsis, and opening the row shows it whole. -->
          <div class="grid-wrap" bind:clientWidth={paneWidth}>
            <table class="grid" style:width={gridWidth}>
              <colgroup>
                <col style:width="calc(2ch + 1rem)" />
                <col style:width={`calc(${indexWidth}ch + 1rem)`} />
                <!-- The last column takes what the pane has to spare. -->
                {#each columnWidths as w, k (columns[k])}
                  <col style:width={k < columns.length - 1 ? `calc(${w}ch + 1rem)` : undefined} />
                {/each}
              </colgroup>
              <thead>
                <tr>
                  <th class="fold-cell"
                    ><FoldAllButton icon {allOpen} what="records" onclick={toggleAll} /></th
                  >
                  <th class="index-cell" aria-sort={gridSort.aria(ROW_KEY)}
                    ><SortButton sort={gridSort} key={ROW_KEY}>#</SortButton></th
                  >
                  {#each columns as path (path)}
                    {@const name = splitPath(path)}
                    <th class="cell" title={path} aria-sort={gridSort.aria(path)}
                      ><SortButton sort={gridSort} key={path}
                        ><span class="head-name"
                          ><span class="path-prefix">{name.prefix}</span>{name.leaf}</span
                        >{#if view.showTypes && columnTypes.get(path)}<span class="head-type"
                            >{': '}{columnTypes.get(path)}</span
                          >{/if}</SortButton
                      ></th
                    >
                  {/each}
                </tr>
              </thead>
              <tbody bind:this={tbody}>
                {#if drawn.above > 0}
                  <tr class="spacer" aria-hidden="true" style:height={`${drawn.above}px`}
                    ><td colspan={columns.length + 2}></td></tr
                  >
                {/if}
                {#each gridOrder.slice(drawn.start, drawn.end) as i (gridRows[i].row)}
                  {@const row = gridRows[i]}
                  {@const m = rowsMaps[i]}
                  {@const open = expanded.has(row.row)}
                  {@const failing = row.exceptions.length > 0}
                  <tr class="grid-row" class:dropped={row.json === null} class:open class:failing>
                    <td class="fold-cell">
                      <button
                        type="button"
                        class="fold"
                        aria-expanded={open}
                        aria-label={`row ${row.row}`}
                        onclick={() => toggleRow(row.row)}>{open ? '▾︎' : '▸︎'}</button
                      >
                    </td>
                    <td class="index-cell"
                      >{#if failing}<span class="warn">⚠</span>{/if}{row.row}</td
                    >
                    {#each columns as path (path)}
                      {@const field = fieldAt(m.before, m.after, path)}
                      <td class="cell">
                        {#if !field}
                          <span class="dim">&mdash;</span>
                        {:else if field.state === 'removed'}
                          <!-- What the step took away is what the grid is
                           being read for. The value stands, struck
                           through, rather than an em dash saying only
                           that something used to be here. -->
                          <span class="state-removed">{formatDiffValue(field.before)}</span>
                        {:else}
                          <span class="state-{field.state}">{formatDiffValue(field.after)}</span>
                        {/if}
                      </td>
                    {/each}
                  </tr>

                  {#if open}
                    {@const sides = rowSides(row)}
                    {@const marks = rowMarks(m)}
                    <!-- Named apart from its row, which is named by its fold button. -->
                    <tr
                      class="expanded-row"
                      aria-label={`record ${row.row}`}
                      bind:offsetHeight={openHeights[row.row]}
                    >
                      <td
                        class="expanded-cell"
                        colspan={columns.length + 2}
                        style:--pane-width={`${paneWidth}px`}
                      >
                        <div class="row-head-wrap">
                          <div class="row-head">
                            <span class="row-index">row {row.row}</span>
                            <!-- Beside the row number: pushing it to the far
                             side needed the row-head to reliably span the
                             visible width, which a `colspan` cell wider
                             than the viewport does not give it. -->
                            {#if marks.added > 0}<span class="mark added">+{marks.added}</span>{/if}
                            {#if marks.removed > 0}<span class="mark removed"
                                >&minus;{marks.removed}</span
                              >{/if}
                            {#if marks.changed > 0}<span class="mark changed">~{marks.changed}</span
                              >{/if}
                            {#if row.json === null}
                              <span class="badge dropped-badge">dropped</span>
                            {/if}
                          </div>
                          {#each row.exceptions as a}
                            {@const by = a.step ? stepNumbers.get(a.step) : undefined}
                            <p class="exception">
                              {#if by}<button
                                  type="button"
                                  class="link-button"
                                  onclick={() => onjump(by.step)}
                                  >{stepName(by.step, by.number)}</button
                                >{': '}{/if}{a.message}
                            </p>
                          {/each}
                        </div>
                        <div class="record-body">
                          <RecordFields
                            before={sides.before}
                            after={sides.after}
                            keep={unfoldedPath}
                            declared={sides.declared}
                            changedOnly={view.changedOnly}
                            fullPaths={view.fullPaths}
                            showTypes={view.showTypes}
                            touched={touchedOn(row.row)}
                            {onjump}
                          />
                        </div>
                      </td>
                    </tr>
                  {/if}
                {/each}
                {#if drawn.below > 0}
                  <tr class="spacer" aria-hidden="true" style:height={`${drawn.below}px`}
                    ><td colspan={columns.length + 2}></td></tr
                  >
                {/if}
              </tbody>
            </table>
          </div>
        {/if}
      </div>
    {/if}
  </div>
</section>

<style>
  /* The sample and what is shown of it on one row, and the search on the next: the search has a
     row of its own to be read in. */
  .output-bar {
    display: flex;
    align-items: center;
    gap: 0.7rem;
  }

  .output-bar + .output-bar {
    margin-top: 0.5rem;
  }
  .count {
    font-size: var(--text-note);
    white-space: nowrap;
  }

  .find-chip {
    color: var(--ink);
    font-size: var(--text-note);
  }

  /* The failing rows are what to read first, so their count stands out until read. */
  .failing {
    flex: none;
  }

  /* A grid item is `min-width: auto` by default, so a record wider than the
     column would push past it rather than scroll inside its own half. */
  .right-col {
    min-width: 0;
    overflow-x: auto;
  }

  .right-col {
    display: flex;
    flex-direction: column;
    gap: 0;
  }

  /* Beside the steps, the records scroll on their own: the 500th record and the step that set it
     are on screen together. */
  @media (min-width: 1001px) {
    .right-col {
      position: sticky;
      top: var(--clear-top, 0px);
      /* Its grid and dictionary scroll themselves; a short pane must not clip a menu opened in it. */
      overflow: visible;
      /* From where it stands on screen to the bottom of the window, clear of what the page holds
         below it and of the bars held over the page: the Save bar, and the agent panel under it. */
      max-height: calc(
        100vh - max(var(--pane-top), var(--clear-top, 0px)) - var(--pane-after) -
          var(--clear-bottom, 0px) - var(--agent-panel, 0px)
      );
    }

    /* The grid is what scrolls, so the controls above it and its header row stay put. */
    .records,
    .results {
      display: flex;
      flex-direction: column;
      flex: 1;
      min-height: 0;
    }

    /* What the editor passes in is its own markup, so these reach past this component's. */
    .right-col > :global(:not(.records)),
    .records > :global(:not(.results)) {
      flex-shrink: 0;
    }

    .grid-wrap {
      overflow: auto;
      min-height: 0;
    }
  }

  .output-actions {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    margin-top: 0.5rem;
  }

  .results {
    margin-top: 1rem;
    min-height: 200px;
  }

  /* A rule under it, not just a gap, so the row it names and the fields
     below it read as three things -- the folded row above, this head, the
     record -- rather than one tinted block. */
  .row-head-wrap {
    padding: 0.4rem 0;
    margin-bottom: 0.4rem;
    border-bottom: 1px solid var(--border);
  }

  .row-head {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .row-index {
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--ink);
  }

  .badge {
    font-size: var(--text-micro);
    font-weight: 600;
    padding: 0.15rem 0.5rem;
    border-radius: var(--radius);
    max-width: 100%;
    white-space: normal;
    background: color-mix(in srgb, var(--error) 15%, transparent);
    color: var(--error);
  }

  .badge.dropped-badge {
    background: color-mix(in srgb, var(--dim) 20%, transparent);
    color: var(--dim);
  }

  /* A record is one line until it is asked for. The whole grid scrolls
     sideways rather than dropping columns: a field stays where it was, and
     the alphabetical order means it stays findable once scrolled to. */
  /* The bar sits at the bottom of the padding box, so without this it is
     drawn over the last row rather than under it. */
  .grid-wrap {
    overflow-x: auto;
    padding-bottom: 0.85rem;
    scrollbar-gutter: stable;
  }

  /* At least the width of the panel, so a pipeline that has produced one
     field spends the whole panel on it rather than leaving it empty; wider
     than the panel when the columns need it, which is what scrolls. */
  .grid {
    border-collapse: collapse;
    table-layout: fixed;
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .grid th,
  .grid td {
    text-align: left;
    font-weight: inherit;
    padding: 0.15rem 0.5rem;
    border-bottom: 1px solid var(--border);
  }

  .grid thead th {
    color: var(--dim);
    font-size: var(--text-micro);
    position: sticky;
    top: 0;
    /* Above the rows scrolling under it, which come later in the page. */
    z-index: 1;
    background: var(--bg);
  }

  /* Tinted the same as the record beneath it, so the two read as one open
     group -- but still a hairline apart, or the row that was clicked and
     the record it opened would run together into one shape. */
  .grid-row.open > td {
    background: color-mix(in srgb, var(--ink) 8%, transparent);
  }

  .grid-row.dropped .cell {
    opacity: 0.55;
  }

  .fold {
    border: none;
    background: none;
    padding: 0;
    color: var(--dim);
    font: inherit;
    cursor: pointer;
  }

  .index-cell {
    color: var(--dim);
    text-align: right;
  }

  .cell {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* The rail marks how far the open record reaches, and stays put while the
     grid scrolls sideways under it. A record the step changed takes the
     added color, as a diff does. */
  .expanded-cell {
    background: color-mix(in oklch, var(--ink) 6%, transparent);
    box-shadow: inset 3px 0 0 var(--border);
    padding: 0.5rem 0.5rem 0.5rem 0.75rem;
  }

  /* The record moves with its row as the grid scrolls sideways, and wraps to
     the pane's width rather than to the width of every column. */
  .row-head-wrap,
  .record-body {
    max-width: calc(var(--pane-width) - 1.5rem);
  }

  /* A row that failed is the one to read, so it is the one that stands out. */
  .grid-row.failing > td {
    background: color-mix(in srgb, var(--error) 10%, transparent);
  }

  .grid-row.failing .index-cell,
  .warn {
    color: var(--error);
  }

  .warn {
    margin-right: 0.4ch;
  }

  /* Why the row failed, under its head, in the words the step gave. */
  .exception {
    width: max-content;
    max-width: 100%;
    margin: 0.3rem 0 0;
    padding-left: 0.5rem;
    border-left: 2px solid var(--error);
    color: var(--error);
    white-space: normal;
  }

  .spacer td {
    padding: 0;
    border: 0;
  }

  /* The header's name gives way to an ellipsis rather than widening its
     column; the whole path is its title. */
  .head-name {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  /* In a column too narrow for both, the type gives way before the name. Its
     colon sits against the name, as a declaration writes it, inside the sort
     button's gap. */
  .head-type {
    margin-left: -0.25rem;
    min-width: 0;
    flex-shrink: 10;
    overflow: hidden;
    text-overflow: ellipsis;
    font-family: var(--mono);
    opacity: 0.7;
    white-space: nowrap;
  }

  /* The nesting recedes so the eye lands on the field's own name. */
  .path-prefix {
    opacity: 0.6;
  }

  .view-menu {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    padding: 0.6rem 0.75rem;
    white-space: nowrap;
  }

  .inline-checkbox {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin: 0;
    font-size: var(--text-label);
    font-weight: 400;
  }

  .state-added {
    color: var(--added);
  }

  .state-changed {
    color: var(--changed);
  }

  .state-removed {
    color: var(--removed);
    text-decoration: line-through;
  }

  /* What this step did to the row, before reading a line of it. */
  .mark {
    font-family: var(--mono);
    font-size: var(--text-micro);
    font-weight: 600;
  }

  .mark.added {
    color: var(--added);
  }

  .mark.changed {
    color: var(--changed);
  }

  .mark.removed {
    color: var(--removed);
  }
</style>
