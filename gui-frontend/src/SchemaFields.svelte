<!--
  One schema's fields, as its table: each field's path, the type it must hold, its requirement, and what it means. A
  required field is starred, and a recommended one marked °; the requirement is chosen where the schema is the
  group's own. A search narrows it to the fields whose path or description holds the words. The Elasticsearch
  view shows the type each field maps to there, and chooses it where the schema is the group's own. A long table
  shows its first fields until Show all.
-->
<script lang="ts">
  import type { Requirement, SchemaDto, SchemaFieldDto } from './api';
  import Help from './Help.svelte';
  import Segmented from './Segmented.svelte';
  import ShowAll, { SHOWN } from './ShowAll.svelte';
  import { elasticsearchTypes, REQUIREMENT_MARK, REQUIREMENTS } from './schemas';
  import RequirementMark from './RequirementMark.svelte';
  import { count } from './time';

  let {
    of,
    q = '',
    onmap,
    onrequire,
  }: {
    of: Pick<SchemaDto, 'fields'>;
    /** Words a shown field's path or description holds. Blank shows every field. */
    q?: string;
    /** Sets how the field maps in Elasticsearch, or clears it to its type's own. Absent, the view only shows it. */
    onmap?: (field: string, mapping: SchemaFieldDto['elasticsearch']) => void;
    /** Sets a field's requirement. Absent, the table only shows it. */
    onrequire?: (field: string, requirement: Requirement) => void;
  } = $props();

  const VIEWS = [
    { value: 'schema', label: 'Schema' },
    { value: 'elasticsearch', label: 'Elasticsearch' },
  ] as const;
  let view: (typeof VIEWS)[number]['value'] = $state('schema');

  /** What a field maps to when it says nothing: a variant's type, or its elements', is Elasticsearch's to find from
   * the first value. */
  function ownType(f: SchemaFieldDto): string {
    return /^(array<)*variant>*$/.test(f.type) ? 'dynamic' : (elasticsearchTypes(f.type)[0] ?? '');
  }

  /** An integer read as a date counts milliseconds, Elasticsearch's own reading of a number, or seconds, which its
   * mapping's format says. */
  const SECONDS = 'date in seconds';
  /** Seconds as a number, and ISO time as a query's bounds are written, which `epoch_second` alone refuses. */
  const SECONDS_FORMAT = 'epoch_second||strict_date_optional_time';
  /** A date its mapping gives another format, set in Edit JSON, which the dropdown names and keeps. */
  const FORMATTED = 'date as formatted';

  function integer(f: SchemaFieldDto): boolean {
    return /^(array<)*i64>*$/.test(f.type);
  }

  /** The choices a field's dropdown offers, each with its label: an integer's date is one for each unit, and a date
   * with another format is one more, named by it. */
  function choices(f: SchemaFieldDto, types: string[]): { value: string; label: string }[] {
    const formatted =
      mapsAs(f) === FORMATTED
        ? [{ value: FORMATTED, label: `date (${f.elasticsearch?.format})` }]
        : [];
    return types.flatMap((t) =>
      t !== 'date'
        ? [{ value: t, label: t }]
        : integer(f)
          ? [
              { value: t, label: 'date (milliseconds)' },
              { value: SECONDS, label: 'date (seconds)' },
              ...formatted,
            ]
          : [{ value: t, label: t }, ...formatted],
    );
  }

  /** The type a field maps as, as the dropdown holds it: a variant's own is the empty choice, Elasticsearch's to find. */
  function mapsAs(f: SchemaFieldDto): string {
    const own = ownType(f);
    const type = f.elasticsearch?.type ?? (own === 'dynamic' ? '' : own);
    const format = f.elasticsearch?.format;
    if (type !== 'date' || format === undefined) return type;
    return integer(f) && [SECONDS_FORMAT, 'epoch_second'].includes(String(format))
      ? SECONDS
      : FORMATTED;
  }

  /** The field's own type saves no mapping: what it maps as of itself needs none. */
  function choose(f: SchemaFieldDto, type: string) {
    // The same type keeps its other parameters.
    if (mapsAs(f) === type) return;
    const own = ownType(f);
    // A date in one unit or the other is the same type, and keeps its other parameters.
    const { format: _, ...date } =
      f.elasticsearch?.type === 'date' ? f.elasticsearch : { type: 'date' };
    if (type === SECONDS) {
      onmap?.(f.field, { ...date, format: SECONDS_FORMAT });
      return;
    }
    onmap?.(f.field, type === '' || type === own ? undefined : type === 'date' ? date : { type });
  }

  /** Whether any field gives an example, so the column is there only where it says something. */
  let examples = $derived(of.fields.some((f) => f.example));

  let found = $derived.by(() => {
    const needle = q.trim().toLowerCase();
    if (!needle) return of.fields;
    return of.fields.filter(
      (f) =>
        f.field.toLowerCase().includes(needle) ||
        (f.description ?? '').toLowerCase().includes(needle),
    );
  });

  /** Stays on while the search changes, until the schema is closed. */
  let showAll = $state(false);
  let shown = $derived(showAll ? found : found.slice(0, SHOWN));

  /** The widest of `texts`, in characters. */
  function widest(texts: (string | undefined)[]): number {
    return texts.reduce((w, t) => Math.max(w, t?.length ?? 0), 0);
  }
  // The columns are as wide as the widest of all the fields, not of the rows on screen, so they hold still while
  // the browser lays out only what is in view. Field, type and example are monospace, so a character is a `ch`.
  let columns = $derived.by(() => {
    const field = `min(calc(${widest(of.fields.map((f) => f.field)) + 1}ch + 1rem), 45%)`;
    const type = `calc(${Math.max(4, widest(of.fields.map((f) => f.type)))}ch + 1rem)`;
    if (view === 'elasticsearch') return `${field} ${type} minmax(0, 1fr)`;
    // As wide as `recommended` in a dropdown, or as its column's head where it is text.
    const requirement = onrequire ? '11rem' : 'calc(12ch + 1.5rem)';
    const example = examples
      ? ` min(calc(${widest(of.fields.map((f) => f.example))}ch + 1rem), 18rem)`
      : '';
    return `${field} ${type} ${requirement} minmax(0, 1fr)${example}`;
  });
</script>

{#if of.fields.length === 0}
  <p class="dim">No fields.</p>
{:else}
  <div class="views">
    <!-- The toggle names itself to a screen reader. -->
    <span class="dim" aria-hidden="true">Fields shown as</span>
    <Segmented label="Fields shown as" options={[...VIEWS]} bind:value={view} />
  </div>
  <table
    class="fields"
    class:mapped={view === 'elasticsearch'}
    class:editable={onrequire !== undefined}
    style:--columns={columns}
  >
    <thead>
      <tr>
        <th>Field</th>
        <th>Type</th>
        {#if view === 'elasticsearch'}<th
            >Elasticsearch <Help label="Elasticsearch"
              >Each field is indexed as this Elasticsearch type. The first in its list is the type
              its schema type maps to, and a field left on it saves no mapping of its own.</Help
            ></th
          >{:else}<th>Requirement</th><th>Description</th>
          {#if examples}<th>Example</th>{/if}{/if}
      </tr>
    </thead>
    <tbody>
      {#each shown as f (f.field)}
        <tr>
          <td class="name" title={f.field}>
            <span class="mono">{f.field}</span>
          </td>
          <td class="mono">{f.type}</td>
          {#if view === 'elasticsearch'}
            {@const types = elasticsearchTypes(f.type)}
            <!-- An integer's unit is its choice of type, not another setting. -->
            {@const others = Object.keys(f.elasticsearch ?? {}).filter(
              (k) => k !== 'type' && !(k === 'format' && [SECONDS, FORMATTED].includes(mapsAs(f))),
            )}
            <td class="mapping">
              {#if types.length === 0}
                <span class="dim">—</span>
              {:else if onmap}
                <select
                  aria-label={`Elasticsearch type of ${f.field}`}
                  value={mapsAs(f)}
                  onchange={(e) => choose(f, e.currentTarget.value)}
                >
                  {#if ownType(f) === 'dynamic'}<option value="">dynamic</option>{/if}
                  {#each choices(f, types) as c (c.value)}<option value={c.value}>{c.label}</option
                    >{/each}
                </select>
              {:else}
                <span class="mono"
                  >{choices(f, types).find((c) => c.value === mapsAs(f))?.label ??
                    f.elasticsearch?.type ??
                    ownType(f)}</span
                >
              {/if}
              {#if others.length > 0}<span class="dim"
                  >+ {count(others.length, 'more setting')}:
                  <span class="mono">{others.join(', ')}</span></span
                >{/if}
            </td>
          {:else}
            <td class="requirement">
              {#if onrequire}
                <!-- Colored as RequirementMark colors it, so a chosen requirement reads as a shown one. -->
                <select
                  class={f.requirement ?? 'optional'}
                  aria-label={`Requirement of ${f.field}`}
                  value={f.requirement ?? 'optional'}
                  onchange={(e) => onrequire(f.field, e.currentTarget.value as Requirement)}
                >
                  {#each REQUIREMENTS as r (r)}<option value={r}>{REQUIREMENT_MARK[r]}</option
                    >{/each}
                </select>
              {:else}
                <RequirementMark of={f.requirement ?? 'optional'} />
              {/if}
            </td>
            <td class="about">{f.description ?? ''}</td>
            {#if examples}<td class="example mono" title={f.example}
                ><div>{f.example ?? ''}</div></td
              >{/if}
          {/if}
        </tr>
      {/each}
    </tbody>
  </table>
  {#if shown.length < found.length}
    <div class="more">
      <ShowAll
        shown={SHOWN}
        of={found.length < of.fields.length
          ? `the ${found.length} fields that hold “${q.trim()}”`
          : `${found.length} fields`}
        onshowall={() => (showAll = true)}
      />
    </div>
  {:else if found.length < of.fields.length}
    <p class="dim">{count(found.length, 'field')} of {of.fields.length} hold “{q.trim()}”.</p>
  {/if}
{/if}

<style>
  .views {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.4rem;
    font-size: var(--text-label);
  }

  .mapping {
    white-space: nowrap;
  }

  .requirement select.required {
    color: var(--error);
    font-weight: 600;
  }

  .requirement select.recommended {
    color: var(--warn);
    font-weight: 600;
  }

  .requirement select {
    width: 100%;
  }

  /* A row of dropdowns reads level with its text. */
  .mapped tr,
  .editable tr {
    align-items: center;
  }

  .mapping .dim {
    margin-left: 0.4rem;
  }

  /* Each row a grid of the same columns, not a table's layout, so a row off screen can be left unlaid: a table lays
     out every row to size its columns. The columns come from `columns`, measured in the monospace each row is set in
     so that a `ch` is one of the fields' characters. */
  .fields {
    display: block;
    width: 100%;
  }

  .fields thead,
  .fields tbody {
    display: block;
  }

  .fields tr {
    display: grid;
    grid-template-columns: var(--columns);
    border-bottom: 1px solid var(--border);
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .fields :is(th, td):not(.mono) {
    font-family: var(--font-body);
  }

  .fields tbody tr {
    content-visibility: auto;
    contain-intrinsic-size: auto 1.6rem;
  }

  .more {
    margin-top: 0.4rem;
  }

  th,
  td {
    padding: 0.25rem 0.5rem;
    text-align: left;
    vertical-align: top;
    font-size: var(--text-note);
  }

  th {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  .name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* An example can be a whole JSON document; two lines of it say what it is. */
  .example {
    max-width: 18rem;
  }

  .example div {
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow-wrap: anywhere;
  }

  .about {
    overflow-wrap: anywhere;
  }

  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
