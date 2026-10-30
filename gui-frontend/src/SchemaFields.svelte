<!--
  One schema's fields, as its table: each field's path, the type it must hold, and what it means. A field a row must
  hold is starred. A search narrows it to the fields whose path or description holds the words. The Elasticsearch
  view shows the type each field maps to there, and chooses it where the schema is the group's own.
-->
<script lang="ts">
  import type { SchemaDto, SchemaFieldDto } from './api';
  import Help from './Help.svelte';
  import Segmented from './Segmented.svelte';
  import { elasticsearchTypes } from './schemas';
  import { count } from './time';

  let {
    of,
    q = '',
    onmap,
  }: {
    of: Pick<SchemaDto, 'fields'>;
    /** Words a shown field's path or description holds. Blank shows every field. */
    q?: string;
    /** Sets how the field maps in Elasticsearch, or clears it to its type's own. Absent, the view only shows it. */
    onmap?: (field: string, mapping: SchemaFieldDto['elasticsearch']) => void;
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
    return integer(f) && format === 'epoch_second' ? SECONDS : FORMATTED;
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
      onmap?.(f.field, { ...date, format: 'epoch_second' });
      return;
    }
    onmap?.(f.field, type === '' || type === own ? undefined : type === 'date' ? date : { type });
  }

  /** Whether any field gives an example, so the column is there only where it says something. */
  let examples = $derived(of.fields.some((f) => f.example));

  let shown = $derived.by(() => {
    const needle = q.trim().toLowerCase();
    if (!needle) return of.fields;
    return of.fields.filter(
      (f) =>
        f.field.toLowerCase().includes(needle) ||
        (f.description ?? '').toLowerCase().includes(needle),
    );
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
  <table class="fields" class:mapped={view === 'elasticsearch'}>
    <thead>
      <tr>
        <th>Field</th>
        <th>Type</th>
        {#if view === 'elasticsearch'}<th
            >Elasticsearch <Help label="Elasticsearch"
              >Each field is indexed as this Elasticsearch type. The first in its list is the type
              its schema type maps to, and a field left on it saves no mapping of its own.</Help
            ></th
          >{:else}<th>Description</th>
          {#if examples}<th>Example</th>{/if}{/if}
      </tr>
    </thead>
    <tbody>
      {#each shown as f (f.field)}
        <tr>
          <td class="name">
            <span class="mono">{f.field}</span>{#if f.required}<span
                class="required"
                title="required">*</span
              >{/if}
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
            <td class="about">{f.description ?? ''}</td>
            {#if examples}<td class="example mono" title={f.example}
                ><div>{f.example ?? ''}</div></td
              >{/if}
          {/if}
        </tr>
      {/each}
    </tbody>
  </table>
  {#if shown.length < of.fields.length}
    <p class="dim">{count(shown.length, 'field')} of {of.fields.length} hold “{q.trim()}”.</p>
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

  /* A row of dropdowns reads level with its text. */
  .mapped td {
    vertical-align: middle;
  }

  .mapping .dim {
    margin-left: 0.4rem;
  }

  .fields {
    width: 100%;
    border-collapse: collapse;
  }

  th,
  td {
    padding: 0.25rem 0.5rem;
    border-bottom: 1px solid var(--border);
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

  .required {
    margin-left: 0.15rem;
    color: var(--error);
  }

  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
