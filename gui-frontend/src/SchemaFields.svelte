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

  function choose(f: SchemaFieldDto, type: string) {
    if (!type) onmap?.(f.field, undefined);
    // The same type keeps its other parameters.
    else if (f.elasticsearch?.type === type) return;
    else onmap?.(f.field, { type });
  }

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
              >Each field is indexed as this Elasticsearch type. Leave it on default to use the type
              its schema type maps to.</Help
            ></th
          >{:else}<th>Description</th>{/if}
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
            {@const others = Object.keys(f.elasticsearch ?? {}).filter((k) => k !== 'type')}
            <td class="mapping">
              {#if types.length === 0}
                <span class="dim">—</span>
              {:else if onmap}
                <select
                  aria-label={`Elasticsearch type of ${f.field}`}
                  value={f.elasticsearch?.type ?? ''}
                  onchange={(e) => choose(f, e.currentTarget.value)}
                >
                  <option value="">default ({ownType(f)})</option>
                  {#each types as t (t)}<option value={t}>{t}</option>{/each}
                </select>
              {:else}
                <span class="mono">{f.elasticsearch?.type ?? ownType(f)}</span>
              {/if}
              {#if others.length > 0}<span class="dim"
                  >+ {count(others.length, 'more setting')}:
                  <span class="mono">{others.join(', ')}</span></span
                >{/if}
            </td>
          {:else}
            <td class="about">{f.description ?? ''}</td>
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
