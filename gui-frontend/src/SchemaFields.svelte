<!--
  One schema's fields, as its table: each field's path, the type it must hold, and what it means. A field a row must
  hold is starred. A search narrows it to the fields whose path or description holds the words.
-->
<script lang="ts">
  import type { SchemaDto } from './api';
  import { count } from './time';

  let {
    of,
    q = '',
  }: {
    of: Pick<SchemaDto, 'fields'>;
    /** Words a shown field's path or description holds. Blank shows every field. */
    q?: string;
  } = $props();

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
  <table class="fields">
    <thead>
      <tr>
        <th>Field</th>
        <th>Type</th>
        <th>Description</th>
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
          <td class="about">{f.description ?? ''}</td>
        </tr>
      {/each}
    </tbody>
  </table>
  {#if shown.length < of.fields.length}
    <p class="dim">{count(shown.length, 'field')} of {of.fields.length} hold “{q.trim()}”.</p>
  {/if}
{/if}

<style>
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
