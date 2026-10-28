<!--
  One schema's fields, as a table: each field's name, the schema's own type for it, what a pipeline writes to keep
  it, and what it means. A field that holds an object opens to its fields in place, read when it is opened: a
  schema's objects may name each other, and themselves, so a schema is read an object at a time.
-->
<script lang="ts">
  import { schemaObject, type SchemaField, type SchemaObject } from './api';
  import { reported } from './errors.svelte';

  let {
    of,
  }: {
    /** One of the bundle's, by its JSON Schema. */
    of: { schema: Record<string, unknown> };
  } = $props();

  /** Each object read, by its index in the schema. */
  let objects = $state(new Map<number, SchemaObject>());
  let root = $state<number | null>(null);
  let problem = $state('');
  /** The fields opened, by their path. */
  let opened = $state(new Set<string>());

  async function read(at?: number): Promise<SchemaObject | null> {
    try {
      const res = await schemaObject(of, at);
      if (!res.ok) {
        problem = res.errors.map((e) => e.message).join(' ');
        return null;
      }
      objects = new Map(objects).set(res.at, res);
      return res;
    } catch (e) {
      reported(e);
      return null;
    }
  }

  $effect(() => {
    void of;
    objects = new Map();
    opened = new Set();
    problem = '';
    root = null;
    void read().then((o) => (root = o?.at ?? null));
  });

  async function toggle(path: string, f: SchemaField) {
    if (opened.has(path)) {
      opened.delete(path);
      opened = new Set(opened);
      return;
    }
    if (f.object !== undefined && !objects.has(f.object)) await read(f.object);
    opened = new Set(opened).add(path);
  }

  let rootObject = $derived(root === null ? undefined : objects.get(root));
</script>

{#if problem}
  <p class="problem">{problem}</p>
{:else if !rootObject}
  <p class="dim">Loading…</p>
{:else}
  <table class="fields">
    <thead>
      <tr>
        <th>Field</th>
        <th>Type</th>
        <th>Write as</th>
        <th>Description</th>
      </tr>
    </thead>
    <tbody>
      {@render rows(rootObject, '', 0)}
    </tbody>
  </table>
  {#if rootObject.fields.length === 0}<p class="dim">No fields.</p>{/if}
{/if}

{#snippet rows(o: SchemaObject, under: string, depth: number)}
  {#each o.fields as f (f.name)}
    {@const path = under ? `${under}.${f.name}` : f.name}
    {@const inner = f.object === undefined ? undefined : objects.get(f.object)}
    <tr>
      <td class="name" style:padding-left="{0.5 + depth * 1.2}rem">
        {#if f.object !== undefined}
          <button
            type="button"
            class="fold"
            aria-expanded={opened.has(path)}
            aria-label="{opened.has(path) ? 'Close' : 'Open'} {path}"
            onclick={() => toggle(path, f)}>{opened.has(path) ? '▾' : '▸'}</button
          >
        {/if}
        <span class="mono">{f.name}</span>{#if f.required}<span class="required" title="required"
            >*</span
          >{/if}
      </td>
      <td class="mono">{f.typeName}{f.array ? '[]' : ''}</td>
      <td class="mono">{f.writtenAs}</td>
      <td class="about">{f.description}</td>
    </tr>
    {#if inner && opened.has(path)}
      {@render rows(inner, path, depth + 1)}
    {/if}
  {/each}
{/snippet}

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

  .fold {
    padding: 0 0.3rem;
    border: none;
    background: none;
    color: var(--dim);
  }

  .required {
    margin-left: 0.15rem;
    color: var(--error);
  }

  .problem {
    color: var(--error);
  }

  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
