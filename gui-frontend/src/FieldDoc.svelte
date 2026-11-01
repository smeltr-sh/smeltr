<!--
  A schema field's documentation, as the pipeline editor shows it while a field is chosen: what it means, an example
  of what it holds and the Elasticsearch type it maps to. `head` names the field first, for a pane that stands apart
  from the row that names it.
-->
<script lang="ts">
  import type { SchemaFieldDto } from './api';
  import FieldDescription from './FieldDescription.svelte';
  import RequirementMark from './RequirementMark.svelte';
  import { ownElasticsearchType } from './schemas';

  let { field, head = false }: { field: SchemaFieldDto; head?: boolean } = $props();

  let mapsTo = $derived(field.elasticsearch?.type ?? ownElasticsearchType(field.type));
</script>

<div class="field-doc">
  {#if head}
    <div class="head">
      <code class="path">{field.field}</code>
      <code class="dim">{field.type}</code>
      <span class="requirement"><RequirementMark of={field.requirement ?? 'optional'} /></span>
    </div>
  {/if}
  {#if field.description}<FieldDescription text={field.description} />{/if}
  {#if field.example || mapsTo}
    <dl>
      {#if field.example}<dt>Example</dt>
        <dd><code>{field.example}</code></dd>{/if}
      {#if mapsTo}<dt>Elasticsearch type</dt>
        <dd><code>{mapsTo}</code></dd>{/if}
    </dl>
  {/if}
</div>

<style>
  .field-doc {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    font-size: var(--text-note);
    line-height: 1.45;
    color: var(--ink);
  }

  .field-doc :global(code) {
    font-family: var(--mono);
    font-size: 0.92em;
  }

  .head {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }

  .path {
    font-weight: 600;
  }

  .requirement {
    margin-left: auto;
    font-size: var(--text-micro);
  }

  dl {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.2rem 0.75rem;
    margin: 0.15rem 0 0;
    color: var(--dim);
  }

  dd {
    margin: 0;
    min-width: 0;
    overflow-wrap: anywhere;
    color: var(--ink);
  }
</style>
