<!--
  "Login recommends a, b and c; the rows lack them.": the fields a schema recommends and a pipeline's rows lack,
  the first few named and a button for the rest. The editor's note and review's Schemas tab both say it.
-->
<script lang="ts">
  import ShowAll from './ShowAll.svelte';
  import { RECOMMENDED_NAMED } from './schemas';

  let { schema, fields }: { schema: string; fields: string[] } = $props();

  let all = $state(false);
  let named = $derived(all ? fields : fields.slice(0, RECOMMENDED_NAMED));
</script>

{schema} recommends {#each named as f, i (f)}{#if i > 0}{i === fields.length - 1
      ? ' and '
      : ', '}{/if}<code>{f}</code>{/each}{#if named.length < fields.length}{' '}and
  <ShowAll more={fields.length - named.length} onshowall={() => (all = true)} />{/if}; the rows lack
{fields.length === 1 ? 'it' : 'them'}.

<style>
  code {
    font-family: var(--mono);
    font-size: 0.92em;
  }
</style>
