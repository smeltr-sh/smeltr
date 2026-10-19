<script lang="ts">
  import Prose from './Prose.svelte';
  import { recordField } from '#lib/connector-reference.ts';
  let { names }: { names: string[] } = $props();
</script>

<div class="table-scroll">
  <table class="fields">
    <thead
      ><tr><th scope="col">Field</th><th scope="col">Type</th><th scope="col">Holds</th></tr></thead
    >
    <tbody>
      {#each names.map(recordField) as f (f.name)}
        <tr>
          <th scope="row"><code>{f.name}</code></th>
          <td><code>{f.type}</code></td>
          <td><Prose text={f.what} /></td>
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .fields thead th,
  .fields th[scope='row'],
  .fields td:nth-child(2) {
    white-space: nowrap;
  }
  /* On a phone each field becomes a heading line over what it holds. */
  @media (max-width: 40rem) {
    .fields thead {
      display: none;
    }
    .fields tr {
      display: block;
      padding: 0.5rem 0;
      border-bottom: 1px solid var(--border);
    }
    .fields th[scope='row'],
    .fields td {
      display: inline;
      padding: 0 0.5rem 0 0;
      border: 0;
    }
    .fields td:nth-child(3) {
      display: block;
      padding-top: 0.35rem;
    }
  }
</style>
