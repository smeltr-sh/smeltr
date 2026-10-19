<!--
  One connector kind's settings in the order its GUI form asks for them, each Advanced section of the form under a
  heading of its own.
-->
<script lang="ts">
  import SettingsTable from './SettingsTable.svelte';
  import { formGroups, type ConnectorGroup } from '#lib/connector-reference.ts';
  let { group, type }: { group: ConnectorGroup; type: string } = $props();
</script>

{#each formGroups(group, type) as g, i (i)}
  {#if g.section}
    <h3>{g.section === 'Advanced' ? 'Advanced' : `Advanced: ${g.section}`}</h3>
  {/if}
  <SettingsTable settings={g.settings} />
{/each}
