<!--
  A selected stack's panel (design doc "The map", Stacks), docked where an object's panel is: the
  stack's members, each with its reading, each a link that selects it -- and a selected member
  stands out of its stack on the map, with its own path and its own panel.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { MapCard } from './map';
  import MapRows, { type MapRow } from './MapRows.svelte';
  import MapPanel from './MapPanel.svelte';

  let {
    stack,
    system,
    rows,
    onclose,
    controls,
  }: {
    stack: MapCard;
    /** What the members' system is called. */
    system: string;
    rows: MapRow[];
    onclose: () => void;
    controls?: Snippet;
  } = $props();
</script>

<MapPanel kind={stack.kind} group={stack.group} title={stack.name} {onclose}>
  <div class="sub dim">{system}</div>
  <MapRows {rows} />
  {#if controls}<div class="controls">{@render controls()}</div>{/if}
</MapPanel>

<style>
  .sub {
    margin-bottom: 0.75rem;
    font-size: var(--text-label);
  }

  .dim {
    color: var(--dim);
  }

  .controls {
    margin-top: 1rem;
  }
</style>
