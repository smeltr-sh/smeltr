<!--
  How far back a view's charts and counts reach (`CHART_RANGES`), picked from behind a clock. A pick
  is remembered, so every picker opens on it next.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import ClockIcon from './ClockIcon.svelte';
  import { CHART_RANGES, rememberChartRange } from './map';

  let {
    range,
    onpick,
  }: {
    /** The range in force, by its `CHART_RANGES` name. */
    range: string;
    onpick: (range: string) => void;
  } = $props();

  let open = $state(false);
</script>

<ChipPicker kind="row" compact ariaLabel="Chart range" title="Chart range" bind:open>
  {#snippet face()}<ClockIcon />{/snippet}
  {#each CHART_RANGES as r (r.range)}
    <button
      type="button"
      class="pick"
      class:on={r.range === range}
      onclick={() => {
        open = false;
        rememberChartRange(r.range);
        onpick(r.range);
      }}>{r.label}</button
    >
  {/each}
</ChipPicker>
