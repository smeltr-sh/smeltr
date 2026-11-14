<!--
  The preset a source, or its senders, is judged against its normal at (`throughput::Preset`): an
  editor picks one, and anyone else reads it.
-->
<script lang="ts">
  import Help from './Help.svelte';
  import Segmented from './Segmented.svelte';
  import type { Preset } from './api';

  let {
    value,
    label,
    editor,
    pick,
  }: {
    value: Preset;
    /** What it is the preset of, for a screen reader. */
    label: string;
    editor: boolean;
    pick: (preset: Preset) => void;
  } = $props();

  const PRESETS: { value: Preset; label: string }[] = [
    { value: 'fast', label: 'Fast' },
    { value: 'sensitive', label: 'Sensitive' },
    { value: 'balanced', label: 'Balanced' },
    { value: 'quiet', label: 'Quiet' },
  ];
</script>

<div class="preset">
  <h3>
    Preset
    <Help label="Preset"
      >A source warns when its rows stay below their expected range for some 5-minute readings in a
      row. A preset sets how wide that range is and how many readings it waits.<br /><b>Fast</b>: a
      stop warns in about 5 minutes. A wide range, so only a deep drop warns, and brief dips warn
      most often.<br /><b>Sensitive</b>: a stop warns in about 15 minutes. The narrowest range, so
      it sees the smallest drops; brief dips warn often.<br /><b>Balanced</b>: a stop warns in about
      20 minutes. A narrow range; brief dips seldom warn.<br /><b>Quiet</b>: a stop warns in about
      30 minutes. A wide range, so only a deep drop warns; brief dips almost never do.<br />A source
      starts at Balanced and its senders at Quiet, since every sender's dips warn on their own.</Help
    >
  </h3>
  {#if editor}
    <Segmented {label} options={PRESETS} bind:value={() => value, (preset) => pick(preset)} />
  {:else}
    <span class="dim small">{PRESETS.find((p) => p.value === value)?.label}</span>
  {/if}
</div>

<style>
  .preset {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.3rem 0.6rem;
  }

  h3 {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    margin: 0;
    font-size: var(--text-body);
    font-weight: 600;
  }

  .small {
    font-size: var(--text-label);
  }
</style>
