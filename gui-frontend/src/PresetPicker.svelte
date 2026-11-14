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
  const HELP =
    'How soon a drop warns, against how often a brief dip warns too. A stop warns in about 5 minutes at Fast, 15 at Sensitive, 20 at Balanced and 30 at Quiet, and the sooner it warns, the more dips warn. A source starts at Balanced and its senders at Quiet, since each sender warns on its own.';
</script>

<div class="preset">
  <h3>Preset <Help label="Preset">{HELP}</Help></h3>
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
