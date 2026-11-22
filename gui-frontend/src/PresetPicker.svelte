<!--
  The preset a source, or its senders, is judged against its normal at (`throughput::Preset`): an
  editor picks one, and anyone else reads it.
-->
<script lang="ts">
  import { ask } from './Confirm.svelte';
  import Help from './Help.svelte';
  import Segmented from './Segmented.svelte';
  import type { Preset } from './api';

  let {
    value,
    label,
    of,
    editor,
    pick,
  }: {
    value: Preset;
    /** What it is the preset of, for a screen reader. */
    label: string;
    /** Whose stop the preset warns of: the source's, or each of its senders'. */
    of: 'source' | 'sender';
    editor: boolean;
    pick: (preset: Preset) => void;
  } = $props();

  const PRESETS: { value: Preset; label: string }[] = [
    { value: 'fast', label: 'Fast' },
    { value: 'sensitive', label: 'Sensitive' },
    { value: 'balanced', label: 'Balanced' },
    { value: 'quiet', label: 'Quiet' },
    { value: 'off', label: 'Off' },
  ];
  /** About how long after a stop each preset warns: its wait in 5-minute readings (`throughput::Preset`). */
  const STOP_MINUTES: Record<Preset, number | null> = {
    fast: 5,
    sensitive: 15,
    balanced: 20,
    quiet: 30,
    off: null,
  };

  const named = (preset: Preset) => PRESETS.find((p) => p.value === preset)?.label ?? preset;

  /** A pick changes when stops warn, so it is asked first: one click must not change it unseen. */
  async function choose(to: Preset) {
    if (to === value) return;
    const [now, was] = [STOP_MINUTES[to], STOP_MINUTES[value]];
    const what =
      now === null
        ? `a ${of} that stops never warns`
        : `a ${of} warns about ${now} minutes after it stops${was === null ? '' : `, instead of about ${was}`}`;
    const ok = await ask({
      title: of === 'source' ? "change the source's preset" : "change the senders' preset",
      body: `From ${named(value)} to ${named(to)}: ${what}.`,
      action: `Change to ${named(to)}`,
    });
    if (ok) pick(to);
  }
</script>

<div class="preset">
  <h3>
    Preset
    <Help label="Preset"
      >A source warns when its rows stay below their expected range for a set number of 5-minute
      readings in a row. A preset sets how wide that range is and how many readings it waits. A stop
      reads below any range, so only the wait sets how soon it warns.<br /><b>Fast</b>: a stop warns
      in about 5 minutes. A wide range, so only a deep drop warns, and brief dips warn most often.<br
      /><b>Sensitive</b>: a stop warns in about 15 minutes. The narrowest range, so it sees the
      smallest drops; brief dips warn often.<br /><b>Balanced</b>: a stop warns in about 20 minutes.
      A narrow range; brief dips seldom warn.<br /><b>Quiet</b>: a stop warns in about 30 minutes. A
      wide range, so only a deep drop warns; brief dips almost never do.<br /><b>Off</b>: a stop
      never warns. The range is still drawn, and what is read is learned as normal, so a lasting
      drop becomes the normal.<br />A source starts at Balanced and its senders at Quiet, since each
      sender warns on its own.</Help
    >
  </h3>
  {#if editor}
    <Segmented
      {label}
      options={PRESETS}
      bind:value={() => value, (preset) => void choose(preset)}
    />
  {:else}
    <span class="dim small">{named(value)}</span>
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
