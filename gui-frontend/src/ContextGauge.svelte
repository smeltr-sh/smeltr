<!--
  How full the model's context was on a conversation's last request, as a circle that fills, beside the model
  chip. Pressing it says how much in tokens and when the older turns are summarized, and offers to summarize them
  now or clear them.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';

  let {
    used,
    window: size,
    busy = false,
    onrestart,
  }: {
    used: number;
    window: number;
    /** While a turn runs, when neither may be asked. */
    busy?: boolean;
    /** Begin the agent's reading again, from a summary or from nothing. */
    onrestart: (summarize: boolean) => void;
  } = $props();

  function restart(summarize: boolean) {
    open = false;
    onrestart(summarize);
  }

  /** Where the leader summarizes the older turns (`model::compact_at`). */
  const COMPACT_AT = 0.75;
  const RADIUS = 7;
  const AROUND = 2 * Math.PI * RADIUS;

  let open = $state(false);
  const share = $derived(Math.min(1, used / size));
  const percent = $derived(Math.round(share * 100));
  const tokens = (n: number) => n.toLocaleString();
</script>

<ChipPicker bind:open kind="row" compact ariaLabel="Context {percent}% full">
  {#snippet face()}
    <svg viewBox="0 0 18 18" width="18" height="18" aria-hidden="true">
      <circle class="track" cx="9" cy="9" r={RADIUS} />
      <circle
        class="fill"
        class:near={share >= COMPACT_AT - 0.1}
        cx="9"
        cy="9"
        r={RADIUS}
        stroke-dasharray="{share * AROUND} {AROUND}"
        transform="rotate(-90 9 9)"
      />
    </svg>
  {/snippet}

  <div class="details">
    <div class="pick-note">
      <strong>Context {percent}% full</strong><br />
      {tokens(used)} of {tokens(size)} tokens. Older turns are summarized at 75% full.
    </div>
    <button type="button" class="pick" disabled={busy} onclick={() => restart(true)}>
      Summarize now
    </button>
    <button type="button" class="pick" disabled={busy} onclick={() => restart(false)}>Clear</button>
  </div>
</ChipPicker>

<style>
  .track,
  .fill {
    fill: none;
    stroke-width: 2.5;
  }
  .track {
    stroke: var(--border);
  }
  .fill {
    stroke: var(--accent);
    stroke-linecap: round;
  }
  .fill.near {
    stroke: var(--error);
  }
  .details {
    width: 18rem;
  }
</style>
