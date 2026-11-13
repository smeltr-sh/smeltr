<!--
  A capture, from the moment it starts to the moment it is saved or discarded.

  Its own dialog, because its body moves every second and its buttons change
  meaning: while the capture runs there is one thing to do, stop it, and nothing
  to refuse yet; once it is done, stopped or not, the question is the usual one
  -- keep this, or not -- with the name as the field that answers it.
  STYLE_GUIDE.md "Dialogs".

  Both ends show what they hold the same way: the records in the grid a pipeline's preview
  draws them in.
-->
<script lang="ts">
  import Button, { BUSY, INCOMPLETE } from './Button.svelte';
  import RecordsGrid, { type GridRow, type LeadColumn } from './RecordsGrid.svelte';
  import InlineError from './InlineError.svelte';
  import Dialog from './Dialog.svelte';
  import Figure from './Figure.svelte';
  import Figures from './Figures.svelte';
  import { heldCount, heldRows, progressText, shownRecords, workerChips } from './capture';
  import { captureDownloadUrl, captureEndText, type CaptureStatus } from './api';
  import { NOT_IN_DEMO } from './access';

  let {
    status,
    session,
    raw = false,
    source,
    destination,
    lost = false,
    busy,
    error,
    onstop,
    onsave,
    use,
    ondiscard,
  }: {
    /** `null` until the leader has answered the first poll. */
    status: CaptureStatus | null;
    /** The capture's id, which its download names. */
    session?: string;
    /** It holds what senders wrote as sent, which the dialog warns of before it is kept. */
    raw?: boolean;
    /** The source's id, for the header and the default name. */
    source?: string;
    /** The destination's id, in place of `source`. */
    destination?: string;
    /** No session is left to stop: it never started, or the leader forgot it. */
    lost?: boolean;
    busy: boolean;
    error: string;
    onstop: () => void;
    /** Saves it as a sample named `name`; absent for a viewer, who may not save one. */
    onsave?: (name: string) => void;
    /** The primary action on a finished capture, and its label. One with a `title` is a form's own purpose for it,
     * which names the dialog. */
    use?: { title?: string; label: string; take: () => void; off?: string };
    /** Discard: the session goes. */
    ondiscard: () => void;
  } = $props();

  // Read once: the source is fixed for the session, and a default
  // name is a starting point rather than something that follows the source.
  // svelte-ignore state_referenced_locally
  let saveName = $state(`${source ?? destination}, captured`);
  /** The worker whose records the tables show, or none for every worker. Save
   * takes every held record whichever is chosen. */
  let only = $state<string | null>(null);

  /** Nothing more to wait for: the capture ended, or there is none. */
  let done = $derived((status?.done ?? false) || lost);
  let chips = $derived(status ? workerChips(status) : []);
  let shown = $derived(status ? shownRecords(status, only) : []);
  let held = $derived(status ? heldCount(status) : 0);
  /** Each shown record as a row of the grid, the same on both sides: a capture changes nothing. */
  let records = $derived(heldRows(shown, status?.columns ?? []));
  let rows = $derived<GridRow[]>(
    records.map((r, i) => ({
      key: i,
      label: String(i),
      before: r.record,
      after: r.record,
      dropped: false,
      exceptions: r.exceptions.map((message) => ({ message })),
    })),
  );
  const leads: LeadColumn[] = [
    { name: 'Worker', cell: (row) => ({ text: records[row.key]?.worker ?? '' }) },
  ];

  function pick(worker: string) {
    only = only === worker ? null : worker;
  }

  function reason(worker: string): string | null {
    const end = status?.ended.find(([w]) => w === worker);
    return end ? captureEndText(end[1]) : null;
  }
</script>

<Dialog
  open
  wide
  title={use?.title ??
    (destination !== undefined
      ? `${done ? 'Captured' : 'Capturing'} at ${destination}`
      : `${done ? 'Captured' : 'Capturing'} from ${source}`)}
  oncancel={() => {
    // Escape refuses, and while the capture runs there is nothing to refuse yet.
    if (done && !busy) ondiscard();
  }}
  onsubmit={() => {
    if (!done) onstop();
    else if (use) use.take();
    else onsave?.(saveName.trim());
  }}
>
  <div class="content">
    {#if !status}
      {#if !lost}<p class="dim">Waiting for the first worker to pick this up&hellip;</p>{/if}
    {:else}
      <Figures>
        <Figure label="Records">
          {held} <span class="of">of {status.maxMatches}</span>
          {#snippet after()}
            <div class="meter" aria-hidden="true">
              <div
                class="meter-fill"
                style:width="{Math.min(100, (100 * held) / status.maxMatches)}%"
              ></div>
            </div>
          {/snippet}
        </Figure>
        <Figure label={done ? 'Ended' : 'Time left'}>
          {#if status.stopped}stopped{:else if done}finished{:else}{status.secondsLeft}s{/if}
        </Figure>
      </Figures>

      <p class="dim">{progressText(status)}</p>

      {#if chips.length > 0}
        <div class="chips">
          {#each chips as chip (chip.worker)}
            <button
              type="button"
              class="chip {chip.state}"
              class:on={only === chip.worker}
              aria-pressed={only === chip.worker}
              title={reason(chip.worker) ?? chip.state}
              onclick={() => pick(chip.worker)}
            >
              <span class="dot" aria-hidden="true"></span>
              <span class="mono">{chip.worker}</span>
              <span class="count">{chip.count}</span>
            </button>
          {/each}
          <span class="hint">
            {only ? 'click again to show all' : 'click a worker to show only its records'}
          </span>
        </div>
      {/if}

      {#if only && reason(only)}
        <p class="note"><span class="mono">{only}</span>: {reason(only)}</p>
      {:else}
        {#each status.ended as [worker, why] (worker)}
          {#if typeof why !== 'string'}
            <p class="note failed"><span class="mono">{worker}</span>: {captureEndText(why)}</p>
          {/if}
        {/each}
      {/if}

      {#if status.truncated}
        <p class="note">
          This capture reached the ceiling on what one session may hold, and stopped taking records
          there.
        </p>
      {/if}

      {#if held > 0}
        <RecordsGrid {rows} remember="capture" openOn={done} {leads} />
      {/if}

      {#if done && raw && held > 0}
        <p class="critical">
          These are the exact bytes senders wrote, so they may hold passwords or tokens. Check them
          before you save or share them.
        </p>
      {/if}
      {#if done && onsave}
        <div class="name" class:off={held === 0}>
          <label for="capture-name">Save as</label>
          <input
            id="capture-name"
            bind:value={saveName}
            disabled={held === 0}
            onkeydown={(e) => {
              // Enter here saves, as Save sample would, though the window's primary may be another.
              if (e.key !== 'Enter' || e.isComposing || !use) return;
              e.preventDefault();
              if (!busy && saveName.trim()) onsave?.(saveName.trim());
            }}
          />
        </div>
      {/if}
    {/if}

    <InlineError message={error} />
  </div>

  {#snippet actions()}
    {#if done}
      <Button type="button" onclick={ondiscard} disabled={busy ? BUSY : ''}>Discard</Button>
      {#if session && held > 0}
        {#if NOT_IN_DEMO}<Button type="button" disabled={NOT_IN_DEMO}>Download</Button>{:else}<a
            class="btn"
            href={captureDownloadUrl(session)}
            download>Download</a
          >{/if}
      {/if}
      {#if onsave}
        <Button
          type={use ? 'button' : 'submit'}
          class={use ? '' : 'primary'}
          onclick={use ? () => onsave(saveName.trim()) : undefined}
          data-guide="capture-save"
          disabled={busy
            ? BUSY
            : !status
              ? 'Wait for the capture to start.'
              : held === 0
                ? 'Nothing has been captured yet.'
                : !saveName.trim()
                  ? INCOMPLETE
                  : ''}
        >
          {busy ? 'Saving…' : 'Save sample'}
        </Button>
      {/if}
      {#if use}
        <Button
          type="submit"
          class="primary"
          disabled={busy ? BUSY : !status ? 'Wait for the capture to start.' : (use.off ?? '')}
          >{use.label}</Button
        >
      {/if}
    {:else}
      <Button
        type="submit"
        class="primary"
        data-guide="capture-stop"
        disabled={busy ? BUSY : !status ? 'Wait for the capture to start.' : ''}
        >Stop capture</Button
      >
    {/if}
  {/snippet}
</Dialog>

<style>
  .content {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }
  p {
    margin: 0;
    font-size: var(--text-body);
    line-height: 1.5;
  }

  .of {
    font-size: var(--text-label);
    font-weight: 400;
    color: var(--dim);
  }
  .meter {
    height: 4px;
    margin-top: 0.3rem;
    border-radius: 2px;
    background: var(--border);
    overflow: hidden;
  }
  .meter-fill {
    height: 100%;
    background: var(--accent);
  }

  .chips {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.35rem;
  }
  .chip {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.15rem 0.55rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: none;
    color: var(--ink);
    font: inherit;
    font-size: var(--text-note);
    cursor: pointer;
  }
  .chip .count {
    color: var(--dim);
  }
  /* The dot is the worker's state: hollow while nothing has come, filled once
     records have, and the accent once it has said why it stopped. */
  .dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    border: 1px solid var(--dim);
  }
  .chip.filling .dot {
    background: var(--dim);
  }
  .chip.answered .dot {
    background: var(--accent);
    border-color: var(--accent);
  }
  /* The chosen worker is ink and weight, so its dot keeps saying its state. */
  .chip.on {
    border-color: var(--ink);
    font-weight: 600;
  }
  .hint {
    margin-left: auto;
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .note {
    font-size: var(--text-note);
    color: var(--dim);
  }
  .note.failed {
    color: var(--error);
  }

  .name {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: var(--text-label);
  }
  .name.off {
    opacity: 0.5;
  }
  .name label {
    font-weight: 600;
    color: var(--dim);
    white-space: nowrap;
  }
  .name input {
    flex-grow: 1;
  }
</style>
