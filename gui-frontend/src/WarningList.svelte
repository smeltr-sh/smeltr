<!--
  The below normal warnings of a source's past year, or of one of its sender's, newest first, the
  one that holds first of all (`throughput::Past`). Each says whether it was normal or an outage,
  which an editor changes, and folds open on the rows around it against the range. The one that
  holds is lowered to info here too.
-->
<script lang="ts">
  import Button, { BUSY } from './Button.svelte';
  import ChevronIcon from './ChevronIcon.svelte';
  import RowsChart from './RowsChart.svelte';
  import Segmented from './Segmented.svelte';
  import StateMark from './StateMark.svelte';
  import {
    lowerSender,
    lowerSourceRows,
    markWarning,
    type ApiError,
    type PastSpan,
    type Sender,
  } from './api';
  import type { Busy } from './busy.svelte';
  import { dateTime, lasted } from './time';

  let {
    group,
    source,
    sender,
    spans,
    editor,
    busy,
    changed,
  }: {
    group: string;
    source: string;
    sender?: Sender;
    spans: PastSpan[];
    editor: boolean;
    /** One change at a time, shared with the rest of the tab. */
    busy: Busy;
    /** Reads the warnings again after a change, with what the leader refused, if anything. */
    changed: (errors: ApiError[]) => Promise<void>;
  } = $props();

  const WAS: { value: 'normal' | 'outage'; label: string }[] = [
    { value: 'normal', label: 'Normal' },
    { value: 'outage', label: 'Outage' },
  ];
  const HOUR = 60 * 60_000;
  let open = $state<number | null>(null);
  let label = $derived(sender ? 'Sender below normal' : 'Rows below normal');

  function change(act: () => Promise<{ ok: boolean; errors: ApiError[] }>) {
    // One change at a time, each read back before the next, so the list never shows an older answer last.
    if (busy.on) return;
    void busy.run(async () => {
      const answer = await act();
      await changed(answer.ok ? [] : answer.errors);
    });
  }

  function mark(span: PastSpan, was: 'normal' | 'outage') {
    change(() => markWarning(group, source, span.fromMillis, was, sender));
  }

  function lower() {
    change(() => (sender ? lowerSender(group, source, sender) : lowerSourceRows(group, source)));
  }

  /** The rows around `span`: the last day while it holds, else from 12 hours before it until 12
   * hours after, or until now. */
  function around(span: PastSpan) {
    const [from, to] = span.holds
      ? [span.toMillis - 24 * HOUR, span.toMillis]
      : [span.fromMillis - 12 * HOUR, Math.min(span.toMillis + 12 * HOUR, Date.now())];
    return {
      from,
      to,
      left: span.holds ? '24 hours ago' : dateTime(from / 1000),
      right: span.holds ? 'now' : dateTime(to / 1000),
    };
  }
</script>

{#if spans.length === 0}
  <p class="dim small none">None in the past year.</p>
{:else}
  <ul class="list">
    {#each spans as span (span.fromMillis)}
      {@const isOpen = open === span.fromMillis}
      {@const when = dateTime(span.fromMillis / 1000)}
      <li>
        <div class="row">
          <button
            type="button"
            class="fold"
            aria-expanded={isOpen}
            onclick={() => (open = isOpen ? null : span.fromMillis)}
          >
            <ChevronIcon open={isOpen} />
            <span class="up"
              >{#if span.holds}<StateMark state="warning" label="up now" />{/if}</span
            >
            <span class="mono when"
              ><span class="nowrap">{when} &middot;</span>
              <span class="nowrap">{lasted(span.toMillis - span.fromMillis)}</span></span
            >
          </button>
          <span class="acts">
            {#if span.holds && span.loweredBy}
              <span class="dim small">Lowered to info by {span.loweredBy}</span>
            {:else if span.holds && editor}
              <Button class="compact" type="button" disabled={busy.on ? BUSY : ''} onclick={lower}
                >Lower to info</Button
              >
            {/if}
            {#if editor}
              <Segmented
                label="Warnings: {when}"
                options={WAS}
                bind:value={() => (span.learned ? 'normal' : 'outage'), (was) => mark(span, was)}
              />
            {:else}
              <span class="dim small">{span.learned ? 'Normal' : 'Outage'}</span>
            {/if}
          </span>
        </div>
        {#if isOpen}
          {@const a = around(span)}
          <div class="detail">
            <RowsChart
              {group}
              {source}
              {sender}
              fromMillis={a.from}
              toMillis={a.to}
              marks={[
                {
                  from: span.fromMillis,
                  to: span.holds ? null : span.toMillis,
                  label,
                  failed: false,
                },
              ]}
              left={a.left}
              right={a.right}
            />
          </div>
        {/if}
      </li>
    {/each}
  </ul>
{/if}

<style>
  .list {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .list li {
    padding: 0.45rem 0;
    border-top: 1px solid var(--border);
  }

  .list li:first-child {
    border-top: 0;
  }

  .row {
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    gap: 0.4rem 0.6rem;
    font-size: var(--text-label);
  }

  .fold {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem;
    min-width: 0;
    padding: 0;
    border: 0;
    background: none;
    color: var(--ink);
    font: inherit;
    text-align: left;
    cursor: pointer;
  }

  .up {
    display: inline-flex;
    width: 0.9rem;
  }

  .acts {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: flex-end;
    gap: 0.5rem;
    margin-left: auto;
  }

  .detail {
    margin: 0.5rem 0 0.3rem 1.35rem;
  }

  .detail :global(.sparkline svg) {
    height: 110px;
  }

  .none {
    margin: 0;
  }

  .small {
    font-size: var(--text-label);
  }

  .nowrap {
    white-space: nowrap;
  }
</style>
