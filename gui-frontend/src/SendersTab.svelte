<!--
  A source's senders (`docs/architecture/control-plane.md`): each field of its
  records that may name who sends them, what the pick read of it, and folded under it the senders it
  watches, each against its own normal over the last day. An editor stops watching a field, adds one
  the pick cannot offer, stops watching fields that come later, and lowers a sender's warning to info.
-->
<script lang="ts">
  import Button, { BUSY } from './Button.svelte';
  import ChevronIcon from './ChevronIcon.svelte';
  import Help from './Help.svelte';
  import Sparkline from './Sparkline.svelte';
  import StateMark from './StateMark.svelte';
  import { holds } from './access';
  import {
    errorMessage,
    errorText,
    getSourceSenders,
    lowerSender,
    putSenderFields,
    type FieldNowDto,
    type SenderNowDto,
  } from './api';
  import { Busy } from './busy.svelte';
  import { expectedSlots, perSecond, slotTimes } from './focus';
  import { count } from './time';
  import { untrack } from 'svelte';

  let { group, source, nowMillis }: { group: string; source: string; nowMillis?: number } =
    $props();

  const FIVE_MINUTES = 5 * 60_000;
  const DAY = 24 * 60 * 60_000;
  /** The senders a field shows before Show all: those below normal come first. */
  const SHOWN = 6;
  const HELP =
    'Fields whose values name who sends this source its rows. Each value that sent steadily for a week is watched, and warns when it sends below its normal.';

  let fields = $state<FieldNowDto[] | null>(null);
  let watchesNew = $state(true);
  let problem = $state('');
  /** Folded open or closed by hand; a field with a sender below normal starts open. */
  let folded = $state<Record<string, boolean>>({});
  let showAll = $state<Record<string, boolean>>({});
  let adding = $state('');
  const busy = new Busy();
  let editor = $derived(holds(group, 'editor'));
  let times = $derived(slotTimes(FIVE_MINUTES, DAY, nowMillis ?? Date.now()));

  function read() {
    const asked = { group, source };
    const current = () => asked.group === group && asked.source === source;
    return getSourceSenders(asked.group, asked.source)
      .then((answer) => {
        if (!current()) return;
        fields = answer.ok ? answer.fields : null;
        watchesNew = answer.ok ? answer.watchesNew : true;
        problem = answer.ok ? '' : errorText(answer.errors);
      })
      .catch((e) => {
        if (current()) problem = errorMessage(e);
      });
  }

  $effect(() => {
    void [group, source];
    untrack(() => {
      fields = null;
      problem = '';
    });
    read();
  });

  type Edited = Pick<FieldNowDto, 'field' | 'checked' | 'added' | 'new'>;

  /** Saves the source's fields as `change` leaves them, whole, then reads them back. Once new fields
   * are not watched, every field watched but not added stays watched. */
  function edit(change: (fields: Edited[], lock: { watchesNew: boolean }) => Edited[] | void) {
    if (busy.on || !fields) return;
    const lock = { watchesNew };
    let edited: Edited[] = fields.map(({ field, checked, added, new: arrived }) => ({
      field,
      checked,
      added,
      new: arrived,
    }));
    edited = change(edited, lock) ?? edited;
    const edits = {
      unchecked: edited.filter((f) => !f.checked && !f.new).map((f) => f.field),
      added: edited.filter((f) => f.added).map((f) => f.field),
      locked: lock.watchesNew
        ? null
        : edited.filter((f) => f.checked && !f.added).map((f) => f.field),
    };
    void busy.run(async () => {
      const answer = await putSenderFields(group, source, edits);
      problem = answer.ok ? '' : errorText(answer.errors);
      await read();
    });
  }

  function watch(field: string, on: boolean) {
    edit((all) => {
      for (const f of all.filter((f) => f.field === field)) {
        f.checked = on;
        f.new = false;
      }
    });
  }

  function remove(field: string) {
    edit((all) => all.filter((f) => f.field !== field));
  }

  function add(event: SubmitEvent) {
    event.preventDefault();
    const field = adding.trim();
    if (!field) return;
    edit((all) => [
      ...all.filter((f) => f.field !== field),
      { field, checked: true, added: true, new: false },
    ]);
    adding = '';
  }

  function watchNew(on: boolean) {
    edit((_, lock) => {
      lock.watchesNew = on;
    });
  }

  function lower(field: string, sender: SenderNowDto) {
    if (busy.on) return;
    void busy.run(async () => {
      const answer = await lowerSender(group, source, { field, value: sender.value });
      problem = answer.ok ? '' : errorText(answer.errors);
      await read();
    });
  }

  function below(f: FieldNowDto): number {
    return f.senders.filter((s) => s.below).length;
  }

  function isOpen(f: FieldNowDto): boolean {
    return f.senders.length > 0 && (folded[f.field] ?? below(f) > 0);
  }

  /** What the pick read of a field, as its row says it. */
  function picked(f: FieldNowDto): string {
    if (f.new) return 'New';
    if (!f.checked) return 'Not watched';
    switch (f.pick?.state) {
      case 'watched':
        return f.senders.length === 0 ? 'No steady senders' : count(f.senders.length, 'sender');
      case 'learning':
        return `Learning until ${new Date(f.pick.untilMillis).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`;
      case 'oneValue':
        return 'One value';
      case 'tooMany':
        return 'Over 256 values';
      default:
        return 'No values yet';
    }
  }

  function points(s: SenderNowDto): (number | null)[] {
    const at = new Map(s.points);
    return times.map((t) => at.get(t) ?? null);
  }
</script>

<section>
  <div class="head">
    <h3>Fields <Help label="Fields">{HELP}</Help></h3>
    {#if fields}
      <label class="small"
        ><input
          type="checkbox"
          checked={watchesNew}
          disabled={!editor || busy.on}
          onchange={(e) => watchNew(e.currentTarget.checked)}
        /> Watch new fields</label
      >
    {/if}
  </div>
  {#if problem}<p class="problem">{problem}</p>{/if}
  {#if !fields}
    {#if !problem}<p class="dim">Reading&hellip;</p>{/if}
  {:else if fields.length === 0}
    <p class="dim small">No field of this source's records has been counted yet.</p>
  {:else}
    <ul class="fields">
      {#each fields as f (f.field)}
        {@const open = isOpen(f)}
        {@const n = below(f)}
        <li>
          <div class="field" class:open>
            <input
              type="checkbox"
              aria-label="Watch {f.field}"
              checked={f.checked}
              disabled={!editor || busy.on}
              onchange={(e) => watch(f.field, e.currentTarget.checked)}
            />
            {#if f.senders.length > 0}
              <button
                type="button"
                class="fold"
                aria-expanded={open}
                onclick={() => (folded[f.field] = !open)}
                ><ChevronIcon {open} /> <span class="mono">{f.field}</span></button
              >
            {:else}
              <span class="mono name" class:dim={!f.checked}>{f.field}</span>
            {/if}
            <span class="dim">{picked(f)}</span>
            {#if n > 0}<span class="warn"><StateMark state="warning" /> {n} below</span>{/if}
            {#if f.added && editor}
              <Button
                type="button"
                class="push"
                disabled={busy.on ? BUSY : ''}
                onclick={() => remove(f.field)}>Remove</Button
              >
            {/if}
          </div>
          {#if open}
            {@const shown = showAll[f.field] ? f.senders : f.senders.slice(0, SHOWN)}
            <table class="readings senders">
              <thead>
                <tr
                  ><th>Sender</th><th class="chart-col">Last 24 hours</th><th class="num">Rows/s</th
                  ><th class="num">Expected</th><th></th></tr
                >
              </thead>
              <tbody>
                {#each shown as s (s.value)}
                  <tr>
                    <td class="mono value"
                      >{#if s.below}<StateMark state="warning" label="below normal" />{/if}
                      {s.value}</td
                    >
                    <td class="chart-col">
                      <Sparkline
                        compact
                        series={[
                          {
                            label: `${s.value} rows/s`,
                            color: s.below ? 'var(--warn)' : 'var(--ink)',
                            points: points(s),
                          },
                        ]}
                        band={expectedSlots(s.expected, times)}
                        {times}
                      />
                    </td>
                    <td class="num mono" class:warn={s.below} data-label="Rows/s"
                      >{s.now === null ? '—' : perSecond(s.now)}</td
                    >
                    <td class="num" data-label="Expected"
                      >{#if s.low !== null && s.high !== null}{perSecond(s.low)} to {perSecond(
                          s.high,
                        )}{/if}</td
                    >
                    <td class="num">
                      {#if s.below && s.loweredBy}
                        <span class="dim small">Lowered to info by {s.loweredBy}</span>
                      {:else if s.below && editor}
                        <Button
                          type="button"
                          disabled={busy.on ? BUSY : ''}
                          onclick={() => lower(f.field, s)}>Lower to info</Button
                        >
                      {/if}
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
            {#if shown.length < f.senders.length}
              <p class="more dim small">
                Showing {shown.length} of {f.senders.length}
                <Button type="button" onclick={() => (showAll[f.field] = true)}>Show all</Button>
              </p>
            {/if}
          {/if}
        </li>
      {/each}
    </ul>
  {/if}
  {#if editor && fields}
    <form class="add" onsubmit={add}>
      <input placeholder="Field" aria-label="Field" bind:value={adding} />
      <Button type="submit" disabled={busy.on ? BUSY : adding.trim() ? '' : 'Name a field first.'}
        >Add field</Button
      >
    </form>
  {/if}
</section>

<style>
  section {
    padding: 0.85rem 0;
  }

  .head {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 0.3rem 0.6rem;
    margin: 0 0 0.5rem;
  }

  .head label {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
  }

  h3 {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    margin: 0;
    font-size: var(--text-body);
    font-weight: 600;
  }

  .fields {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .fields > li {
    border-top: 1px solid var(--border);
  }

  .fields > li:first-child {
    border-top: 0;
  }

  .field {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.3rem 0.6rem;
    padding: 0.5rem 0;
    font-size: var(--text-label);
  }

  .field.open {
    background: var(--panel);
  }

  .fold {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0;
    border: 0;
    background: none;
    color: var(--ink);
    font: inherit;
    cursor: pointer;
  }

  .name {
    padding-left: 1.35em;
  }

  .warn {
    color: var(--warn);
  }

  .field :global(.push) {
    margin-left: auto;
  }

  .senders {
    margin: 0.2rem 0 0.4rem 1.6rem;
    width: calc(100% - 1.6rem);
  }

  .value {
    white-space: nowrap;
  }

  .chart-col {
    width: 45%;
  }

  /* A narrow panel has no room for a day's chart beside each sender. */
  @container (max-width: 32rem) {
    .chart-col {
      display: none;
    }
  }

  .more {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin: 0 0 0.6rem 1.6rem;
  }

  .add {
    display: flex;
    gap: 0.6rem;
    padding-top: 0.85rem;
    border-top: 1px solid var(--border);
  }

  .add input {
    flex: 1;
    max-width: 24rem;
  }

  .small {
    font-size: var(--text-label);
  }

  .problem {
    color: var(--error);
  }
</style>
