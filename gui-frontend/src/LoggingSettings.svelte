<!--
  The levels the fleet keeps its log lines at (`leader::log_levels`): a default for every line, and overrides, each
  for a class, a class and a type, or a service. The leader keeps a change at once, and each worker from its next
  heartbeat.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import Panel from './Panel.svelte';
  import Field from './Field.svelte';
  import CrossButton from './CrossButton.svelte';
  import Button, { BUSY } from './Button.svelte';
  import InlineSaveBar from './InlineSaveBar.svelte';
  import {
    getLogLevels,
    putLogLevels,
    type LevelOverrideDto,
    type LogLevelsDto,
    type LogLevelsResult,
  } from './api';

  /** An absent default, shown as a choice of its own: each process keeps its startup log level. */
  const STARTUP = '';

  /** What an override names, as one value a select can hold: `class:sources` or `service:kafka`. */
  type Named = string;

  let saved = $state<LogLevelsDto | null>(null);
  let labels = $state<string[]>([]);
  let classes = $state<string[]>([]);
  let services = $state<string[]>([]);
  let adding = $state<Named>('');
  let addingType = $state('');
  let addingLevel = $state('trace');
  let defaultShown = $state(STARTUP);
  let rows = $state<LevelOverrideDto[]>([]);
  let busy = $state(false);
  let message = $state('');

  function adopt(result: LogLevelsResult) {
    if (!result.levels) return;
    saved = result.levels;
    labels = result.labels;
    if (result.classes.length > 0) classes = result.classes;
    if (result.services.length > 0) services = result.services;
    defaultShown = result.levels.default ?? STARTUP;
    rows = (result.levels.overrides ?? []).map((o) => ({ ...o }));
  }

  /** What `o` names, one text for each thing an override can name. */
  const key = (o: LevelOverrideDto) => `${o.class ?? ''}|${o.type ?? ''}|${o.service ?? ''}`;

  /** The override the add row makes. */
  const added = $derived.by((): LevelOverrideDto | null => {
    const [kind, name] = adding.split(':');
    if (!name) return null;
    if (kind === 'service') return { service: name, level: addingLevel };
    const type = addingType.trim();
    return { class: name, ...(type && { type }), level: addingLevel };
  });

  /** What the form says, as the leader stores it. */
  const shown = $derived.by((): LogLevelsDto => ({
    ...(defaultShown !== STARTUP && { default: defaultShown }),
    ...(rows.length > 0 && { overrides: rows.map((o) => ({ ...o })) }),
  }));

  /** Levels as one comparable text: the order overrides were added in is not a change. */
  const same = (l: LogLevelsDto) =>
    JSON.stringify([l.default, (l.overrides ?? []).map((o) => `${key(o)}=${o.level}`).sort()]);
  const dirty = $derived(saved !== null && same(saved) !== same(shown));

  // A message about the last save says nothing once the form changes again.
  $effect(() => {
    if (dirty) message = '';
  });

  $effect(() => {
    getLogLevels().then(adopt).catch(reported);
  });

  async function save(event: SubmitEvent) {
    event.preventDefault();
    if (busy) return;
    busy = true;
    message = '';
    try {
      const result = await putLogLevels(shown);
      if (!result.ok) return;
      adopt(result);
      message = 'saved';
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }
</script>

{#if saved}
  <Panel title="Log levels" onsubmit={save}>
    <Field label="Default level">
      {#snippet help()}
        The level every log line keeps unless an override names it. Choosing "startup log level"
        leaves each process at the level it started with: <code>SMELTR_LOG_LEVEL</code> or the
        top-level
        <code>log_level</code>, or info if neither is set.
      {/snippet}
      <select bind:value={defaultShown}>
        <option value={STARTUP}>startup log level</option>
        {#each labels as label (label)}
          <option value={label}>{label}</option>
        {/each}
      </select>
    </Field>

    <Field label="Overrides" group optional>
      {#snippet help()}
        A log line has a class, the part of Smeltr it is about, and a type, what happened. A line
        that talks to an outside service, such as kafka, names that service too. An override keeps a
        class, a class and one type, or a service at its own level. A line takes the first override
        that names it: its class and type, then its service, then its class.
      {/snippet}
      <table>
        <thead><tr><th>For</th><th>Level</th><th></th></tr></thead>
        <tbody>
          {#each rows as row, i (key(row))}
            {@const named = row.service ?? [row.class, row.type].filter(Boolean).join(' ')}
            <tr>
              <td>
                {#if row.service}
                  service <code>{row.service}</code>
                {:else}
                  class <code>{row.class}</code>{#if row.type}, type <code>{row.type}</code>{/if}
                {/if}
              </td>
              <td>
                <select bind:value={row.level} aria-label="level for {named}">
                  {#each labels as label (label)}<option value={label}>{label}</option>{/each}
                </select>
              </td>
              <td
                ><CrossButton
                  removes
                  label="Remove override"
                  onclick={() => rows.splice(i, 1)}
                /></td
              >
            </tr>
          {/each}
          <tr>
            <td class="for">
              <select bind:value={adding} aria-label="class or service">
                <option value="" disabled>choose…</option>
                <optgroup label="Classes">
                  {#each classes as c (c)}<option value="class:{c}">{c}</option>{/each}
                </optgroup>
                <optgroup label="Services">
                  {#each services as s (s)}<option value="service:{s}">{s}</option>{/each}
                </optgroup>
              </select>
              {#if adding.startsWith('class:')}
                <input bind:value={addingType} placeholder="type (optional)" aria-label="type" />
              {/if}
            </td>
            <td>
              <select bind:value={addingLevel} aria-label="level">
                {#each labels as label (label)}<option value={label}>{label}</option>{/each}
              </select>
            </td>
            <td>
              <Button
                type="button"
                disabled={added === null
                  ? 'Choose a class or a service first.'
                  : rows.some((r) => key(r) === key(added))
                    ? 'An override already names this.'
                    : ''}
                onclick={() => {
                  if (added) rows.push(added);
                  adding = '';
                  addingType = '';
                  addingLevel = 'trace';
                }}>Add</Button
              >
            </td>
          </tr>
        </tbody>
      </table>
    </Field>

    {#if message}
      <p class="message" role="status">{message}</p>
    {/if}
    <InlineSaveBar
      {dirty}
      label="Save log levels"
      disabled={busy ? BUSY : ''}
      ondiscard={() =>
        saved && adopt({ ok: true, errors: [], levels: saved, classes: [], services: [], labels })}
    />
  </Panel>
{/if}

<style>
  p {
    margin: 0;
  }

  table {
    border-collapse: collapse;
    width: max-content;
  }

  th {
    text-align: left;
    font-weight: 600;
    padding: 0 1.2rem 0.3rem 0;
  }

  td {
    padding: 0.25rem 1.2rem 0.25rem 0;
    border-top: 1px solid var(--border);
  }

  .for {
    display: flex;
    gap: 0.5rem;
  }
</style>
