<!--
  The levels the fleet keeps its log lines at (`leader::log_levels`): a default for every area, and an override for any
  area kept at its own. The leader keeps a change at once, and each worker from its next heartbeat.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import Panel from './Panel.svelte';
  import Field from './Field.svelte';
  import CrossButton from './CrossButton.svelte';
  import InlineSaveBar from './InlineSaveBar.svelte';
  import { getLogLevels, putLogLevels, type LogLevelsDto, type LogLevelsResult } from './api';

  /** An absent default, shown as a choice of its own: each process keeps its startup log level. */
  const STARTUP = '';

  type Override = { key: string; level: string };

  let saved = $state<LogLevelsDto | null>(null);
  let labels = $state<string[]>([]);
  let areas = $state<string[]>([]);
  let adding = $state('');
  let addingLevel = $state('trace');
  let defaultShown = $state(STARTUP);
  let rows = $state<Override[]>([]);
  let busy = $state(false);
  let message = $state('');

  function adopt(result: LogLevelsResult) {
    if (!result.levels) return;
    saved = result.levels;
    labels = result.labels;
    if (result.areas.length > 0) areas = result.areas;
    defaultShown = result.levels.default ?? STARTUP;
    rows = Object.entries(result.levels.areas ?? {}).map(([key, level]) => ({ key, level }));
  }

  /** What the form says, as the leader stores it. A row with no area, or one an earlier row names, is not written. */
  const shown = $derived.by((): LogLevelsDto => {
    const areas: Record<string, string> = {};
    for (const row of rows) areas[row.key] = row.level;
    return {
      ...(defaultShown !== STARTUP && { default: defaultShown }),
      ...(Object.keys(areas).length > 0 && { areas }),
    };
  });

  /** Levels as one comparable text: the order overrides were added in is not a change. */
  const same = (l: LogLevelsDto) =>
    JSON.stringify([l.default, Object.entries(l.areas ?? {}).sort()]);
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
        The level every area keeps unless an override names it. Choosing "startup log level" leaves
        each process at the level it started with: <code>SMELTR_LOG_LEVEL</code> or the top-level
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
        An area is the part of Smeltr a log line comes from, such as kafka or store. An override
        keeps one area at its own level.
      {/snippet}
      <table>
        <thead><tr><th>Area</th><th>Level</th><th></th></tr></thead>
        <tbody>
          {#each rows as row, i (row.key)}
            <tr>
              <td><code>{row.key}</code></td>
              <td>
                <select bind:value={row.level} aria-label="level for {row.key}">
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
            <td>
              <select bind:value={adding} aria-label="area">
                <option value="" disabled>choose…</option>
                {#each areas.filter((a) => !rows.some((r) => r.key === a)) as area (area)}
                  <option value={area}>{area}</option>
                {/each}
              </select>
            </td>
            <td>
              <select bind:value={addingLevel} aria-label="level">
                {#each labels as label (label)}<option value={label}>{label}</option>{/each}
              </select>
            </td>
            <td>
              <button
                type="button"
                disabled={adding === ''}
                title={adding === '' ? 'Choose an area first.' : undefined}
                onclick={() => {
                  rows.push({ key: adding, level: addingLevel });
                  adding = '';
                  addingLevel = 'trace';
                }}>Add</button
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
      disabled={busy}
      ondiscard={() => saved && adopt({ ok: true, errors: [], levels: saved, areas: [], labels })}
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
</style>
