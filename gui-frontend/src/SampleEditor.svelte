<!--
  A saved sample, edited where its row is on the Samples page: its name, the source it stands
  for, and its records (`sampleEdit.ts`). Ticking records and keeping or removing them, editing
  a record's cells and pasting more all change a draft; Save writes it in one commit under the
  same id, so every test on the sample still finds it, and Cancel leaves no trace.

  An edit is a changed input to every test on the sample, so those are named before the save,
  which goes through whatever they would say: a config that breaks a test is always saved.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import Field from './Field.svelte';
  import CrossButton from './CrossButton.svelte';
  import Clamped from './Clamped.svelte';
  import { SvelteSet } from 'svelte/reactivity';
  import FindBox, { type FindFilter } from './FindBox.svelte';
  import InlineError from './InlineError.svelte';
  import {
    getSample,
    previewPipeline,
    putSample,
    type SampleDto,
    type SampleRecordDto,
  } from './api';
  import { buildFileConfig } from './pipeline';
  import {
    cellMistyped,
    cellText,
    editColumns,
    matches,
    pastedRecords,
    withCell,
    type EditColumn,
  } from './sampleEdit';

  let {
    id,
    tests,
    oncancel,
    onsaved,
  }: {
    id: string;
    /** The names of the tests run over this sample. */
    tests: string[];
    oncancel: () => void;
    onsaved: () => void;
  } = $props();

  // Past this many, the rows a search leaves are shown on asking: a sample holds up to a thousand.
  const SHOWN = 100;

  /** A draft record, keyed so a row keeps its tick and its place while others are removed. */
  type Row = { key: number; record: SampleRecordDto; changed: boolean };

  let sample: SampleDto | null = $state(null);
  let loadFailed = $state(false);
  let name = $state('');
  let source = $state('');
  let rows: Row[] = $state([]);
  let columns: EditColumn[] = $state([]);
  let selected: Set<number> = $state(new Set());
  let editing: number | null = $state(null);
  let query = $state('');
  let find = $state<FindFilter>({ kind: 'none' });
  /** How many records the sample holds as saved. */
  let saved = $state(0);
  let findError = $state('');
  let showAll = $state(false);
  let pasted = $state('');
  let saving = $state(false);

  async function load() {
    try {
      const got = await getSample(id);
      if (!got.ok || !got.sample) {
        loadFailed = true;
        return;
      }
      sample = got.sample;
      name = got.sample.name;
      source = got.sample.source ?? '';
      rows = got.sample.records.map((record, key) => ({ key, record, changed: false }));
      saved = rows.length;
      columns = editColumns(got.sample);
    } catch (e) {
      loadFailed = true;
      reported(e);
    }
  }
  load();

  // An expression runs over the sample as saved, through no steps: its rows are the saved records' places, which are
  // the draft's keys until the draft changes a record or drops one.
  const config = JSON.stringify(buildFileConfig('sample', 'sample', [], [], []));
  const evaluate = (expression: string) =>
    previewPipeline({ config, sample: id, showInternal: true, filter: expression });
  let drafted = $derived(
    rows.length !== saved || rows.some((r) => r.changed) || pasted.trim() !== '',
  );
  const keeps = (r: Row) =>
    find.kind === 'expression'
      ? find.rows.has(r.key)
      : find.kind !== 'keyword' || matches(r.record, find.text);
  let found = $derived(rows.filter(keeps));
  let shown = $derived(showAll ? found : found.slice(0, SHOWN));
  let adding = $derived(pastedRecords(pasted, columns));
  let pasteError = $derived('error' in adding ? adding.error : '');
  let total = $derived(rows.length + ('records' in adding ? adding.records.length : 0));
  let allShownTicked = $derived(shown.length > 0 && shown.every((r) => selected.has(r.key)));

  function tick(keys: number[], on: boolean) {
    const next = new Set(selected);
    for (const key of keys) {
      if (on) next.add(key);
      else next.delete(key);
    }
    selected = next;
  }

  /** Keeps the ticked records, or drops them: across the whole sample, not only the rows a search shows. */
  function keepTicked(keep: boolean) {
    rows = rows.filter((r) => selected.has(r.key) === keep);
    selected = new Set();
    editing = null;
  }

  // Which long cells the reader has opened, by record and column.
  const opened = new SvelteSet<string>();
  /** Past this many characters a cell is given room to be read in: with every internal field a column, the table
   * would otherwise squeeze it to a sliver. */
  const LONG = 60;

  function open(e: Event, key: number) {
    if (!(e.target as Element).closest('.tick, .right, .more')) editing = key;
  }

  function remove(key: number) {
    rows = rows.filter((r) => r.key !== key);
    tick([key], false);
    if (editing === key) editing = null;
  }

  function setCell(key: number, column: EditColumn, text: string) {
    rows = rows.map((r) =>
      r.key === key ? { ...r, record: withCell(r.record, column, text), changed: true } : r,
    );
  }

  async function save() {
    if (!sample || 'error' in adding) return;
    saving = true;
    try {
      const res = await putSample({
        ...sample,
        name: name.trim(),
        source: source.trim() || undefined,
        records: [...rows.map((r) => r.record), ...adding.records],
      });
      // A refusal is the error bar's to say.
      if (res.ok) onsaved();
    } catch (e) {
      reported(e);
    } finally {
      saving = false;
    }
  }

  let canSave = $derived(name.trim() !== '' && total > 0 && pasteError === '' && !saving);
</script>

{#if loadFailed}
  <!-- The error bar says why. -->
{:else if !sample}
  <p class="dim">Loading…</p>
{:else}
  <form
    class="form"
    onsubmit={(e) => {
      e.preventDefault();
      if (canSave) void save();
    }}
  >
    <div class="pair">
      <Field required label="Name"><input bind:value={name} required /></Field>
      <Field optional label="Source it stands for"><input bind:value={source} /></Field>
    </div>

    <Field label="Records" group>
      <div class="actions">
        <div class="search">
          <FindBox
            bind:query
            bind:filter={find}
            bind:error={findError}
            {evaluate}
            paused={drafted}
            remember={`smeltr-sample-expressions:${id}`}
            id="find-records"
            label="Search records"
            placeholder="Search records"
          />
        </div>
        <span class="count">{selected.size} of {rows.length} selected</span>
        <span class="spacer"></span>
        <button
          type="button"
          class="compact"
          disabled={selected.size === 0}
          onclick={() => keepTicked(true)}>Keep only selected</button
        >
        <button
          type="button"
          class="compact"
          disabled={selected.size === 0 || selected.size === rows.length}
          onclick={() => keepTicked(false)}>Remove selected</button
        >
      </div>
      <InlineError message={findError} />
      {#if find.kind === 'paused'}
        <p class="dim">
          Not applied: an expression filters the saved sample, and you have unsaved changes.
        </p>
      {/if}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th class="tick pin">
                <input
                  type="checkbox"
                  aria-label="Select every record shown"
                  checked={allShownTicked}
                  onchange={(e) =>
                    tick(
                      shown.map((r) => r.key),
                      e.currentTarget.checked,
                    )}
                />
              </th>
              <th class="num pin">#</th>
              {#each columns as c (c.name)}
                <th class="mono"
                  >{c.name}{#if c.type}<small>{c.type}</small>{/if}</th
                >
              {/each}
              <th class="pin end"></th>
            </tr>
          </thead>
          <tbody>
            {#each shown as r (r.key)}
              <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_noninteractive_element_interactions -->
              <!-- Tabbing into a record opens it as surely as a click. Its tick and its remove leave it as it
                   is: opening grows the row, and the next tick would land on the wrong place. -->
              <tr
                class:open={editing === r.key}
                class:changed={r.changed && editing !== r.key}
                onclick={(e) => open(e, r.key)}
                onfocusin={(e) => open(e, r.key)}
              >
                <td class="tick pin">
                  <input
                    type="checkbox"
                    aria-label={`Select record ${r.key + 1}`}
                    checked={selected.has(r.key)}
                    onchange={(e) => tick([r.key], e.currentTarget.checked)}
                  />
                </td>
                <td class="mono dim num pin">{r.key + 1}</td>
                {#each columns as c (c.name)}
                  {#if editing === r.key}
                    <td>
                      <input
                        class="mono"
                        aria-label={`${c.name}, record ${r.key + 1}`}
                        aria-invalid={cellMistyped(r.record, c.name)}
                        value={cellText(r.record, c.name)}
                        onchange={(e) => setCell(r.key, c, e.currentTarget.value)}
                        onkeydown={(e) => {
                          // Enter settles the cell, as in a grid, rather than saving the whole sample.
                          if (e.key !== 'Enter') return;
                          e.preventDefault();
                          setCell(r.key, c, e.currentTarget.value);
                        }}
                      />
                    </td>
                  {:else}
                    {@const text = cellText(r.record, c.name)}
                    {@const at = `${r.key}/${c.name}`}
                    <td
                      class="mono"
                      class:long={text.length > LONG}
                      class:error={cellMistyped(r.record, c.name)}
                    >
                      <Clamped
                        {text}
                        open={opened.has(at)}
                        ontoggle={() => (opened.has(at) ? opened.delete(at) : opened.add(at))}
                        label={`${c.name}, record ${r.key + 1}`}
                      />
                    </td>
                  {/if}
                {/each}
                <td class="right pin end">
                  <CrossButton
                    removes
                    label={`Remove record ${r.key + 1}`}
                    onclick={(e) => {
                      e.stopPropagation();
                      remove(r.key);
                    }}
                  />
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
      {#if found.length > shown.length}
        <div class="actions">
          <span class="count">{shown.length} of {found.length} shown</span>
          <button type="button" class="compact" onclick={() => (showAll = true)}>Show all</button>
        </div>
      {/if}
    </Field>

    <Field label="Add records" error={pasteError || null}>
      <textarea rows="2" placeholder="One record per line" bind:value={pasted}></textarea>
    </Field>

    {#if tests.length > 0}
      <p class="critical">Re-lock after saving: {tests.join(', ')}</p>
    {/if}

    <div class="actions">
      <span class="spacer"></span>
      <button type="button" onclick={oncancel} disabled={saving}>Cancel</button>
      <button class="primary" type="submit" disabled={!canSave}
        >{saving ? 'Saving…' : 'Save sample'}</button
      >
    </div>
  </form>
{/if}

<style>
  .form {
    display: grid;
    gap: 0.7rem;
  }
  .pair {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.8rem;
  }
  @media (max-width: 720px) {
    .pair {
      grid-template-columns: 1fr;
    }
  }
  .pair input,
  textarea {
    width: 100%;
    box-sizing: border-box;
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .spacer {
    flex-grow: 1;
  }
  .search {
    display: flex;
    flex: 1;
    max-width: 24rem;
  }
  .count {
    color: var(--dim);
    font-size: var(--text-note);
  }
  .mono {
    font-family: var(--mono);
  }
  td.long {
    min-width: 28rem;
  }
  .table-wrap {
    overflow-x: auto;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-body);
  }
  th,
  td {
    text-align: left;
    padding: 0.25rem 0.4rem;
    border-bottom: 1px solid var(--border);
    vertical-align: middle;
  }
  th {
    color: var(--dim);
    font-weight: 600;
    font-size: var(--text-label);
    white-space: nowrap;
  }
  th small {
    display: block;
    font-family: var(--mono);
    font-weight: 400;
    font-size: var(--text-note);
  }
  td input:not([type='checkbox']) {
    width: 100%;
    min-width: 6rem;
    box-sizing: border-box;
  }
  .tick {
    width: 2rem;
    box-sizing: border-box;
  }
  /* With every field a column the table runs past the screen and scrolls; a record's tick, number and remove stay in
     view at its edges. Opaque, so the fields scroll under them. */
  .pin {
    position: sticky;
    left: 0;
    z-index: 1;
    background: var(--panel);
  }
  .num.pin {
    left: 2rem;
  }
  .pin.end {
    left: auto;
    right: 0;
  }
  .right {
    text-align: right;
  }
  /* The record being edited: the open rail every open thing takes. */
  tr.open > .tick {
    box-shadow: inset 3px 0 0 var(--accent);
  }
  /* A record changed in this draft, marked as a diff marks one (STYLE_GUIDE.md, "Cards"). */
  tr.changed {
    background: color-mix(in oklch, var(--added) 7%, transparent);
  }
  tr.changed > .tick {
    box-shadow: inset 3px 0 0 var(--added);
  }
  tr.changed > .pin {
    background: color-mix(in oklch, var(--added) 7%, var(--panel));
  }
</style>
