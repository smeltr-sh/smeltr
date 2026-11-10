<!--
  Adding a sample: the way it is made, then that way's form. A paste or an uploaded file ends one of
  two ways: Use these records keeps them unsaved at the leader for whoever wrote them (`holdPaste`)
  and hands them to the page that opened this (`onuse`); Save sample makes them a sample on the
  branch. A capture is `CaptureFlow`'s, at a source or a destination: a worker already reading a
  source taps what is flowing, one that is not opens it and never commits.
-->
<script lang="ts" module>
  export type SampleWay = 'paste' | 'upload' | 'capture' | 'output' | 'input';
</script>

<script lang="ts">
  import { Busy } from './busy.svelte';
  import { reported } from './errors.svelte';
  import Button, { BUSY, INCOMPLETE } from './Button.svelte';
  import { untrack } from 'svelte';
  import Dialog from './Dialog.svelte';
  import TypeCatalog, { type CatalogType } from './TypeCatalog.svelte';
  import CaptureFlow from './CaptureFlow.svelte';
  import type { ComponentProps } from 'svelte';
  import Field from './Field.svelte';
  import InlineError from './InlineError.svelte';
  import { STEP_GLYPHS, type Glyph } from './stepGlyphs';
  import { SYSTEM_GLYPHS } from './systemGlyphs';
  import { mintSampleId } from './ids';
  import { neverLocked } from './Locked.svelte';
  import { count } from './time';
  import {
    errorText,
    holdPaste,
    putSample,
    recordsFromText,
    sourceIdsInScope,
    saveCapture,
    type CaptureStatus,
    type SampleDto,
  } from './api';
  import { heldCount } from './capture';
  import { NOT_IN_DEMO, mayCapture } from './access';

  let {
    open,
    start,
    target,
    onuse,
    output,
    input,
    oncancel,
    onsaved,
  }: {
    open: boolean;
    /** Opens straight at this way's form, without the catalog. */
    start?: SampleWay;
    /** What a capture starts on (`CaptureFlow`). */
    target?: ComponentProps<typeof CaptureFlow>['target'];
    /** What to do with a capture or a paste the leader keeps for the person after Use these records. Without it, a
     * paste is only saved. */
    onuse?: ComponentProps<typeof CaptureFlow>['onuse'];
    /** A pipeline's output rows, as a sample holds them: what the dialog saves when it opens at `output`, the
     * `SampleWay` for a pipeline's output. */
    output?: () => Promise<Pick<SampleDto, 'columns' | 'records'>>;
    /** The finished capture the leader holds for whoever is signed in: what the dialog saves when it opens at
     * `input`, the `SampleWay` for a pipeline's input that is not saved yet. */
    input?: CaptureStatus;
    oncancel: () => void;
    /** Called with the new sample's id once it is saved. Left out for someone who may not save: the dialog then saves
     * nothing. */
    onsaved?: (id: string) => void;
  } = $props();

  // Whoever may open it may use records, even on a page they may only read; only `onsaved` saves.
  neverLocked();

  // A page with a turned corner: records written by hand.
  const PAGE: Glyph = {
    d: 'M10 8 H42 L56 22 V56 H10 Z M16 14 V50 H50 V26 H38 V14 Z M44 15 V22 H51 Z',
    evenodd: true,
  };

  const WAYS = (
    [
      { type: 'paste', label: 'Paste', category: 'Sample', glyph: PAGE },
      { type: 'upload', label: 'Upload', category: 'Sample', glyph: STEP_GLYPHS.call },
      { type: 'capture', label: 'Capture', category: 'Sample', glyph: SYSTEM_GLYPHS.splunkHec },
    ] satisfies CatalogType<SampleWay>[]
  ).filter((way) => way.type !== 'capture' || mayCapture());
  const TITLES: Record<SampleWay, string> = {
    paste: 'paste a sample',
    upload: 'upload a sample',
    capture: 'capture a sample',
    output: 'save output as sample',
    input: 'save input as sample',
  };

  let way: SampleWay | null = $state(null);
  const busy = new Busy();
  /** The `output` records are on their way. Loading saves nothing, so Cancel stays live. */
  let loading = $state(false);
  /** Bumped as the dialog opens or closes, so a load answering a closed opening is dropped. */
  let opening = 0;

  // Paste and upload: an uploaded file is a paste that came from disk, one
  // record per non-empty line.
  let name = $state('');
  let source = $state('');
  /** What `source` may name. */
  let sources = $state<string[]>([]);
  let text = $state('');
  /** The records `output` returned, for the `output` `SampleWay`. */
  let taken = $state<Pick<SampleDto, 'columns' | 'records'> | null>(null);
  let records = $derived(way === 'output' ? (taken?.records ?? []) : recordsFromText(text));
  /** How many records `save` keeps. */
  let kept = $derived(way === 'input' ? (input ? heldCount(input) : 0) : records.length);
  /** Why the leader refused to save the capture: the input's own, since a capture says what went wrong in its flow. */
  let error = $state('');

  // Every opening starts clean, so a cancel leaves nothing for the next one.
  $effect(() => {
    opening++;
    if (!open) return;
    untrack(() => {
      name = '';
      source = '';
      text = '';
      const at = opening;
      sourceIdsInScope().then(
        (ids) => at === opening && (sources = ids),
        // Unread, the list offers no source, and the records are used or saved standing for none.
        () => {},
      );
      taken = null;
      error = '';
      loading = false;
      pick(start ?? null);
      if (start === 'output' && output) void take(output);
    });
  });

  async function take(output: () => Promise<Pick<SampleDto, 'columns' | 'records'>>) {
    const at = opening;
    loading = true;
    try {
      const answer = await output();
      if (at === opening) taken = answer;
    } catch (e) {
      if (at === opening) reported(e);
    } finally {
      if (at === opening) loading = false;
    }
  }

  function pick(next: SampleWay | null) {
    // Records pasted are not an upload's, nor the reverse.
    if (next !== way) text = '';
    way = next;
  }

  async function pickFile(e: Event) {
    const file = (e.target as HTMLInputElement).files?.[0];
    if (!file) return;
    text = await file.text();
    if (!name) name = file.name.replace(/\.[^.]+$/, '');
  }

  /** A new sample's id is minted, never the name: a test names its sample by
   * id, so a rename must leave the id alone. */
  async function save() {
    if (!name.trim() || kept === 0) return;
    if (way === 'input') return saveInput();
    await busy.run(async () => {
      const id = mintSampleId();
      const res = await putSample({
        id,
        name: name.trim(),
        source: source || undefined,
        columns: taken?.columns,
        records,
      });
      // A refusal is the dialog's error bar's to say.
      if (res.ok) onsaved?.(id);
    });
  }

  /** The capture saved as it is held, standing for the source it read, which then holds it no longer. */
  async function saveInput() {
    if (!input) return;
    error = '';
    await busy.run(async () => {
      const id = mintSampleId();
      const res = await saveCapture(input.session, id, name.trim(), input.source);
      if (res.ok) onsaved?.(id);
      else error = errorText(res.errors) || 'could not save';
    });
  }

  /** Keeps what was pasted or uploaded, unsaved, and hands it to the page. */
  async function use() {
    if (!onuse || NOT_IN_DEMO || records.length === 0) return;
    error = '';
    await busy.run(async () => {
      const res = await holdPaste(records, source || undefined);
      if (res.ok && res.capture) error = onuse(res.capture) ?? '';
      else error = errorText(res.errors) || 'could not use these records';
    });
  }
  let usable = $derived(onuse && (way === 'paste' || way === 'upload'));

  let canSave = $derived(name.trim() !== '' && kept > 0);

  function submit() {
    if (!busy.on && usable) void use();
    else if (!busy.on && way && canSave) void save();
  }
</script>

<Dialog
  open={open && way !== 'capture'}
  title={way ? TITLES[way] : 'add sample'}
  wide
  oncancel={() => busy.on || oncancel()}
  onback={way && !start ? () => pick(null) : undefined}
  onsubmit={submit}
>
  {#if way === 'paste' || way === 'upload'}
    <div class="form-body">
      {#if way === 'upload'}
        <Field required label="File">
          {#snippet help()}One record per non-empty line; each line becomes <code>_raw</code
            >.{/snippet}
          <input type="file" accept=".json,.ndjson,.jsonl,.txt,.log" onchange={pickFile} />
        </Field>
      {/if}
      {#if onsaved}
        <!-- Only Save sample needs a name: Use these records keeps the records unnamed. -->
        <Field required={!usable} label="Name">
          <input bind:value={name} placeholder="orders-west, ordinary hour" />
        </Field>
      {/if}
      <Field
        optional
        label="Source it stands for"
        help="A route selects on a source, so a sample with none runs through a pipeline but not through a route."
      >
        <select bind:value={source} class="mono">
          <option value=""></option>
          {#each sources as s (s)}<option value={s}>{s}</option>{/each}
        </select>
      </Field>
      {#if way === 'paste'}
        <Field required label="Records">
          {#snippet help()}One record per line; each line becomes <code>_raw</code>.{/snippet}
          <textarea bind:value={text} rows="8" spellcheck="false"></textarea>
        </Field>
      {/if}
      <span class="dim">{count(records.length, 'record')}</span>
      <InlineError message={error} />
    </div>
  {:else if way === 'output'}
    <div class="form-body">
      <Field required label="Name">
        <input bind:value={name} placeholder="orders-west, enriched" />
      </Field>
      <span class="dim">{count(records.length, 'record')}</span>
    </div>
  {:else if way === 'input'}
    <div class="form-body">
      <Field required label="Name">
        <input bind:value={name} placeholder="orders-west, ordinary hour" />
      </Field>
      <span class="dim">{count(kept, 'record')}</span>
      <InlineError message={error} />
    </div>
  {:else}
    <TypeCatalog types={WAYS} onpick={pick} />
  {/if}
  {#snippet actions()}
    <Button type="button" onclick={oncancel} disabled={busy.on ? BUSY : ''}>Cancel</Button>
    {#if usable}
      <Button
        type="submit"
        class="primary"
        disabled={busy.on ? BUSY : NOT_IN_DEMO || (records.length === 0 ? INCOMPLETE : '')}
        >Use these records</Button
      >
    {/if}
    {#if way && onsaved}
      <Button
        type={usable ? 'button' : 'submit'}
        class={usable ? '' : 'primary'}
        onclick={usable ? () => void save() : undefined}
        disabled={busy.on || loading ? BUSY : !canSave ? INCOMPLETE : ''}
      >
        {loading ? 'Loading…' : busy.on ? 'Saving…' : 'Save sample'}
      </Button>
    {/if}
  {/snippet}
</Dialog>

{#if open && way === 'capture'}
  <CaptureFlow
    {target}
    {onuse}
    {oncancel}
    onback={start ? undefined : () => pick(null)}
    {onsaved}
  />
{/if}

<style>
  .form-body {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }
  input,
  select,
  textarea {
    width: 100%;
    box-sizing: border-box;
  }
  textarea {
    font-family: var(--mono);
    resize: vertical;
  }
</style>
