<!--
  Adding a sample: the way it is made, then that way's form. A paste and an uploaded file are saved
  by the primary action. A capture is `CaptureFlow`'s, at a source or a destination: a worker
  already reading a source taps what is flowing, one that is not opens it and never commits.
-->
<script lang="ts" module>
  import { reported } from './errors.svelte';
  export type SampleWay = 'paste' | 'upload' | 'capture';
</script>

<script lang="ts">
  import { untrack } from 'svelte';
  import Dialog from './Dialog.svelte';
  import TypeCatalog, { type CatalogType } from './TypeCatalog.svelte';
  import CaptureFlow from './CaptureFlow.svelte';
  import type { ComponentProps } from 'svelte';
  import Field from './Field.svelte';
  import { STEP_GLYPHS, type Glyph } from './stepGlyphs';
  import { SYSTEM_GLYPHS } from './systemGlyphs';
  import { mintSampleId } from './ids';
  import { count } from './time';
  import { putSample, recordsFromText } from './api';
  import { mayCapture } from './access';

  let {
    open,
    start,
    target,
    oncancel,
    onsaved,
  }: {
    open: boolean;
    /** Opens straight at this way's form, without the catalog. */
    start?: SampleWay;
    /** What a capture starts on (`CaptureFlow`). */
    target?: ComponentProps<typeof CaptureFlow>['target'];
    oncancel: () => void;
    /** The new sample's id, once it is saved. */
    onsaved: (id: string) => void;
  } = $props();

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
  };

  let way: SampleWay | null = $state(null);
  let busy = $state(false);

  // Paste and upload: an uploaded file is a paste that came from disk, one
  // record per non-empty line.
  let name = $state('');
  let source = $state('');
  let text = $state('');
  let records = $derived(recordsFromText(text));

  // Every opening starts clean, so a cancel leaves nothing for the next one.
  $effect(() => {
    if (!open) return;
    untrack(() => {
      name = '';
      source = '';
      text = '';
      pick(start ?? null);
    });
  });

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
    if (!name.trim() || records.length === 0) return;
    busy = true;
    try {
      const id = mintSampleId();
      const res = await putSample({
        id,
        name: name.trim(),
        source: source.trim() || undefined,
        records,
      });
      // A refusal is the dialog's error bar's to say.
      if (res.ok) onsaved(id);
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  let canSave = $derived(name.trim() !== '' && records.length > 0);

  function submit() {
    if (!busy && way && canSave) void save();
  }
</script>

<Dialog
  open={open && way !== 'capture'}
  title={way ? TITLES[way] : 'add sample'}
  wide
  oncancel={() => busy || oncancel()}
  onback={way && !start ? () => pick(null) : undefined}
  onsubmit={submit}
>
  {#if way === 'paste' || way === 'upload'}
    <div class="form-body">
      {#if way === 'upload'}
        <Field label="File">
          {#snippet help()}One record per non-empty line; each line becomes <code>_raw</code
            >.{/snippet}
          <input type="file" accept=".json,.ndjson,.jsonl,.txt,.log" onchange={pickFile} />
        </Field>
      {/if}
      <Field label="Name">
        <input bind:value={name} placeholder="orders-west, ordinary hour" />
      </Field>
      <Field
        label="Source it stands for"
        help="A route selects on a source, so a sample with none runs through a pipeline but not through a route."
      >
        <input bind:value={source} placeholder="orders-west" class="mono" />
      </Field>
      {#if way === 'paste'}
        <Field label="Records">
          {#snippet help()}One per line; each line becomes <code>_raw</code>.{/snippet}
          <textarea bind:value={text} rows="8" spellcheck="false"></textarea>
        </Field>
      {/if}
      <span class="dim">{count(records.length, 'record')}</span>
    </div>
  {:else}
    <TypeCatalog types={WAYS} onpick={pick} />
  {/if}
  {#snippet actions()}
    <button type="button" onclick={oncancel} disabled={busy}>Cancel</button>
    {#if way}
      <button type="submit" class="primary" disabled={busy || !canSave}>
        {busy ? 'Saving…' : 'Save sample'}
      </button>
    {/if}
  {/snippet}
</Dialog>

{#if open && way === 'capture'}
  <CaptureFlow {target} {oncancel} onback={start ? undefined : () => pick(null)} {onsaved} />
{/if}

<style>
  .form-body {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }
  input,
  textarea {
    width: 100%;
    box-sizing: border-box;
  }
  textarea {
    font-family: var(--mono);
    resize: vertical;
  }
  .mono {
    font-family: var(--mono);
  }
  .dim {
    color: var(--dim);
  }
</style>
