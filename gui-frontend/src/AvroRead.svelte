<!--
  How a source that reads payloads makes them its record: as text, or as Avro into the fields it declares. A stream
  names the schema registry its messages' schemas are in; an object is an Avro file, which holds its own.
-->
<script module lang="ts">
  import type { AvroField, CaptureStatus, SqlField, StreamReadFormat } from './api';

  /** What a source reading payloads holds that its format decides: Avro refuses an encoding and a framing beside
   * it. */
  export type Read = StreamReadFormat & {
    encoding?: string;
    framing?: string;
    lineBreaker?: string;
  };

  /** Sets `value`'s format. Absent is text, so choosing it writes nothing and drops what only Avro takes; choosing
   * Avro drops what it refuses and starts a stream's registry. */
  export function setFormat(value: Read, format: 'text' | 'avro', stream: boolean) {
    if (format === 'text') {
      delete value.format;
      delete value.schemaRegistry;
      delete value.fields;
      return;
    }
    value.format = 'avro';
    delete value.encoding;
    delete value.framing;
    delete value.lineBreaker;
    if (stream) value.schemaRegistry ??= { url: '' };
    value.fields ??= [];
  }

  /** The fields a finished fill declares (`fromSchema`) and the worker whose message they are from, or why it has
   * none. */
  export function filledFrom(
    declared: SqlField[],
    status: CaptureStatus,
  ): { fields: SqlField[]; worker: string } | { error: string } {
    const rows = status.batches.flatMap((b) => b.rows);
    if (status.avro?.length)
      return { fields: fromSchema(declared, status.avro), worker: rows[0]?.worker ?? '' };
    const failed = status.ended.map(([, end]) => end).find((end) => typeof end !== 'string');
    if (failed) return { error: failed.failed.trim() };
    // A message that did not read holds why in its first field.
    const unread = rows.find((r) => r.failed.includes(0));
    if (unread) return { error: String(unread.values[0]) };
    if (rows.length)
      return { error: 'The message’s schema is not a record, so it has no fields to fill.' };
    return { error: 'No message arrived. Fill again once one is on its way.' };
  }

  /** The fields `schema` declares, in its order, each typed as it reads unless `declared` already types it. */
  export function fromSchema(declared: SqlField[], schema: AvroField[]): SqlField[] {
    return schema.map((f) => ({
      name: f.name,
      type: declared.find((d) => d.name === f.name)?.type ?? f.suggested,
    }));
  }
</script>

<script lang="ts">
  import Field from './Field.svelte';
  import SchemaRegistryFields from './SchemaRegistryFields.svelte';
  import CaptureFlow from './CaptureFlow.svelte';
  import { FIELD_TYPES, type IdentityDto, type SecretDto, type SourceDto } from './api';

  let {
    value,
    stream,
    secrets,
    identities,
  }: {
    /** The source itself, as the page edits it. */
    value: Read;
    /** Whether it is a Kafka or Event Hubs source, whose Avro messages name a schema registry. */
    stream: boolean;
    /** The secret catalog, for the registry's password. */
    secrets: SecretDto[];
    /** Every identity the source may name, for the one a capture of it reads under. */
    identities: IdentityDto[];
  } = $props();

  // A fill is a capture of the source as it stands on the page: the worker answers with the fields of the schema its
  // newest message was written with.
  let filling = $state<{ source: SourceDto; identity?: IdentityDto } | null>(null);
  let filled = $state<{ worker: string; fields: number } | null>(null);

  function startFill() {
    filled = null;
    const source = $state.snapshot(value) as unknown as SourceDto & Read;
    // A row not yet named is one the load refuses, and the capture builds the source as the load does.
    source.fields = source.fields?.filter((f) => f.name.trim() !== '');
    const named = 'identity' in source ? source.identity : undefined;
    const identity = identities.find((i) => i.id === named);
    filling = { source, identity: identity && ($state.snapshot(identity) as IdentityDto) };
  }

  function take(status: CaptureStatus): string | undefined {
    const fill = filledFrom(value.fields ?? [], status);
    if ('error' in fill) return fill.error;
    value.fields = fill.fields;
    filled = { worker: fill.worker, fields: fill.fields.length };
  }
</script>

<Field field="format" label="format">
  {#snippet help()}
    <code>text</code>: each payload is text, in <code>_raw</code>.
    {#if stream}
      <code>avro</code>: each message is Avro, its schema in the schema registry.
    {:else}
      <code>avro</code>: each object is an Avro file, which holds its own schema.
    {/if}
  {/snippet}
  <select
    value={value.format ?? 'text'}
    onchange={(e) => setFormat(value, e.currentTarget.value as 'text' | 'avro', stream)}
  >
    <option value="text">text</option>
    <option value="avro"
      >{stream ? 'avro — one Avro message per record' : 'avro — records from Avro files'}</option
    >
  </select>
</Field>

{#if value.format === 'avro'}
  {#if stream && value.schemaRegistry}
    <SchemaRegistryFields registry={value.schemaRegistry} {secrets} />
  {/if}

  <Field field="fields" label="Avro fields, as fields of the record" group>
    {#snippet help()}
      Each field to read, with a type. Name a nested field with dots, such as <code>user.id</code>.
      Top-level fields you don't list are kept.
    {/snippet}
    <div class="rows">
      {#each value.fields ?? [] as f, i (i)}
        <div class="row">
          <input
            type="text"
            aria-label="field {i + 1} name"
            bind:value={f.name}
            placeholder="user.id"
          />
          <select aria-label="field {i + 1} type" bind:value={f.type}>
            {#each FIELD_TYPES as t (t)}<option value={t}>{t}</option>{/each}
            <!-- A type the list has no name for, such as an array a fill declared, is still shown. -->
            {#if !FIELD_TYPES.includes(f.type)}<option value={f.type}>{f.type}</option>{/if}
          </select>
          <button
            type="button"
            aria-label="remove field {i + 1}"
            onclick={() => value.fields?.splice(i, 1)}>×</button
          >
        </div>
      {/each}
      <div class="actions">
        <button type="button" onclick={() => (value.fields ??= []).push({ name: '', type: 'str' })}
          >+ field</button
        >
        <button type="button" onclick={startFill}>Fill from a message</button>
        <span class="dim">
          {#if filled}
            Filled from a message on worker {filled.worker || '?'} · {filled.fields} fields
          {/if}
        </span>
      </div>
    </div>
  </Field>
  {#if filling}
    <CaptureFlow
      editing={{ ...filling, maxMatches: 1 }}
      use={{ title: 'Fill the Avro fields from a message', label: 'Fill fields', take }}
      oncancel={() => (filling = null)}
    />
  {/if}
{/if}

<style>
  /* One grid for every row, so the type column is as wide as its widest type, an array's among them. */
  .rows {
    display: grid;
    grid-template-columns: 1fr minmax(7rem, max-content) auto;
    gap: 0.3rem 0.4rem;
  }

  .row {
    display: contents;
  }

  input,
  select {
    font-family: var(--mono);
  }

  .actions {
    grid-column: 1 / -1;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.6rem;
  }
</style>
