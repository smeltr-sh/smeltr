<!--
  The Delivery section a table destination's form ends with: how its Parquet
  files are written and uploaded, and how long records wait for a commit.
  Mutates `value` in place.
-->
<script lang="ts" module>
  import { byteCount } from './bytes';
  import { LIMITS, type Compression } from './api';
  import type { ByteCount } from './bytes';

  /** The settings the Delivery section holds, as every table destination names them. */
  export type TableDelivery = {
    compression?: Compression;
    inFlight?: number;
    maxObjectBytes?: ByteCount;
    bufferBytes?: ByteCount;
    stallAfterSecs?: number;
    fileIntervalSecs?: number;
  };

  /** Drives the Delivery header's marker: every setting it hides. */
  export function tableDeliveryModified(d: TableDelivery): boolean {
    return (
      (d.compression ?? 'zstd') !== 'zstd' ||
      (d.inFlight ?? LIMITS.inFlight.default) !== LIMITS.inFlight.default ||
      byteCount(d.maxObjectBytes, LIMITS.maxObjectBytes.default) !==
        LIMITS.maxObjectBytes.default ||
      byteCount(d.bufferBytes, LIMITS.objectBufferBytes.default) !==
        LIMITS.objectBufferBytes.default ||
      (d.stallAfterSecs ?? LIMITS.stallAfterSecs.default) !== LIMITS.stallAfterSecs.default ||
      (d.fileIntervalSecs ?? LIMITS.tableFileIntervalSecs.default) !==
        LIMITS.tableFileIntervalSecs.default
    );
  }
</script>

<script lang="ts">
  import Advanced from './Advanced.svelte';
  import BytesInput from './BytesInput.svelte';
  import Field from './Field.svelte';
  import { exactBytes } from './bytes';
  import { numberOrUnset } from './page';
  import { COMPRESSIONS } from './api';

  let {
    value: d,
    calls,
    commit,
  }: {
    value: TableDelivery;
    /** What one call may be made to, as "the store". */
    calls: string;
    /** What one commit adds to the table, as "a version to the table's log". */
    commit: string;
  } = $props();
</script>

<Advanced label="Delivery" modified={tableDeliveryModified(d)}>
  <Field field="compression" label="compression">
    {#snippet help()}
      How each Parquet file compresses its columns.
    {/snippet}
    <select
      value={d.compression ?? 'zstd'}
      onchange={(e) => {
        const v = e.currentTarget.value as Compression;
        if (v === 'zstd') delete d.compression;
        else d.compression = v;
      }}
    >
      {#each COMPRESSIONS as codec (codec)}
        <option value={codec}>{codec}</option>
      {/each}
    </select>
  </Field>
  <Field field="inFlight" label="parts uploading at once">
    {#snippet help()}
      How many parts of one file upload at once.
    {/snippet}
    <input
      type="number"
      min={LIMITS.inFlight.min}
      max={LIMITS.inFlight.max}
      value={d.inFlight ?? LIMITS.inFlight.default}
      oninput={(e) => (d.inFlight = numberOrUnset(e.currentTarget))}
    />
  </Field>
  <Field field="maxObjectBytes" label="largest file">
    {#snippet help()}
      The largest data file it writes, compressed.
    {/snippet}
    <BytesInput
      value={d.maxObjectBytes}
      fallback={LIMITS.maxObjectBytes.default}
      onchange={(n) => (d.maxObjectBytes = n)}
    />
  </Field>
  <Field field="bufferBytes" label="memory">
    {#snippet help()}
      Memory it may use on each worker for records not yet committed. Files are committed early when
      it fills.
    {/snippet}
    <BytesInput
      value={d.bufferBytes}
      fallback={LIMITS.objectBufferBytes.default}
      onchange={(n) => (d.bufferBytes = n)}
    />
  </Field>
  <Field field="stallAfterSecs" label="stall after (seconds)">
    {#snippet help()}
      How long one call to {calls}, or one commit, may take before it counts as stalled.
    {/snippet}
    <input
      type="number"
      min={LIMITS.stallAfterSecs.min}
      max={LIMITS.stallAfterSecs.max}
      value={d.stallAfterSecs ?? LIMITS.stallAfterSecs.default}
      oninput={(e) => (d.stallAfterSecs = numberOrUnset(e.currentTarget))}
    />
  </Field>
  <Field field="fileIntervalSecs" label="file interval (seconds)">
    {#snippet help()}
      The longest records wait before they are committed to the table. Each commit adds {commit}, so
      a short interval makes many. Sources feeding this destination are held open until their
      records are committed, for up to this long. Between {LIMITS.tableFileIntervalSecs.min} and
      {LIMITS.tableFileIntervalSecs.max} seconds.
    {/snippet}
    <input
      type="number"
      min={LIMITS.tableFileIntervalSecs.min}
      max={LIMITS.tableFileIntervalSecs.max}
      value={d.fileIntervalSecs ?? LIMITS.tableFileIntervalSecs.default}
      oninput={(e) => (d.fileIntervalSecs = numberOrUnset(e.currentTarget))}
    />
  </Field>
</Advanced>

<style>
  input,
  select {
    font-family: var(--mono);
  }
</style>
