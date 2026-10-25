<!--
  The delivery fields a request destination's Advanced section holds: how many
  requests start in flight, how large one grows, how much a worker holds and
  how long a batch may take. Each form gives its own words for the size.
  Mutates `value` in place.
-->
<script lang="ts" module>
  import { byteCount } from './bytes';
  import { LIMITS } from './api';
  import type { ByteCount } from './bytes';

  /** The settings the fields hold, as every request destination names them; a size is `maxRequestBytes` or
   * Elasticsearch's `maxBulkBytes`. */
  export type RequestDelivery = {
    inFlight?: number;
    maxRequestBytes?: ByteCount;
    maxBulkBytes?: ByteCount;
    bufferBytes?: ByteCount;
    stallAfterSecs?: number;
  };

  export type SizeKey = 'maxRequestBytes' | 'maxBulkBytes';

  /** Whether any field differs from its default, for the Advanced header's marker; `sizeDefault` is the
   * destination's own default for `size`. */
  export function requestDeliveryModified(
    d: RequestDelivery,
    size: SizeKey,
    sizeDefault: number,
  ): boolean {
    return (
      (d.inFlight ?? LIMITS.inFlight.default) !== LIMITS.inFlight.default ||
      byteCount(d[size], sizeDefault) !== sizeDefault ||
      byteCount(d.bufferBytes, LIMITS.bufferBytes.default) !== LIMITS.bufferBytes.default ||
      (d.stallAfterSecs ?? LIMITS.stallAfterSecs.default) !== LIMITS.stallAfterSecs.default
    );
  }
</script>

<script lang="ts">
  import type { Snippet } from 'svelte';
  import BytesInput from './BytesInput.svelte';
  import Field from './Field.svelte';
  import { exactBytes } from './bytes';
  import { numberOrUnset } from './page';

  let {
    value: d,
    size = 'maxRequestBytes',
    sizeLabel = 'request size',
    sizeDefault,
    sizeHelp,
    requests = 'Requests',
    concurrent = true,
    bufferHelp,
    including = 'retries included',
  }: {
    value: RequestDelivery;
    size?: SizeKey;
    sizeLabel?: string;
    sizeDefault: number;
    sizeHelp: Snippet;
    /** What is in flight, as "Requests" or "Bulk requests". */
    requests?: string;
    /** Whether the destination sends more than one request at once; one that does not has no concurrency to set. */
    concurrent?: boolean;
    /** The buffer's second sentence, in place of what dividing it by the size gives. */
    bufferHelp?: Snippet;
    /** What one batch's delivery time counts besides the request, as "retries included". */
    including?: string;
  } = $props();
</script>

{#if concurrent}
  <Field field="inFlight" label="starting concurrency">
    {#snippet help()}
      {requests} in flight at once when the destination starts, per worker; it adapts from there.
    {/snippet}
    <input
      type="number"
      min={LIMITS.inFlight.min}
      max={LIMITS.inFlight.max}
      value={d.inFlight ?? LIMITS.inFlight.default}
      oninput={(e) => (d.inFlight = numberOrUnset(e.currentTarget))}
    />
  </Field>
{/if}
<Field field={size} label={sizeLabel} help={sizeHelp}>
  <BytesInput value={d[size]} fallback={sizeDefault} onchange={(n) => (d[size] = n)} />
</Field>
<Field field="bufferBytes" label="delivery buffer">
  {#snippet help()}
    The most one worker holds for this destination, queued or sent and not yet acknowledged.
    {#if bufferHelp}{@render bufferHelp()}{:else}Divided by the {sizeLabel}, it is the most requests
      in flight.{/if}
  {/snippet}
  <BytesInput
    value={d.bufferBytes}
    fallback={LIMITS.bufferBytes.default}
    onchange={(n) => (d.bufferBytes = n)}
  />
</Field>
<Field field="stallAfterSecs" label="stall after (seconds)">
  {#snippet help()}
    How long one batch may take to deliver, {including}, before this destination counts as stalled.
  {/snippet}
  <input
    type="number"
    min={LIMITS.stallAfterSecs.min}
    max={LIMITS.stallAfterSecs.max}
    value={d.stallAfterSecs ?? LIMITS.stallAfterSecs.default}
    oninput={(e) => (d.stallAfterSecs = numberOrUnset(e.currentTarget))}
  />
</Field>

<style>
  input {
    font-family: var(--mono);
  }
</style>
