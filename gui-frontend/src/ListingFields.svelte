<!--
  What a listing source (S3, GCS, Azure Blob or SFTP) takes besides
  where it lists and how it signs in: which keys it reads, what it reads
  first, what it does with an object once it is committed, and how often it
  lists. Every listing source draws the same block, so the four read alike.

  Mutates `value` in place, as `ConnectorAccess` does; the page's Save
  commits. `show` picks the half: what every form shows, or the poll interval,
  which sits under the form's Advanced.
-->
<script module lang="ts">
  import { LIMITS, type AfterCommit, type ListingSettings, type ListStart } from './api';

  /** `new` only beside `keep`: only the ledger `keep` writes records the first listing as read. */
  export function canStartNew(d: ListingSettings): boolean {
    return (d.afterCommit ?? 'keep') === 'keep';
  }

  /** Absent is `everything`, so choosing it writes nothing. */
  export function setStart(d: ListingSettings, start: ListStart) {
    if (start === 'everything') delete d.start;
    else d.start = start;
  }

  /** Sets what happens after commit, and unsets what the load refuses beside it: a move prefix with anything but
   * `move`, and `new` with anything but `keep`. */
  export function setAfterCommit(d: ListingSettings, after: AfterCommit) {
    if (after === 'keep') delete d.afterCommit;
    else d.afterCommit = after;
    if (after !== 'move') delete d.movePrefix;
    if (!canStartNew(d)) delete d.start;
  }

  /** For the Advanced header's marker: whether the half under Advanced is set off its default. */
  export function listingModified(d: ListingSettings): boolean {
    return (
      (d.pollIntervalSecs ?? LIMITS.pollIntervalSecs.default) !== LIMITS.pollIntervalSecs.default
    );
  }
</script>

<script lang="ts">
  import Field from './Field.svelte';
  import Nested from './Nested.svelte';
  import { numberOrUnset } from './page';

  let {
    value,
    noun,
    within,
    show,
  }: {
    /** The source the listing settings sit in, flattened. */
    value: ListingSettings;
    /** What one listed entry is called: `object`, `blob` or `file`. */
    noun: string;
    /** What is listed: `bucket`, `container` or `directory`. */
    within: string;
    show: 'main' | 'advanced';
  } = $props();

  const optional = (v: string) => (v === '' ? undefined : v);
</script>

{#if show === 'main'}
  <Field field="prefix" label="prefix">
    {#snippet help()}
      Only {noun}s whose name in the {within} starts with it. It does not start with <code>/</code>.
      Empty is the whole {within}.
    {/snippet}
    <input
      type="text"
      value={value.prefix ?? ''}
      oninput={(e) => (value.prefix = optional(e.currentTarget.value))}
      placeholder="app/"
    />
  </Field>
  <Field field="afterCommit" label="after commit">
    {#snippet help()}
      What it does with {noun === 'object' ? 'an' : 'a'}
      {noun} once every record is delivered. <code>keep</code> leaves it and records it as read,
      which needs only read access and costs a full listing per poll. <code>delete</code> deletes
      it.
      <code>move</code> moves it under the move prefix in place of the prefix.
    {/snippet}
    <select
      value={value.afterCommit ?? 'keep'}
      onchange={(e) => setAfterCommit(value, e.currentTarget.value as AfterCommit)}
    >
      <option value="keep">keep</option>
      <option value="delete">delete</option>
      <option value="move">move</option>
    </select>
  </Field>
  {#if value.afterCommit === 'move'}
    <Nested>
      <Field field="movePrefix" label="move prefix">
        {#snippet help()}
          Where <code>move</code> puts {noun === 'object' ? 'an' : 'a'}
          {noun}, in place of the prefix, and outside it. It does not start with <code>/</code>, and
          ends in <code>/</code> when the prefix does or is empty, and only then.
        {/snippet}
        <input
          type="text"
          value={value.movePrefix ?? ''}
          oninput={(e) => (value.movePrefix = optional(e.currentTarget.value))}
          placeholder="done/"
        />
      </Field>
    </Nested>
  {/if}
  <Field field="start" label="start at">
    {#snippet help()}
      What it reads first: <code>everything</code> its first listing finds, or only what arrives
      after it, <code>new</code>. <code>new</code> needs after commit <code>keep</code>.
    {/snippet}
    <select
      value={value.start ?? 'everything'}
      onchange={(e) => setStart(value, e.currentTarget.value as ListStart)}
    >
      <option value="everything">everything</option>
      <option value="new" disabled={!canStartNew(value)}>new</option>
    </select>
  </Field>
{:else}
  <Field field="pollIntervalSecs" label="poll interval (seconds)">
    {#snippet help()}
      Seconds from the start of one listing to the start of the next. At least {LIMITS
        .pollIntervalSecs.min}.
    {/snippet}
    <input
      type="number"
      min={LIMITS.pollIntervalSecs.min}
      value={value.pollIntervalSecs ?? LIMITS.pollIntervalSecs.default}
      oninput={(e) => (value.pollIntervalSecs = numberOrUnset(e.currentTarget))}
    />
  </Field>
{/if}

<style>
  input,
  select {
    font-family: var(--mono);
  }
</style>
