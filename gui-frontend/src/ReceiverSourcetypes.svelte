<!--
  A receiver's `sourcetypes`: each sourcetype's name to the `lineBreaker` that ends its events. A sourcetype not
  named, or named with no line breaker, is an event per line, as `DEFAULT_LINE_BREAKER` breaks it.
-->
<script module lang="ts">
  import type { Sourcetype } from './api';
  import { shadowed } from './KeyedRows.svelte';

  /** Splunk's default `LINE_BREAKER`, which a row's blank line breaker stands for: its hint. */
  export const DEFAULT_LINE_BREAKER = '([\\r\\n]+)';

  /** One sourcetype as it is edited. */
  export type SourcetypeRow = { key: string; lineBreaker: string };

  /** The `sourcetypes` map `rows` write, or `undefined` when none is named: an empty map is omitted, as the DTO
   * serializes it. A named row is written with its `lineBreaker` as typed, or with none when that is blank. A name
   * given twice keeps its first row. */
  export function writeSourcetypes(rows: SourcetypeRow[]): Record<string, Sourcetype> | undefined {
    const named = rows.filter((r, i) => r.key.trim() !== '' && !shadowed(rows, i));
    if (named.length === 0) return undefined;
    return Object.fromEntries(
      named.map((r) => [
        r.key.trim(),
        r.lineBreaker.trim() === '' ? {} : { lineBreaker: r.lineBreaker },
      ]),
    );
  }
</script>

<script lang="ts">
  import KeyedRows from './KeyedRows.svelte';
  import LineBreakerRefusal from './LineBreakerRefusal.svelte';
  import { untrack, type Snippet } from 'svelte';

  // `value` is a plain prop: this mutates the `$state` source it is handed and never reassigns it.
  let {
    value,
    help,
  }: {
    value: { sourcetypes?: Record<string, Sourcetype> };
    help: Snippet;
  } = $props();

  let rows: SourcetypeRow[] = $state(
    untrack(() =>
      Object.entries(value.sourcetypes ?? {}).map(([key, t]) => ({
        key,
        lineBreaker: t.lineBreaker ?? '',
      })),
    ),
  );

  function sync() {
    const sourcetypes = writeSourcetypes(rows);
    if (sourcetypes === undefined) delete value.sourcetypes;
    else value.sourcetypes = sourcetypes;
  }

  // A blank line breaker the file already holds is written as none, as an edited row's is, so a save does not send it.
  if (
    untrack(() => Object.values(value.sourcetypes ?? {}).some((t) => t.lineBreaker?.trim() === ''))
  )
    sync();
</script>

<KeyedRows
  {rows}
  field="sourcetypes"
  label="sourcetypes"
  keyLabel="sourcetype"
  keyPlaceholder="app:xml"
  add="+ sourcetype"
  blank={() => ({ key: '', lineBreaker: '' })}
  {help}
  refusal={(i) =>
    shadowed(rows, i)
      ? 'Another sourcetype above has this name, so this one is not saved.'
      : undefined}
  onchange={sync}
>
  {#snippet cells(row)}
    <input
      type="text"
      class="breaker"
      placeholder={DEFAULT_LINE_BREAKER}
      value={row.lineBreaker}
      oninput={(e) => {
        row.lineBreaker = e.currentTarget.value;
        sync();
      }}
      aria-label="line breaker"
    />
  {/snippet}
  {#snippet below(row)}
    <LineBreakerRefusal pattern={row.lineBreaker} />
  {/snippet}
</KeyedRows>

<style>
  .breaker {
    width: 18rem;
  }
</style>
