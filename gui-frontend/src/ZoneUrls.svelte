<!--
  A destination's `zoneUrls`: each zone's name to the address a worker in that zone sends to instead of `url`. A
  worker whose zone has no row, or that has no zone, sends to `url`.
-->
<script module lang="ts">
  import { tlsRefusal, urlWarning } from './http';
  import type { TlsDto } from './api';
  import { shadowed } from './KeyedRows.svelte';

  /** One zone as it is edited: its name and its URL. */
  export type ZoneRow = { key: string; url: string };

  /** Why `rows[i]` cannot be saved, or nothing. The save refuses a zone with no URL and a URL with no zone; a row
   * with neither is not written. */
  export function zoneRefusal(rows: ZoneRow[], i: number, tls: TlsDto): string | undefined {
    const { key, url } = rows[i];
    if (key.trim() === '' && url.trim() === '') return undefined;
    if (key.trim() === '') return 'Name the zone this URL is for.';
    if (url.trim() === '') return 'Give this zone a URL.';
    if (shadowed(rows, i)) return 'This zone is listed twice; only the first row is saved.';
    return tlsRefusal(url, tls);
  }

  /** The `zoneUrls` map `rows` write, or `undefined` when none is named: an empty map is omitted, as the DTO
   * serializes it. A zone given twice keeps its first row. */
  export function writeZones(rows: ZoneRow[]): Record<string, string> | undefined {
    const named = rows.filter(
      (r, i) => (r.key.trim() !== '' || r.url.trim() !== '') && !shadowed(rows, i),
    );
    if (named.length === 0) return undefined;
    return Object.fromEntries(named.map((r) => [r.key.trim(), r.url]));
  }
</script>

<script lang="ts">
  import KeyedRows from './KeyedRows.svelte';
  import { untrack, type Snippet } from 'svelte';

  // `value` is a plain prop: this mutates the `$state` destination it is handed and never reassigns it.
  let {
    value,
    sends,
    placeholder,
    help,
  }: {
    value: { zoneUrls?: Record<string, string> } & TlsDto;
    /** Whether a credential goes with a request besides one in the URL, as `UrlField` takes it. */
    sends: boolean;
    /** An address in a zone, as the form's `url` placeholder is one. */
    placeholder: string;
    help: Snippet;
  } = $props();

  let rows: ZoneRow[] = $state(
    untrack(() => Object.entries(value.zoneUrls ?? {}).map(([key, url]) => ({ key, url }))),
  );

  function sync() {
    const zones = writeZones(rows);
    if (zones === undefined) delete value.zoneUrls;
    else value.zoneUrls = zones;
  }
</script>

<KeyedRows
  {rows}
  field="zoneUrls"
  label="zone URLs"
  keyLabel="zone"
  keyPlaceholder="us-east-1a"
  add="+ zone"
  blank={() => ({ key: '', url: '' })}
  {help}
  refusal={(i) => zoneRefusal(rows, i, value)}
  onchange={sync}
>
  {#snippet cells(row)}
    <input
      type="text"
      class="url"
      {placeholder}
      value={row.url}
      oninput={(e) => {
        row.url = e.currentTarget.value;
        sync();
      }}
      aria-label="zone URL"
    />
  {/snippet}
  {#snippet below(row)}
    {@const warning = urlWarning(row.url, sends)}
    {#if warning}<div class="warning">{warning}</div>{/if}
  {/snippet}
</KeyedRows>

<style>
  .url {
    width: 22rem;
  }

  .warning {
    color: var(--warn);
    font-size: var(--text-note);
    line-height: 1.4;
    max-width: 34rem;
  }
</style>
