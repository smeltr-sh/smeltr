<!--
  A receiver's `tokens`: each token's name to the credential holding it, which a sender presents to be admitted.
  A HEC receiver's token also says whether its senders use indexer acknowledgement, so `acknowledges` adds that
  column and writes each token as `{token, acknowledgement}`.
-->
<script module lang="ts">
  import type { HecToken, SecretRef } from './api';
  import { shadowed } from './KeyedRows.svelte';

  /** One token as it is edited: its credential is `undefined` while it is still being named. */
  export type TokenRow = { key: string; ref: SecretRef | undefined; ack: boolean };

  /** The `tokens` map `rows` write, or `undefined` when none is named: an empty map is omitted, as the DTO
   * serializes it, and so is an acknowledgement left off. A HEC receiver's token is `{token, acknowledgement}`.
   * A name given twice keeps its first row. */
  export function writeTokens(
    rows: TokenRow[],
    acknowledges: boolean,
  ): Record<string, SecretRef> | Record<string, HecToken> | undefined {
    const named = rows.filter(
      (r, i) => r.key.trim() !== '' && r.ref !== undefined && !shadowed(rows, i),
    );
    if (named.length === 0) return undefined;
    if (!acknowledges) return Object.fromEntries(named.map((r) => [r.key.trim(), r.ref!]));
    return Object.fromEntries(
      named.map((r) => [
        r.key.trim(),
        r.ack ? { token: r.ref!, acknowledgement: true } : { token: r.ref! },
      ]),
    );
  }
</script>

<script lang="ts">
  import KeyedRows from './KeyedRows.svelte';
  import { untrack, type Snippet } from 'svelte';
  import SecretRefInput from './SecretRefInput.svelte';
  import type { SecretDto } from './api';

  // `value` is a plain prop: this mutates the `$state` source it is handed and never reassigns it.
  let {
    value,
    acknowledges = false,
    secrets = [],
    help,
  }: {
    value: { tokens?: Record<string, SecretRef> | Record<string, HecToken> };
    /** Whether each token carries `acknowledgement`, as a HEC receiver's does. */
    acknowledges?: boolean;
    secrets?: SecretDto[];
    help: Snippet;
  } = $props();

  let rows: TokenRow[] = $state(
    untrack(() =>
      Object.entries(value.tokens ?? {}).map(([key, t]) =>
        acknowledges
          ? { key, ref: (t as HecToken).token, ack: (t as HecToken).acknowledgement ?? false }
          : { key, ref: t as SecretRef, ack: false },
      ),
    ),
  );

  function sync() {
    const tokens = writeTokens(rows, acknowledges);
    if (tokens === undefined) delete value.tokens;
    else value.tokens = tokens;
  }
</script>

<KeyedRows
  {rows}
  field="tokens"
  label="tokens"
  keyLabel="token name"
  keyPlaceholder="collectors"
  add="+ token"
  blank={() => ({ key: '', ref: undefined, ack: false })}
  {help}
  refusal={(i) =>
    shadowed(rows, i) ? 'Another token above has this name, so this one is not saved.' : undefined}
  onchange={sync}
>
  {#snippet cells(row)}
    <SecretRefInput
      value={row.ref}
      {secrets}
      label="token"
      onchange={(v) => {
        row.ref = v;
        sync();
      }}
    />
    {#if acknowledges}
      <label class="ack" title="Its senders use indexer acknowledgement">
        <input
          type="checkbox"
          checked={row.ack}
          onchange={(e) => {
            row.ack = e.currentTarget.checked;
            sync();
          }}
        />
        acknowledgement
      </label>
    {/if}
  {/snippet}
</KeyedRows>

<style>
  .ack {
    display: flex;
    gap: 0.25rem;
    align-items: center;
    font-size: var(--text-note);
  }
</style>
