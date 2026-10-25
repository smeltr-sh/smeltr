<!--
  Editor for a connector's named values in two maps: committed values, and
  credentials, which name where a value lives rather than carrying it. Two
  labeled tables rather than a "secret?" checkbox, because a checkbox can be
  forgotten and the failure is a password in git.

  The caller says what the names are (Kafka's librdkafka properties, an HTTP
  destination's headers), which the tables leave alone (`hidden`), and what a
  save would refuse, which each row says in itself rather than at the save.
-->
<script module lang="ts">
  /** What the two tables call themselves and their rows. */
  export type PropsLabels = {
    /** The committed table's label; `literalKey` names its field. */
    literal: string;
    credentials: string;
    /** One row, as its add button and its inputs' names say it. */
    noun: string;
    /** Placeholders: a committed name and value, and a credential's name. */
    literalName: string;
    literalValue: string;
    credentialName: string;
  };
</script>

<script lang="ts" generics="K extends string">
  import Field from './Field.svelte';
  import { untrack, type Snippet } from 'svelte';
  import SecretRefInput from './SecretRefInput.svelte';
  import { editable } from './access';
  import type { SecretDto, SecretRef } from './api';

  // `value` is a plain prop: this mutates the `$state` element it is handed
  // and never reassigns it. `secrets` is the catalog, for pointing a
  // credential at one the leader holds.
  let {
    value,
    literalKey,
    labels,
    literalHelp,
    credentialsHelp,
    secrets = [],
    hidden = () => false,
    refusal,
  }: {
    value: { [P in K]?: Record<string, string> } & { credentials?: Record<string, SecretRef> };
    /** The committed map's name in the file. */
    literalKey: K;
    labels: PropsLabels;
    literalHelp: Snippet;
    credentialsHelp: Snippet;
    secrets?: SecretDto[];
    /** Entries another part of the form owns: neither shown nor dropped here. */
    hidden?: (key: string) => boolean;
    /** Why a row cannot be saved, or nothing. `val` is a committed row's value; a credential row has none. */
    refusal: (key: string, credential: boolean, val: string | undefined) => string | undefined;
  } = $props();

  const readOnly = !editable();

  // Rows are the editing state and the maps are rebuilt from them: a key
  // mid-edit is briefly blank or duplicated, and must not lose its row.
  type Row = { key: string; val: string };
  const toRows = (m: Record<string, string> | undefined): Row[] =>
    Object.entries(m ?? {})
      .filter(([key]) => !hidden(key))
      .map(([key, val]) => ({ key, val }));

  /** A credential row's value is a reference, not text: `undefined` while
   * it is still being named. */
  type CredentialRow = { key: string; ref: SecretRef | undefined };
  const toCredentialRows = (m: Record<string, SecretRef> | undefined): CredentialRow[] =>
    Object.entries(m ?? {})
      .filter(([key]) => !hidden(key))
      .map(([key, ref]) => ({ key, ref }));

  let literal = $state(untrack(() => toRows(value[literalKey])));
  let credentials = $state(untrack(() => toCredentialRows(value.credentials)));

  /** The entries these tables neither show nor own. Read from `value` each
   * time, so the owner's latest answer survives an edit here. */
  const kept = <T,>(m: Record<string, T> | undefined): Record<string, T> =>
    Object.fromEntries(Object.entries(m ?? {}).filter(([key]) => hidden(key)));

  function sync() {
    const p: Record<string, string> = kept(value[literalKey]);
    for (const r of literal) if (r.key.trim() !== '' && !hidden(r.key)) p[r.key.trim()] = r.val;
    const c: Record<string, SecretRef> = kept(value.credentials);
    for (const r of credentials) {
      if (r.key.trim() !== '' && r.ref !== undefined && !hidden(r.key)) c[r.key.trim()] = r.ref;
    }
    // An empty map is omitted, as the DTO serializes it.
    if (Object.keys(p).length > 0) value[literalKey] = p as (typeof value)[K];
    else delete value[literalKey];
    if (Object.keys(c).length > 0) value.credentials = c;
    else delete value.credentials;
  }
</script>

{#snippet name(key: string, placeholder: string, set: (v: string) => void)}
  <input
    type="text"
    {placeholder}
    value={key}
    oninput={(e) => {
      set(e.currentTarget.value);
      sync();
    }}
    aria-label="{labels.noun} name"
  />
{/snippet}

{#snippet remove(drop: () => void)}
  {#if !readOnly}<button
      type="button"
      class="compact"
      title="remove"
      onclick={() => {
        drop();
        sync();
      }}>&#10005;</button
    >{/if}
{/snippet}

{#snippet refused(reason: string | undefined)}
  {#if reason}
    <div class="refused">{reason}</div>
  {/if}
{/snippet}

{#snippet literalTable()}
  <div class="rows">
    {#each literal as row, i (i)}
      <div class="row">
        {@render name(row.key, labels.literalName, (v) => (row.key = v))}
        <input
          type="text"
          placeholder={labels.literalValue}
          bind:value={row.val}
          oninput={sync}
          aria-label="{labels.noun} value"
        />
        {@render remove(() => literal.splice(i, 1))}
      </div>
      {@render refused(refusal(row.key, false, row.val))}
    {/each}
  </div>
{/snippet}

{#snippet credentialTable()}
  <div class="rows">
    {#each credentials as row, i (i)}
      <div class="row">
        {@render name(row.key, labels.credentialName, (v) => (row.key = v))}
        <SecretRefInput
          value={row.ref}
          {secrets}
          onchange={(v) => {
            row.ref = v;
            sync();
          }}
        />
        {@render remove(() => credentials.splice(i, 1))}
      </div>
      {@render refused(refusal(row.key, true, undefined))}
    {/each}
  </div>
{/snippet}

<div class="props">
  <Field optional field={literalKey} label={labels.literal} group help={literalHelp}>
    {@render literalTable()}
    {#if !readOnly}
      <button type="button" class="add-small" onclick={() => literal.push({ key: '', val: '' })}
        >+ {labels.noun}</button
      >
    {/if}
  </Field>

  <Field optional field="credentials" label={labels.credentials} group help={credentialsHelp}>
    {@render credentialTable()}
    {#if !readOnly}
      <button
        type="button"
        class="add-small"
        onclick={() => credentials.push({ key: '', ref: undefined })}>+ credential</button
      >
    {/if}
  </Field>
</div>

<style>
  .props {
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }

  /* What the save would refuse, said in the row that caused it. */
  .refused {
    color: var(--error);
    font-size: var(--text-note);
    line-height: 1.4;
    max-width: 34rem;
  }

  .rows {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    max-width: 100%;
    overflow-x: auto;
  }

  .row {
    display: flex;
    gap: 0.35rem;
    align-items: center;
    width: max-content;
  }

  .row input {
    flex: none;
    font-family: var(--mono);
  }

  /* Dashed to read as adding rather than acting on what is already there. */
  .add-small {
    align-self: flex-start;
    background: none;
    border: 1px dashed var(--dim);
    padding: 0.2rem 0.55rem;
    font-size: var(--text-note);
  }
</style>
