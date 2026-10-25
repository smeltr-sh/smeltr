<!--
  A Delta destination's form: where its table is, by its root in S3 or as a
  Unity Catalog table, how a created one is partitioned, how its bucket is
  signed for, and, under Advanced, where the bucket is reached and the
  delivery limits. Mutates `value` in place; the
  page's Save commits.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import Advanced from './Advanced.svelte';
  import TableDelivery from './TableDelivery.svelte';
  import Field from './Field.svelte';
  import Nested from './Nested.svelte';
  import UrlField from './UrlField.svelte';
  import IdentityPicker from './IdentityPicker.svelte';
  import ConnectorAccess from './ConnectorAccess.svelte';
  import SecretRefInput from './SecretRefInput.svelte';
  import TokenListInput from './TokenListInput.svelte';
  import type { IdentityDto, SecretDto } from './api';
  import {
    TABLE_NAMINGS,
    catalogTableRefusal,
    clientIdRefusal,
    endpointRefusal,
    locationRefusal,
    namingOf,
    partitionRefusal,
    regionRefusal,
    setNaming,
    storageModified,
    workspaceRefusal,
    type DeltaDestination,
    type TableNaming,
  } from './delta';

  let {
    value: d,
    secrets,
    identities,
  }: { value: DeltaDestination; secrets: SecretDto[]; identities: IdentityDto[] } = $props();

  const naming = $derived(namingOf(d));

  // A refusal in a closed section would point at a field no one can see.
  let storageOpen = $state(untrack(() => storageModified(d)));
  $effect(() => {
    if (endpointRefusal(d)) storageOpen = true;
  });

  /** An empty text box unsets the field rather than naming `""`. */
  const optional = (v: string) => (v.trim() === '' ? undefined : v);
</script>

<Field label="table named by">
  {#snippet help()}
    Its root in S3, or a Unity Catalog table, whose root and storage credentials the catalog gives.
  {/snippet}
  <select value={naming} onchange={(e) => setNaming(d, e.currentTarget.value as TableNaming)}>
    {#each Object.entries(TABLE_NAMINGS) as [choice, label] (choice)}
      <option value={choice}>{label}</option>
    {/each}
  </select>
</Field>
<Nested>
  {#if d.unityCatalog}
    <UrlField
      field="unityCatalog.workspace"
      label="workspace"
      bind:value={d.unityCatalog.workspace}
      sends
      error={workspaceRefusal(d.unityCatalog.workspace) ?? null}
      placeholder="https://dbc-1234.cloud.databricks.com"
    >
      {#snippet help()}
        The Databricks workspace's address, and nothing after its host.
      {/snippet}
    </UrlField>
    <Field
      field="unityCatalog.table"
      label="table"
      error={catalogTableRefusal(d.unityCatalog.table) ?? null}
    >
      {#snippet help()}
        The external table it writes, by its three parts. A missing table is created from the
        route's record.
      {/snippet}
      <input type="text" bind:value={d.unityCatalog.table} placeholder="main.web.access_log" />
    </Field>
    <Field field="region" label="region" error={regionRefusal(d) ?? null}>
      {#snippet help()}
        The region of the table's bucket.
      {/snippet}
      <input
        type="text"
        value={d.region ?? ''}
        oninput={(e) => (d.region = optional(e.currentTarget.value))}
        placeholder="us-west-2"
      />
    </Field>
    <Field
      field="unityCatalog.clientId"
      label="client id"
      error={clientIdRefusal(d.unityCatalog.clientId) ?? null}
    >
      {#snippet help()}
        The OAuth client id of the service principal it signs in as.
      {/snippet}
      <input type="text" bind:value={d.unityCatalog.clientId} />
    </Field>
    <Field field="unityCatalog.clientSecret" label="client secret">
      {#snippet help()}
        The secret holding the service principal's OAuth secret.
      {/snippet}
      <SecretRefInput
        value={d.unityCatalog.clientSecret}
        {secrets}
        label="client secret"
        onchange={(v) => {
          if (d.unityCatalog) d.unityCatalog.clientSecret = v ?? '';
        }}
      />
    </Field>
  {:else}
    <Field field="location" label="location" error={locationRefusal(d.location ?? '') ?? null}>
      {#snippet help()}
        The table's root in S3. A missing table is created there from the route's record.
      {/snippet}
      <input type="text" bind:value={d.location} placeholder="s3://lake/tables/access_log" />
    </Field>
  {/if}
</Nested>
<Field
  field="partitionBy"
  label="partition by"
  group
  error={partitionRefusal(d.partitionBy ?? []) ?? null}
>
  {#snippet help()}
    The columns a table it creates is partitioned by, each a top-level column of the record. A table
    that exists keeps its own. Default none: an unpartitioned table.
  {/snippet}
  <TokenListInput
    bind:values={
      () => d.partitionBy ?? [],
      (columns) => {
        if (columns.length > 0) d.partitionBy = columns;
        else delete d.partitionBy;
      }
    }
    placeholder="host"
  />
</Field>
{#if naming === 'location'}
  <ConnectorAccess store="s3" value={d} show="credential" {secrets} />
{/if}
<IdentityPicker
  value={d.identity}
  {identities}
  farEnd="destination"
  onchange={(v) => (d.identity = v)}
/>
<Advanced label="Storage" modified={storageModified(d)} bind:open={storageOpen}>
  {#if naming === 'unityCatalog'}
    <UrlField
      field="endpoint"
      label="endpoint"
      bind:value={d.endpoint}
      sends
      optional
      error={endpointRefusal(d) ?? null}
      placeholder="https://s3.amazonaws.com"
    >
      {#snippet help()}
        An S3-compatible service's address. Left empty, AWS.
      {/snippet}
    </UrlField>
  {:else}
    <ConnectorAccess
      store="s3"
      value={d}
      show="advanced"
      errors={{ endpoint: endpointRefusal(d) }}
    />
  {/if}
</Advanced>
<TableDelivery value={d} calls="the store" commit="a version to the table's log" />

<style>
  input,
  select {
    font-family: var(--mono);
  }
</style>
