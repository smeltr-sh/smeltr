<!--
  An Iceberg destination's form: the table it writes, how it signs in to the
  catalog, and, under Advanced, the catalog's and storage's own settings and
  the delivery limits. Mutates `value` in place; the page's Save commits.
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
    SIGN_INS,
    SIGNING_NAMES,
    catalogRefusal,
    catalogStorageModified,
    namespaceRefusal,
    partitionRefusal,
    regionRefusal,
    setSignIn,
    signInOf,
    signingNameRefusal,
    type CatalogSignIn,
    type IcebergDestination,
  } from './iceberg';

  let {
    value: d,
    secrets,
    identities,
  }: { value: IcebergDestination; secrets: SecretDto[]; identities: IdentityDto[] } = $props();

  const signIn = $derived(signInOf(d));
  const partitionError = $derived(
    (d.partitionBy ?? []).map(partitionRefusal).find((r) => r !== undefined),
  );

  // A refusal in a closed section would point at a field no one can see.
  let storageOpen = $state(untrack(() => catalogStorageModified(d)));
  $effect(() => {
    if (regionRefusal(d)) storageOpen = true;
  });

  /** An empty text box unsets the field rather than naming `""`. */
  const optional = (v: string) => (v.trim() === '' ? undefined : v);
</script>

<UrlField
  field="catalog"
  label="catalog"
  bind:value={d.catalog}
  sends
  error={catalogRefusal(d) ?? null}
  placeholder="https://polaris.example.com/api/catalog"
>
  {#snippet help()}
    The REST catalog's base address, before <code>/v1</code>.
  {/snippet}
</UrlField>
<div class="pair">
  <Field field="namespace" label="namespace" error={namespaceRefusal(d.namespace) ?? null}>
    {#snippet help()}
      The table's namespace, its levels joined by dots.
    {/snippet}
    <input type="text" bind:value={d.namespace} placeholder="analytics.web" />
  </Field>
  <Field field="table" label="table">
    {#snippet help()}
      The table it writes. A missing table is created from the route's record.
    {/snippet}
    <input type="text" bind:value={d.table} placeholder="access_log" />
  </Field>
</div>
<Field field="partitionBy" label="partition by" group error={partitionError ?? null}>
  {#snippet help()}
    How a table it creates is partitioned: a column, or <code>year</code>, <code>month</code>,
    <code>day</code> or <code>hour</code> of one, or <code>bucket(n, column)</code> or
    <code>truncate(width, column)</code>. A table that exists keeps its own. Default none: an
    unpartitioned table.
  {/snippet}
  <TokenListInput
    bind:values={
      () => d.partitionBy ?? [],
      (terms) => {
        if (terms.length > 0) d.partitionBy = terms;
        else delete d.partitionBy;
      }
    }
    comma={false}
    placeholder="day(ts)"
  />
</Field>
<Field label="catalog sign-in">
  {#snippet help()}
    What it signs in to the catalog with. AWS signatures sign every catalog call for Glue's Iceberg
    endpoint or S3 Tables, with the storage credential and role under Catalog and storage, else the
    host's credentials.
  {/snippet}
  <select value={signIn} onchange={(e) => setSignIn(d, e.currentTarget.value as CatalogSignIn)}>
    {#each Object.entries(SIGN_INS) as [choice, label] (choice)}
      <option value={choice}>{label}</option>
    {/each}
  </select>
</Field>
{#if signIn !== 'none'}
  <Nested>
    {#if signIn === 'token'}
      <Field field="token" label="token">
        {#snippet help()}
          The secret holding a bearer token for the catalog.
        {/snippet}
        <SecretRefInput
          value={d.token}
          {secrets}
          label="token"
          onchange={(v) => (d.token = v ?? '')}
        />
      </Field>
    {:else if signIn === 'client'}
      <Field field="clientId" label="client id">
        {#snippet help()}
          The OAuth client it signs in to the catalog as.
        {/snippet}
        <input type="text" bind:value={d.clientId} />
      </Field>
      <Field field="clientSecret" label="client secret">
        {#snippet help()}
          The secret holding the OAuth client's secret.
        {/snippet}
        <SecretRefInput
          value={d.clientSecret}
          {secrets}
          label="client secret"
          onchange={(v) => (d.clientSecret = v ?? '')}
        />
      </Field>
    {:else if signIn === 'aws'}
      {@const known = (SIGNING_NAMES as readonly string[]).includes(d.signingName ?? '')}
      <Field
        field="signingName"
        label="signing service"
        error={signingNameRefusal(d) ?? regionRefusal(d) ?? null}
      >
        {#snippet help()}
          <code>glue</code> for Glue's Iceberg endpoint, <code>s3tables</code> for S3 Tables. Calls
          are signed for the region the catalog's AWS address names, else its <code>region</code>.
        {/snippet}
        <select value={d.signingName} onchange={(e) => (d.signingName = e.currentTarget.value)}>
          {#each SIGNING_NAMES as name (name)}
            <option value={name}>{name}</option>
          {/each}
          {#if !known}<option value={d.signingName}>{d.signingName}</option>{/if}
        </select>
      </Field>
    {/if}
  </Nested>
{/if}
<IdentityPicker
  value={d.identity}
  {identities}
  farEnd="destination"
  onchange={(v) => (d.identity = v)}
/>
<Advanced label="Catalog and storage" modified={catalogStorageModified(d)} bind:open={storageOpen}>
  <Field field="warehouse" label="warehouse">
    {#snippet help()}
      The warehouse the catalog serves the table from.
    {/snippet}
    <input
      type="text"
      value={d.warehouse ?? ''}
      oninput={(e) => (d.warehouse = optional(e.currentTarget.value))}
      placeholder="the catalog's default"
    />
  </Field>
  {#if signIn === 'client'}
    <UrlField
      field="tokenUrl"
      label="token URL"
      bind:value={d.tokenUrl}
      sends
      optional
      placeholder="https://auth.example.com/oauth2/token"
    >
      {#snippet help()}
        Where the client's token is obtained. Default: the catalog's own <code
          >/v1/oauth/tokens</code
        >.
      {/snippet}
    </UrlField>
    <Field field="scope" label="scope">
      {#snippet help()}
        The scope the client's token is asked for. Use the scope your catalog takes.
      {/snippet}
      <input
        type="text"
        value={d.scope ?? ''}
        oninput={(e) => (d.scope = optional(e.currentTarget.value))}
      />
    </Field>
  {/if}
  <ConnectorAccess store="iceberg" value={d} show="credential" {secrets} />
  <ConnectorAccess
    store="iceberg"
    value={d}
    show="advanced"
    errors={{ region: regionRefusal(d) }}
  />
</Advanced>
<TableDelivery value={d} calls="the store or the catalog" commit="a snapshot" />

<style>
  /* The namespace beside the table it holds. */
  .pair {
    display: flex;
    gap: 0.8rem;
    align-items: flex-start;
  }

  .pair > :global(*) {
    flex: 1;
  }

  code {
    font-family: var(--mono);
    font-size: 0.9em;
  }

  input,
  select {
    font-family: var(--mono);
  }
</style>
