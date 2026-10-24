<!--
  Picks a certificate from the list on Admin -> Certificates by name. Its add
  link opens the upload there and comes back here once it is added.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import { reported } from './errors.svelte';
  import { listCertificates, type CertificateDto } from './api';
  import { expiry } from './time';

  let {
    value = $bindable(),
    label = 'Certificate',
    note,
  }: {
    value: string | undefined;
    label?: string;
    /** What the chosen certificate must carry, for the page that knows. */
    note?: string;
  } = $props();

  let certificates: CertificateDto[] = $state([]);
  listCertificates()
    .then((listed) => (certificates = listed))
    .catch(reported);

  const chosen = $derived(certificates.find((c) => c.name === value)?.chain[0]);
</script>

<Field {label}>
  {#snippet note()}
    {#if chosen}
      {chosen.names.join(', ') || chosen.subject} · {expiry(chosen.notAfterMillis)}
    {:else if note}
      {note}
    {/if}
    &mdash; <a href="#/admin/certificates/new">add a certificate</a>
  {/snippet}
  <select bind:value>
    <option value={undefined} disabled>choose one</option>
    {#each certificates as c (c.name)}
      <option value={c.name}>{c.name}</option>
    {/each}
  </select>
</Field>

<style>
  select {
    width: 100%;
  }
</style>
