<!--
  Picks a certificate from the list on Admin -> Certificates by name. Its add
  link opens the upload there and comes back here once it is added. The note
  reads the newest version's leaf beside the name: the one a worker presents.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import Field from './Field.svelte';
  import { reported } from './errors.svelte';
  import { listCertificates, type CertificateDto } from './api';
  import { expiry } from './time';

  let {
    value = $bindable(),
    label = 'Certificate',
    note,
    help,
    field,
    warning = null,
    optional = false,
    usableAs,
  }: {
    value: string | undefined;
    label?: string;
    /** What the chosen certificate must carry, for the page that knows. */
    note?: Snippet;
    help?: Snippet | string;
    /** The setting's name in the file, for its save errors (`Field`). */
    field?: string;
    /** What to say under the field while nothing is chosen, for a choice that should not stay empty. */
    warning?: string | null;
    /** Offers "none", for a choice that may present nothing. */
    optional?: boolean;
    /** The side it is presented from: only a certificate usable from it is offered, since a worker refuses any
     * other. */
    usableAs: 'server' | 'client';
  } = $props();

  /** `undefined` until the list arrives, so a name it does not hold is not offered as missing before then. */
  let certificates: CertificateDto[] | undefined = $state();
  listCertificates()
    .then((listed) => (certificates = listed))
    .catch(reported);

  const usable = (c: CertificateDto) => (usableAs === 'server' ? c.forServers : c.forClients);
  const offered = $derived(certificates?.filter(usable));
  const entry = $derived(certificates?.find((c) => c.name === value));
  const chosen = $derived(entry?.chain[0]);
</script>

{#snippet shownNote()}
  {#if chosen}
    {chosen.names.join(', ') || chosen.subject} · {expiry(chosen.notAfterMillis)}
  {:else if note}
    {@render note()}
  {/if}
  {#if chosen || note}&mdash;{/if}
  <a href="#/admin/certificates/new">add a certificate</a>
{/snippet}

<Field {label} {help} {field} {optional} {warning} note={shownNote}>
  <select bind:value>
    {#if optional}
      <option value={undefined}>none</option>
    {:else}
      <option value={undefined} disabled>choose one</option>
    {/if}
    {#each offered ?? [] as c (c.name)}
      <option value={c.name}>{c.name}</option>
    {/each}
    {#if value !== undefined && certificates && !entry}
      <option {value}>{value} (not listed)</option>
    {:else if entry && !usable(entry)}
      <option {value}>{value} (not usable as a {usableAs} certificate)</option>
    {/if}
  </select>
</Field>

<style>
  select {
    width: 100%;
  }
</style>
