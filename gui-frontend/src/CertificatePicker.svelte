<!--
  Picks a certificate from the list on Admin -> Certificates by name. Its add
  link opens the upload there and comes back here once it is added.

  An identity names a version too, as a reference to a stored secret does:
  `version` is the one it names, which the note reads beside the name.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import Field from './Field.svelte';
  import { reported } from './errors.svelte';
  import { currentVersion, listCertificates, type CertificateDto } from './api';
  import { expiry } from './time';

  let {
    value = $bindable(),
    label = 'Certificate',
    note,
    help,
    field,
    version,
    optional = false,
  }: {
    value: string | undefined;
    label?: string;
    /** What the chosen certificate must carry, for the page that knows. */
    note?: string | Snippet;
    help?: Snippet | string;
    /** The setting's name in the file, for its save errors (`Field`). */
    field?: string;
    /** The version a reference names, where it names one. */
    version?: number;
    /** Offers "none", for a choice that may present nothing. */
    optional?: boolean;
  } = $props();

  let certificates: CertificateDto[] = $state([]);
  listCertificates()
    .then((listed) => (certificates = listed))
    .catch(reported);

  const entry = $derived(certificates.find((c) => c.name === value));
  /** The chain is the newest version's, so it describes the one named only when that is the newest. */
  const newest = $derived(
    entry && (version === undefined || version === currentVersion(entry)?.version),
  );
  const chosen = $derived(newest ? entry?.chain[0] : undefined);
</script>

<Field {label} {help} {field}>
  {#snippet note()}
    {#if version !== undefined && value}
      version {version}{#if chosen}&nbsp;·{/if}
    {/if}
    {#if chosen}
      {chosen.names.join(', ') || chosen.subject} · {expiry(chosen.notAfterMillis)}
    {:else if typeof note === 'function'}
      {@render note()}
    {:else if note}
      {note}
    {/if}
    &mdash; <a href="#/admin/certificates/new">add a certificate</a>
  {/snippet}
  <select bind:value>
    {#if optional}
      <option value={undefined}>none</option>
    {:else}
      <option value={undefined} disabled>choose one</option>
    {/if}
    {#each certificates as c (c.name)}
      <option value={c.name}>{c.name}</option>
    {/each}
    {#if value !== undefined && certificates.length > 0 && !entry}
      <option {value}>{value} (not listed)</option>
    {/if}
  </select>
</Field>

<style>
  select {
    width: 100%;
  }
</style>
