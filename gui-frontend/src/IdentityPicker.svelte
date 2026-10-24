<!--
  Which identity a connector connects under: what this worker shows the far end,
  and which authority it verifies that far end against.

  A picker rather than three fields on the connector, because both halves are the
  same organization's PKI and a deployment usually speaks one. The identities are
  authored on Build -> Resources -> Connection identities; this only names one
  of them.
-->
<script lang="ts">
  import { insideDialog } from './Dialog.svelte';
  import Field from './Field.svelte';
  import type { IdentityDto } from './api';

  let {
    value,
    identities,
    /** `source` or `destination` -- what the far end is, in the words the page already uses. `sender` is a receiver's
     * far end: it connects in, so the identity is what the receiver serves rather than what it presents. */
    farEnd,
    onchange,
  }: {
    value: string | undefined;
    identities: IdentityDto[];
    farEnd: 'source' | 'destination' | 'sender';
    onchange: (v: string | undefined) => void;
  } = $props();

  const linked = !insideDialog();
  const chosen = $derived(identities.find((i) => i.id === value));
</script>

{#snippet edit()}<a href="#/build/identities">edit</a>{/snippet}

<Field field="identity" label="identity" note={chosen && linked ? edit : undefined}>
  {#snippet help()}
    {#if farEnd === 'sender'}
      The certificate and key it serves TLS with. If the identity has authorities, every sender must
      present a certificate that chains to one of them. None: plain HTTP, and tokens cross the
      network in the clear.
    {:else}
      Pick one when the {farEnd} requires a client certificate, or its own certificate comes from your
      organization's authority rather than a public one. None: no certificate, and the platform's public
      roots.
    {/if}
  {/snippet}
  <select
    value={value ?? ''}
    onchange={(e) => onchange(e.currentTarget.value === '' ? undefined : e.currentTarget.value)}
  >
    <option value="">none</option>
    {#each identities as i (i.id)}
      <option value={i.id}>{i.id}</option>
    {/each}
    {#if value !== undefined && !identities.some((i) => i.id === value)}
      <option {value}>{value} (not listed)</option>
    {/if}
  </select>
</Field>

<style>
  select {
    width: 100%;
  }
</style>
