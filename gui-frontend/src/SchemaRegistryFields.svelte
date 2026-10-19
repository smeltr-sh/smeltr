<!--
  The schema registry a Kafka or Event Hubs connector's Avro schemas are in: its URL, and a sign-in where it needs
  one. A source reads each message's schema there; a destination registers its own.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import SecretRefInput from './SecretRefInput.svelte';
  import type { SchemaRegistry, SecretDto } from './api';

  let {
    registry,
    secrets,
  }: {
    registry: SchemaRegistry;
    /** The secret catalog, for the password. */
    secrets: SecretDto[];
  } = $props();

  /** The username and password sign in together: clearing the username clears both. */
  function setUsername(username: string) {
    if (username.trim() === '') {
      delete registry.username;
      delete registry.password;
    } else registry.username = username;
  }
</script>

<Field field="schemaRegistry.url" label="schema registry">
  {#snippet help()}
    The schema registry that holds your Avro schemas: its URL.
  {/snippet}
  <input type="text" bind:value={registry.url} placeholder="https://registry.example.com" />
</Field>
<Field field="schemaRegistry.username" label="registry username">
  {#snippet note()}Only if the registry needs a sign-in.{/snippet}
  <input
    type="text"
    value={registry.username ?? ''}
    oninput={(e) => setUsername(e.currentTarget.value)}
  />
</Field>
{#if registry.username !== undefined}
  <Field field="schemaRegistry.password" label="registry password">
    <SecretRefInput
      value={registry.password}
      {secrets}
      label="registry password"
      onchange={(v) => (registry.password = v === '' ? undefined : v)}
    />
  </Field>
{/if}

<style>
  input {
    font-family: var(--mono);
  }
</style>
