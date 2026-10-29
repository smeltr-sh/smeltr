<!--
  How a Kafka client reaches its brokers: the protocol, the SASL mechanism
  under it, and what that mechanism asks for. It writes librdkafka
  properties into the same two maps `KafkaPropsEditor` shows by hand -- a
  committed value in one, a secret's name in the other -- so there is one
  file underneath and two ways to write it.

  Asked in the order it is decided. A protocol that does not authenticate
  asks for no mechanism, and a mechanism nothing selects asks for no
  fields.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import SecretRefInput from './SecretRefInput.svelte';
  import {
    PROTOCOLS,
    asksForMechanism,
    fieldsFor,
    readSecurity,
    writeSecurity,
    setCredential,
    setField,
    TLS_PROTOCOLS,
    type Protocol,
  } from './kafka';
  import type { KafkaProps, SecretDto, SecretRef } from './api';

  let {
    value,
    secrets = [],
    mechanisms = [],
    hasTls = false,
  }: {
    /** Mutated in place, as `KafkaPropsEditor` mutates it: this writes the
     * connector's own property maps, and the page's Save is what commits. */
    value: KafkaProps;
    secrets?: SecretDto[];
    /** What this leader's build can select, from `GET /api/system`.
     * Empty is that answer not arrived, or a leader older than the field;
     * the section then shows what the file says and offers no menu, rather
     * than reading as a build that carries nothing. */
    mechanisms?: string[];
    /** Whether the TLS settings answer for TLS. */
    hasTls?: boolean;
  } = $props();

  let security = $derived(readSecurity(value));
  let fields = $derived(fieldsFor(security));
  let negotiatesTls = $derived(
    security.protocol !== undefined && TLS_PROTOCOLS.includes(security.protocol),
  );
  /** Whether the leader has said what its build carries. An empty list is a
   * question unanswered -- the request failed, or the leader is older than
   * the field -- not a build that carries nothing. */
  let known = $derived(mechanisms.length > 0);
  /** Whether the mechanism the file names is one the list offers. */
  let listed = $derived(
    security.mechanism !== undefined && mechanisms.includes(security.mechanism),
  );
  /** A mechanism the file names and this build cannot select. Shown rather
   * than dropped: a bundle written against another build is still what this
   * source says, and the save is where it is refused. */
  let unavailable = $derived(known && security.mechanism !== undefined && !listed);

  const literal = (prop: string): string => value.properties?.[prop] ?? '';
  const credential = (prop: string): SecretRef | undefined => value.credentials?.[prop];
</script>

<div class="security">
  <Field field="properties.security.protocol" label="protocol">
    {#snippet help()}
      <code>SSL</code> and <code>SASL_SSL</code> use TLS; the two <code>SASL_</code> protocols also
      log in.
      {#if negotiatesTls && hasTls}
        The certificate, the key and the trusted authorities come from the TLS settings above.
      {:else if negotiatesTls}
        No TLS setting is filled, so this client presents no certificate and trusts the public
        roots.
      {/if}
    {/snippet}
    <!-- Unset is shown as PLAINTEXT, librdkafka's own default. -->
    <select
      value={security.protocol ?? 'PLAINTEXT'}
      onchange={(e) => {
        writeSecurity(value, {
          protocol: e.currentTarget.value as Protocol,
          mechanism: security.mechanism,
        });
      }}
    >
      {#each PROTOCOLS as protocol (protocol)}
        <option value={protocol}>{protocol}</option>
      {/each}
    </select>
  </Field>

  {#if asksForMechanism(security.protocol)}
    <Field
      label="mechanism"
      error={unavailable
        ? 'This build carries no provider for it, and the save is refused. Pick another, or deploy a build that carries it.'
        : null}
    >
      {#snippet help()}
        How this client logs in. <code>GSSAPI</code> (Kerberos) is listed only in a build linked
        with a Kerberos provider.{#if !known}
          This leader did not say which mechanisms its build carries, so this shows the value saved
          in the config repository, and there is nothing to choose from.{/if}
      {/snippet}
      <select
        value={security.mechanism ?? ''}
        onchange={(e) => {
          const picked = e.currentTarget.value;
          writeSecurity(value, {
            protocol: security.protocol,
            mechanism: picked === '' ? undefined : picked,
          });
        }}
      >
        <option value="">none</option>
        {#each mechanisms as mechanism (mechanism)}
          <option value={mechanism}>{mechanism}</option>
        {/each}
        {#if security.mechanism !== undefined && !listed}
          <option value={security.mechanism}
            >{security.mechanism}{known ? ' (not in this build)' : ''}</option
          >
        {/if}
      </select>
    </Field>

    {#each fields as f (f.prop)}
      <Field label={f.label} required={!f.optional} optional={f.optional}>
        {#snippet help()}
          {f.note} Written as <code>{f.prop}</code>.
        {/snippet}
        {#if f.secret}
          <SecretRefInput
            value={credential(f.prop)}
            {secrets}
            label={f.label}
            onchange={(v) => setCredential(value, f.prop, v)}
          />
        {:else}
          <input
            type="text"
            value={literal(f.prop)}
            oninput={(e) => setField(value, f.prop, e.currentTarget.value)}
            aria-label={f.label}
          />
        {/if}
      </Field>
    {/each}
  {/if}
</div>

<style>
  .security {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }

  input,
  select {
    font-family: var(--mono);
  }
</style>
