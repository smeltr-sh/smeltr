<!--
  The TLS a connector speaks of its own: the certificate it presents, from the
  list on Admin -> Certificates, and the authorities it verifies the far end
  against. A source or destination presents it as a client and trusts the
  public roots beside its authorities unless told not to; a receiver serves it
  and admits a sender whose certificate chains to its authorities alone.

  The fields are the connector's own (`TlsDto`), so `value` is the connector.
-->
<script lang="ts">
  import Advanced from './Advanced.svelte';
  import CertificatePicker from './CertificatePicker.svelte';
  import Field from './Field.svelte';
  import InlineError from './InlineError.svelte';
  import Toggle from './Toggle.svelte';
  import {
    describeCertificates,
    errorMessage,
    type DescribedCertificate,
    type TlsDto,
  } from './api';
  import { expiry } from './time';

  let {
    value,
    /** `source` or `destination` -- what the far end is, in the words the page already uses. `sender` is a
     * receiver's far end: it connects in, so the certificate is what the receiver serves rather than presents. */
    farEnd,
  }: {
    value: TlsDto;
    farEnd: 'source' | 'destination' | 'sender';
  } = $props();

  const serves = $derived(farEnd === 'sender');

  // The leader reads the PEM: parsing X.509 in the browser would be a second implementation of what `leader::tls`
  // already does, and the two would drift. A field nobody has pasted into asks nothing.
  type Described = { certificates: DescribedCertificate[] } | { error: string } | null;

  /** Keyed by the PEM itself, so an edit that changes nothing asks nothing. */
  let described = $state<Record<string, Described>>({});

  $effect(() => {
    const text = (value.authorities ?? '').trim();
    if (text === '' || text in described) return;
    described[text] = null;
    describeCertificates(text)
      .then((res) => (described[text] = res))
      .catch((e) => (described[text] = { error: errorMessage(e) }));
  });

  const readBack = $derived.by((): Described => {
    const text = (value.authorities ?? '').trim();
    return text === '' ? null : (described[text] ?? null);
  });

  /** An empty text box unsets the field rather than naming `""`. */
  function optional(text: string): string | undefined {
    return text.trim() === '' ? undefined : text;
  }
</script>

<CertificatePicker
  bind:value={value.certificate}
  label={serves ? 'server certificate' : 'client certificate'}
  field="certificate"
  optional
  warning={serves && value.certificate === undefined
    ? 'With none, traffic is received unencrypted. Strongly recommended to choose a certificate.'
    : null}
>
  {#snippet help()}
    {#if serves}
      The certificate this receiver serves TLS with, chosen from the list on
      <a href="#/admin/certificates">Admin &rarr; Certificates</a>. Workers serve its newest
      version.
    {:else}
      The certificate this worker presents to the {farEnd}, chosen from the list on
      <a href="#/admin/certificates">Admin &rarr; Certificates</a>. Needed only when the {farEnd} requires
      a client certificate. Workers present its newest version.
    {/if}
  {/snippet}
</CertificatePicker>

<Field optional field="authorities" label="authorities">
  {#snippet help()}
    {#if serves}
      Authorities a sender's certificate must chain to. With any, every sender must present a
      certificate. PEM, one or more certificates.
    {:else}
      Authorities the {farEnd}'s certificate may chain to, beside the public roots unless trust
      public roots is off. Needed only when the
      {farEnd}'s certificate comes from your organization's authority rather than a public one. PEM,
      one or more certificates.
    {/if}
  {/snippet}
  <textarea
    rows="4"
    spellcheck="false"
    value={value.authorities ?? ''}
    oninput={(e) => (value.authorities = optional(e.currentTarget.value))}></textarea>
</Field>
{#if readBack && 'error' in readBack}
  <div class="read-back bad"><InlineError message={readBack.error} /></div>
{:else if readBack}
  {#each readBack.certificates as cert, i (cert.fingerprint + i)}
    <p class="read-back">
      <span class="subject">{cert.subject}</span>
      <span class="sep">&middot;</span>
      <span>{expiry(cert.notAfterMillis)}</span>
      <span class="sep">&middot;</span>
      <span class="mono">{cert.fingerprint.slice(0, 16)}&hellip;</span>
    </p>
  {/each}
{/if}
{#if !serves}
  <Advanced label="Advanced TLS" modified={value.trustPublic === false}>
    <Field field="trustPublic" label="trust public roots">
      {#snippet help()}
        Whether this worker also trusts the public roots: Mozilla's and its host's own. Off, only
        certificates from the authorities above are accepted.
      {/snippet}
      <Toggle
        bind:checked={
          () => value.trustPublic !== false, (on) => (value.trustPublic = on ? undefined : false)
        }
        ariaLabel="trust public roots"
      />
    </Field>
  </Advanced>
{/if}

<style>
  textarea {
    width: 100%;
    font-family: var(--mono);
    font-size: 0.78rem;
    line-height: 1.45;
    resize: vertical;
  }

  /* What the leader read back out of the PEM above it, tied to that field by the bar down its
     left edge rather than by position alone. */
  .read-back {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.4rem;
    margin: -0.4rem 0 0;
    border-left: 3px solid var(--accent);
    border-radius: var(--radius);
    padding: 0.25rem 0.5rem;
    font-size: var(--text-note);
    color: var(--dim);
  }

  .read-back.bad {
    border-left-color: var(--error);
    color: var(--error);
  }

  .read-back .subject {
    font-weight: 600;
    color: var(--ink);
  }

  .read-back .sep {
    opacity: 0.5;
  }

  .mono {
    font-family: var(--mono);
  }
</style>
