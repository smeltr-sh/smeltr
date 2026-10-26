<!--
  The certificate list: each a chain and its private key, kept by the leader
  as a leader-stored secret is. The leader serves a copy of one; a key is
  never shown again once uploaded.

  Opened at #/admin/certificates/new by a chooser's add link, it starts on the
  upload and goes back to that chooser once the certificate is added.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import { reported } from './errors.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Field from './Field.svelte';
  import Dialog from './Dialog.svelte';
  import Segmented from './Segmented.svelte';
  import { ask } from './Confirm.svelte';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';
  import {
    listCertificates,
    createCertificate,
    issueCertificate,
    ISSUED_VALIDITY_DAYS,
    replaceCertificate,
    deleteSecret,
    currentVersion,
    type CertificateDto,
  } from './api';
  import { ago, expiringSoon, expiry } from './time';

  let { adding: startAdding = false }: { adding?: boolean } = $props();

  let certificates: CertificateDto[] = $state([]);
  let loading = $state(true);

  async function load() {
    loading = true;
    try {
      certificates = await listCertificates();
    } catch (e) {
      reported(e);
    } finally {
      loading = false;
    }
  }
  load();

  /** The certificate being uploaded or replaced; `name` is null while adding a new one. */
  let editing = $state<{ name: string | null } | null>(
    untrack(() => (startAdding ? { name: null } : null)),
  );
  let newName = $state('');
  let newDescription = $state('');
  let pem = $state('');
  let privateKey = $state('');
  let saving = $state(false);
  /** How a new certificate arrives: pasted, or issued by this leader. */
  let origin = $state<'upload' | 'issued'>('upload');
  const ORIGINS = [
    { value: 'upload' as const, label: 'Upload' },
    { value: 'issued' as const, label: 'Issued by this leader' },
  ];
  let issuedNames = $state('');
  let validityDays = $state(365);
  const namesAsked = $derived(
    issuedNames
      .split(/[\s,]+/)
      .map((n) => n.trim())
      .filter((n) => n !== ''),
  );

  function open(name: string | null) {
    editing = { name };
    newName = '';
    newDescription = '';
    pem = '';
    privateKey = '';
    origin = 'upload';
    issuedNames = '';
    validityDays = 365;
  }

  const nameProblem = (name: string): string | null =>
    certificates.some((c) => c.name === name.trim()) ? `${name.trim()} is taken` : null;

  const issuing = $derived(editing?.name === null && origin === 'issued');

  const canSave = $derived(
    !saving &&
      (issuing ? namesAsked.length > 0 : pem.trim() !== '' && privateKey.trim() !== '') &&
      (editing?.name != null || (newName.trim() !== '' && nameProblem(newName) === null)),
  );

  /** Refused while the upload is in flight: closing then would hide whether it landed. */
  function cancel() {
    if (saving) return;
    editing = null;
    if (startAdding) history.back();
  }

  async function save() {
    if (!canSave || !editing) return;
    saving = true;
    try {
      const res = issuing
        ? await issueCertificate(newName.trim(), newDescription.trim(), namesAsked, validityDays)
        : editing.name === null
          ? await createCertificate(newName.trim(), newDescription.trim(), pem, privateKey)
          : await replaceCertificate(editing.name, pem, privateKey);
      if (!res.ok) return;
      editing = null;
      privateKey = '';
      if (startAdding) {
        history.back();
        return;
      }
      await load();
    } catch (e) {
      reported(e);
    } finally {
      saving = false;
    }
  }

  async function remove(c: CertificateDto) {
    const yes = await ask({
      title: 'delete certificate',
      body: `Delete ${c.name}. Every version and its private key go with it.`,
      action: 'Delete',
      danger: true,
    });
    if (!yes) return;
    try {
      const res = await deleteSecret(c.name);
      if (res.ok) await load();
    } catch (e) {
      reported(e);
    }
  }

  const leaf = (c: CertificateDto) => c.chain[0];

  /** What the newest version can be used for, as its Extended Key Usage and Key Usage permit. */
  const purpose = (c: CertificateDto) =>
    c.forServers && c.forClients
      ? 'Server and client'
      : c.forServers
        ? 'Server'
        : c.forClients
          ? 'Client'
          : 'Neither';
  const sort = new TableSort<'name' | 'names' | 'purpose' | 'expires' | 'setAt'>();
  let sorted = $derived(
    sort.apply(certificates, {
      name: (c) => c.name,
      names: (c) => leaf(c)?.names.join(', '),
      purpose: (c) => (leaf(c) ? purpose(c) : undefined),
      expires: (c) => leaf(c)?.notAfterMillis,
      setAt: (c) => currentVersion(c)?.setAt,
    }),
  );
</script>

<SectionShell>
  <PageHeader title="Certificates">
    {#snippet help()}
      Certificates and their private keys, for the leader and for sources and destinations to
      present. Usable as says what a certificate's Extended Key Usage permits: the leader and a
      receiver use a server certificate, and a connector connecting out uses a client certificate.
      Neither means no TLS connection accepts it, and -- means this leader cannot open it.
    {/snippet}
  </PageHeader>

  <div class="toolbar">
    <button type="button" class="primary" onclick={() => open(null)} disabled={editing !== null}>
      + Add certificate
    </button>
  </div>

  <Dialog
    open={editing !== null}
    title={editing?.name ? `replace ${editing.name}` : 'new certificate'}
    oncancel={cancel}
    onsubmit={save}
  >
    <div class="form-body">
      {#if editing?.name === null}
        <Segmented label="How it arrives" options={ORIGINS} bind:value={origin} />
        <Field required label="Name" error={nameProblem(newName)}>
          <input bind:value={newName} placeholder="receiver-tls" class="mono" />
        </Field>
      {/if}
      {#if issuing}
        <Field
          required
          label="Issued for"
          help="DNS names and IP addresses, one per line. This leader signs it with its own authority, which whatever connects to it must trust. The leader renews it on its own before it expires, and workers use the new version from their next heartbeat."
        >
          <textarea
            bind:value={issuedNames}
            rows="3"
            spellcheck="false"
            placeholder={'receiver.example.com\n10.0.0.7'}></textarea>
        </Field>
        <Field
          label="Valid for"
          help="Each renewal is valid for the same length of time. A certificate never outlasts this leader's authority: if the authority expires first, so does the certificate."
        >
          <select bind:value={validityDays}>
            {#each ISSUED_VALIDITY_DAYS as days}
              <option value={days}
                >{days < 365
                  ? `${days} days`
                  : days === 365
                    ? '1 year'
                    : `${days / 365} years`}</option
              >
            {/each}
          </select>
        </Field>
      {:else}
        <Field
          required
          label="Certificate"
          help="PEM, the full chain. For a private authority, end it with the root."
        >
          <textarea bind:value={pem} rows="5" spellcheck="false"></textarea>
        </Field>
        <Field required label="Private key" help="PEM. Stored encrypted and never shown again.">
          <textarea bind:value={privateKey} rows="5" spellcheck="false"></textarea>
        </Field>
      {/if}
      {#if editing?.name === null}
        <Field
          optional
          label="Description"
          help="A note shown under the certificate's name in the list, to say what it is for."
        >
          <input bind:value={newDescription} />
        </Field>
      {/if}
    </div>
    {#snippet actions()}
      <button type="button" onclick={cancel} disabled={saving}>Cancel</button>
      <button type="submit" class="primary" disabled={!canSave}>
        {saving ? 'Saving…' : editing?.name ? 'Replace' : 'Add'}
      </button>
    {/snippet}
  </Dialog>

  {#if loading}
    <p class="dim">Loading…</p>
  {:else if certificates.length === 0}
    <p class="dim">No certificates yet.</p>
  {:else}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th aria-sort={sort.aria('name')}><SortButton {sort} key="name">Name</SortButton></th>
            <th aria-sort={sort.aria('names')}
              ><SortButton {sort} key="names">Serves</SortButton></th
            >
            <th aria-sort={sort.aria('purpose')}
              ><SortButton {sort} key="purpose">Usable as</SortButton></th
            >
            <th aria-sort={sort.aria('expires')}
              ><SortButton {sort} key="expires">Expires</SortButton></th
            >
            <th aria-sort={sort.aria('setAt')}
              ><SortButton {sort} key="setAt">Last set</SortButton></th
            >
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each sorted as c (c.name)}
            {@const current = currentVersion(c)}
            <tr>
              <td>
                <span class="mono">{c.name}</span>
                {#if c.served}<span class="badge" title="The certificate this leader serves."
                    >leader</span
                  >{/if}
                {#if c.kept === 'issued'}<span
                    class="badge"
                    title="Issued by this leader, which renews it on its own, so it has no Replace button. To use your own certificate, add one under another name."
                    >issued</span
                  >{/if}
                {#if c.kept === 'acme'}<span
                    class="badge"
                    title="Ordered over ACME by this leader, which renews it on its own, so it has no Replace button. To use your own certificate, add one under another name."
                    >ACME</span
                  >{/if}
                {#if c.description}<small class="note">{c.description}</small>{/if}
              </td>
              <td class="mono">{leaf(c)?.names.join(', ') || '--'}</td>
              <td class="one-line">{leaf(c) ? purpose(c) : '--'}</td>
              <td class="one-line" class:soon={leaf(c) && expiringSoon(leaf(c))}>
                {leaf(c) ? expiry(leaf(c).notAfterMillis) : '--'}
              </td>
              <td class="dim one-line">{current ? ago(current.setAt / 1000) : '--'}</td>
              <td class="row-actions">
                {#if !c.kept}
                  <button type="button" onclick={() => open(c.name)}>Replace</button>
                {/if}
                <button type="button" onclick={() => remove(c)}>Delete</button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</SectionShell>

<style>
  .toolbar {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin: 1.2rem 0;
  }

  .form-body {
    display: grid;
    gap: 0.7rem;
  }

  .form-body input,
  .form-body textarea {
    width: 100%;
  }

  .form-body textarea {
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-body);
  }

  th {
    text-align: left;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
    padding: 0.4rem 0.6rem;
  }

  td {
    padding: 0.5rem 0.6rem;
    border-bottom: 1px solid var(--border);
  }

  tbody tr:last-child td {
    border-bottom: none;
  }

  th,
  .one-line,
  .row-actions {
    white-space: nowrap;
  }

  .note {
    display: block;
    color: var(--dim);
    font-size: var(--text-note);
    font-weight: 400;
    line-height: 1.4;
  }

  .soon {
    color: var(--warn);
  }

  .badge {
    margin-left: 0.4rem;
    padding: 0 0.4rem;
    border: 1px solid var(--border);
    border-radius: 0.6rem;
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
