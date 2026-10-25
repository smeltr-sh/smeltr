<!--
  Connection identities page: `identities.json`, read through `GET /api/config`
  and written with `PUT /api/identities`, plus the raw-JSON page.

  An identity is two halves and either may be empty: what this worker presents
  to the far end, and which authorities it verifies that far end against. A source
  or destination presents a client certificate; a receiver serves its certificate
  and verifies its senders. What it presents is a certificate from the list on
  Admin -> Certificates, named by name and version like a stored secret.
-->
<script lang="ts">
  import InlineError from './InlineError.svelte';
  import Connections from './Connections.svelte';
  import EditInShared from './EditInShared.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import SaveBar from './SaveBar.svelte';
  import ValidationItem from './ValidationItem.svelte';
  import { provideValidation } from './validation.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import HeldEdit from './HeldEdit.svelte';
  import { SectionEditor } from './sectionEditor.svelte';
  import PencilIcon from './PencilIcon.svelte';
  import CertificatePicker from './CertificatePicker.svelte';
  import Help from './Help.svelte';
  import Field from './Field.svelte';
  import Advanced from './Advanced.svelte';
  import Toggle from './Toggle.svelte';
  import Dialog from './Dialog.svelte';
  import JsonEditPage from './JsonEditPage.svelte';
  import { resourcesTabs } from './nav';
  import {
    sharedConfig,
    putIdentities,
    isLeaderStored,
    describeCertificates,
    errorMessage,
    type IdentityDto,
    type DescribedCertificate,
  } from './api';
  import { expiry } from './time';
  import { idProblem, suggestId } from './ids';
  import { editable } from './access';

  let { showJson = false }: { showJson?: boolean } = $props();

  const readOnly = !editable();

  let identities: IdentityDto[] = $state([]);
  /** What `shared/` declares, which this group's connectors may name as well. Never saved from here. */
  let sharedIdentities: IdentityDto[] = $state([]);
  /** Which of this scope's own rows is open. A row is a line until a pencil opens it. */
  let editing: number | null = $state(null);
  const validation = provideValidation('identities');
  const editor = new SectionEditor<IdentityDto[]>('identities', {
    get: () => identities,
    set: (value) => (identities = value),
    pick: (config) => config.identities,
    put: putIdentities,
    validation,
    onLoaded: loadShared,
    // A row open to change is a row of the edit being put back.
    onDiscard: () => (editing = null),
  });

  async function loadShared() {
    sharedIdentities = (await sharedConfig())?.identities ?? [];
  }

  void editor.load();

  /** Names the certificate picked at the version it is at now: a reference written today presents today's
   * certificate, and replacing it on the list is what moves the reference. A pick takes out what a pasted
   * certificate left. */
  function choose(identity: IdentityDto, name: string | undefined, version: number | undefined) {
    delete identity.key;
    if (name === undefined) {
      delete identity.certificate;
      return;
    }
    identity.certificate = { secret: name, version: version ?? 1 };
  }

  /** A row at rest, in one line. */
  function summary(i: IdentityDto): string {
    const presents = i.certificate ? 'presents a certificate' : 'presents nothing';
    const trusts =
      i.trustPublic === false
        ? 'trusts its own authorities only'
        : i.authorities
          ? 'public roots and its own authorities'
          : 'public roots';
    return `${presents} · ${trusts}`;
  }

  // -- what a certificate says about itself ---------------------------------
  //
  // The leader reads the PEM: parsing X.509 in the browser would be a second
  // implementation of what `leader::tls` already does, and the two would drift.
  // A field nobody has typed a certificate into asks nothing.

  type Described = { certificates: DescribedCertificate[] } | { error: string } | null;

  /** Keyed by the PEM itself, so an edit that changes nothing asks nothing. */
  let described = $state<Record<string, Described>>({});

  function describe(pem: string | undefined) {
    const text = (pem ?? '').trim();
    if (text === '' || text in described) return;
    described[text] = null;
    describeCertificates(text)
      .then((res) => (described[text] = res))
      .catch((e) => (described[text] = { error: errorMessage(e) }));
  }

  $effect(() => {
    for (const i of adding ? [...identities, adding] : identities) describe(i.authorities);
  });

  const readBack = (pem: string | undefined): Described =>
    pem && pem.trim() !== '' ? (described[pem.trim()] ?? null) : null;

  function taken(except?: number): string[] {
    return identities.filter((_, n) => n !== except).map((i) => i.id);
  }

  /** A new identity being filled in, before it joins the page. */
  let adding: IdentityDto | null = $state(null);

  /** Every id a new one may not take: this scope's and `shared`'s. */
  const unavailable = () => [...taken(), ...sharedIdentities.map((i) => i.id)];

  function add() {
    if (!adding || idProblem(adding.id, unavailable())) return;
    identities.push(adding);
    adding = null;
  }

  function removeIdentity(n: number) {
    identities.splice(n, 1);
    editing = null;
  }

  /** An empty text box unsets the field rather than naming `""`. */
  function optional(value: string): string | undefined {
    return value.trim() === '' ? undefined : value;
  }
</script>

{#snippet describedCertificate(pem: string | undefined)}
  {@const answer = readBack(pem)}
  {#if answer && 'error' in answer}
    <div class="read-back bad"><InlineError message={answer.error} /></div>
  {:else if answer}
    {#each answer.certificates as cert, i (cert.fingerprint + i)}
      <p class="read-back">
        <span class="subject mono">{cert.subject}</span>
        <span class="sep">&middot;</span>
        <span>{expiry(cert.notAfterMillis)}</span>
        <span class="sep">&middot;</span>
        <span class="mono">{cert.fingerprint.slice(0, 16)}&hellip;</span>
      </p>
    {/each}
  {/if}
{/snippet}

{#snippet fields(identity: IdentityDto)}
  <Field optional field="description" label="description">
    <input
      type="text"
      value={identity.description ?? ''}
      oninput={(e) => (identity.description = optional(e.currentTarget.value))}
      placeholder="the west brokers, over Acme's own PKI"
    />
  </Field>

  <CertificatePicker
    label="presents"
    field="certificate"
    optional
    value={isLeaderStored(identity.certificate) ? identity.certificate.secret : undefined}
    onpick={(name, version) => choose(identity, name, version)}
    version={isLeaderStored(identity.certificate) ? identity.certificate.version : undefined}
  >
    {#snippet help()}
      The certificate this worker shows the far end, chosen from the list on
      <a href="#/admin/certificates">Admin &rarr; Certificates</a>. For a source or destination it is
      the client certificate, needed only when the far end requires one; for a receiver it is the
      certificate the receiver serves.
    {/snippet}
  </CertificatePicker>

  <Field optional field="authorities" label="trusts">
    {#snippet help()}
      Authorities a far end's certificate may chain to, added to the public roots. A receiver admits
      a sender whose certificate chains to these alone. PEM, one or more certificates.
    {/snippet}
    <textarea
      rows="4"
      spellcheck="false"
      value={identity.authorities ?? ''}
      oninput={(e) => (identity.authorities = optional(e.currentTarget.value))}></textarea>
  </Field>
  {@render describedCertificate(identity.authorities)}
  <Advanced label="Advanced" modified={identity.trustPublic === false}>
    <Field field="trustPublic" label="trust public roots">
      {#snippet help()}
        Whether a source or destination under it also trusts Mozilla's roots and this host's own.
        Off trusts only the authorities above, so a certificate from any other authority cannot
        stand in for the far end.
      {/snippet}
      <Toggle
        bind:checked={
          () => identity.trustPublic !== false,
          (on) => (identity.trustPublic = on ? undefined : false)
        }
        ariaLabel="trust public roots"
      />
    </Field>
  </Advanced>
{/snippet}

{#if editor.loading}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else if editor.loadFailed}
  <SectionShell />
{:else if showJson}
  <JsonEditPage
    title="Edit JSON — Connection identities"
    initialText={JSON.stringify(identities, null, 2)}
    backHash="#/build/identities"
    onSave={(text) => editor.saveFromJson(text)}
  />
{:else}
  <SectionShell>
    <PageHeader title="Connection identities">
      {#snippet help()}
        An identity is the TLS setup a connector names: the certificate this worker presents, and
        the authorities it uses to check the far end. A source or destination names it as its
        client identity, and a receiver as its server identity.
      {/snippet}
    </PageHeader>
    <SecondaryTabs tabs={resourcesTabs()} active="#/build/identities" ariaLabel="Resources" />

    <div class="form">
      <UnreadableFiles problems={editor.bundleProblems} onchanged={() => editor.load()} />

      {#if editor.moved}
        <HeldEdit
          what="identities"
          rows={editor.moved.rows}
          onrestore={() => editor.restoreHeld()}
          ondiscard={() => editor.dropHeld()}
        />
      {/if}

      {#if editor.conflict}
        <SaveConflictPanel
          conflict={editor.conflict}
          onDiscard={() => location.reload()}
          onRetry={() => editor.save()}
        />
      {/if}

      {#if !readOnly}
        <div class="toolbar">
          <button
            type="button"
            class="primary"
            onclick={() =>
              (adding = {
                id: suggestId('identity', [...taken(), ...sharedIdentities.map((i) => i.id)]),
              })}>+ Add identity</button
          >
          <button type="button" onclick={() => (location.hash = '#/build/identities/json')}>
            Edit JSON&hellip;
          </button>
        </div>
      {/if}

      {#if sharedIdentities.length > 0}
        <div class="band">from shared/ &mdash; every group may name these</div>
        {#each sharedIdentities as identity (identity.id)}
          <div class="rest fromshared">
            <span class="rest-id">{identity.id}</span>
            <span class="rest-meta">{summary(identity)}</span>
            <span class="rest-gap"></span>
            <EditInShared hash="#/build/identities" />
          </div>
        {/each}
        <div class="band">this group</div>
      {/if}

      {#each identities as identity, n (n)}
        <ValidationItem id={identity.id} open={() => (editing = n)}>
          {#if editing !== n}
            <div class="rest">
              <span class="rest-id">{identity.id}</span>
              <span class="rest-meta">{summary(identity)}</span>
              <span class="rest-gap"></span>
              <button
                type="button"
                class="pencil"
                title="Edit {identity.id}"
                aria-label="Edit {identity.id}"
                onclick={() => (editing = n)}><PencilIcon /></button
              >
              {#if !readOnly}
                <button type="button" onclick={() => removeIdentity(n)}>Remove</button>
              {/if}
            </div>
          {:else}
            <article class="card">
              <div class="card-head">
                <div class="grow">
                  <Field required field="id" label="id">
                    <input type="text" bind:value={identity.id} />
                  </Field>
                </div>
                {#if !readOnly}
                  <button type="button" onclick={() => removeIdentity(n)}>Remove</button>
                {/if}
              </div>

              <Connections kind="identity" id={identity.id} />
              {@render fields(identity)}

              <div class="done-row">
                <button type="button" onclick={() => (editing = null)}>Done</button>
              </div>
            </article>
          {/if}
        </ValidationItem>
      {/each}
      {#if identities.length === 0}
        <p class="dim">No identities of this group's own yet.</p>
      {/if}

      <Dialog
        open={adding !== null}
        title="new identity"
        wide
        oncancel={() => (adding = null)}
        onsubmit={add}
      >
        {#if adding}
          {@const draft = adding}
          <div class="form-body">
            <Field required label="id" error={idProblem(draft.id, unavailable())}>
              <input type="text" bind:value={draft.id} />
            </Field>
            {@render fields(draft)}
          </div>
        {/if}
        {#snippet actions()}
          <button type="button" onclick={() => (adding = null)}>Cancel</button>
          <button
            type="submit"
            class="primary"
            disabled={!adding || idProblem(adding.id, unavailable()) !== null}>Add</button
          >
        {/snippet}
      </Dialog>
      <SaveBar
        dirty={editor.isDirty}
        saving={editor.saving}
        onsave={() => void editor.save()}
        ondiscard={() => editor.discard()}
      />
    </div>
  </SectionShell>
{/if}

<style>
  /* A row is a line until a pencil opens it: a page of identities should read as a list of them. */
  .rest {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--panel);
    padding: 0.5rem 0.7rem;
    margin-bottom: 0.5rem;
  }

  .rest-id {
    font-family: var(--mono);
    font-weight: 600;
    font-size: var(--text-label);
  }

  .rest-meta {
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .rest-gap {
    flex: 1;
  }

  /* `shared/`'s own: shown, because a group's connectors may name them, and not editable here. */
  .rest.fromshared {
    background: none;
    border-style: dashed;
    border-left: 3px solid oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  .rest.fromshared .rest-id,
  .rest.fromshared .rest-meta {
    color: var(--dim);
  }

  .band {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    margin: 1rem 0 0.4rem;
  }

  .pencil {
    display: inline-flex;
    padding: 0.15rem;
    font-size: inherit;
    border: none;
    border-radius: var(--radius);
    background: none;
    color: var(--dim);
    cursor: pointer;
  }

  .pencil:hover {
    color: var(--ink);
  }

  .card {
    display: grid;
    gap: 0.7rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 0.9rem;
    margin-bottom: 0.5rem;
  }

  /* The button lines up with the input, not with the field's name. */
  .card-head {
    display: flex;
    align-items: flex-end;
    gap: 0.75rem;
  }

  .grow {
    flex: 1 1 auto;
  }

  input,
  textarea {
    width: 100%;
  }

  textarea {
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

  .form-body {
    display: grid;
    gap: 0.7rem;
  }

  .done-row {
    display: flex;
    align-items: center;
    gap: 0.6rem;
  }

  .form {
    max-width: var(--width-prose);
  }

  .toolbar {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin: 1.2rem 0;
  }

  .mono {
    font-family: var(--mono);
  }

  .dim {
    color: var(--dim);
  }
</style>
