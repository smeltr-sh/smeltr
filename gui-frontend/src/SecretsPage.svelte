<!--
  The secret catalog: which credentials exist, where each one's value
  lives, and which one the fleet is running. Never a value -- no route
  returns one, and this screen has no way to show one.

  A version is bookkeeping for rotation, not something an admin manages:
  New value stores it and points every reference at it in the same action.
  Pointing only drafts a change, though, so a row can still say "stored, not
  deployed" for a moment afterward -- until that change is reviewed and
  deploys, an admin who read only the row's name would believe it is live.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import { reported } from './errors.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Help from './Help.svelte';
  import Field from './Field.svelte';
  import Nested from './Nested.svelte';
  import PasswordField from './PasswordField.svelte';
  import Dialog from './Dialog.svelte';
  import { ask } from './Confirm.svelte';
  import SortButton from './SortButton.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import EncryptionKeySettings, { keyProblem } from './EncryptionKeySettings.svelte';
  import { SECRETS_TABS, secretsHash, type SecretsSection } from './nav';
  import { TableSort } from './sort.svelte';
  import {
    errorText,
    listSecrets,
    createSecret,
    storeSecretValue,
    deleteSecret,
    verifySecret,
    pointSecretNow,
    currentVersion,
    currentBranch,
    getEncryptionKey,
    type EncryptionKeyDto,
    type SecretDto,
  } from './api';
  import { ago } from './time';

  let { section }: { section: SecretsSection } = $props();

  let secrets: SecretDto[] = $state([]);
  let loading = $state(true);
  // Read beside the list: a broken key is the reason every row says unusable, and the row cannot say so.
  let key = $state<EncryptionKeyDto | null>(null);

  async function load() {
    loading = true;
    const before = key;
    try {
      const [listed, read] = await Promise.all([listSecrets(), getEncryptionKey()]);
      secrets = listed;
      // A regeneration that answered while this was loading is newer than what this read.
      if (key === before) key = read;
    } catch (e) {
      reported(e);
    } finally {
      loading = false;
    }
  }
  // Again on each tab: regenerating the key on the other one changes what every row here says.
  $effect(() => {
    void section;
    // Untracked: load reads the key it then writes, which would otherwise run it again.
    untrack(load);
  });

  /** Which versions `main` runs -- more than one when two groups are on
   * different versions, which is what a part-way rotation looks like.
   * Read as a count and a staleness badge; never a number an admin picks. */
  const deployedVersions = (s: SecretDto): number[] => [
    ...new Set(s.deployed.map(([, version]) => version)),
  ];

  /** A value stored and not yet deployed anywhere. The one thing an admin
   * needs told without asking, because storing looked like the whole job. */
  const staged = (s: SecretDto): boolean => {
    const current = currentVersion(s)?.version;
    return current !== undefined && !deployedVersions(s).includes(current);
  };

  let adding = $state(false);
  let newName = $state('');
  let newCustody: SecretDto['custody'] = $state('leader-stored');
  let newDescription = $state('');
  let newValue = $state('');
  let saving = $state(false);

  function startAdding() {
    adding = true;
    newName = '';
    newCustody = 'leader-stored';
    newDescription = '';
    newValue = '';
  }

  /** Why the new name cannot be added, if it cannot. An empty one only disables Add: it is where
   * every new secret starts. */
  const nameProblem = (name: string): string | null =>
    secrets.some((s) => s.name === name.trim()) ? `${name.trim()} is taken` : null;

  const canAdd = $derived(
    !saving &&
      newName.trim() !== '' &&
      nameProblem(newName) === null &&
      (newCustody !== 'leader-stored' || newValue.length > 0),
  );

  /** Refused while the create is in flight: closing then would hide whether it landed. */
  function cancelAdding() {
    if (!saving) adding = false;
  }

  async function add() {
    if (!canAdd) return;
    saving = true;
    try {
      const res = await createSecret(
        newName.trim(),
        newCustody,
        newDescription.trim(),
        newCustody === 'leader-stored' ? newValue : undefined,
      );
      if (res.ok) {
        adding = false;
        newValue = '';
        await load();
      }
    } catch (e) {
      reported(e);
    } finally {
      saving = false;
    }
  }

  // One row at a time is open, so each of these holds the name rather than a
  // flag: two rows cannot both be mid-edit.
  type Panel = 'store' | 'check';
  let open = $state<{ name: string; panel: Panel } | null>(null);
  let storeValue = $state('');
  let checkValue = $state('');
  /** The last check's answer, for the row it was asked about, so a "yes"
   * never reads as another row's. */
  let checkAnswer = $state<{ name: string; matches: boolean } | null>(null);
  let rowNote = $state('');
  let busy = $state(false);

  function openPanel(s: SecretDto, panel: Panel) {
    open = { name: s.name, panel };
    storeValue = '';
    checkValue = '';
    checkAnswer = null;
    rowNote = '';
  }

  const showing = (s: SecretDto, panel: Panel) => open?.name === s.name && open.panel === panel;

  /** Stores a new version and, in the same action, points every reference to
   * this secret at it. An admin never chooses a version or a branch for
   * either half -- the branch is whichever one is already being edited. */
  async function store(name: string) {
    busy = true;
    try {
      const stored = await storeSecretValue(name, storeValue);
      if (!stored.ok) return;
      open = null;
      storeValue = '';
      secrets = await listSecrets();
      const version = currentVersion(secrets.find((s) => s.name === name)!)?.version;
      if (version === undefined) return;
      const pointed = await pointSecretNow(name, version);
      if (!pointed.ok) {
        rowNote = `Stored, but nothing was pointed at it: ${errorText(pointed.errors) || 'the change was not drafted'}.`;
      } else if (pointed.references === 0) {
        rowNote = 'Stored. Nothing references it yet.';
      } else {
        rowNote =
          `Stored, and pointed ${pointed.references} reference${pointed.references === 1 ? '' : 's'} ` +
          `across ${pointed.groups.join(', ')} at it. It deploys when the change does.`;
      }
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function check(name: string) {
    busy = true;
    try {
      const version = currentVersion(secrets.find((s) => s.name === name)!)?.version;
      if (version === undefined) return;
      const { matches } = await verifySecret(name, version, checkValue);
      checkAnswer = { name, matches };
      checkValue = '';
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function remove(s: SecretDto) {
    const yes = await ask({
      title: 'delete secret',
      body:
        s.custody === 'leader-stored'
          ? `Delete ${s.name}. Every version goes with it. Nothing can recover them, and any bundle naming this secret stops deploying until the reference is taken out.`
          : `Delete ${s.name}. The name stops being listed. Hosts keep the variable; bundles naming it keep working.`,
      action: 'Delete',
      danger: true,
    });
    if (!yes) return;
    busy = true;
    try {
      const res = await deleteSecret(s.name);
      if (res.ok) await load();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  const CUSTODY_LABEL: Record<SecretDto['custody'], string> = {
    'worker-local': 'Worker host',
    'leader-stored': 'Leader',
  };

  // Running sorts by the newest version deployed: a part-way rotation lists
  // several, and the newest is how far it has got.
  const secretSort = new TableSort<'name' | 'custody' | 'stored' | 'running' | 'setAt'>();
  let sortedSecrets = $derived(
    secretSort.apply(secrets, {
      name: (s) => s.name,
      custody: (s) => CUSTODY_LABEL[s.custody],
      stored: (s) => currentVersion(s)?.version,
      running: (s) => {
        const running = s.custody === 'worker-local' ? [] : deployedVersions(s);
        return running.length > 0 ? Math.max(...running) : undefined;
      },
      setAt: (s) => currentVersion(s)?.setAt,
    }),
  );
</script>

<SectionShell>
  <PageHeader title="Secrets">
    {#snippet help()}
      Credentials that sources and destinations name: the configuration holds only the name, never
      the value.
    {/snippet}
  </PageHeader>

  <SecondaryTabs tabs={SECRETS_TABS} active={secretsHash(section)} ariaLabel="Secrets" />

  {#if section === 'key'}
    {#if key}
      <div class="key"><EncryptionKeySettings bind:key /></div>
    {:else if loading}
      <p class="dim">Loading…</p>
    {/if}
  {:else}
    {#if keyProblem(key)}
      <p class="critical" role="alert">{keyProblem(key, ' under Encryption key')}</p>
    {/if}

    <div class="toolbar">
      <button type="button" class="primary" onclick={startAdding} disabled={adding}>
        + Add secret
      </button>
    </div>

    <Dialog open={adding} title="new secret" oncancel={cancelAdding} onsubmit={add}>
      <div class="form-body">
        <Field required label="Name" error={nameProblem(newName)}>
          {#snippet help()}The name a source or destination picks it by. Letters, digits, - _ and .{/snippet}
          <input bind:value={newName} placeholder="kafka-west-sasl" class="mono" />
        </Field>
        <Field label="Value lives on">
          {#snippet help()}
            <strong>The leader</strong>: stored here, encrypted, and sent to the workers whose group
            needs it. <strong>The worker host</strong>: an environment variable on each worker host;
            the leader learns only its name. Fixed once the secret exists.
          {/snippet}
          <select bind:value={newCustody}>
            <option value="leader-stored">The leader</option>
            <option value="worker-local">The worker host</option>
          </select>
        </Field>
        {#if newCustody === 'leader-stored'}
          <Nested>
            <Field required label="Value">
              {#snippet help()}Stored encrypted and never shown again.{/snippet}
              <PasswordField bind:value={newValue} ariaLabel="value" />
            </Field>
          </Nested>
        {/if}
        <Field optional label="Description">
          <input bind:value={newDescription} placeholder="the west broker's SCRAM password" />
        </Field>
      </div>
      {#snippet actions()}
        <button type="button" onclick={cancelAdding} disabled={saving}>Cancel</button>
        <button type="submit" class="primary" disabled={!canAdd}>
          {saving ? 'Adding…' : 'Add'}
        </button>
      {/snippet}
    </Dialog>

    {#if rowNote}<p class="note-line">{rowNote}</p>{/if}

    {#if loading}
      <p class="dim">Loading…</p>
    {:else if secrets.length === 0}
      <p class="dim">No secrets declared yet.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th aria-sort={secretSort.aria('name')}
                ><SortButton sort={secretSort} key="name">Name</SortButton></th
              >
              <th aria-sort={secretSort.aria('custody')}
                ><SortButton sort={secretSort} key="custody">Value lives on</SortButton></th
              >
              <th class="num" aria-sort={secretSort.aria('stored')}
                ><SortButton sort={secretSort} key="stored">Stored</SortButton></th
              >
              <th aria-sort={secretSort.aria('running')}
                ><SortButton sort={secretSort} key="running">Running</SortButton></th
              >
              <th aria-sort={secretSort.aria('setAt')}
                ><SortButton sort={secretSort} key="setAt">Last set</SortButton></th
              >
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each sortedSecrets as s (s.name)}
              {@const current = currentVersion(s)}
              {@const running = deployedVersions(s)}
              <tr>
                <td>
                  <span class="mono">{s.name}</span>
                  {#if s.description}<small class="note">{s.description}</small>{/if}
                </td>
                <td class="one-line">
                  {CUSTODY_LABEL[s.custody]}
                  {#if current && !current.usable}
                    <small class="unusable">
                      unusable &mdash; encrypted with a key this leader no longer has
                    </small>
                  {/if}
                </td>
                <td class="num mono">{current ? `v${current.version}` : '--'}</td>
                <td class="mono one-line">
                  {#if s.custody === 'worker-local'}
                    <span class="dim">--</span>
                  {:else if running.length === 0}
                    <span class="dim">nothing</span>
                  {:else}
                    {running.map((v) => `v${v}`).join(', ')}
                  {/if}
                  {#if staged(s)}
                    <small class="staged">stored, not deployed</small>
                  {/if}
                </td>
                <td class="dim one-line">{current ? ago(current.setAt / 1000) : '--'}</td>
                <td class="row-actions">
                  {#if s.custody === 'leader-stored'}
                    <button type="button" onclick={() => openPanel(s, 'check')}> Check </button>
                    <button type="button" onclick={() => openPanel(s, 'store')}> New value </button>
                  {/if}
                  <button type="button" onclick={() => remove(s)}>Delete</button>
                </td>
              </tr>

              {#if showing(s, 'store')}
                <tr>
                  <td colspan="6">
                    <div class="row-edit">
                      <PasswordField
                        bind:value={storeValue}
                        ariaLabel="the new value"
                        placeholder="the new value"
                      />
                      <button
                        type="button"
                        class="primary"
                        disabled={busy || storeValue.length === 0}
                        onclick={() => store(s.name)}
                      >
                        {busy ? 'Storing…' : 'Store'}
                      </button>
                      <button type="button" onclick={() => (open = null)}>Cancel</button>
                      <Help label="New value">
                        Stores a new version and points every reference to this secret at it, as a
                        change on your branch.
                      </Help>
                    </div>
                  </td>
                </tr>
              {/if}

              {#if showing(s, 'check')}
                <tr>
                  <td colspan="6">
                    <div class="row-edit">
                      <PasswordField
                        bind:value={checkValue}
                        ariaLabel="the value you think is stored"
                        placeholder="the value you think is stored"
                      />
                      <button
                        type="button"
                        disabled={busy || checkValue.length === 0}
                        onclick={() => check(s.name)}
                      >
                        {busy ? 'Checking…' : 'Check'}
                      </button>
                      <button type="button" onclick={() => (open = null)}>Close</button>
                      {#if checkAnswer && checkAnswer.name === s.name}
                        <span class={checkAnswer.matches ? 'match' : 'no-match'}>
                          {checkAnswer.matches
                            ? 'That matches what is stored.'
                            : 'That does not match.'}
                        </span>
                      {/if}
                      <small class="note">A few attempts a minute; the leader logs every one.</small
                      >
                    </div>
                  </td>
                </tr>
              {/if}
            {/each}
          </tbody>
        </table>
      </div>
      <p class="note-line">
        Open changes are reviewed and deployed in <a href="#/changes">Branches</a>. Current branch:
        <code class="mono">{currentBranch()}</code>.
      </p>
    {/if}
  {/if}
</SectionShell>

<style>
  .key {
    max-width: 52rem;
  }

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
  .form-body select {
    width: 100%;
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

  /* Each cell beside the name holds one short answer -- a custody, a
     version, an age -- and reads as one line rather than breaking between
     words when the frame is narrow. */
  th,
  .one-line {
    white-space: nowrap;
  }

  .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .row-actions {
    white-space: nowrap;
  }

  /* A panel opened under its row: its controls on one line, wrapping only
     where the frame is too narrow to hold them. */
  .row-edit {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
    padding: 0.6rem 0.2rem;
  }

  /* A value is short, so the field keeps its own width rather than growing
     across the frame. */
  .row-edit :global(.pw) {
    flex: 0 1 22rem;
  }

  .staged {
    display: block;
    color: var(--warn);
    font-size: var(--text-note);
  }

  .match {
    color: var(--added);
  }

  .no-match {
    color: var(--error);
    font-size: var(--text-note);
  }

  /* A sentence, inside a cell that otherwise holds one line. */
  .unusable {
    display: block;
    white-space: normal;
    color: var(--error);
    font-size: var(--text-note);
  }

  /* The note register: one line stating what a field takes. Weight 400 --
     it is a sentence, not a second label. */
  .note {
    display: block;
    color: var(--dim);
    font-size: var(--text-note);
    font-weight: 400;
    line-height: 1.4;
  }

  .note-line {
    color: var(--ink);
    font-size: var(--text-note);
  }

  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
