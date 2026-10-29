<!--
  Who can use this leader, and what an administrator does to them: every
  account under Accounts, however it signs in, under Roles what each role
  gives, and under Single sign-on the provider itself.

  A local account is always an administrator. An SSO account holds what the
  roles named for its provider groups give it, so the roster shows those roles
  and the Roles tab is where what they give is changed. An integration, which
  signs in with a key, holds the roles it is given, or everything as an
  administrator.
-->
<script lang="ts">
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import OidcSettings from './OidcSettings.svelte';
  import IntegrationAccess from './IntegrationAccess.svelte';
  import IntegrationFold from './IntegrationFold.svelte';
  import { toHash } from './logs';
  import Help from './Help.svelte';
  import Field from './Field.svelte';
  import Dialog from './Dialog.svelte';
  import SortButton from './SortButton.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import Segmented from './Segmented.svelte';
  import { ask, type Question } from './Confirm.svelte';
  import { TableSort } from './sort.svelte';
  import { ACCOUNTS_TABS, accountsHash, type AccountsSection } from './nav';
  import { LEVELS } from './access';
  import { scopeHue } from './scope';
  import {
    SHARED_SCOPE,
    changeAccount,
    createIntegration,
    createLocalAccount,
    deleteRole,
    listAccounts,
    listRoles,
    listWorkerGroups,
    putRole,
    refused,
    resetAccountPassword,
    revokeAccountSessions,
    type AccountDto,
    type CreatedIntegrationKeyDto,
    type RoleDto,
    type TemporaryPasswordDto,
  } from './api';
  import { session, type AccessLevel } from './session';

  let { section }: { section: AccountsSection } = $props();

  type Action = 'reset' | 'revoke' | 'enable' | 'disable';

  /** What a new account can sign in with. An SSO account is made at its first sign-in. */
  const SIGN_INS: { value: 'local' | 'integration'; label: string }[] = [
    { value: 'local', label: 'Password' },
    { value: 'integration', label: 'Key' },
  ];

  let accounts = $state<AccountDto[]>([]);
  let loading = $state(true);

  let open = $state<string | null>(null);
  let renaming = $state(false);
  let creating = $state(false);

  let renameTo = $state('');
  let newSignIn = $state<'local' | 'integration'>('local');
  let newUsername = $state('');
  let newDisplayName = $state('');
  let newAdministrator = $state(false);
  let newRoles = $state<string[]>([]);

  let message = $state('');
  let busy = $state(false);

  /** Shown once, and not retrievable afterward -- so each stays on screen
   * until it is dismissed, and another account's is added rather than put in
   * its place. Replacing one would destroy the only copy of a password
   * somebody is still waiting to be given. A second reset of the same account
   * does replace its first: that password no longer signs anybody in. */
  let handovers = $state<TemporaryPasswordDto[]>([]);
  /** Each new key, with the integration it is for, until it is dismissed: it is the only copy. */
  let keyHandovers = $state<(CreatedIntegrationKeyDto & { integration: string })[]>([]);

  const me = $derived(session().userId);

  function keyCount(account: AccountDto): string {
    const n = account.keys?.length ?? 0;
    return n === 1 ? '1 key' : `${n} keys`;
  }

  function signIn(account: AccountDto): string {
    return { local: 'Password', oidc: 'SSO', integration: 'Key' }[account.authSource];
  }

  /** What the Roles column shows: a local account is an administrator by being local, and an
   * integration one by being made one. */
  function rolesOf(account: AccountDto): string[] {
    if (account.authSource === 'local') return ['administrator'];
    return account.administrator ? ['administrator', ...account.roles] : account.roles;
  }

  const accountSort = new TableSort<'account' | 'state' | 'signIn' | 'roles'>();
  const sortedAccounts = $derived(
    accountSort.apply(accounts, {
      account: (p) => p.displayName,
      state: (p) => (p.enabled ? 'Enabled' : 'Disabled'),
      signIn: signIn,
      roles: (p) => rolesOf(p).join(' '),
    }),
  );

  async function refresh() {
    try {
      accounts = await listAccounts();
    } catch (e) {
      reported(e);
    } finally {
      loading = false;
    }
  }

  // Read each time Accounts opens, so someone who signed in since shows without a reload. The roles
  // are what an integration can be given.
  $effect(() => {
    if (section !== 'accounts') return;
    void refresh();
    void refreshRoles();
  });

  function reset() {
    renaming = false;
    creating = false;
    renameTo = '';
    newSignIn = 'local';
    newUsername = '';
    newDisplayName = '';
    newAdministrator = false;
    newRoles = [];
  }

  function startRename(account: AccountDto) {
    reset();
    message = '';
    open = account.id;
    renaming = true;
    renameTo = account.displayName;
  }

  const createReady = $derived(
    newDisplayName.trim() !== '' && (newSignIn === 'integration' || newUsername.trim() !== ''),
  );

  async function create() {
    if (busy || !createReady) return;
    busy = true;
    try {
      if (newSignIn === 'local') {
        const result = await createLocalAccount(newUsername, newDisplayName);
        if (refused(result)) return;
        handovers = [...handovers, result];
      } else {
        const result = await createIntegration(newDisplayName, newRoles, newAdministrator);
        if (refused(result)) return;
        // Opened, so the next thing it needs, a key, is one click away.
        open = result.id;
      }
      message = '';
      reset();
      await refresh();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function rename(event: SubmitEvent, account: AccountDto) {
    event.preventDefault();
    if (busy) return;
    busy = true;

    try {
      const result = await changeAccount(account.id, { displayName: renameTo });
      if (refused(result)) return;
      message = 'display name updated';
      reset();
      await refresh();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  /** What the person loses, for the two actions that take something away. */
  const ASKS: Partial<Record<Action, (name: string) => Question>> = {
    reset: (name) => ({
      title: 'reset password',
      body: `Sign ${name} out everywhere and give them a temporary password.`,
      action: 'Reset password',
      danger: true,
    }),
    disable: (name) => ({
      title: 'disable account',
      body: `Sign ${name} out everywhere and stop them signing in.`,
      action: 'Disable account',
      danger: true,
    }),
  };

  async function perform(account: AccountDto, action: Action) {
    if (busy) return;
    const question = ASKS[action];
    if (question && !(await ask(question(account.displayName)))) return;
    if (busy) return;
    reset();
    message = '';
    busy = true;

    try {
      if (action === 'enable' || action === 'disable') {
        const result = await changeAccount(account.id, { enabled: action === 'enable' });
        if (refused(result)) return;
        message =
          action === 'enable' ? 'account enabled' : 'account disabled; its sessions are over';
      } else if (action === 'reset') {
        const result = await resetAccountPassword(account.id);
        if (refused(result)) return;
        handovers = [...handovers.filter((h) => h.userId !== result.userId), result];
      } else {
        const result = await revokeAccountSessions(account.id);
        if (refused(result)) return;
        message = `signed out everywhere (${result.ended} ${result.ended === 1 ? 'session' : 'sessions'})`;
      }
      await refresh();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  /** After an integration's fold changed it: the roster read again, then what it did said. */
  async function integrationChanged(said: string) {
    await refresh();
    message = said;
  }

  // -- roles ---------------------------------------------------------------

  let roles = $state<RoleDto[]>([]);
  let rolesLoading = $state(true);
  /** Every area a role can give, `shared` first, each with the hue it is drawn in. */
  let areas = $state<{ id: string; hue: number | null }[]>([{ id: SHARED_SCOPE, hue: null }]);

  /** The role whose row is open, and what its form holds. `''` is no level in that area. */
  let openRole = $state<string | null>(null);
  let draft = $state<{ administrator: boolean; areas: Record<string, AccessLevel | ''> }>({
    administrator: false,
    areas: {},
  });
  /** A role named in `+ Role` and not saved yet: a row of its own until Save or Cancel. */
  let unsaved = $state<string | null>(null);
  let naming = $state(false);
  let newRole = $state('');
  let roleError = $state('');

  async function refreshRoles() {
    try {
      roles = await listRoles();
    } catch (e) {
      reported(e);
    } finally {
      rolesLoading = false;
    }
  }

  async function loadAreas() {
    const res = await listWorkerGroups().catch(() => null);
    if (!res?.ok) return;
    const sharedHue = res.sharedHue ?? null;
    areas = [
      { id: SHARED_SCOPE, hue: sharedHue },
      ...res.groups.map((g) => ({ id: g.id, hue: scopeHue(g.id, res.groups, sharedHue) })),
    ];
  }

  $effect(() => {
    if (section !== 'roles') return;
    void refreshRoles();
    void loadAreas();
  });

  const shownRoles = $derived(
    unsaved === null
      ? roles
      : [...roles, { name: unsaved, administrator: false, areas: {} }].sort((a, b) =>
          a.name.localeCompare(b.name),
        ),
  );

  function hueOf(area: string): number | null {
    return areas.find((a) => a.id === area)?.hue ?? null;
  }

  /** What a role gives, `shared` first and then by area. */
  function heldBy(role: RoleDto): [string, AccessLevel][] {
    return Object.entries(role.areas).sort(([a], [b]) =>
      a === SHARED_SCOPE ? -1 : b === SHARED_SCOPE ? 1 : a.localeCompare(b),
    );
  }

  /** The form's areas: every one there is, and any the role names that has since gone, so a save
   * does not drop it unseen. */
  const formAreas = $derived.by(() => {
    const role = shownRoles.find((r) => r.name === openRole);
    const gone = Object.keys(role?.areas ?? {}).filter((a) => !areas.some((x) => x.id === a));
    return [...areas, ...gone.sort().map((id) => ({ id, hue: null }))];
  });

  /** True once nothing unsaved stands in the way: no role is waiting for its
   * first save, or the reader agreed to drop it. Only one new role is held at
   * a time, so opening another row or naming another role asks first. */
  async function dropUnsaved(): Promise<boolean> {
    if (unsaved === null) return true;
    const ok = await ask({
      title: 'unsaved role',
      body: `The role ${unsaved} has not been saved yet.`,
      action: 'Discard role',
      danger: true,
    });
    if (!ok) return false;
    if (openRole === unsaved) openRole = null;
    unsaved = null;
    return true;
  }

  async function openRow(role: RoleDto) {
    roleError = '';
    if (openRole === role.name) {
      closeRow();
      return;
    }
    if (unsaved !== role.name && !(await dropUnsaved())) return;
    openRole = role.name;
    draft = { administrator: role.administrator, areas: { ...role.areas } };
  }

  function closeRow() {
    if (openRole === unsaved) unsaved = null;
    openRole = null;
    roleError = '';
  }

  /** What the form would save: the levels chosen, without the areas left at none. */
  const draftAreas = $derived(
    Object.fromEntries(
      Object.entries(draft.areas).filter((e): e is [string, AccessLevel] => e[1] !== ''),
    ),
  );
  /** A role that gives nothing is a role nobody needs, so it is never saved. */
  const givesSomething = $derived(draft.administrator || Object.keys(draftAreas).length > 0);

  const nameTaken = $derived(roles.some((r) => r.name === newRole.trim()));

  async function startNaming() {
    if (!(await dropUnsaved())) return;
    newRole = '';
    roleError = '';
    naming = true;
  }

  /** A group the provider names that no role does yet, opened on the Roles tab as a new role. */
  async function roleFor(group: string) {
    if (!(await dropUnsaved())) return;
    location.hash = '#/admin/accounts/roles';
    newRole = group;
    named();
  }

  function named() {
    const name = newRole.trim();
    if (name === '' || nameTaken) return;
    naming = false;
    unsaved = name;
    openRole = name;
    draft = { administrator: false, areas: {} };
    roleError = '';
  }

  async function saveRole(event: SubmitEvent, name: string) {
    event.preventDefault();
    if (busy || !givesSomething) return;
    busy = true;
    roleError = '';
    try {
      const res = await putRole(name, draft.administrator, draft.administrator ? {} : draftAreas);
      if (res.ok === false) return void (roleError = res.errors[0]?.message ?? 'that did not work');
      if (unsaved === name) unsaved = null;
      openRole = null;
      await refreshRoles();
    } catch (e) {
      roleError = unreported(e);
    } finally {
      busy = false;
    }
  }

  async function removeRole(name: string) {
    if (busy) return;
    if (unsaved === name) return closeRow();
    const ok = await ask({
      title: 'remove role',
      body: `Remove the role ${name}. Accounts in this group lose what it gives.`,
      action: 'Remove role',
      danger: true,
    });
    if (!ok || busy) return;
    busy = true;
    roleError = '';
    try {
      const res = await deleteRole(name);
      if (res.ok === false) return void (roleError = res.errors[0]?.message ?? 'that did not work');
      openRole = null;
      await refreshRoles();
    } catch (e) {
      roleError = unreported(e);
    } finally {
      busy = false;
    }
  }
</script>

<SectionShell>
  <PageHeader title="Accounts">
    {#snippet beside()}
      {#if section === 'oidc'}
        <!-- The form saves itself. -->
      {:else if section === 'roles'}
        <button class="primary" type="button" onclick={startNaming}>+ Add role</button>
        <button
          type="button"
          onclick={() => (location.hash = toHash({ levels: [], site: 'leader.sign_in_refused' }))}
          >Refused sign-ins</button
        >
      {:else}
        <button
          class="primary"
          type="button"
          onclick={() => {
            reset();
            message = '';
            creating = true;
          }}
        >
          + Add account
        </button>
      {/if}
    {/snippet}
    {#snippet help()}
      <p>
        A role is a group your provider names. It gives viewer, editor or deployer in each area:
        shared or a worker group. Administrators hold everything, and local accounts are always
        administrators.
      </p>
    {/snippet}
  </PageHeader>

  <SecondaryTabs tabs={ACCOUNTS_TABS} active={accountsHash(section)} ariaLabel="Accounts" />

  {#each handovers as pending (pending.userId)}
    <section class="handover">
      <h2>Hand this over to <code>{pending.username}</code></h2>
      <p class="critical">Copy the password below now: it is shown only once.</p>
      <p class="secret"><code>{pending.temporaryPassword}</code></p>
      <button
        type="button"
        onclick={() => (handovers = handovers.filter((h) => h.userId !== pending.userId))}
      >
        Done
      </button>
    </section>
  {/each}
  {#each keyHandovers as pending (pending.id)}
    <section class="handover">
      <h2>A new key for {pending.integration}</h2>
      <p class="critical">Copy the key below now: it is shown only once.</p>
      <p class="secret"><code>{pending.key}</code></p>
      <p>Send it as <code>Authorization: Bearer &lt;key&gt;</code>.</p>
      <button
        type="button"
        onclick={() => (keyHandovers = keyHandovers.filter((h) => h.id !== pending.id))}
      >
        Done
      </button>
    </section>
  {/each}

  <Dialog open={creating} title="new account" oncancel={() => busy || reset()} onsubmit={create}>
    <div class="fields">
      <div class="sign-in">
        <Segmented label="Signs in with" options={SIGN_INS} bind:value={newSignIn} />
      </div>
      {#if newSignIn === 'local'}
        <div class="pair">
          <Field required label="Display name">
            <input bind:value={newDisplayName} required />
          </Field>
          <Field required label="Username">
            <input bind:value={newUsername} autocomplete="off" required />
          </Field>
        </div>
      {:else}
        <Field required label="Display name">
          {#snippet help()}The name its changes are recorded under, in the audit and in history.{/snippet}
          <input bind:value={newDisplayName} autocomplete="off" required />
        </Field>
        <IntegrationAccess {roles} bind:administrator={newAdministrator} bind:chosen={newRoles} />
      {/if}
    </div>
    {#snippet actions()}
      <button type="button" onclick={reset} disabled={busy}>Cancel</button>
      <button class="primary" type="submit" disabled={busy || !createReady}>Add</button>
    {/snippet}
  </Dialog>

  <Dialog open={naming} title="new role" oncancel={() => (naming = false)} onsubmit={named}>
    <Field required label="Role" error={nameTaken ? 'There is a role by this name already.' : null}>
      {#snippet help()}
        The group exactly as your provider names it. Accounts in that group hold what the role
        gives.
      {/snippet}
      <input bind:value={newRole} autocomplete="off" required />
    </Field>
    {#snippet actions()}
      <button type="button" onclick={() => (naming = false)}>Cancel</button>
      <button class="primary" type="submit" disabled={newRole.trim() === '' || nameTaken}
        >Add</button
      >
    {/snippet}
  </Dialog>

  {#if section === 'oidc'}
    <OidcSettings />
  {:else if section === 'roles'}
    {#if rolesLoading}
      <p>Reading the roles…</p>
    {:else if shownRoles.length === 0}
      <p class="dim">No roles yet.</p>
    {:else}
      <table>
        <thead>
          <tr>
            <th>Role</th>
            <th>Access</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each shownRoles as role (role.name)}
            <tr class:expanded={openRole === role.name}>
              <td><div class="name mono">{role.name}</div></td>
              <td>
                <div class="pills">
                  {#if role.administrator}
                    <span class="pill">administrator</span>
                  {:else}
                    {#each heldBy(role) as [area, level] (area)}
                      <span class="pill area"
                        ><span
                          class="dot"
                          class:hued={hueOf(area) !== null}
                          style:--h={hueOf(area)}
                          aria-hidden="true"
                        ></span>{area}
                        <span class="level">{level}</span></span
                      >
                    {/each}
                  {/if}
                </div>
              </td>
              <td class="right">
                <button
                  type="button"
                  aria-expanded={openRole === role.name}
                  onclick={() => openRow(role)}
                >
                  {openRole === role.name ? 'Close' : 'Manage'}
                </button>
              </td>
            </tr>
            {#if openRole === role.name}
              <tr class="fold">
                <td colspan="3">
                  <form class="panel" onsubmit={(e) => saveRole(e, role.name)}>
                    <label class="check"
                      ><input type="checkbox" bind:checked={draft.administrator} /> Administrator</label
                    >
                    {#if !draft.administrator}
                      <Field label="Access" group>
                        {#snippet help()}
                          Viewers read, editors save on branches, deployers approve and deploy.
                          Holding any area gives viewer in shared too.
                        {/snippet}
                        <div class="levels">
                          {#each formAreas as area (area.id)}
                            <span class="area-name"
                              ><span
                                class="dot"
                                class:hued={area.hue !== null}
                                style:--h={area.hue}
                                aria-hidden="true"
                              ></span>{area.id}</span
                            >
                            <select aria-label="{area.id} level" bind:value={draft.areas[area.id]}>
                              <option value="">—</option>
                              {#each LEVELS as level (level)}<option value={level}>{level}</option
                                >{/each}
                            </select>
                          {/each}
                        </div>
                      </Field>
                    {/if}
                    <InlineError message={roleError} />
                    <div class="actions split">
                      <button type="button" onclick={() => removeRole(role.name)} disabled={busy}
                        >Remove role</button
                      >
                      <span class="buttons">
                        <button type="button" onclick={closeRow}>Cancel</button>
                        <button class="primary" type="submit" disabled={busy || !givesSomething}
                          >Save</button
                        >
                      </span>
                    </div>
                  </form>
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
    {/if}
  {:else}
    {#if message}<p class="message" role="status">{message}</p>{/if}
    {#if loading}
      <p>Reading the roster…</p>
    {:else}
      <table>
        <thead>
          <tr>
            <th aria-sort={accountSort.aria('account')}
              ><SortButton sort={accountSort} key="account">Account</SortButton></th
            >
            <th aria-sort={accountSort.aria('state')}
              ><SortButton sort={accountSort} key="state">State</SortButton></th
            >
            <th class="wide" aria-sort={accountSort.aria('signIn')}
              ><SortButton sort={accountSort} key="signIn">Sign-in</SortButton></th
            >
            <th aria-sort={accountSort.aria('roles')}
              ><SortButton sort={accountSort} key="roles">Roles</SortButton></th
            >
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each sortedAccounts as account (account.id)}
            <tr class:expanded={open === account.id}>
              <td>
                <div class="name">{account.displayName}{account.id === me ? ' · you' : ''}</div>
                <!-- An OIDC account is known by its provider's login, which the
                   provider may not share. -->
                {#if account.authSource === 'oidc'}
                  {#if account.providerLogin}<div class="dim mono">
                      {account.providerLogin}
                    </div>{/if}
                {:else if account.authSource === 'integration'}
                  <div class="dim">{keyCount(account)}</div>
                {:else}
                  <div class="dim mono">{account.username ?? '—'}</div>
                {/if}
              </td>
              <td
                ><span class="pill" class:off={!account.enabled}
                  >{account.enabled ? 'Enabled' : 'Disabled'}</span
                ></td
              >
              <td class="wide dim">{signIn(account)}</td>
              <td>
                <div class="pills">
                  {#each rolesOf(account) as role (role)}
                    <span class="pill" class:mono={account.authSource !== 'local'}>{role}</span>
                  {/each}
                  {#each account.groups.filter((g) => !account.roles.includes(g)) as group (group)}
                    <button type="button" class="pill ghost mono" onclick={() => roleFor(group)}
                      >+ {group}</button
                    >
                  {/each}
                </div>
              </td>
              <td class="right">
                <button
                  type="button"
                  aria-expanded={open === account.id}
                  onclick={() => {
                    const closing = open === account.id;
                    reset();
                    message = '';
                    open = closing ? null : account.id;
                  }}
                >
                  {open === account.id ? 'Close' : 'Manage'}
                </button>
              </td>
            </tr>
            {#if open === account.id}
              <tr class="fold">
                <td colspan="5">
                  {#if account.authSource === 'integration'}
                    <IntegrationFold
                      {account}
                      {roles}
                      onchanged={integrationChanged}
                      onkey={(created) =>
                        (keyHandovers = [
                          ...keyHandovers,
                          { ...created, integration: account.displayName },
                        ])}
                    />
                  {:else}
                    <div class="row">
                      <button type="button" onclick={() => startRename(account)}>Rename</button>
                      <!-- Not for your own account. A reset ends every session it
                         belongs to, including this one, so the page holding the
                         only copy of the new password would be gone before
                         anybody could read it. Change password does this safely.
                         Nor for an OIDC account, which has no password here. -->
                      {#if account.id !== me && account.authSource !== 'oidc'}
                        <button
                          type="button"
                          disabled={busy}
                          onclick={() => perform(account, 'reset')}>Reset password</button
                        >
                      {/if}
                      <button
                        type="button"
                        disabled={busy}
                        onclick={() => perform(account, 'revoke')}>Revoke sessions</button
                      >
                      {#if account.enabled}
                        <button
                          type="button"
                          disabled={busy}
                          onclick={() => perform(account, 'disable')}>Disable account</button
                        >
                      {:else}
                        <button
                          type="button"
                          disabled={busy}
                          onclick={() => perform(account, 'enable')}>Enable account</button
                        >
                      {/if}
                    </div>

                    {#if renaming}
                      <form class="panel" onsubmit={(e) => rename(e, account)}>
                        <Field required label="Display name">
                          {#snippet help()}Only the name shown in smeltr changes.{/snippet}
                          <input bind:value={renameTo} required />
                        </Field>
                        <div class="actions">
                          <button type="button" onclick={reset}>Cancel</button>
                          <button class="primary" type="submit" disabled={busy}
                            >Save display name</button
                          >
                        </div>
                      </form>
                    {/if}
                  {/if}
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
    {/if}
  {/if}
</SectionShell>

<style>
  table {
    width: 100%;
    border-collapse: collapse;
    text-align: left;
  }

  th {
    color: var(--dim);
    font-size: var(--text-micro);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-weight: 700;
  }

  th,
  td {
    padding: 0.7rem 0.5rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }

  tr.expanded td {
    background: var(--panel);
    border-bottom-color: transparent;
  }

  .fold td {
    background: var(--panel);
    padding: 0 0.75rem 1rem;
  }

  .name {
    font-size: var(--text-body);
    font-weight: 600;
  }

  .mono {
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .dim {
    color: var(--dim);
    font-size: var(--text-body);
  }

  /* A paragraph is a sentence someone reads, so it takes the text color around it
     (ink, or the handover's own); a username or an auth source in a row stays dim. */
  .right {
    text-align: right;
  }

  .pill {
    display: inline-block;
    font-size: var(--text-note);
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0.1rem 0.5rem;
  }

  /* A group with no role yet: an offer to make one, drawn apart from the roles held. */
  .pill.ghost {
    border-style: dashed;
    color: var(--dim);
    background: transparent;
    cursor: pointer;
  }

  .pill.ghost:hover {
    color: var(--text);
    border-color: var(--accent);
  }

  .pill.off {
    color: var(--dim);
  }

  .pills {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }

  .pill.area {
    white-space: nowrap;
  }

  .level {
    color: var(--dim);
  }

  /* An area in the hue it is drawn in everywhere else (STYLE_GUIDE.md, "Group hues"); dim until
     the groups have been read. */
  .dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 999px;
    margin-right: 0.35rem;
    vertical-align: 1px;
    background: var(--dim);
  }

  .dot.hued {
    background: oklch(var(--scope-l) var(--scope-c) var(--h));
  }

  .levels {
    display: grid;
    grid-template-columns: max-content 9rem;
    gap: 0.4rem 1rem;
    align-items: center;
  }

  .area-name {
    font-family: var(--mono);
    font-size: var(--text-body);
  }

  .check {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: var(--text-body);
  }

  .actions.split {
    justify-content: space-between;
  }

  .buttons {
    display: flex;
    gap: 0.5rem;
  }

  .row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    padding: 0.9rem 0 0;
  }

  .panel {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    padding: 1rem;
    margin: 1rem 0 0;
    max-width: 34rem;
    display: grid;
    gap: 0.8rem;
  }

  .panel p {
    margin: 0;
  }

  /* Only as wide as its choices, not the dialog. */
  .sign-in {
    justify-self: start;
  }

  .fields {
    display: grid;
    gap: 0.8rem;
  }

  .pair {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.9rem;
  }

  input:not([type='checkbox']) {
    width: 100%;
  }

  .actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
    margin-top: 0.2rem;
  }

  .handover {
    background: var(--comment-bg);
    border-left: 3px solid var(--comment-border);
    color: var(--comment-ink);
    padding: 1rem 1.1rem;
    margin-bottom: 1.2rem;
    max-width: 44rem;
  }

  .handover h2 {
    font-family: var(--font-display);
    font-size: var(--text-section);
    font-weight: 600;
    margin: 0 0 0.4rem;
  }

  .secret code {
    font-family: var(--mono);
    font-size: 1rem;
    user-select: all;
  }

  code {
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .message {
    color: var(--ink);
    font-size: var(--text-body);
  }

  @media (max-width: 720px) {
    .wide {
      display: none;
    }
    .pair {
      grid-template-columns: 1fr;
    }
  }
</style>
