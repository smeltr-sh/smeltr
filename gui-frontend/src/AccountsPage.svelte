<!--
  Who can use this leader, and what an administrator does to them: every
  account under Accounts, however it signs in, under Roles what each role
  gives, and under Single sign-on the provider itself.

  A local or key account holds the roles an administrator gives it. An OIDC
  account holds the roles its provider groups give it, so the roster shows
  those roles and the Roles tab is where what they give, and which groups give
  them, is changed. The administrator role holds everything.
-->
<script lang="ts">
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import OidcSettings from './OidcSettings.svelte';
  import AccountAccess from './AccountAccess.svelte';
  import AccountFold from './AccountFold.svelte';
  import Chips from './Chips.svelte';
  import { toHash } from './logs';
  import Help from './Help.svelte';
  import Field from './Field.svelte';
  import Dialog from './Dialog.svelte';
  import SortButton from './SortButton.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import Segmented from './Segmented.svelte';
  import { ask } from './Confirm.svelte';
  import { TableSort } from './sort.svelte';
  import { ACCOUNTS_TABS, accountsHash, type AccountsSection } from './nav';
  import { LEVELS } from './access';
  import { scopeHue } from './scope';
  import {
    SHARED_SCOPE,
    changeAccount,
    createKeyAccount,
    createLocalAccount,
    deleteRole,
    listAccounts,
    listRoles,
    listWorkerGroups,
    putRole,
    refused,
    type AccountDto,
    type CreatedKeyDto,
    type RoleDto,
    type TemporaryPasswordDto,
  } from './api';
  import { session, type AccessLevel } from './session';

  let { section }: { section: AccountsSection } = $props();

  /** What a new account can sign in with. An OIDC account is made at its first sign-in. */
  const SIGN_INS: { value: 'local' | 'key'; label: string }[] = [
    { value: 'local', label: 'Password' },
    { value: 'key', label: 'Key' },
  ];

  let accounts = $state<AccountDto[]>([]);
  let loading = $state(true);

  let open = $state<string | null>(null);
  let creating = $state(false);

  let newSignIn = $state<'local' | 'key'>('local');
  let newUsername = $state('');
  let newDisplayName = $state('');
  let newRoles = $state<string[]>([]);

  let message = $state('');
  let busy = $state(false);

  /** Shown once, and not retrievable afterward -- so each stays on screen
   * until it is dismissed, and another account's is added rather than put in
   * its place. Replacing one would destroy the only copy of a password
   * somebody is still waiting to be given. A second reset of the same account
   * does replace its first: that password no longer signs anybody in. */
  let handovers = $state<TemporaryPasswordDto[]>([]);
  /** Each new key, with the account it is for, until it is dismissed: it is the only copy. */
  let keyHandovers = $state<(CreatedKeyDto & { account: string })[]>([]);

  const me = $derived(session().userId);

  function keyCount(account: AccountDto): string {
    const n = account.keys?.length ?? 0;
    return n === 1 ? '1 key' : `${n} keys`;
  }

  /** How each kind of account signs in, as the Sign-in column and its chips name it. */
  const SIGN_IN_LABELS: Record<AccountDto['authSource'], string> = {
    local: 'Password',
    oidc: 'SSO',
    key: 'Key',
  };

  function signIn(account: AccountDto): string {
    return SIGN_IN_LABELS[account.authSource];
  }

  let query = $state('');
  let kind = $state<AccountDto['authSource'] | null>(null);
  const kinds = $derived(
    (['local', 'oidc', 'key'] as const).map((key) => ({
      key,
      label: SIGN_IN_LABELS[key],
      count: accounts.filter((a) => a.authSource === key).length,
    })),
  );

  /** Whether `account` is one the search names: by display name, username or provider login. */
  function matches(account: AccountDto, text: string): boolean {
    const needle = text.trim().toLowerCase();
    if (needle === '') return true;
    return [account.displayName, account.username, account.providerLogin].some((name) =>
      name?.toLowerCase().includes(needle),
    );
  }

  const shownAccounts = $derived(
    accounts.filter((a) => (kind === null || a.authSource === kind) && matches(a, query)),
  );

  const accountSort = new TableSort<'account' | 'state' | 'signIn' | 'roles'>();
  const sortedAccounts = $derived(
    accountSort.apply(shownAccounts, {
      account: (p) => p.displayName,
      state: (p) => (p.enabled ? 'Enabled' : 'Disabled'),
      signIn: signIn,
      roles: (p) => p.roles.join(' '),
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
  // are what a local or key account can be given.
  $effect(() => {
    if (section !== 'accounts') return;
    void refresh();
    void refreshRoles();
  });

  function reset() {
    creating = false;
    newSignIn = 'local';
    newUsername = '';
    newDisplayName = '';
    newRoles = [];
  }

  const createReady = $derived(
    newDisplayName.trim() !== '' && (newSignIn === 'key' || newUsername.trim() !== ''),
  );

  async function create() {
    if (busy || !createReady) return;
    busy = true;
    try {
      if (newSignIn === 'local') {
        const result = await createLocalAccount(newUsername, newDisplayName, newRoles);
        if (refused(result)) return;
        handovers = [...handovers, result];
      } else {
        const result = await createKeyAccount(newDisplayName, newRoles);
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

  /** After an account's fold changed it: the roster read again, then what it did said. */
  async function accountChanged(said: string) {
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
  /** The groups as typed, one to a line. */
  let draft = $state<{ groups: string; areas: Record<string, AccessLevel | ''> }>({
    groups: '',
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
      : [...roles, { name: unsaved, builtIn: false, groups: [], areas: {} }].sort((a, b) =>
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
    draft = { groups: role.groups.join('\n'), areas: { ...role.areas } };
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
  /** The groups the form would save, each once. */
  const draftGroups = $derived([
    ...new Set(
      draft.groups
        .split('\n')
        .map((g) => g.trim())
        .filter((g) => g !== ''),
    ),
  ]);
  const openBuiltIn = $derived(shownRoles.find((r) => r.name === openRole)?.builtIn ?? false);
  /** A role that gives nothing is a role nobody needs, so it is never saved. */
  const givesSomething = $derived(openBuiltIn || Object.keys(draftAreas).length > 0);

  const nameTaken = $derived(roles.some((r) => r.name === newRole.trim()));

  async function startNaming() {
    if (!(await dropUnsaved())) return;
    newRole = '';
    roleError = '';
    naming = true;
  }

  /** Whether any role lists `group`. */
  function givesRole(group: string): boolean {
    return roles.some((r) => r.groups.includes(group));
  }

  /** A group the provider names that gives no role yet, opened on the Roles tab as a new role it gives. */
  async function roleFor(group: string) {
    if (!(await dropUnsaved())) return;
    location.hash = '#/admin/accounts/roles';
    newRole = group;
    named(group);
  }

  function named(group?: string) {
    const name = newRole.trim();
    if (name === '' || nameTaken) return;
    naming = false;
    unsaved = name;
    openRole = name;
    draft = { groups: group ?? '', areas: {} };
    roleError = '';
  }

  async function saveRole(event: SubmitEvent, name: string) {
    event.preventDefault();
    if (busy || !givesSomething) return;
    busy = true;
    roleError = '';
    try {
      const res = await putRole(name, draftGroups, openBuiltIn ? {} : draftAreas);
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
      body: `Remove the role ${name}. Accounts holding it lose what it gives.`,
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
      <!-- Password and SSO sign-ins are both refused into it, so it belongs to every tab. -->
      <button
        type="button"
        onclick={() => (location.hash = toHash({ levels: [], site: 'leader.sign_in_refused' }))}
        >Refused sign-ins</button
      >
    {/snippet}
    {#snippet help()}
      <p>
        A role gives viewer, editor or deployer in each area: shared or a worker group. An SSO
        account holds the roles its provider groups give it; a password or key account holds the
        roles you give it. The administrator role holds everything.
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
      <h2>A new key for {pending.account}</h2>
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
      <Field label="Signs in with" group>
        <div class="sign-in">
          <Segmented label="Signs in with" options={SIGN_INS} bind:value={newSignIn} />
        </div>
      </Field>
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
      {/if}
      <AccountAccess {roles} bind:chosen={newRoles} />
    </div>
    {#snippet actions()}
      <button type="button" onclick={reset} disabled={busy}>Cancel</button>
      <button class="primary" type="submit" disabled={busy || !createReady}>Add</button>
    {/snippet}
  </Dialog>

  <Dialog open={naming} title="new role" oncancel={() => (naming = false)} onsubmit={() => named()}>
    <Field required label="Role" error={nameTaken ? 'There is a role by this name already.' : null}>
      {#snippet help()}What the role is called. You can add the groups that give it next.{/snippet}
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
            <th>Groups</th>
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
                  {#each role.groups as group (group)}
                    <span class="pill mono">{group}</span>
                  {:else}
                    <span class="dim">—</span>
                  {/each}
                </div>
              </td>
              <td>
                <div class="pills">
                  {#if role.builtIn}
                    <span class="pill">everything</span>
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
                <td colspan="4">
                  <form class="panel" onsubmit={(e) => saveRole(e, role.name)}>
                    <Field label="Groups">
                      {#snippet help()}
                        One to a line, exactly as your provider names it. An SSO account in any of
                        them holds this role.
                      {/snippet}
                      <textarea rows="3" bind:value={draft.groups} autocomplete="off"></textarea>
                    </Field>
                    {#if role.builtIn}
                      <p class="dim">The administrator role holds everything, in every area.</p>
                    {:else}
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
                      {#if role.builtIn}
                        <span></span>
                      {:else}
                        <button type="button" onclick={() => removeRole(role.name)} disabled={busy}
                          >Remove role</button
                        >
                      {/if}
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
      <div class="tools">
        <Chips
          label="Filter accounts by sign-in"
          allCount={accounts.length}
          items={kinds}
          bind:value={kind}
        />
        <label class="search"
          ><span aria-hidden="true">⌕</span><input
            type="search"
            bind:value={query}
            placeholder="Search by name, username or login"
            aria-label="Search accounts"
          /></label
        >
      </div>
      {#if sortedAccounts.length === 0}
        <p class="dim">No account matches.</p>
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
                  {:else if account.authSource === 'key'}
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
                    {#each account.roles as role (role)}
                      <span class="pill mono">{role}</span>
                    {/each}
                    {#each account.groups.filter((g) => !givesRole(g)) as group (group)}
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
                    <AccountFold
                      {account}
                      {roles}
                      {me}
                      onchanged={accountChanged}
                      onkey={(created) =>
                        (keyHandovers = [
                          ...keyHandovers,
                          { ...created, account: account.displayName },
                        ])}
                      onpassword={(result) =>
                        (handovers = [
                          ...handovers.filter((h) => h.userId !== result.userId),
                          result,
                        ])}
                    />
                  </td>
                </tr>
              {/if}
            {/each}
          </tbody>
        </table>
      {/if}
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

  .panel textarea {
    width: 100%;
    box-sizing: border-box;
    resize: vertical;
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .actions.split {
    justify-content: space-between;
  }

  .buttons {
    display: flex;
    gap: 0.5rem;
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

  .tools {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.5rem;
  }

  .search {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex: 1;
    min-width: 14rem;
    max-width: 28rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0 0.7rem;
    color: var(--dim);
  }

  .search input {
    width: 100%;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--ink);
    font: inherit;
    padding: 1px 2px;
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
