<!--
  What Admin -> Accounts opens under an account's row: what an administrator
  does to it, which is what its kind of account has. Every account is renamed
  and disabled alike; a local or key account's access is given here, a local
  account's password is reset here, and a key account's keys are made and
  revoked here. A new password or key goes up to the page with `onpassword`
  or `onkey`, which shows it once, until it is dismissed.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import Field from './Field.svelte';
  import AccountAccess from './AccountAccess.svelte';
  import { ask } from './Confirm.svelte';
  import { ago, expiry, localDateTime } from './time';
  import {
    changeAccount,
    createKey,
    refused,
    resetAccountPassword,
    revokeAccountSessions,
    revokeKey,
    type AccountChange,
    type AccountDto,
    type CreatedKeyDto,
    type RoleDto,
    type TemporaryPasswordDto,
  } from './api';

  let {
    account,
    roles,
    me,
    onchanged,
    onkey,
    onpassword,
  }: {
    account: AccountDto;
    roles: RoleDto[];
    /** The signed-in account's id. */
    me: string | undefined;
    /** After a change landed, with the status line that says what it did. */
    onchanged: (message: string) => Promise<void>;
    onkey: (created: CreatedKeyDto) => void;
    onpassword: (handover: TemporaryPasswordDto) => void;
  } = $props();

  let busy = $state(false);
  let form = $state<'rename' | 'access' | 'key' | null>(null);
  let draftName = $state('');
  let draftRoles = $state<string[]>([]);
  let draftAdministrator = $state(false);
  let draftLabel = $state('');
  let draftDays = $state<number | null | undefined>(null);

  const isKey = $derived(account.authSource === 'key');
  const keys = $derived(account.keys ?? []);
  /** Days a new key lasts: empty never expires, and anything else is a whole day or more. */
  const daysValid = $derived(
    draftDays === null ||
      draftDays === undefined ||
      (Number.isInteger(draftDays) && draftDays >= 1),
  );

  function startForm(which: 'rename' | 'access' | 'key') {
    form = which;
    draftName = account.displayName;
    draftRoles = [...account.roles];
    draftAdministrator = account.administrator;
    draftLabel = '';
    draftDays = null;
  }

  /** One change, answered with the account as it now stands. Whether it was made. */
  async function change(what: AccountChange, done: string): Promise<boolean> {
    if (busy) return false;
    busy = true;
    try {
      const result = await changeAccount(account.id, what);
      if (refused(result)) return false;
      form = null;
      await onchanged(done);
      return true;
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
    return false;
  }

  async function saveAccess(event: SubmitEvent) {
    event.preventDefault();
    // One field per request: the administrator flag first, then the roles, each only when it changed. A
    // refused one stops there, and the form stays open.
    if (draftAdministrator !== account.administrator) {
      if (!(await change({ administrator: draftAdministrator }, 'access updated'))) return;
    }
    const same =
      draftRoles.length === account.roles.length &&
      draftRoles.every((r) => account.roles.includes(r));
    if (!same && !(await change({ roles: draftRoles }, 'access updated'))) return;
    form = null;
  }

  async function setEnabled(enabled: boolean) {
    if (
      !enabled &&
      !(await ask({
        title: 'disable account',
        body: isKey
          ? `Disabling "${account.displayName}" revokes every key it holds and stops it calling. Enabling it again does not bring them back.`
          : `Sign ${account.displayName} out everywhere and stop them signing in.`,
        action: 'Disable account',
        danger: true,
      }))
    )
      return;
    const done = enabled
      ? isKey
        ? 'account enabled; it needs a new key'
        : 'account enabled'
      : isKey
        ? 'account disabled; its keys are revoked'
        : 'account disabled; its sessions are over';
    await change({ enabled }, done);
  }

  /** One of the calls that answers something other than the account, then the roster read again. */
  async function act<T extends object>(call: () => Promise<T | { status: number; error: string }>) {
    if (busy) return null;
    busy = true;
    try {
      const result = await call();
      if (refused(result)) return null;
      return result;
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
    return null;
  }

  async function resetPassword() {
    if (busy) return;
    const ok = await ask({
      title: 'reset password',
      body: `Sign ${account.displayName} out everywhere and give them a temporary password.`,
      action: 'Reset password',
      danger: true,
    });
    if (!ok) return;
    form = null;
    const result = await act(() => resetAccountPassword(account.id));
    if (result === null) return;
    await onchanged('');
    onpassword(result);
  }

  async function revokeSessions() {
    form = null;
    const result = await act(() => revokeAccountSessions(account.id));
    if (result === null) return;
    await onchanged(
      `signed out everywhere (${result.ended} ${result.ended === 1 ? 'session' : 'sessions'})`,
    );
  }

  async function makeKey(event: SubmitEvent) {
    event.preventDefault();
    if (!daysValid) return;
    const result = await act(() => createKey(account.id, draftLabel, draftDays ?? null));
    if (result === null) return;
    form = null;
    // Read again first, so the key is shown beside the row that lists it.
    await onchanged('');
    onkey(result);
  }

  async function revoke(key: { id: string; label: string }) {
    if (busy) return;
    const ok = await ask({
      title: 'revoke key',
      body: `${key.label ? `The key "${key.label}"` : 'This key'} of "${account.displayName}" stops working: a script calling with it is refused from its next request.`,
      action: 'Revoke key',
      danger: true,
    });
    if (!ok) return;
    if ((await act(() => revokeKey(account.id, key.id))) === null) return;
    await onchanged('key revoked');
  }

  function when(millis: number | null): string {
    return millis === null ? 'never' : ago(millis / 1000);
  }

  /** Rounded up to the minute, so a key made for 30 days reads 30 days, not 29, the moment it is made. */
  function expires(millis: number | null): string {
    if (millis === null) return 'never';
    if (millis <= Date.now()) return 'expired';
    return expiry(millis + 59_999).replace(/^expires /, '');
  }
</script>

<div class="row">
  <button type="button" onclick={() => startForm('rename')}>Rename</button>
  {#if account.authSource !== 'oidc'}
    <button type="button" onclick={() => startForm('access')}>Change access</button>
  {/if}
  <!-- Not for your own account. A reset ends every session it belongs to,
     including this one, so the page holding the only copy of the new password
     would be gone before anybody could read it. Change password does this
     safely. -->
  {#if account.authSource === 'local' && account.id !== me}
    <button type="button" disabled={busy} onclick={resetPassword}>Reset password</button>
  {/if}
  {#if !isKey}
    <button type="button" disabled={busy} onclick={revokeSessions}>Revoke sessions</button>
  {/if}
  {#if isKey && account.enabled}
    <button type="button" onclick={() => startForm('key')}>+ New key</button>
  {/if}
  {#if account.enabled}
    <button type="button" disabled={busy} onclick={() => setEnabled(false)}>Disable account</button>
  {:else}
    <button type="button" disabled={busy} onclick={() => setEnabled(true)}>Enable account</button>
  {/if}
</div>

{#if form === 'rename'}
  <form
    class="panel"
    onsubmit={(e) => {
      e.preventDefault();
      void change({ displayName: draftName }, 'display name updated');
    }}
  >
    <Field required label="Display name">
      {#snippet help()}Only the name shown in smeltr changes.{/snippet}
      <input bind:value={draftName} required />
    </Field>
    <div class="actions">
      <button type="button" onclick={() => (form = null)}>Cancel</button>
      <button class="primary" type="submit" disabled={busy}>Save display name</button>
    </div>
  </form>
{:else if form === 'access'}
  <form class="panel" onsubmit={saveAccess}>
    <AccountAccess {roles} bind:administrator={draftAdministrator} bind:chosen={draftRoles} />
    <div class="actions">
      <button type="button" onclick={() => (form = null)}>Cancel</button>
      <button class="primary" type="submit" disabled={busy}>Save access</button>
    </div>
  </form>
{:else if form === 'key'}
  <form class="panel" onsubmit={makeKey}>
    <Field label="Label">
      {#snippet help()}Where the key is used, so you know which to revoke.{/snippet}
      <input bind:value={draftLabel} autocomplete="off" />
    </Field>
    <Field
      label="Expires in (days)"
      error={daysValid ? null : 'A whole number of days, 1 or more.'}
    >
      {#snippet help()}Leave it empty for a key that never expires.{/snippet}
      <input type="number" min="1" step="1" bind:value={draftDays} autocomplete="off" />
    </Field>
    <div class="actions">
      <button type="button" onclick={() => (form = null)}>Cancel</button>
      <button class="primary" type="submit" disabled={busy || !daysValid}>Create key</button>
    </div>
  </form>
{/if}

{#if isKey}
  {#if keys.length === 0}
    <p class="dim keys-empty">No keys.</p>
  {:else}
    <table class="keys" aria-label="Keys">
      <thead>
        <tr>
          <th>Key</th>
          <th>Created</th>
          <th>Expires</th>
          <th>Last used</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {#each keys as key (key.id)}
          <tr>
            <td>{key.label || '—'}</td>
            <td class="dim">{when(key.createdAt)}</td>
            <td
              class="dim"
              title={key.expiresAt === null ? undefined : localDateTime(key.expiresAt)}
              >{expires(key.expiresAt)}</td
            >
            <td class="dim">{when(key.lastUsedAt)}</td>
            <td class="right">
              <button type="button" disabled={busy} onclick={() => revoke(key)}>Revoke</button>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
{/if}

<style>
  .keys {
    width: 100%;
    max-width: 44rem;
    margin-top: 1rem;
    border-collapse: collapse;
    text-align: left;
  }

  .keys th {
    color: var(--dim);
    font-size: var(--text-micro);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-weight: 700;
  }

  .keys td,
  .keys th {
    background: var(--panel);
    padding: 0.45rem 0.5rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }

  .keys-empty {
    margin: 1rem 0 0;
  }

  .dim {
    color: var(--dim);
    font-size: var(--text-body);
  }

  .right {
    text-align: right;
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

  input {
    width: 100%;
  }

  .actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
    margin-top: 0.2rem;
  }
</style>
