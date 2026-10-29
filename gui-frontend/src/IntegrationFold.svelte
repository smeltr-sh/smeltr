<!--
  What Admin -> Accounts opens under an integration's row: what an
  administrator does to it, and its keys. A new key goes up to the page with
  `onkey`, which shows it once, until it is dismissed.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import Field from './Field.svelte';
  import IntegrationAccess from './IntegrationAccess.svelte';
  import { ask } from './Confirm.svelte';
  import { ago } from './time';
  import {
    changeAccount,
    createIntegrationKey,
    refused,
    revokeIntegrationKey,
    type AccountChange,
    type AccountDto,
    type CreatedIntegrationKeyDto,
    type RoleDto,
  } from './api';

  let {
    account,
    roles,
    onchanged,
    onkey,
  }: {
    account: AccountDto;
    roles: RoleDto[];
    /** After a change landed, with the status line that says what it did. */
    onchanged: (message: string) => Promise<void>;
    onkey: (created: CreatedIntegrationKeyDto) => void;
  } = $props();

  let busy = $state(false);
  let form = $state<'rename' | 'roles' | 'key' | null>(null);
  let draftName = $state('');
  let draftRoles = $state<string[]>([]);
  let draftAdministrator = $state(false);
  let draftLabel = $state('');

  const keys = $derived(account.keys ?? []);

  function startForm(which: 'rename' | 'roles' | 'key') {
    form = which;
    draftName = account.displayName;
    draftRoles = [...account.roles];
    draftAdministrator = account.administrator;
    draftLabel = '';
  }

  /** One change, answered with the integration as it now stands. */
  async function change(what: AccountChange, done: string) {
    if (busy) return;
    busy = true;
    try {
      const result = await changeAccount(account.id, what);
      if (refused(result)) return;
      form = null;
      await onchanged(done);
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function saveRoles(event: SubmitEvent) {
    event.preventDefault();
    // One field per request: the administrator flag first, then the roles, each only when it changed.
    if (draftAdministrator !== account.administrator) {
      await change({ administrator: draftAdministrator }, 'access updated');
    }
    const same =
      draftRoles.length === account.roles.length &&
      draftRoles.every((r) => account.roles.includes(r));
    if (!same) await change({ roles: draftRoles }, 'access updated');
    form = null;
  }

  async function setEnabled(enabled: boolean) {
    if (
      !enabled &&
      !(await ask({
        title: 'disable integration',
        body: `Disabling "${account.displayName}" revokes every key it holds and stops it calling. Enabling it again does not bring them back.`,
        action: 'Disable integration',
        danger: true,
      }))
    )
      return;
    await change(
      { enabled },
      enabled
        ? 'integration enabled; it needs a new key'
        : 'integration disabled; its keys are revoked',
    );
  }

  async function makeKey(event: SubmitEvent) {
    event.preventDefault();
    if (busy) return;
    busy = true;
    try {
      const result = await createIntegrationKey(account.id, draftLabel);
      if (refused(result)) return;
      form = null;
      // Read again first, so the key is shown beside the row that lists it.
      await onchanged('');
      onkey(result);
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function revoke(key: { id: string; label: string }) {
    if (busy) return;
    const ok = await ask({
      title: 'revoke key',
      body: `${key.label ? `The key "${key.label}"` : 'This key'} of "${account.displayName}" stops working: a script calling with it is refused from its next request.`,
      action: 'Revoke key',
      danger: true,
    });
    if (!ok || busy) return;
    busy = true;
    try {
      const result = await revokeIntegrationKey(account.id, key.id);
      if (refused(result)) return;
      await onchanged('key revoked');
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  function when(millis: number | null): string {
    return millis === null ? 'never' : ago(millis / 1000);
  }
</script>

<div class="row">
  <button type="button" onclick={() => startForm('rename')}>Rename</button>
  <button type="button" onclick={() => startForm('roles')}>Change access</button>
  {#if account.enabled}
    <button type="button" onclick={() => startForm('key')}>+ New key</button>
    <button type="button" disabled={busy} onclick={() => setEnabled(false)}
      >Disable integration</button
    >
  {:else}
    <button type="button" disabled={busy} onclick={() => setEnabled(true)}
      >Enable integration</button
    >
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
      <input bind:value={draftName} required />
    </Field>
    <div class="actions">
      <button type="button" onclick={() => (form = null)}>Cancel</button>
      <button class="primary" type="submit" disabled={busy}>Save display name</button>
    </div>
  </form>
{:else if form === 'roles'}
  <form class="panel" onsubmit={saveRoles}>
    <IntegrationAccess {roles} bind:administrator={draftAdministrator} bind:chosen={draftRoles} />
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
    <div class="actions">
      <button type="button" onclick={() => (form = null)}>Cancel</button>
      <button class="primary" type="submit" disabled={busy}>Create key</button>
    </div>
  </form>
{/if}

{#if keys.length === 0}
  <p class="dim keys-empty">No keys.</p>
{:else}
  <table class="keys" aria-label="Keys">
    <thead>
      <tr>
        <th>Key</th>
        <th>Created</th>
        <th>Last used</th>
        <th></th>
      </tr>
    </thead>
    <tbody>
      {#each keys as key (key.id)}
        <tr>
          <td>{key.label || '—'}</td>
          <td class="dim">{when(key.createdAt)}</td>
          <td class="dim">{when(key.lastUsedAt)}</td>
          <td class="right">
            <button type="button" disabled={busy} onclick={() => revoke(key)}>Revoke</button>
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
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
