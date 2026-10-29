<!--
  Admin -> Accounts -> Integrations: the accounts scripts call the API as, and
  their keys. An integration holds what its roles give, or everything as an
  administrator. A new key is shown once, when it is made, and stays on screen
  until it is dismissed.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import Field from './Field.svelte';
  import Dialog from './Dialog.svelte';
  import { ask } from './Confirm.svelte';
  import { ago } from './time';
  import {
    changeIntegration,
    createIntegration,
    createIntegrationKey,
    listIntegrations,
    listRoles,
    refused,
    revokeIntegrationKey,
    type CreatedIntegrationKeyDto,
    type IntegrationChange,
    type IntegrationDto,
    type RoleDto,
  } from './api';

  let { adding = $bindable(false) }: { adding?: boolean } = $props();

  let integrations = $state<IntegrationDto[]>([]);
  let roles = $state<RoleDto[]>([]);
  let loading = $state(true);
  let busy = $state(false);
  let message = $state('');

  /** The integration whose row is open, and which of its forms. */
  let open = $state<string | null>(null);
  let form = $state<'rename' | 'roles' | 'key' | null>(null);
  let draftName = $state('');
  let draftRoles = $state<string[]>([]);
  let draftAdministrator = $state(false);
  let draftLabel = $state('');

  /** Each new key until it is dismissed: it is the only copy. */
  let handovers = $state<(CreatedIntegrationKeyDto & { integration: string })[]>([]);

  async function refresh() {
    try {
      [integrations, roles] = await Promise.all([listIntegrations(), listRoles()]);
    } catch (e) {
      reported(e);
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    void refresh();
  });

  function startAdding() {
    draftName = '';
    draftRoles = [];
    draftAdministrator = false;
  }

  $effect(() => {
    if (adding) startAdding();
  });

  function toggle(row: IntegrationDto) {
    message = '';
    form = null;
    open = open === row.id ? null : row.id;
  }

  function startForm(row: IntegrationDto, which: 'rename' | 'roles' | 'key') {
    message = '';
    form = which;
    draftName = row.name;
    draftRoles = [...row.roles];
    draftAdministrator = row.administrator;
    draftLabel = '';
  }

  function toggleRole(name: string) {
    draftRoles = draftRoles.includes(name)
      ? draftRoles.filter((r) => r !== name)
      : [...draftRoles, name];
  }

  async function create() {
    if (busy || draftName.trim() === '') return;
    busy = true;
    try {
      const result = await createIntegration(draftName, draftRoles, draftAdministrator);
      if (refused(result)) return;
      adding = false;
      open = result.id;
      form = null;
      await refresh();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  /** One change, answered with the integration as it now stands. */
  async function change(row: IntegrationDto, what: IntegrationChange, done: string) {
    if (busy) return;
    busy = true;
    try {
      const result = await changeIntegration(row.id, what);
      if (refused(result)) return;
      message = done;
      form = null;
      await refresh();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function saveRoles(event: SubmitEvent, row: IntegrationDto) {
    event.preventDefault();
    // One field per request: the administrator flag first, then the roles, each only when it changed.
    if (draftAdministrator !== row.administrator) {
      await change(row, { administrator: draftAdministrator }, 'access updated');
    }
    const same =
      draftRoles.length === row.roles.length && draftRoles.every((r) => row.roles.includes(r));
    if (!same) await change(row, { roles: draftRoles }, 'access updated');
    form = null;
  }

  async function setEnabled(row: IntegrationDto, enabled: boolean) {
    if (
      !enabled &&
      !(await ask({
        title: 'disable integration',
        body: `Revoke every key ${row.name} holds and stop it calling. Enabling it again brings no key back.`,
        action: 'Disable integration',
        danger: true,
      }))
    )
      return;
    await change(
      row,
      { enabled },
      enabled
        ? 'integration enabled; it needs a new key'
        : 'integration disabled; its keys are revoked',
    );
  }

  async function makeKey(event: SubmitEvent, row: IntegrationDto) {
    event.preventDefault();
    if (busy) return;
    busy = true;
    try {
      const result = await createIntegrationKey(row.id, draftLabel);
      if (refused(result)) return;
      handovers = [...handovers, { ...result, integration: row.name }];
      form = null;
      await refresh();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function revoke(row: IntegrationDto, key: { id: string; label: string }) {
    if (busy) return;
    const ok = await ask({
      title: 'revoke key',
      body: `Revoke ${key.label ? `the key ${key.label}` : 'this key'} of ${row.name}. A script calling with it is refused from its next request.`,
      action: 'Revoke key',
      danger: true,
    });
    if (!ok || busy) return;
    busy = true;
    try {
      const result = await revokeIntegrationKey(row.id, key.id);
      if (refused(result)) return;
      message = 'key revoked';
      await refresh();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  /** What the Roles column shows: an administrator holds everything without one. */
  function rolesOf(row: IntegrationDto): string[] {
    return row.administrator ? ['administrator', ...row.roles] : row.roles;
  }

  function when(millis: number | null): string {
    return millis === null ? 'never' : ago(millis / 1000);
  }
</script>

{#each handovers as pending (pending.id)}
  <section class="handover">
    <h2>A new key for {pending.integration}</h2>
    <p class="critical">Copy the key below now: it is shown only once.</p>
    <p class="secret"><code>{pending.key}</code></p>
    <p>Send it as <code>Authorization: Bearer</code> and the key.</p>
    <button type="button" onclick={() => (handovers = handovers.filter((h) => h.id !== pending.id))}
      >Done</button
    >
  </section>
{/each}

<Dialog
  open={adding}
  title="new integration"
  oncancel={() => busy || (adding = false)}
  onsubmit={create}
>
  <div class="fields">
    <Field required label="Name">
      {#snippet help()}What its changes are recorded under, in the audit and in history.{/snippet}
      <input bind:value={draftName} autocomplete="off" required />
    </Field>
    {@render access()}
  </div>
  {#snippet actions()}
    <button type="button" onclick={() => (adding = false)} disabled={busy}>Cancel</button>
    <button class="primary" type="submit" disabled={busy || draftName.trim() === ''}>Add</button>
  {/snippet}
</Dialog>

{#snippet access()}
  <label class="check"
    ><input type="checkbox" bind:checked={draftAdministrator} /> Administrator</label
  >
  {#if !draftAdministrator}
    <Field label="Roles" group>
      {#snippet help()}It holds what these roles give.{/snippet}
      {#if roles.length === 0}
        <p class="dim">No roles yet. Add one under Roles.</p>
      {:else}
        <div class="roles">
          {#each roles as role (role.name)}
            <label class="check mono"
              ><input
                type="checkbox"
                checked={draftRoles.includes(role.name)}
                onchange={() => toggleRole(role.name)}
              />
              {role.name}</label
            >
          {/each}
        </div>
      {/if}
    </Field>
  {/if}
{/snippet}

{#if message}<p class="message" role="status">{message}</p>{/if}
{#if loading}
  <p>Reading the integrations…</p>
{:else if integrations.length === 0}
  <p class="dim">No integrations yet.</p>
{:else}
  <table>
    <thead>
      <tr>
        <th>Integration</th>
        <th>State</th>
        <th>Roles</th>
        <th class="wide">Keys</th>
        <th></th>
      </tr>
    </thead>
    <tbody>
      {#each integrations as row (row.id)}
        <tr class:expanded={open === row.id}>
          <td><div class="name">{row.name}</div></td>
          <td
            ><span class="pill" class:off={!row.enabled}
              >{row.enabled ? 'Enabled' : 'Disabled'}</span
            ></td
          >
          <td>
            <div class="pills">
              {#each rolesOf(row) as role (role)}
                <span class="pill" class:mono={role !== 'administrator'}>{role}</span>
              {/each}
            </div>
          </td>
          <td class="wide dim">{row.keys.length}</td>
          <td class="right">
            <button type="button" aria-expanded={open === row.id} onclick={() => toggle(row)}>
              {open === row.id ? 'Close' : 'Manage'}
            </button>
          </td>
        </tr>
        {#if open === row.id}
          <tr class="fold">
            <td colspan="5">
              <div class="row">
                <button type="button" onclick={() => startForm(row, 'rename')}>Rename</button>
                <button type="button" onclick={() => startForm(row, 'roles')}>Change access</button>
                {#if row.enabled}
                  <button type="button" onclick={() => startForm(row, 'key')}>+ New key</button>
                  <button type="button" disabled={busy} onclick={() => setEnabled(row, false)}
                    >Disable integration</button
                  >
                {:else}
                  <button type="button" disabled={busy} onclick={() => setEnabled(row, true)}
                    >Enable integration</button
                  >
                {/if}
              </div>

              {#if form === 'rename'}
                <form
                  class="panel"
                  onsubmit={(e) => {
                    e.preventDefault();
                    void change(row, { name: draftName }, 'name updated');
                  }}
                >
                  <Field required label="Name">
                    <input bind:value={draftName} required />
                  </Field>
                  <div class="actions">
                    <button type="button" onclick={() => (form = null)}>Cancel</button>
                    <button class="primary" type="submit" disabled={busy}>Save name</button>
                  </div>
                </form>
              {:else if form === 'roles'}
                <form class="panel" onsubmit={(e) => saveRoles(e, row)}>
                  {@render access()}
                  <div class="actions">
                    <button type="button" onclick={() => (form = null)}>Cancel</button>
                    <button class="primary" type="submit" disabled={busy}>Save access</button>
                  </div>
                </form>
              {:else if form === 'key'}
                <form class="panel" onsubmit={(e) => makeKey(e, row)}>
                  <Field label="Label">
                    {#snippet help()}Where the key is used, so you know which to revoke.{/snippet}
                    <input bind:value={draftLabel} autocomplete="off" />
                  </Field>
                  <div class="actions">
                    <button type="button" onclick={() => (form = null)}>Cancel</button>
                    <button class="primary" type="submit" disabled={busy}>Make key</button>
                  </div>
                </form>
              {/if}

              {#if row.keys.length === 0}
                <p class="dim keys-empty">No keys.</p>
              {:else}
                <table class="keys" aria-label="Keys">
                  <thead>
                    <tr>
                      <th>Key</th>
                      <th>Made</th>
                      <th>Last used</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {#each row.keys as key (key.id)}
                      <tr>
                        <td>{key.label || '—'}</td>
                        <td class="dim">{when(key.createdAt)}</td>
                        <td class="dim">{when(key.lastUsedAt)}</td>
                        <td class="right">
                          <button type="button" disabled={busy} onclick={() => revoke(row, key)}
                            >Revoke</button
                          >
                        </td>
                      </tr>
                    {/each}
                  </tbody>
                </table>
              {/if}
            </td>
          </tr>
        {/if}
      {/each}
    </tbody>
  </table>
{/if}

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

  .keys {
    margin-top: 1rem;
    max-width: 44rem;
  }

  .keys td,
  .keys th {
    background: var(--panel);
    padding: 0.45rem 0.5rem;
  }

  .keys-empty {
    margin: 1rem 0 0;
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

  .pill.off {
    color: var(--dim);
  }

  .pills {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }

  .check {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: var(--text-body);
  }

  .roles {
    display: grid;
    gap: 0.3rem;
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

  .fields {
    display: grid;
    gap: 0.8rem;
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
    word-break: break-all;
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
  }
</style>
