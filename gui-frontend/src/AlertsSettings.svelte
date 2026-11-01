<!--
  Admin -> System -> Alerts: the one mail server, the channels warnings are sent to, and which issues
  each takes. Passwords and secrets go in and never come back out: the leader says only whether one
  is stored.

  The channels are one list on the leader, so every change to them -- a dialog's, a mute's, the
  table's ticks -- saves the whole list as the page holds it.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import {
    conditionLabel,
    getAlertSettings,
    listWorkerGroups,
    putAlertChannels,
    putAlertMail,
    testAlertChannel,
    type AlertChannel,
    type AlertChannelDto,
    type AlertFormat,
    type AlertSettingsDto,
    type AlertSettingsResult,
    type WarningCondition,
  } from './api';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import Help from './Help.svelte';
  import LeaderSecretField from './LeaderSecretField.svelte';
  import Panel from './Panel.svelte';
  import Toggle from './Toggle.svelte';
  import { ago, dateTime, fromLocalInput } from './time';

  let settings = $state<AlertSettingsDto | null>(null);
  let loadFailed = $state(false);
  let busy = $state(false);
  let groupNames = $state<string[]>([]);

  // The mail server's form.
  let enabled = $state(false);
  let host = $state('');
  let port = $state(587);
  let encryption = $state<'startTls' | 'tls'>('startTls');
  let from = $state('');
  let username = $state('');
  let password = $state('');
  let replacingPassword = $state(false);

  // What each channel takes, as ticked and not yet saved.
  let ticks = $state<Record<string, WarningCondition[]>>({});

  function adopt(next: AlertSettingsDto) {
    settings = next;
    enabled = next.mail?.enabled ?? false;
    host = next.mail?.host ?? '';
    port = next.mail?.port ?? 587;
    encryption = next.mail?.encryption ?? 'startTls';
    from = next.mail?.from ?? '';
    username = next.mail?.username ?? '';
    password = '';
    replacingPassword = false;
    ticks = Object.fromEntries(next.channels.map((c) => [c.id, [...c.conditions]]));
  }

  function answer(result: AlertSettingsResult): boolean {
    if (!result.ok || !result.settings) return false;
    adopt(result.settings);
    return true;
  }

  $effect(() => {
    getAlertSettings()
      .then((result) => {
        if (result.settings) adopt(result.settings);
        else loadFailed = true;
      })
      .catch((e) => {
        loadFailed = true;
        reported(e);
      });
    listWorkerGroups()
      .then((result) => (groupNames = result.groups.map((g) => g.id)))
      .catch(reported);
  });

  const mailOn = $derived(settings?.mail?.enabled ?? false);
  // A password is for the host it was saved with: the leader forgets it when the host changes.
  const passwordKept = $derived(
    settings?.mail != null && settings.mail.passwordSet && host.trim() === settings.mail.host,
  );
  const mailDirty = $derived(
    settings !== null &&
      (enabled !== (settings.mail?.enabled ?? false) ||
        (enabled &&
          (host.trim() !== (settings.mail?.host ?? '') ||
            port !== (settings.mail?.port ?? 587) ||
            encryption !== (settings.mail?.encryption ?? 'startTls') ||
            from.trim() !== (settings.mail?.from ?? '') ||
            username.trim() !== (settings.mail?.username ?? '') ||
            password !== ''))),
  );

  async function saveMail(event: SubmitEvent) {
    event.preventDefault();
    if (busy) return;
    busy = true;
    try {
      answer(
        await putAlertMail({
          enabled,
          host: host.trim(),
          port,
          encryption,
          from: from.trim(),
          username: username.trim() || undefined,
          password: password === '' ? undefined : password,
        }),
      );
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  // Turned off, it saves at once: there is nothing left to fill in.
  $effect(() => {
    if (!enabled && settings?.mail?.enabled && !busy) {
      void saveMailOff();
    }
  });

  async function saveMailOff() {
    if (!settings?.mail) return;
    busy = true;
    try {
      const { passwordSet: _, ...mail } = settings.mail;
      answer(await putAlertMail({ ...mail, enabled: false }));
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  /** The channels as the page holds them, ticks included. */
  function held(): AlertChannel[] {
    return (settings?.channels ?? []).map(({ secretSet: _, failing: __, ...c }) => ({
      ...c,
      conditions: ticks[c.id] ?? c.conditions,
    }));
  }

  async function saveChannels(channels: (AlertChannel & { secret?: string })[]): Promise<boolean> {
    busy = true;
    try {
      return answer(await putAlertChannels(channels));
    } catch (e) {
      reported(e);
      return false;
    } finally {
      busy = false;
    }
  }

  const ticksDirty = $derived(
    settings !== null &&
      settings.channels.some(
        (c) =>
          (ticks[c.id] ?? []).length !== c.conditions.length ||
          (ticks[c.id] ?? []).some((t) => !c.conditions.includes(t)),
      ),
  );

  function tick(channel: string, condition: WarningCondition, on: boolean) {
    const now = ticks[channel] ?? [];
    ticks[channel] = on ? [...now, condition] : now.filter((c) => c !== condition);
  }

  async function saveTicks(event: SubmitEvent) {
    event.preventDefault();
    if (!busy) await saveChannels(held());
  }

  // -- the channel dialog ------------------------------------------------------

  type Draft = {
    id: string;
    type: 'webhook' | 'email';
    name: string;
    format: AlertFormat;
    url: string;
    secretHeader: string;
    to: string;
    groupsTaken: 'all' | 'these';
    groups: string[];
    includeMessage: boolean;
  };

  let draft = $state<Draft | null>(null);
  let secret = $state('');
  let replacingSecret = $state(false);
  let tested = $state<{ ok: boolean; text: string } | null>(null);

  const editing = $derived(draft !== null && draft.id !== '');
  const savedDraft = $derived(settings?.channels.find((c) => c.id === draft?.id));

  function open(type: 'webhook' | 'email', from?: AlertChannelDto) {
    const kind = from?.kind;
    draft = {
      id: from?.id ?? '',
      type,
      name: from?.name ?? '',
      format: kind?.type === 'webhook' ? kind.format : 'genericJson',
      url: kind?.type === 'webhook' ? kind.url : '',
      secretHeader: kind?.type === 'webhook' ? (kind.secretHeader ?? '') : '',
      to: kind?.type === 'email' ? kind.to.join(', ') : '',
      groupsTaken: from?.groups == null ? 'all' : 'these',
      groups: from?.groups ?? [],
      includeMessage: from?.includeMessage ?? false,
    };
    secret = '';
    replacingSecret = false;
    tested = null;
  }

  // A secret is for the URL it was saved with: the leader forgets it when the URL changes.
  const secretKept = $derived(
    savedDraft !== undefined &&
      savedDraft.secretSet &&
      savedDraft.kind.type === 'webhook' &&
      draft?.url.trim() === savedDraft.kind.url,
  );

  function fromDraft(d: Draft): AlertChannel & { secret?: string } {
    const kind: AlertChannel['kind'] =
      d.type === 'webhook'
        ? {
            type: 'webhook',
            url: d.url.trim(),
            format: d.format,
            secretHeader: d.format === 'pagerDuty' ? undefined : d.secretHeader.trim() || undefined,
          }
        : {
            type: 'email',
            to: d.to
              .split(',')
              .map((a) => a.trim())
              .filter((a) => a !== ''),
          };
    return {
      id: d.id,
      name: d.name.trim(),
      kind,
      groups: d.groupsTaken === 'all' ? undefined : d.groups,
      includeMessage: d.includeMessage,
      conditions: savedDraft ? (ticks[d.id] ?? savedDraft.conditions) : [],
      mutedUntil: savedDraft?.mutedUntil,
      secret: secret === '' ? undefined : secret,
    };
  }

  /** What a test of `channel` found, as the line that says so. */
  async function test(
    channel: AlertChannel & { secret?: string },
  ): Promise<{ ok: boolean; text: string } | null> {
    busy = true;
    try {
      const result = await testAlertChannel(channel);
      return result.ok
        ? {
            ok: true,
            text:
              result.status === undefined
                ? `Test sent. ${result.host} accepted it.`
                : `Test sent. ${result.host} answered ${result.status}.`,
          }
        : { ok: false, text: result.errors.map((e) => e.message).join(' ') };
    } catch (e) {
      reported(e);
      return null;
    } finally {
      busy = false;
    }
  }

  async function sendTest() {
    if (draft) tested = await test(fromDraft(draft));
  }

  // A row's test, by channel, until the page reads the channels again.
  let rowTested = $state<Record<string, { ok: boolean; text: string } | null>>({});

  async function testRow(id: string) {
    const channel = held().find((c) => c.id === id);
    if (channel) rowTested[id] = await test(channel);
  }

  async function keep() {
    if (!draft || busy) return;
    const channel = fromDraft(draft);
    const others = held().filter((c) => c.id !== channel.id);
    if (
      await saveChannels(
        editing ? held().map((c) => (c.id === channel.id ? channel : c)) : [...others, channel],
      )
    )
      draft = null;
  }

  async function remove() {
    if (!draft || busy) return;
    const id = draft.id;
    if (await saveChannels(held().filter((c) => c.id !== id))) draft = null;
  }

  // -- muting --------------------------------------------------------------------

  let muting = $state<AlertChannelDto | null>(null);
  let muteFor = $state<'1h' | '4h' | 'morning' | 'at'>('4h');
  let muteAt = $state('');

  function until(): number | undefined {
    const now = new Date();
    switch (muteFor) {
      case '1h':
        return now.getTime() + 3_600_000;
      case '4h':
        return now.getTime() + 4 * 3_600_000;
      case 'morning': {
        const next = new Date(now);
        next.setDate(now.getDate() + 1);
        next.setHours(9, 0, 0, 0);
        return next.getTime();
      }
      case 'at':
        return fromLocalInput(muteAt);
    }
  }

  async function mute(id: string, mutedUntil: number | undefined) {
    if (busy) return;
    if (await saveChannels(held().map((c) => (c.id === id ? { ...c, mutedUntil } : c))))
      muting = null;
  }

  const muted = (c: AlertChannelDto) => c.mutedUntil !== undefined && c.mutedUntil > Date.now();

  function where(c: AlertChannelDto): string {
    return c.kind.type === 'webhook' ? c.kind.url : c.kind.to.join(', ');
  }

  const FORMATS: { value: AlertFormat; label: string }[] = [
    { value: 'genericJson', label: 'Generic JSON' },
    { value: 'slack', label: 'Slack' },
    { value: 'teams', label: 'Microsoft Teams' },
    { value: 'pagerDuty', label: 'PagerDuty' },
  ];
  const formatLabel = (f: AlertFormat) => FORMATS.find((x) => x.value === f)?.label ?? f;
</script>

{#if loadFailed}
  <!-- The error bar says why. -->
{:else if settings === null}
  <p>Reading what this leader is set to…</p>
{:else}
  <Panel title="Mail server" onsubmit={saveMail}>
    {#snippet actions()}
      <span class="inline label"
        >Enabled <Toggle bind:checked={enabled} ariaLabel="Mail server enabled" /></span
      >
    {/snippet}
    {#if enabled}
      <div class="row">
        <Field required label="Host"
          ><input bind:value={host} placeholder="smtp.example.com" /></Field
        >
        <Field label="Port"
          ><input type="number" min="1" max="65535" bind:value={port} class="port" /></Field
        >
        <Field label="Encryption">
          <select bind:value={encryption}>
            <option value="startTls">STARTTLS</option>
            <option value="tls">TLS</option>
          </select>
        </Field>
      </div>
      <Field required label="From"
        ><input bind:value={from} placeholder="smeltr@example.com" /></Field
      >
      <div class="row">
        <Field optional label="Username"><input bind:value={username} /></Field>
        <Field optional label="Password" group>
          <LeaderSecretField
            bind:value={password}
            bind:replacing={replacingPassword}
            stored={passwordKept}
            ariaLabel="password"
          />
        </Field>
      </div>
      <div class="buttons">
        <button class="primary" type="submit" disabled={busy || !mailDirty}>Save</button>
      </div>
    {/if}
  </Panel>

  <Panel title="Channels">
    {#snippet actions()}
      <span class="inline">
        <button type="button" class="compact" onclick={() => open('webhook')}>+ Webhook</button>
        {#if mailOn}
          <button type="button" class="compact" onclick={() => open('email')}>+ Email</button>
        {/if}
      </span>
    {/snippet}
    {#if settings.channels.length === 0}
      <p class="dim">No channels yet.</p>
    {:else}
      <div class="channels" role="table" aria-label="Channels">
        <div class="channel head dim" role="row">
          <span role="columnheader">Name</span><span role="columnheader">Sends to</span><span
            role="columnheader">Groups</span
          ><span role="columnheader">Message</span><span></span>
        </div>
        {#each settings.channels as c (c.id)}
          <div class="channel" role="row">
            <span role="cell"
              ><strong>{c.name}</strong><br /><span class="dim note"
                >{c.kind.type === 'webhook' ? 'Webhook' : 'Email'}</span
              ></span
            >
            <span role="cell" class="mono note sends"
              >{where(c)}{#if c.kind.type === 'webhook'}<br /><span class="dim"
                  >{formatLabel(c.kind.format)}{c.secretSet
                    ? ` · ${c.kind.format === 'pagerDuty' ? 'routing key' : 'secret header'} stored`
                    : ''}</span
                >{/if}</span
            >
            <span role="cell" class="note">{c.groups ? c.groups.join(', ') : 'All groups'}</span>
            <span role="cell" class="note dim">{c.includeMessage ? 'Included' : 'Left out'}</span>
            <span role="cell" class="actions">
              <button type="button" class="compact" disabled={busy} onclick={() => testRow(c.id)}
                >Send test</button
              >
              {#if muted(c)}
                <button
                  type="button"
                  class="compact"
                  disabled={busy}
                  onclick={() => mute(c.id, undefined)}>Unmute</button
                >
              {:else}
                <button type="button" class="compact" disabled={busy} onclick={() => (muting = c)}
                  >Mute…</button
                >
              {/if}
              <button
                type="button"
                class="compact"
                disabled={busy}
                onclick={() => open(c.kind.type, c)}>Edit</button
              >
            </span>
            {#if c.kind.type === 'email' && !mailOn}
              <span class="line warn">Not sent: the mail server is off.</span>
            {:else if muted(c) && c.mutedUntil}
              <span class="line dim">Muted until {dateTime(c.mutedUntil / 1000)}</span>
            {/if}
            {#if rowTested[c.id]?.ok}
              <span class="line ok" role="status">✓ {rowTested[c.id]?.text}</span>
            {:else if rowTested[c.id]}
              <span class="line"><InlineError inline message={rowTested[c.id]?.text ?? ''} /></span>
            {/if}
            {#if c.failing && (c.kind.type !== 'email' || mailOn)}
              <span class="line"
                ><InlineError
                  inline
                  message="Last send failed {ago(c.failing.last / 1000)}: {c.failing
                    .message}. Smeltr sends it again every minute."
                /></span
              >
            {/if}
          </div>
        {/each}
      </div>
    {/if}
  </Panel>

  {#if settings.channels.length > 0}
    <Panel title="What each channel gets" onsubmit={saveTicks}>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col">Issue</th>
              {#each settings.channels as c (c.id)}<th scope="col" class="tick">{c.name}</th>{/each}
            </tr>
          </thead>
          <tbody>
            {#each settings.conditions as condition (condition)}
              <tr>
                <th scope="row">{conditionLabel(condition)}</th>
                {#each settings.channels as c (c.id)}
                  <td class="tick"
                    ><input
                      type="checkbox"
                      aria-label="{c.name}: {conditionLabel(condition)}"
                      checked={(ticks[c.id] ?? []).includes(condition)}
                      onchange={(e) => tick(c.id, condition, e.currentTarget.checked)}
                    /></td
                  >
                {/each}
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
      <div class="buttons">
        <button class="primary" type="submit" disabled={busy || !ticksDirty}>Save</button>
      </div>
    </Panel>
  {/if}
{/if}

<Dialog
  open={draft !== null}
  title={draft === null
    ? ''
    : editing
      ? `Edit ${savedDraft?.name ?? ''}`
      : draft.type === 'webhook'
        ? 'Add a webhook'
        : 'Add an email channel'}
  oncancel={() => (draft = null)}
  onsubmit={keep}
>
  {#if draft}
    <div class="form-body">
      <Field required label="Name"><input bind:value={draft.name} /></Field>
      {#if draft.type === 'webhook'}
        <Field label="Format">
          <select bind:value={draft.format}>
            {#each FORMATS as f (f.value)}<option value={f.value}>{f.label}</option>{/each}
          </select>
        </Field>
        <Field required label="URL"><input bind:value={draft.url} class="mono" /></Field>
        {#if draft.format === 'pagerDuty'}
          <Field required label="Routing key" group>
            {#snippet help()}The routing key of the PagerDuty service to open incidents on, which
              PagerDuty calls its integration key. Stored encrypted.{/snippet}
            <LeaderSecretField
              bind:value={secret}
              bind:replacing={replacingSecret}
              stored={secretKept}
              ariaLabel="routing key"
            />
          </Field>
        {:else}
          <fieldset class="pair">
            <legend
              >Secret header<Help label="Secret header"
                >Sent with every request so the receiver can check it came from Smeltr. The value is
                stored encrypted.</Help
              ></legend
            >
            <div class="row">
              <Field optional label="Header name"><input bind:value={draft.secretHeader} /></Field>
              <Field optional label="Header value" group>
                <LeaderSecretField
                  bind:value={secret}
                  bind:replacing={replacingSecret}
                  stored={secretKept}
                  ariaLabel="header value"
                />
              </Field>
            </div>
          </fieldset>
        {/if}
      {:else}
        <Field required label="To" help="Addresses separated by commas.">
          <input bind:value={draft.to} />
        </Field>
      {/if}
      <fieldset class="groups">
        <legend>Groups</legend>
        <label><input type="radio" bind:group={draft.groupsTaken} value="all" /> All groups</label>
        <label><input type="radio" bind:group={draft.groupsTaken} value="these" /> Only these</label
        >
        {#if draft.groupsTaken === 'these'}
          <div class="choices">
            {#each groupNames as g (g)}
              <label><input type="checkbox" bind:group={draft.groups} value={g} /> {g}</label>
            {/each}
          </div>
        {/if}
      </fieldset>
      <label class="check"
        ><input type="checkbox" bind:checked={draft.includeMessage} />
        <span class="label">Include the message</span><Help label="Include the message"
          >A warning's message can quote an error from a source or destination, and that can contain
          a record's values. Without it, a channel gets only the warning's type, subject and
          severity.</Help
        ></label
      >
      {#if tested}
        {#if tested.ok}<p class="ok" role="status">✓ {tested.text}</p>{:else}<InlineError
            message={tested.text}
          />{/if}
      {/if}
    </div>
  {/if}
  {#snippet actions()}
    {#if editing}<button type="button" onclick={remove} disabled={busy}>Remove</button>{/if}
    <button type="button" onclick={() => (draft = null)} disabled={busy}>Cancel</button>
    <button type="button" onclick={sendTest} disabled={busy}>Send test</button>
    <button type="submit" class="primary" disabled={busy}>{editing ? 'Save' : 'Add'}</button>
  {/snippet}
</Dialog>

<Dialog
  open={muting !== null}
  title="Mute {muting?.name ?? ''}"
  oncancel={() => (muting = null)}
  onsubmit={() => muting && mute(muting.id, until())}
>
  <fieldset class="groups">
    <legend
      >Until<Help label="Until"
        >Warnings that start or clear while muted are not sent. Ones still standing when the mute
        ends are sent then. Times are in your browser's time zone.</Help
      ></legend
    >
    <label><input type="radio" bind:group={muteFor} value="1h" /> 1 hour</label>
    <label><input type="radio" bind:group={muteFor} value="4h" /> 4 hours</label>
    <label><input type="radio" bind:group={muteFor} value="morning" /> Tomorrow 09:00</label>
    <label class="inline"
      ><input type="radio" bind:group={muteFor} value="at" /> At
      <input type="datetime-local" bind:value={muteAt} onfocus={() => (muteFor = 'at')} /></label
    >
  </fieldset>
  {#snippet actions()}
    <button type="button" onclick={() => (muting = null)} disabled={busy}>Cancel</button>
    <button type="submit" class="primary" disabled={busy || until() === undefined}>Mute</button>
  {/snippet}
</Dialog>

<style>
  .inline {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .label,
  legend {
    font-size: var(--text-label);
    font-weight: 600;
  }

  .row {
    display: flex;
    flex-wrap: wrap;
    gap: 1.2rem;
  }

  .port {
    width: 6rem;
  }

  .buttons {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
  }

  .channel {
    display: grid;
    grid-template-columns: 9rem minmax(0, 1fr) 6rem 5rem 15rem;
    gap: 0.4rem 1rem;
    align-items: center;
    padding: 0.6rem 0;
    border-bottom: 1px solid var(--border);
  }

  .channel.head {
    font-size: var(--text-label);
    font-weight: 600;
    padding-top: 0;
  }

  .note {
    font-size: var(--text-note);
  }

  .mono {
    font-family: var(--mono);
  }

  .sends {
    overflow-wrap: anywhere;
  }

  .actions {
    display: flex;
    gap: 0.4rem;
    justify-content: flex-end;
  }

  .line {
    grid-column: 1 / -1;
    font-size: var(--text-note);
  }

  .warn {
    color: var(--warn);
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    border-collapse: collapse;
    width: 100%;
  }

  th,
  td {
    padding: 0.35rem 0.6rem;
    text-align: left;
  }

  tbody tr {
    border-top: 1px solid var(--border);
  }

  tbody th {
    font-weight: 400;
  }

  .tick {
    text-align: center;
    width: 11rem;
  }

  .form-body {
    display: grid;
    gap: 0.9rem;
  }

  fieldset {
    border: 0;
    padding: 0;
    margin: 0;
    display: grid;
    gap: 0.4rem;
  }

  .choices {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    padding-left: 1.6rem;
  }

  .check {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .ok {
    margin: 0;
    font-size: var(--text-note);
  }

  p {
    margin: 0;
  }
</style>
