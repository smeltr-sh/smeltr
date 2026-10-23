<!--
  Admin -> System -> Git remote: the repository whose `main` this leader
  follows, its credential, how often it is fetched, and what the last sync
  found. The token goes in and never comes back out: the leader says only
  whether one is stored.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import {
    getRemoteSettings,
    putRemoteSettings,
    syncRemote,
    type RemoteSettingsDto,
    type RemoteSettingsResult,
  } from './api';
  import Help from './Help.svelte';
  import Panel from './Panel.svelte';
  import Field from './Field.svelte';
  import UnitInput from './UnitInput.svelte';
  import LeaderSecretField from './LeaderSecretField.svelte';
  import UrlField from './UrlField.svelte';
  import { ago, shortCommit } from './time';

  let settings = $state<RemoteSettingsDto | null>(null);
  let loadFailed = $state(false);
  let busy = $state(false);

  let url = $state('');
  let user = $state('');
  let token = $state('');
  let replacingToken = $state(false);
  let timer = $state(true);
  let minutes = $state(1);
  let approvePushes = $state(false);

  function adopt(next: RemoteSettingsDto) {
    settings = next;
    url = next.url;
    user = next.user;
    token = '';
    replacingToken = false;
    timer = next.pollSeconds > 0;
    minutes = next.pollSeconds > 0 ? next.pollSeconds / 60 : 1;
    approvePushes = next.approvePushes;
  }

  function answer(result: RemoteSettingsResult) {
    if (!result.ok || !result.settings) return;
    adopt(result.settings);
  }

  $effect(() => {
    getRemoteSettings()
      .then((result) => {
        if (result.settings) adopt(result.settings);
        else loadFailed = true;
      })
      .catch((e) => {
        loadFailed = true;
        reported(e);
      });
  });

  const pollSeconds = $derived(timer ? Math.round(minutes * 60) : 0);
  const minMinutes = $derived(Math.ceil((settings?.minPollSeconds ?? 60) / 60));
  // A token is for the URL it was saved with: the leader forgets it when the URL changes.
  const tokenKept = $derived(settings !== null && settings.tokenSet && url.trim() === settings.url);
  const pollProblem = $derived(
    settings &&
      timer &&
      (pollSeconds < settings.minPollSeconds || pollSeconds > settings.maxPollSeconds)
      ? `Fetch every ${minMinutes} to ${settings.maxPollSeconds / 60} minutes.`
      : '',
  );
  const dirty = $derived(
    settings !== null &&
      (url.trim() !== settings.url ||
        user.trim() !== settings.user ||
        token !== '' ||
        pollSeconds !== settings.pollSeconds ||
        approvePushes !== settings.approvePushes),
  );

  async function save(event: SubmitEvent) {
    event.preventDefault();
    if (busy || pollProblem) return;
    busy = true;
    try {
      answer(
        await putRemoteSettings({
          url: url.trim(),
          user: user.trim(),
          token: token === '' ? undefined : token,
          pollSeconds,
          approvePushes,
        }),
      );
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function disconnect() {
    busy = true;
    try {
      answer(await putRemoteSettings({ url: '', user: '', pollSeconds: 0, approvePushes: false }));
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  async function syncNow() {
    busy = true;
    try {
      answer(await syncRemote());
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }
</script>

{#if loadFailed}
  <!-- The error bar says why. -->
{:else if settings === null}
  <p>Reading what this leader is set to…</p>
{:else}
  <Panel onsubmit={save}>
    {#snippet title()}
      Git remote
      <Help label="git remote">
        With a remote set, its <code>main</code> is what the fleet runs. The leader fetches it, checks
        that every group loads, and follows it. A commit that does not load leaves the fleet on the last
        one that did, and is reported under Status. Every save is pushed to the remote's branch of the
        same name, and an empty remote is given this leader's main.
      </Help>
    {/snippet}

    <UrlField
      label="URL"
      bind:value={url}
      sends={token !== '' || tokenKept}
      instead="Put the user and password in the user and token fields instead of the URL."
      mono
      placeholder="https://github.com/team/smeltr-config.git"
    >
      {#snippet help()}The repository's HTTP or HTTPS clone URL. Lookup files come from its Git LFS
        server.{/snippet}
    </UrlField>

    <Field label="User">
      {#snippet help()}
        The name the token is sent with. Use the name your host takes with a token.
      {/snippet}
      <input bind:value={user} placeholder="smeltr" />
    </Field>

    <Field label="Token" group>
      {#snippet help()}
        An access token that can read the repository and push branches to it, Git LFS included.
        Stored encrypted and never shown again.
      {/snippet}
      <LeaderSecretField
        bind:value={token}
        bind:replacing={replacingToken}
        stored={tokenKept}
        ariaLabel="token"
      />
    </Field>

    <Field label="Fetch" group error={pollProblem || null}>
      {#snippet help()}
        How often the leader fetches the remote. Off, it fetches only on Sync now.
      {/snippet}
      <UnitInput
        bind:value={minutes}
        unit="minutes"
        min={minMinutes}
        max={settings.maxPollSeconds / 60}
        disabled={!timer}
      >
        {#snippet before()}
          <label class="inline"><input type="checkbox" bind:checked={timer} /> every</label>
        {/snippet}
      </UnitInput>
    </Field>

    <Field label="Approve">
      {#snippet help()}
        Approve squashes a change and pushes it to the remote's main, then deploys it. A host that
        protects main refuses the push, and says why. Off, changes are merged on the host.
      {/snippet}
      <span class="inline"
        ><input type="checkbox" bind:checked={approvePushes} /> pushes to main</span
      >
    </Field>

    {#if settings.url}
      <div class="status" role="status">
        {#if settings.sync.failure}
          <InlineError message={settings.sync.failure.message} />
        {/if}
        {#if settings.sync.syncedAt !== null}
          <p>
            Last heard from the remote {ago(
              settings.sync.syncedAt / 1000,
            )}{#if settings.sync.remoteHead}, its main at <span class="mono"
                >{shortCommit(settings.sync.remoteHead)}</span
              >{:else}, which has no main yet{/if}.
          </p>
        {:else if !settings.sync.failure}
          <p class="dim">Not synced since this leader started.</p>
        {/if}
      </div>
    {/if}

    <div class="buttons">
      {#if settings.url}
        <button type="button" onclick={disconnect} disabled={busy}>Disconnect</button>
        <button type="button" onclick={syncNow} disabled={busy || dirty}>Sync now</button>
      {/if}
      <button class="primary" type="submit" disabled={busy || !dirty || pollProblem !== ''}
        >Save</button
      >
    </div>
  </Panel>
{/if}

<style>
  .inline {
    display: flex;
    align-items: center;
    gap: 0.3rem;
  }

  .mono {
    font-family: var(--mono);
  }

  .dim {
    color: var(--dim);
  }

  p {
    margin: 0;
  }

  .buttons {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
  }
</style>
