<!--
  Admin -> Accounts -> Single sign-on: the OpenID Connect provider people sign
  in through. The client secret goes in and never comes back out: the leader
  says only whether one is stored, and forgets it when the issuer or client
  id changes, since a secret belongs to the client it was issued to.
-->
<script module lang="ts">
  /** What turning sign-on on still lacks, by field label. The leader refuses
   * the same; the form says it first rather than offering a Save it knows is void. */
  export function missingForSignIn(form: {
    issuer: string;
    clientId: string;
    secretHeld: boolean;
  }): string[] {
    return [
      form.issuer.trim() === '' ? 'issuer URL' : '',
      form.clientId.trim() === '' ? 'client ID' : '',
      form.secretHeld ? '' : 'client secret',
    ].filter((name) => name !== '');
  }

  /** Why the leader would refuse `text` as an issuer, or `''` when it would
   * take it. The leader's own rule (`oidc::check_issuer`): an https URL with a
   * host and nothing but a path, or plain http to this machine, which no
   * network carries. */
  export function issuerProblem(text: string): string {
    const issuer = text.trim();
    if (issuer === '') return '';
    if (!/^[\x21-\x7e]+$/.test(issuer) || issuer.includes('\\')) {
      return 'An issuer URL has no spaces or backslashes.';
    }
    let url: URL;
    try {
      url = new URL(issuer);
    } catch {
      return 'Not a URL.';
    }
    const authority = issuer.split('://')[1]?.split(/[/?#]/)[0] ?? '';
    if (authority === '' || url.hostname === '') return "Name the provider's host.";
    if (url.username !== '' || url.password !== '') return 'An issuer URL names no user.';
    if (issuer.includes('?') || issuer.includes('#'))
      return 'An issuer URL has no query or fragment.';
    const loopback =
      url.hostname.toLowerCase() === 'localhost' ||
      /^127\.\d+\.\d+\.\d+$/.test(url.hostname) ||
      url.hostname === '[::1]';
    if (url.protocol === 'https:' || (url.protocol === 'http:' && loopback)) return '';
    return 'Use an https:// URL.';
  }

  /** Whether `text` is still being typed toward a scheme, when saying it is
   * wrong would only interrupt. */
  export function partialScheme(text: string): boolean {
    const typed = text.trim().toLowerCase();
    return 'https://'.startsWith(typed) || 'http://'.startsWith(typed);
  }

  /** Whether the stored client secret survives a save: the leader forgets it
   * when the issuer or client id it was issued for changes. */
  export function keepsStoredSecret(
    stored: { clientSecretSet: boolean; issuer: string; clientId: string },
    issuer: string,
    clientId: string,
  ): boolean {
    return (
      stored.clientSecretSet &&
      issuer.trim() === stored.issuer &&
      clientId.trim() === stored.clientId
    );
  }
</script>

<script lang="ts">
  import { reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import {
    getOidcSettings,
    putOidcSettings,
    refused,
    type OidcSettingsDto,
    type OidcSettingsResult,
  } from './api';
  import Help from './Help.svelte';
  import Panel from './Panel.svelte';
  import Field from './Field.svelte';
  import LeaderSecretField from './LeaderSecretField.svelte';
  import Toggle from './Toggle.svelte';
  import Advanced from './Advanced.svelte';

  const DEFAULT_GROUPS_CLAIM = 'groups';

  let settings = $state<OidcSettingsDto | null>(null);
  let loadFailed = $state(false);
  let busy = $state(false);
  let copied = $state(false);

  let enabled = $state(false);
  let issuer = $state('');
  let clientId = $state('');
  let clientSecret = $state('');
  let replacingSecret = $state(false);
  let groupsClaim = $state(DEFAULT_GROUPS_CLAIM);

  function adopt(next: OidcSettingsDto) {
    settings = next;
    enabled = next.enabled;
    issuer = next.issuer;
    clientId = next.clientId;
    clientSecret = '';
    replacingSecret = false;
    copied = false;
    groupsClaim = next.groupsClaim;
  }

  $effect(() => {
    getOidcSettings()
      .then((result) => {
        if (result.settings) adopt(result.settings);
        else loadFailed = true;
      })
      .catch((e) => {
        loadFailed = true;
        reported(e);
      });
  });

  const issuerError = $derived(issuerProblem(issuer));
  const secretKept = $derived(settings !== null && keepsStoredSecret(settings, issuer, clientId));
  const missing = $derived(
    enabled
      ? missingForSignIn({
          issuer,
          clientId,
          secretHeld: clientSecret !== '' || secretKept,
        })
      : [],
  );
  const claimProblem = $derived(groupsClaim.trim() === '' ? 'Name the claim.' : '');
  const dirty = $derived(
    settings !== null &&
      (enabled !== settings.enabled ||
        issuer.trim() !== settings.issuer ||
        clientId.trim() !== settings.clientId ||
        clientSecret !== '' ||
        groupsClaim.trim() !== settings.groupsClaim),
  );
  const savable = $derived(
    dirty && !busy && issuerError === '' && claimProblem === '' && missing.length === 0,
  );

  function answer(result: OidcSettingsResult) {
    if (!result.ok || !result.settings) return;
    adopt(result.settings);
  }

  async function save(event: SubmitEvent) {
    event.preventDefault();
    if (!savable) return;
    busy = true;
    try {
      const result = await putOidcSettings({
        enabled,
        issuer: issuer.trim(),
        clientId: clientId.trim(),
        clientSecret: clientSecret === '' ? undefined : clientSecret,
        groupsClaim: groupsClaim.trim(),
      });
      if (!refused(result)) answer(result);
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  function discard() {
    if (settings) adopt(settings);
  }

  async function copyRedirect() {
    if (!settings) return;
    try {
      await navigator.clipboard.writeText(settings.redirectUri);
      copied = true;
      setTimeout(() => (copied = false), 2000);
    } catch {
      // No clipboard access: the value is still on screen to select by hand.
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
      Single sign-on
      <Help label="single sign-on">
        People sign in through your organization's identity provider over OpenID Connect. Local
        accounts keep working whatever is set here.
      </Help>
    {/snippet}

    <div class="setting">
      <div>
        <h3>Sign-in</h3>
      </div>
      <div class="fields">
        <Field label="Enabled" group>
          <span class="inline">
            <Toggle bind:checked={enabled} ariaLabel="single sign-on enabled" />
          </span>
        </Field>
      </div>
    </div>

    <div class="setting">
      <div>
        <h3>Provider</h3>
      </div>
      <div class="fields">
        <p class="critical">Register this redirect URI with your provider.</p>
        <Field label="Redirect URI" group>
          <div class="copy-row">
            <code class="code-value">{settings.redirectUri}</code>
            <button type="button" onclick={copyRedirect} aria-live="polite"
              >{copied ? 'Copied' : 'Copy'}</button
            >
          </div>
        </Field>
        <Field label="Issuer URL" error={(!partialScheme(issuer) && issuerError) || null}>
          {#snippet help()}
            The provider's issuer URL. The leader reads <code
              >/.well-known/openid-configuration</code
            >
            under it.
          {/snippet}
          <input bind:value={issuer} placeholder="https://example.okta.com" class="mono" />
        </Field>
        <Field label="Client ID">
          <input bind:value={clientId} class="mono" />
        </Field>
        <Field label="Client secret" group>
          {#snippet help()}
            Stored encrypted and never shown again.
          {/snippet}
          <LeaderSecretField
            bind:value={clientSecret}
            bind:replacing={replacingSecret}
            stored={secretKept}
            ariaLabel="client secret"
          />
        </Field>
        <Advanced modified={groupsClaim.trim() !== DEFAULT_GROUPS_CLAIM}>
          <Field label="Groups claim" error={claimProblem || null}>
            <input bind:value={groupsClaim} class="mono" />
          </Field>
        </Advanced>
      </div>
    </div>

    {#if missing.length > 0}
      <InlineError>Turning single sign-on on needs: {missing.join(', ')}.</InlineError>
    {/if}

    <div class="buttons">
      <button type="button" onclick={discard} disabled={!dirty || busy}>Discard</button>
      <button class="primary" type="submit" disabled={!savable}>Save</button>
    </div>
  </Panel>
{/if}

<style>
  .setting {
    display: grid;
    grid-template-columns: minmax(10rem, 1fr) minmax(13rem, 1.3fr);
    gap: 1.6rem;
    padding-bottom: 0.9rem;
    border-bottom: 1px solid var(--border);
  }

  h3 {
    font-size: var(--text-body);
    font-weight: 600;
    margin: 0 0 0.25rem;
  }

  .fields {
    display: grid;
    gap: 0.9rem;
  }

  .inline {
    display: flex;
    align-items: center;
  }

  /* AdminPage's `.code-value`, sized to share its row with Copy. */
  .copy-row {
    display: flex;
    gap: 0.5rem;
    align-items: center;
  }

  .code-value {
    flex: 1;
    min-width: 0;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.35rem 0.6rem;
    font-family: var(--mono);
    font-size: var(--text-body);
    user-select: all;
    overflow-wrap: anywhere;
  }

  input {
    width: 100%;
    box-sizing: border-box;
  }

  .mono,
  code {
    font-family: var(--mono);
  }

  p {
    margin: 0;
  }

  .buttons {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
  }

  @media (max-width: 720px) {
    .setting {
      grid-template-columns: 1fr;
      gap: 0.8rem;
    }
  }
</style>
