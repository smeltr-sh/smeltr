<!--
  Admin -> Secrets -> Encryption key: the key the leader encrypts stored secrets
  and credential settings with. A key that is missing or does not match what
  it encrypted refuses every stored secret until it is restored or regenerated;
  after a regeneration, each value the new key cannot open is listed to enter
  again.
-->
<script module lang="ts">
  import type { EncryptionKeyDto } from './api';

  /** The critical line while the key is broken, or '' while it is not. `where` ends it: on the Secrets tab
   * it says where Regenerate is. */
  export function keyProblem(key: EncryptionKeyDto | null, where = ''): string {
    if (key === null || (key.state !== 'missing' && key.state !== 'mismatched')) return '';
    const n = key.unopenable.length;
    const secrets = `${n} stored secret${n === 1 ? '' : 's'}`;
    return key.state === 'missing'
      ? `The key file is missing, so this leader cannot open its ${secrets} and refuses new ones. Restore the file and restart the leader, or regenerate the key${where}.`
      : `The key file does not match the stored secrets, so this leader cannot open its ${secrets} and refuses new ones. Restore the original file and restart the leader, or regenerate the key${where}.`;
  }
</script>

<script lang="ts">
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import Help from './Help.svelte';
  import Panel from './Panel.svelte';
  import { ask } from './Confirm.svelte';
  import { errorText, getEncryptionKey, regenerateEncryptionKey } from './api';

  let key = $state<EncryptionKeyDto | null>(null);
  let loadFailed = $state(false);
  let busy = $state(false);
  let error = $state('');

  $effect(() => {
    getEncryptionKey()
      .then((k) => (key = k))
      .catch((e) => {
        loadFailed = true;
        reported(e);
      });
  });

  const broken = $derived(keyProblem(key) !== '');

  /** Where each value is entered again. A secret on the Secrets tab; a setting on its own page. */
  const SETTING_PLACES: Record<string, { label: string; href: string }> = {
    'git.remote.token': { label: 'Git remote', href: '#/admin/system/git' },
    'oidc.client_secret': { label: 'Single sign-on', href: '#/admin/accounts/oidc' },
    'agents.api_key': { label: 'AI agents', href: '#/admin/system/agents' },
    'acme.eab_hmac': { label: 'Certificates', href: '#/admin/system/certificates' },
    'acme.dns.cloudflare.token': { label: 'Certificates', href: '#/admin/system/certificates' },
  };
  const SECRETS_PLACE = { label: 'Secrets', href: '#/admin/secrets' };

  async function regenerate() {
    const ok = await ask({
      title: 'regenerate the encryption key',
      body: 'Every stored secret and sealed setting must be entered again. Restoring the original key file instead keeps them.',
      action: 'Regenerate key',
      danger: true,
    });
    if (!ok) return;
    busy = true;
    error = '';
    try {
      const result = await regenerateEncryptionKey();
      if (!result.ok) {
        error = errorText(result.errors);
        return;
      }
      key = await getEncryptionKey();
    } catch (e) {
      error = unreported(e);
    } finally {
      busy = false;
    }
  }
</script>

{#if loadFailed}
  <!-- The error bar says why. -->
{:else if key === null}
  <p>Reading the encryption key…</p>
{:else}
  <Panel title="Encryption key">
    {#if broken}
      <p class="critical" role="alert">{keyProblem(key)}</p>
    {/if}

    <div class="setting">
      <div>
        <h3>
          Key
          <Help label="Key"
            >Encrypts every stored secret, the Git remote token, the single sign-on client secret
            and the AI agents API key. Keep a backup: without it, a restored leader cannot open
            them.</Help
          >
        </h3>
      </div>
      <div class="fields">
        {#if key.state === 'missing'}
          <span class="bad">missing</span>
        {:else if key.state === 'mismatched'}
          <span><code>{key.keyId}</code> <span class="bad">does not match</span></span>
        {:else if key.keyId}
          <code>{key.keyId}</code>
        {/if}
        <code class="dim">{key.path}</code>
      </div>
    </div>

    {#if !broken && key.unopenable.length > 0}
      <div class="setting">
        <div>
          <h3>To enter again</h3>
        </div>
        <ul class="fields">
          {#each key.unopenable as item (`${item.kind}|${item.name}`)}
            {@const place =
              item.kind === 'secret' ? SECRETS_PLACE : (SETTING_PLACES[item.name] ?? SECRETS_PLACE)}
            <li><code>{item.label}</code> <a href={place.href}>{place.label}</a></li>
          {/each}
        </ul>
      </div>
    {/if}

    <InlineError message={error} />

    {#if broken}
      <div class="buttons">
        <button class="danger" type="button" disabled={busy} onclick={regenerate}>
          Regenerate key
        </button>
      </div>
    {/if}
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

  .setting:last-of-type {
    border-bottom: 0;
  }

  h3 {
    font-size: var(--text-body);
    font-weight: 600;
    margin: 0 0 0.25rem;
  }

  .fields {
    display: grid;
    gap: 0.5rem;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  code {
    font-family: var(--mono);
  }

  .dim {
    color: var(--dim);
  }

  .bad {
    color: var(--error);
    font-weight: 600;
  }

  .buttons {
    display: flex;
    justify-content: flex-end;
  }

  @media (max-width: 720px) {
    .setting {
      grid-template-columns: 1fr;
      gap: 0.8rem;
    }
  }
</style>
