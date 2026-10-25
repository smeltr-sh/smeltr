<!--
  Admin -> System -> AI agents: whether the leader's AI agent is on, and the model endpoint it thinks with. The
  agent's screens are offered only once this is on. The API key goes in and never comes back out: the leader says
  only whether one is stored, and forgets it when the provider or endpoint changes, since a key belongs to the
  service it was issued by.
-->
<script module lang="ts">
  import type { ModelProvider } from './api';

  /** Each provider, with the endpoint and model a new setting starts from. Another OpenAI-compatible service has
   * neither: only its administrator knows them. */
  export const PROVIDERS: {
    value: ModelProvider;
    label: string;
    endpoint: string;
    model: string;
  }[] = [
    {
      value: 'anthropic',
      label: 'Anthropic',
      endpoint: 'https://api.anthropic.com',
      model: 'claude-opus-5-5',
    },
    { value: 'openai', label: 'OpenAI', endpoint: 'https://api.openai.com', model: '' },
    { value: 'openai-compatible', label: 'Other, OpenAI-compatible', endpoint: '', model: '' },
  ];

  /** What turning AI agents on still lacks, by field label. The leader refuses the same; the form says it first
   * rather than offering a Save it knows is void. */
  export function missingForAgents(form: {
    endpoint: string;
    model: string;
    keyHeld: boolean;
  }): string[] {
    return [
      form.endpoint.trim() === '' ? 'endpoint URL' : '',
      form.model.trim() === '' ? 'model' : '',
      form.keyHeld ? '' : 'API key',
    ].filter((name) => name !== '');
  }

  /** The endpoint and model a form moves to with its provider: the new provider's own, except where the
   * administrator typed something other than the old provider's. */
  export function switched(
    from: ModelProvider,
    to: ModelProvider,
    form: { endpoint: string; model: string },
  ): { endpoint: string; model: string } {
    const before = PROVIDERS.find((p) => p.value === from)!;
    const after = PROVIDERS.find((p) => p.value === to)!;
    const typed = (value: string, old: string) => value.trim() !== '' && value.trim() !== old;
    return {
      endpoint: typed(form.endpoint, before.endpoint) ? form.endpoint : after.endpoint,
      model: typed(form.model, before.model) ? form.model : after.model,
    };
  }

  /** Whether the stored key is kept on saving: as the leader decides, while the provider and endpoint stay. */
  export function keepsStoredKey(
    stored: { apiKeySet: boolean; provider: ModelProvider; endpoint: string },
    provider: ModelProvider,
    endpoint: string,
  ): boolean {
    return stored.apiKeySet && provider === stored.provider && endpoint.trim() === stored.endpoint;
  }
</script>

<script lang="ts">
  import { reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import {
    getAgentSettings,
    putAgentSettings,
    refused,
    type AgentSettingsDto,
    type AgentSettingsResult,
  } from './api';
  import { recheck } from './session';
  import Help from './Help.svelte';
  import Panel from './Panel.svelte';
  import Field from './Field.svelte';
  import UnitInput from './UnitInput.svelte';
  import LeaderSecretField from './LeaderSecretField.svelte';
  import Toggle from './Toggle.svelte';

  let settings = $state<AgentSettingsDto | null>(null);
  let loadFailed = $state(false);
  let busy = $state(false);

  let enabled = $state(false);
  let provider = $state<ModelProvider>('anthropic');
  let endpoint = $state('');
  let model = $state('');
  let apiKey = $state('');
  let replacingKey = $state(false);
  let keepDays = $state(90);
  let contextTokens = $state(200000);

  function adopt(next: AgentSettingsDto) {
    settings = next;
    enabled = next.enabled;
    provider = next.provider;
    const defaults = PROVIDERS.find((p) => p.value === next.provider)!;
    endpoint = next.endpoint || defaults.endpoint;
    model = next.model || defaults.model;
    apiKey = '';
    replacingKey = false;
    keepDays = next.keepDays;
    contextTokens = next.contextTokens;
  }

  $effect(() => {
    getAgentSettings()
      .then((result) => {
        if (result.settings) adopt(result.settings);
        else loadFailed = true;
      })
      .catch((e) => {
        loadFailed = true;
        reported(e);
      });
  });

  /** A new provider starts from its own endpoint and model, unless the administrator already typed their own. */
  function choose(next: ModelProvider) {
    ({ endpoint, model } = switched(provider, next, { endpoint, model }));
    provider = next;
  }

  const keyKept = $derived(settings !== null && keepsStoredKey(settings, provider, endpoint));
  const missing = $derived(
    enabled ? missingForAgents({ endpoint, model, keyHeld: apiKey !== '' || keyKept }) : [],
  );
  const dirty = $derived(
    settings !== null &&
      (enabled !== settings.enabled ||
        provider !== settings.provider ||
        endpoint.trim() !== settings.endpoint ||
        model.trim() !== settings.model ||
        apiKey !== '' ||
        keepDays !== settings.keepDays ||
        contextTokens !== settings.contextTokens),
  );
  const savable = $derived(dirty && !busy && missing.length === 0);

  function answer(result: AgentSettingsResult) {
    if (!result.ok || !result.settings) return;
    adopt(result.settings);
    // Whether the agent's screens are offered rides on the session.
    void recheck();
  }

  async function save(event: SubmitEvent) {
    event.preventDefault();
    if (!savable) return;
    busy = true;
    try {
      const result = await putAgentSettings({
        enabled,
        provider,
        endpoint: endpoint.trim(),
        model: model.trim(),
        apiKey: apiKey === '' ? undefined : apiKey,
        keepDays,
        contextTokens,
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
</script>

{#if loadFailed}
  <!-- The error bar says why. -->
{:else if settings === null}
  <p>Reading what this leader is set to…</p>
{:else}
  <Panel onsubmit={save}>
    {#snippet title()}
      AI agents
      <Help label="AI agents">
        Each person’s agent works for them, within the agent permissions they set.
      </Help>
    {/snippet}

    <div class="setting">
      <div>
        <h3>Agents</h3>
      </div>
      <div class="fields">
        <Field label="Enabled" group>
          <span class="inline">
            <Toggle bind:checked={enabled} ariaLabel="AI agents enabled" />
          </span>
        </Field>
      </div>
    </div>

    <div class="setting">
      <div>
        <h3>Model</h3>
      </div>
      <div class="fields">
        <Field label="Provider">
          <select value={provider} onchange={(e) => choose(e.currentTarget.value as ModelProvider)}>
            {#each PROVIDERS as p (p.value)}<option value={p.value}>{p.label}</option>{/each}
          </select>
        </Field>
        <Field required label="Endpoint URL">
          <input
            bind:value={endpoint}
            placeholder={provider === 'openai-compatible' ? 'https://models.example.com' : ''}
            class="mono"
          />
        </Field>
        <Field required label="Model">
          {#snippet help()}
            The model's name as the provider gives it.
          {/snippet}
          <input bind:value={model} class="mono" />
        </Field>
        <Field required label="Context window">
          {#snippet help()}
            The most the model reads at once. Conversations begin being summarized at 75% full.
          {/snippet}
          <UnitInput bind:value={contextTokens} unit="tokens" min={4096} max={10000000} required />
        </Field>
        <Field required label="API key" group>
          {#snippet help()}
            Stored encrypted and never shown again.
          {/snippet}
          <LeaderSecretField
            bind:value={apiKey}
            bind:replacing={replacingKey}
            stored={keyKept}
            ariaLabel="API key"
          />
        </Field>
      </div>
    </div>

    <div class="setting">
      <div>
        <h3>Conversations</h3>
      </div>
      <div class="fields">
        <Field required label="Kept for">
          {#snippet help()}
            A conversation nobody has added to for this long is deleted.
          {/snippet}
          <UnitInput bind:value={keepDays} unit="days" min={1} max={3650} required />
        </Field>
      </div>
    </div>

    {#if missing.length > 0}
      <InlineError>Turning AI agents on needs: {missing.join(', ')}.</InlineError>
    {:else if enabled && dirty}
      <p class="note">Saving asks the endpoint for a short answer, to check the key works.</p>
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

  input,
  select {
    width: 100%;
    box-sizing: border-box;
  }

  .mono {
    font-family: var(--mono);
  }

  p {
    margin: 0;
  }

  .note {
    color: var(--dim);
    font-size: var(--text-body);
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
