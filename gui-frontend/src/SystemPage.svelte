<!--
  What belongs to this leader rather than to any person using it.

  `nav.ts` holds the tab list. Logging is named in the design and has no
  leader-side settings behind it yet, so offering it would be offering a form
  that saves nothing.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import InlineSaveBar from './InlineSaveBar.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Help from './Help.svelte';
  import Version from './Version.svelte';
  import Panel from './Panel.svelte';
  import Field from './Field.svelte';
  import Advanced from './Advanced.svelte';
  import UnitInput from './UnitInput.svelte';
  import Toggle from './Toggle.svelte';
  import { ago } from './time';
  import CertificatesSettings from './CertificatesSettings.svelte';
  import GitRemoteSettings from './GitRemoteSettings.svelte';
  import LicenseSettings from './LicenseSettings.svelte';
  import AgentSettings from './AgentSettings.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import { SYSTEM_SECTIONS, type SystemSection } from './nav';
  import {
    getSessionSettings,
    putSessionSettings,
    getSystemInfo,
    getWarningSettings,
    putWarningSettings,
    getUpdateSettings,
    putUpdateSettings,
    refused,
    type SessionSettingsDto,
    type SystemInfoDto,
    type WarningSettingsDto,
    type UpdateSettingsDto,
  } from './api';

  let { section }: { section: SystemSection } = $props();

  const systemTabs = $derived(
    SYSTEM_SECTIONS.map((s) => ({
      label: s.label,
      hash: `#/admin/system/${s.section}`,
    })),
  );

  let info = $state<SystemInfoDto | null>(null);
  let infoFailed = $state(false);

  $effect(() => {
    if (section !== 'general') return;
    getSystemInfo()
      .then((next) => {
        infoFailed = false;
        info = next;
      })
      .catch((e) => {
        infoFailed = true;
        reported(e);
      });
  });

  let updates = $state<UpdateSettingsDto | null>(null);
  let checkShown = $state(true);
  let updatesMessage = $state('');
  let updatesBusy = $state(false);

  const checkDirty = $derived(updates !== null && checkShown !== updates.check);

  function adoptUpdates(next: UpdateSettingsDto) {
    updates = next;
    checkShown = next.check;
  }

  $effect(() => {
    if (section !== 'general') return;
    getUpdateSettings()
      .then((next) => {
        if (next.settings) adoptUpdates(next.settings);
      })
      .catch(reported);
  });

  async function saveUpdates(event: SubmitEvent) {
    event.preventDefault();
    if (updatesBusy || updates === null) return;
    updatesBusy = true;
    updatesMessage = '';
    try {
      const result = await putUpdateSettings(checkShown);
      if (!result.ok || !result.settings) return;
      adoptUpdates(result.settings);
      updatesMessage = 'saved';
    } catch (e) {
      reported(e);
    } finally {
      updatesBusy = false;
    }
  }

  let warnings = $state<WarningSettingsDto | null>(null);
  let bandShown = $state(99);
  let warningsMessage = $state('');
  let warningsBusy = $state(false);

  const bandDirty = $derived(warnings !== null && bandShown !== warnings.sourceRowsBandPercent);

  function adoptWarnings(next: WarningSettingsDto) {
    warnings = next;
    bandShown = next.sourceRowsBandPercent;
  }

  $effect(() => {
    if (section !== 'general') return;
    getWarningSettings()
      .then((next) => {
        if (next.settings) adoptWarnings(next.settings);
      })
      .catch(reported);
  });

  async function saveWarnings(event: SubmitEvent) {
    event.preventDefault();
    if (warningsBusy || warnings === null) return;
    warningsBusy = true;
    warningsMessage = '';
    try {
      const result = await putWarningSettings(bandShown);
      if (!result.ok || !result.settings) return;
      adoptWarnings(result.settings);
      warningsMessage = 'saved';
    } catch (e) {
      reported(e);
    } finally {
      warningsBusy = false;
    }
  }

  let settings = $state<SessionSettingsDto | null>(null);
  let loadFailed = $state(false);

  // The leader stores seconds; a person thinks in minutes or hours. Each field
  // is shown in the largest unit that leaves it a whole number, so a deadline
  // set to 100 minutes reads as "100 minutes" rather than "1.6666666666666667
  // hours" -- exact either way, but only one of them is readable.
  const UNITS = [
    { label: 'hours', seconds: 3600 },
    { label: 'minutes', seconds: 60 },
    { label: 'seconds', seconds: 1 },
  ];

  function unitFor(seconds: number) {
    return UNITS.find((u) => seconds % u.seconds === 0) ?? UNITS[UNITS.length - 1];
  }

  let idleUnit = $state(UNITS[1]);
  let absoluteUnit = $state(UNITS[0]);
  let idleShown = $state(0);
  let absoluteShown = $state(0);
  let message = $state('');
  let busy = $state(false);

  const idleSeconds = $derived(Math.round(idleShown * idleUnit.seconds));
  const absoluteSeconds = $derived(Math.round(absoluteShown * absoluteUnit.seconds));

  // What the form may offer, in its own units, from what the leader says it
  // will accept. Restating the bounds here is how a form comes to refuse a
  // value the leader allows, or offer one it does not.
  const idleMin = $derived((settings?.minSeconds ?? 60) / idleUnit.seconds);
  const idleMax = $derived((settings?.maxSeconds ?? 0) / idleUnit.seconds);
  const absoluteMin = $derived((settings?.minSeconds ?? 60) / absoluteUnit.seconds);
  const absoluteMax = $derived((settings?.maxSeconds ?? 0) / absoluteUnit.seconds);

  const dirty = $derived(
    settings !== null &&
      (idleSeconds !== settings.idleSeconds || absoluteSeconds !== settings.absoluteSeconds),
  );

  /// Said before saving rather than after being refused. The leader enforces
  /// it too -- this is so the form does not offer a pair it knows is void.
  const impossible = $derived(settings !== null && idleSeconds > absoluteSeconds);

  function adopt(next: SessionSettingsDto) {
    settings = next;
    // Exact, never rounded: a deadline that is not a whole minute would come
    // back as a different number, so an untouched form would read as changed,
    // Discard would repeat the same rounding and never clear it, and saving
    // one field would quietly move the other.
    idleUnit = unitFor(next.idleSeconds);
    absoluteUnit = unitFor(next.absoluteSeconds);
    idleShown = next.idleSeconds / idleUnit.seconds;
    absoluteShown = next.absoluteSeconds / absoluteUnit.seconds;
  }

  $effect(() => {
    if (section !== 'sessions') return;
    getSessionSettings()
      .then((next) => {
        loadFailed = false;
        adopt(next);
      })
      .catch((e) => {
        loadFailed = true;
        reported(e);
      });
  });

  async function save(event: SubmitEvent) {
    event.preventDefault();
    if (busy || settings === null) return;
    busy = true;
    message = '';
    try {
      const result = await putSessionSettings(idleSeconds, absoluteSeconds);
      if (refused(result)) return;
      adopt(result);
      message = 'saved -- these apply to every session on the next request it makes';
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  function discard() {
    if (settings) adopt(settings);
    message = '';
  }
</script>

<SectionShell>
  <PageHeader title="System" />

  <SecondaryTabs
    tabs={systemTabs}
    active={`#/admin/system/${section}`}
    ariaLabel="System settings"
  />

  <div class="sections">
    {#if section === 'general'}
      <Panel title="General" onsubmit={saveUpdates}>
        {#if infoFailed}
          <!-- The error bar says why. -->
        {:else if info === null}
          <p>Reading what this leader is set to…</p>
        {:else}
          <div class="setting">
            <div>
              <h3>Version</h3>
            </div>
            <div class="fields">
              <Version {info} build newerHref={info.newerRelease?.url} />
            </div>
          </div>
          {#if info.release && updates}
            <div class="setting">
              <div>
                <h3>Updates</h3>
              </div>
              <div class="fields">
                <Field label="Check for new releases" group>
                  <Toggle bind:checked={checkShown} ariaLabel="Check for new releases" />
                </Field>
                {#if updates.check && info.releaseCheck}
                  {@const check = info.releaseCheck}
                  <p class="checked">
                    {#if check.failure}
                      Last check failed {ago(check.checkedAt / 1000)}: {check.failure}
                    {:else}
                      Checked {ago(check.checkedAt / 1000)}
                    {/if}
                  </p>
                {/if}
              </div>
            </div>
          {/if}
        {/if}
        {#if updatesMessage}
          <p class="message" role="status">{updatesMessage}</p>
        {/if}
        <InlineSaveBar
          dirty={checkDirty}
          label="Save updates"
          disabled={updatesBusy}
          ondiscard={() => updates && adoptUpdates(updates)}
        />
      </Panel>
      {#if warnings}
        <Panel title="Warnings" onsubmit={saveWarnings}>
          <Advanced
            label="Expected throughput"
            modified={warnings.sourceRowsBandPercent !== warnings.defaultPercent}
          >
            <Field required label="Range">
              {#snippet help()}
                The expected range of a source's throughput based on this time of day.
              {/snippet}
              <UnitInput
                bind:value={bandShown}
                unit="%"
                min={warnings.minPercent}
                max={warnings.maxPercent}
                step="any"
                required
              />
            </Field>
          </Advanced>
          {#if warningsMessage}
            <p class="message" role="status">{warningsMessage}</p>
          {/if}
          <InlineSaveBar
            dirty={bandDirty}
            label="Save warnings"
            disabled={warningsBusy}
            ondiscard={() => warnings && adoptWarnings(warnings)}
          />
        </Panel>
      {/if}
    {:else if section === 'certificates'}
      <CertificatesSettings />
    {:else if section === 'git'}
      <GitRemoteSettings />
    {:else if section === 'agents'}
      <AgentSettings />
    {:else if section === 'license'}
      <LicenseSettings />
    {:else if loadFailed}
      <!-- The error bar says why. -->
    {:else if settings === null}
      <p>Reading what this leader is set to…</p>
    {:else}
      <Panel title="Sessions" onsubmit={save}>
        <div class="setting">
          <div>
            <h3>
              Session lifetime
              <Help label="Session lifetime"
                >Working in Smeltr restarts the idle timeout; a page merely left open does not. The
                maximum session age counts from sign-in and nothing restarts it.</Help
              >
            </h3>
          </div>
          <div class="fields">
            <Field required label="Idle timeout">
              <UnitInput
                bind:value={idleShown}
                unit={idleUnit.label}
                min={idleMin}
                max={idleMax}
                step="any"
                required
              />
            </Field>
            <Field required label="Maximum session age">
              <UnitInput
                bind:value={absoluteShown}
                unit={absoluteUnit.label}
                min={absoluteMin}
                max={absoluteMax}
                step="any"
                required
              />
            </Field>
          </div>
        </div>

        {#if impossible}
          <InlineError>
            An idle timeout longer than the maximum session age could never be reached.
          </InlineError>
        {:else if message}
          <p class="message" role="status">{message}</p>
        {/if}

        <InlineSaveBar
          {dirty}
          label="Save sessions"
          disabled={busy || impossible}
          ondiscard={discard}
        >
          <p class="critical">
            Shortening either deadline signs out every session already past it.
          </p>
        </InlineSaveBar>
      </Panel>
    {/if}
  </div>
</SectionShell>

<style>
  .sections {
    max-width: 52rem;
  }

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
    gap: 0.9rem;
  }

  .checked {
    margin: 0;
    color: var(--dim);
    font-size: var(--text-note);
  }

  p {
    margin: 0;
  }

  @media (max-width: 720px) {
    .setting {
      grid-template-columns: 1fr;
      gap: 0.8rem;
    }
  }
</style>
