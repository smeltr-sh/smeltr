<!--
  What this leader serves TLS with, and what a worker must be given to verify
  it.

  Two certificates, and they are not the same thing to an operator. The
  authority is the one copied to every worker, so replacing it is a visit to
  all of them; the certificate under it is the leader's alone and can be
  replaced without anybody hearing. The page says which is which rather than
  listing two rows of hex and leaving it to be worked out.
-->
<script lang="ts">
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import {
    abandonRotation,
    AUTHORITY_URL,
    beginRotation,
    cutOverRotation,
    getCertificates,
    putCertificate,
    reissueCertificate,
    refused,
    withdrawCertificate,
    ROTATION_WAIT_HOURS,
    AUTHORITY_VALIDITY_DAYS,
    ACME_DIRECTORIES,
    getAcme,
    putAcme,
    orderAcme,
    type AcmeChallenge,
    type AcmeDto,
    type AcmeSettings,
    type AccountRefusal,
    type CertificateSource,
    type CertificatesDto,
    type DescribedCertificate,
    type RotationDto,
  } from './api';
  import { ask } from './Confirm.svelte';
  import Help from './Help.svelte';
  import Panel from './Panel.svelte';
  import Field from './Field.svelte';
  import Collapsible from './Collapsible.svelte';
  import Segmented from './Segmented.svelte';
  import Advanced from './Advanced.svelte';
  import PasswordField from './PasswordField.svelte';
  import TokenListInput from './TokenListInput.svelte';
  import { count, expiry } from './time';
  import { NOT_IN_DEMO } from './access';

  let certificates = $state<CertificatesDto | null>(null);
  let loadFailed = $state(false);
  let message = $state('');
  let formError = $state('');
  let busy = $state(false);

  const SOURCES: { value: CertificateSource; label: string }[] = [
    { value: 'own', label: 'Leader issued' },
    { value: 'upload', label: 'Uploaded' },
    { value: 'acme', label: 'ACME' },
  ];
  /** Where what is served comes from. */
  const inForce = $derived<CertificateSource>(certificates?.source ?? 'own');
  /** The source on screen. Looking at another changes nothing until its own button is pressed, and the page
   * follows the source in force whenever that moves. */
  let source = $state<CertificateSource>('own');
  $effect(() => {
    source = inForce;
  });
  let certificate = $state('');
  let privateKey = $state('');
  let authority = $state('');
  /** When a switch this upload opens cuts over at the latest, as the picker holds it: local time. */
  let cutOverBy = $state('');
  const CUTOVER_MARGIN_MS = 10 * 60 * 1000;
  /** The latest a switch can wait: short of the served certificate's expiry by the leader's margin. */
  const latestCutOver = $derived(
    certificates?.served ? certificates.served.notAfterMillis - CUTOVER_MARGIN_MS : null,
  );
  $effect(() => {
    if (cutOverBy === '' && latestCutOver !== null) cutOverBy = local(latestCutOver);
  });
  /** `millis` as a datetime-local input holds it. */
  function local(millis: number): string {
    const d = new Date(millis - new Date(millis).getTimezoneOffset() * 60_000);
    return d.toISOString().slice(0, 16);
  }
  /** Whether the pasted authority is other than the one in force, which is when serving the upload waits for
   * the fleet and the deadline means anything. False until there is something to tell by. */
  let newAuthority = $state(false);
  $effect(() => {
    const pasted = authority;
    const current = certificates?.authority?.fingerprint;
    if (!current) return;
    fingerprints(pasted).then((each) => {
      if (pasted === authority) newAuthority = each.length > 0 && !each.includes(current);
    });
  });
  /** SHA-256 over each PEM certificate's DER, as the leader names a certificate. Unreadable blocks are skipped. */
  async function fingerprints(pem: string): Promise<string[]> {
    const blocks = pem.match(/-----BEGIN CERTIFICATE-----[^-]+-----END CERTIFICATE-----/g) ?? [];
    const each: string[] = [];
    for (const block of blocks) {
      try {
        const body = block.replace(/-----[^-]+-----/g, '').replace(/\s+/g, '');
        const der = Uint8Array.from(atob(body), (c) => c.charCodeAt(0));
        const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', der));
        each.push(Array.from(digest, (b) => b.toString(16).padStart(2, '0')).join(''));
      } catch {
        // Not base64: the leader says what is wrong with it on upload.
      }
    }
    return each;
  }
  /** Ticks while a rotation is open, for the countdown. */
  let now = $state(Date.now());

  /** The ACME settings as the leader holds them, and the form over them. */
  let acme = $state<AcmeDto | null>(null);
  const OTHER = 'other';
  /** A directory offered by name, or `OTHER` for one typed as a URL. */
  let preset = $state<string>(ACME_DIRECTORIES[0].value);
  let directoryUrl = $state('');
  let nameList = $state<string[]>([]);
  let contact = $state('');
  let agreed = $state(false);
  let challenge = $state<AcmeChallenge>('tls-alpn-01');
  let acmeAuthority = $state('');
  let eabKeyId = $state('');
  let eabHmac = $state('');
  const directory = $derived(preset === OTHER ? directoryUrl.trim() : preset);
  const terms = $derived(ACME_DIRECTORIES.find((d) => d.value === preset)?.terms ?? null);
  /** What an order needs, as the leader will ask for it. */
  const orderable = $derived(directory !== '' && nameList.length > 0);
  const advancedChanged = $derived(
    challenge !== 'tls-alpn-01' ||
      acmeAuthority.trim() !== '' ||
      eabKeyId.trim() !== '' ||
      eabHmac !== '' ||
      (acme?.eabHmacSet ?? false),
  );
  /** The names the leader is reached by that a certificate authority can issue for: every name its own
   * certificate carries but loopback, which no authority reaches. */
  const LOOPBACK = ['localhost', '127.0.0.1', '::1'];

  /** Fills the form from `stored`, defaulting the names to the leader's own public ones. */
  function showAcme(stored: AcmeDto) {
    acme = stored;
    preset =
      stored.directory === '' || ACME_DIRECTORIES.some((d) => d.value === stored.directory)
        ? stored.directory || ACME_DIRECTORIES[0].value
        : OTHER;
    directoryUrl = preset === OTHER ? stored.directory : '';
    nameList =
      stored.names.length > 0
        ? stored.names
        : (certificates?.served?.names ?? []).filter((n) => !LOOPBACK.includes(n.toLowerCase()));
    contact = stored.contact;
    agreed = stored.enabled;
    challenge = stored.challenge;
    acmeAuthority = stored.authority;
    eabKeyId = stored.eabKeyId;
    eabHmac = '';
  }

  function acmeSettings(): AcmeSettings {
    const keyId = eabKeyId.trim();
    return {
      enabled: true,
      directory,
      contact: contact.trim(),
      names: nameList.map((n) => n.toLowerCase()),
      challenge,
      authority: acmeAuthority.trim(),
      eabKeyId: keyId,
      // Typed, it replaces the stored key; left empty, the stored key is kept, unless its id went, which is
      // what removes both.
      eabHmac: eabHmac !== '' ? eabHmac : keyId === '' && acme?.eabHmacSet ? '' : null,
    };
  }

  let waitHours = $state(24);
  // Rotation is the one intentional authority mint. Default to the platform
  // policy, while making any shorter-lived authority a deliberate choice.
  let authorityValidityDays = $state(10 * 365);
  let rotationBusy = $state(false);
  let rotationError = $state('');
  let rotationMessage = $state('');

  $effect(() => {
    Promise.all([getCertificates(), getAcme()])
      .then(([c, a]) => {
        certificates = c;
        showAcme(a);
      })
      .catch((e) => {
        loadFailed = true;
        reported(e);
      });
  });

  // A rotation ends on its own once every worker holds the incoming authority or the
  // deadline passes, with nothing on screen to say so. Poll for that while one is open,
  // and stop the moment it isn't.
  const ROTATION_POLL_MS = 5000;
  let rotationOpen = $derived(certificates?.rotation != null);
  /** The rotation open to change source, if that is what is open. Rotating the leader's own authority to
   * another of its own is shown under Leader issued; a switch concerns both sources and stands in for the Leader
   * issued form, which cannot act while it is open. The upload form stays, since a certificate under the root in
   * force is a renewal and is served at once, whatever is open. */
  const switching = $derived(
    certificates?.rotation && certificates.rotation.installs !== 'nextKey'
      ? certificates.rotation
      : null,
  );
  const switchingTo = $derived(
    SOURCES.find((s) => s.value === switching?.source)?.label ?? 'Leader issued',
  );
  /** A renewal under a root the fleet does not hold yet: the same source, moved to a new root. */
  const newRoot = $derived(inForce === 'acme' && switching?.source === 'acme');
  // Read inside the card's title snippet, where the page's null check does not reach.
  const supplied = $derived(certificates?.supplied ?? false);

  $effect(() => {
    if (!rotationOpen) return;
    const id = setInterval(() => {
      getCertificates()
        .then((c) => (certificates = c))
        .catch(() => {
          // Keep showing what the page last knew; the next tick tries again.
        });
    }, ROTATION_POLL_MS);
    return () => clearInterval(id);
  });
  $effect(() => {
    if (!rotationOpen) return;
    const id = setInterval(() => (now = Date.now()), 1000);
    return () => clearInterval(id);
  });

  /** Time left until `millis`, to the second. */
  function countdown(millis: number): string {
    const left = Math.max(0, Math.floor((millis - now) / 1000));
    const days = Math.floor(left / 86_400);
    const pad = (n: number) => String(n).padStart(2, '0');
    const clock = `${pad(Math.floor(left / 3600) % 24)}:${pad(Math.floor(left / 60) % 60)}:${pad(left % 60)}`;
    return days > 0 ? `${count(days, 'day')} ${clock}` : clock;
  }

  /** A date with its time of day, for a moment somebody is waiting on. */
  function at(millis: number): string {
    return new Date(millis).toLocaleString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    });
  }

  const DAY = 24 * 60 * 60 * 1000;

  function expiringSoon(cert: DescribedCertificate): boolean {
    return cert.notAfterMillis - Date.now() < 30 * DAY;
  }

  function on(millis: number): string {
    return new Date(millis).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  }

  /** Long enough to compare by eye without being a wall of hex. Anybody
   * matching it against a worker's copy is checking the ends. */
  function short(fingerprint: string): string {
    return `${fingerprint.slice(0, 16)}…${fingerprint.slice(-8)}`;
  }

  /** What to say once a source change landed: served at once under the root in force, or published to the
   * fleet first under another. A renewal during a switch leaves the switch as it was, so only a switch this
   * change opened is the publishing one. */
  function switched(served: string): (c: CertificatesDto) => string {
    const open = certificates?.rotation?.startedMillis;
    return (c) =>
      c.rotation && c.rotation.installs !== 'nextKey' && c.rotation.startedMillis !== open
        ? 'the incoming authority is being published to the fleet'
        : served;
  }

  /** Runs `call` under the form's busy flag and says how it went: `formError` on a refusal or a throw, else
   * `ok` takes what the call answered and `message` is what it returns. Whether it went. */
  async function attempt<T extends object>(
    call: () => Promise<T | AccountRefusal>,
    ok: (result: T) => string,
  ): Promise<boolean> {
    if (busy) return false;
    busy = true;
    formError = '';
    message = '';
    try {
      const result = await call();
      if (refused(result)) {
        formError = result.error;
        return false;
      }
      message = ok(result);
      return true;
    } catch (e) {
      formError = unreported(e);
      return false;
    } finally {
      busy = false;
    }
  }

  /** A change to what is served: takes the certificates it answers with, and the upload form is spent. */
  function act(
    call: () => Promise<CertificatesDto | AccountRefusal>,
    done: string | ((c: CertificatesDto) => string),
  ) {
    return attempt(call, (c) => {
      certificates = c;
      certificate = '';
      privateKey = '';
      authority = '';
      return typeof done === 'string' ? done : done(c);
    });
  }

  /** Saves the ACME form. Whether it was saved. */
  function saveAcme() {
    return attempt(
      () => putAcme(acmeSettings()),
      (a) => {
        showAcme(a);
        return 'settings saved';
      },
    );
  }

  const ISSUED = 'this leader is serving the certificate the authority issued';

  /** Orders a certificate with what the form says, and serves it. */
  async function order() {
    if (!(await saveAcme())) return;
    message = '';
    await act(() => orderAcme(), switched(ISSUED));
    // What the order left, however it went: the failure it recorded, or when the next renewal is due.
    getAcme().then(showAcme).catch(reported);
  }

  /** Saves the ACME form while ACME is in force. Other names or another directory no longer fit what is served,
   * so they are ordered at once, where the page shows how it went. */
  async function save() {
    const names = acmeSettings().names.join(',');
    if (acme === null || directory !== acme.directory || names !== acme.names.join(',')) {
      await order();
      return;
    }
    if (!(await saveAcme())) return;
    getCertificates()
      .then((c) => (certificates = c))
      .catch(() => {});
  }

  async function rotationAct(call: () => Promise<CertificatesDto | AccountRefusal>, done: string) {
    if (rotationBusy) return;
    rotationBusy = true;
    rotationError = '';
    rotationMessage = '';
    try {
      const result = await call();
      if (refused(result)) {
        rotationError = result.error;
        return;
      }
      certificates = result;
      rotationMessage = done;
    } catch (e) {
      rotationError = unreported(e);
    } finally {
      rotationBusy = false;
    }
  }
</script>

<!-- A certificate as the page names one: who, by what, until when. -->
{#snippet who(cert: DescribedCertificate, tail: string)}
  <div class="who">
    <h3>{cert.subject || cert.names[0] || 'no name'}</h3>
    <p class="mono">{short(cert.fingerprint)}</p>
    <p class="dim">
      <span class:soon={expiringSoon(cert)}>{expiry(cert.notAfterMillis)}</span>
      · {tail}
    </p>
  </div>
{/snippet}

{#snippet progress(rotation: RotationDto)}
  {@const holding = rotation.workers.filter((w) => w.holdsIncoming).length}
  <div class="setting">
    <p>
      Cuts over as soon as every worker holds the incoming authority, or at {at(
        rotation.deadlineMillis,
      )}, whichever comes first.
    </p>
    <p class="countdown">
      Cutover in <strong>{countdown(rotation.deadlineMillis)}</strong>
      <span class="dim">· started {at(rotation.startedMillis)}</span>
    </p>
  </div>

  <div class="setting">
    <h3>Workers <span class="dim">· {holding} of {rotation.workers.length} hold it</span></h3>
    <ul class="workers">
      {#each rotation.workers as worker (worker.id)}
        <li>
          <span class="mono">{worker.id}</span>
          {#if worker.holdsIncoming}
            <span class="holds">✓ holds it</span>
          {:else}
            <span class="dim">waiting</span>
          {/if}
        </li>
      {/each}
    </ul>
    {#if rotation.notFollowing.length > 0}
      <p class="dim">
        Not waited for: {rotation.notFollowing.join(', ')}. These workers verify this leader by an
        authority their operator gave them and take none from here, so the operator gives them the
        incoming one.
      </p>
    {/if}
  </div>
{/snippet}

{#snippet status(error: string, done: string)}
  {#if error}
    <!-- The error bar says why. -->
  {:else if done}
    <p class="message" role="status">{done}</p>
  {/if}
{/snippet}

<div class="stack">
  {#if loadFailed}
    <!-- The error bar says why. -->
  {:else if certificates === null}
    <p>Reading what this leader serves…</p>
  {:else if !certificates.servingTls}
    <div class="panel">
      <div class="head">
        <h2>Terminated in front of this leader</h2>
        <p>
          This leader serves plain HTTP and holds no certificate of its own. Whatever terminates TLS
          in front of it &mdash; a proxy, a load balancer &mdash; is where its certificate lives,
          and where it is replaced.
        </p>
      </div>
      <div class="setting">
        <p>
          Workers enrolling with this leader verify it with the platform's own roots, so they need
          nothing from here.
        </p>
      </div>
    </div>
  {:else if certificates.authority && certificates.served}
    {#if certificates.restartRequired}
      <InlineError>
        A certificate reached this leader's disk and its socket refused it, so what is shown below
        is not what clients are being served. Restart the leader to serve it.
      </InlineError>
    {/if}
    <Panel>
      {#snippet title()}
        Current authority
        <Help label="Current authority"
          >What every worker trusts to verify this leader, as the file ca.pem.</Help
        >
      {/snippet}

      <div class="setting">
        {@render who(certificates.authority, on(certificates.authority.notAfterMillis))}
        <!-- A link is not a control a disabled fieldset reaches, and the demo has no file to give. -->
        {#if NOT_IN_DEMO}<button type="button" disabled title={NOT_IN_DEMO}>Download ca.pem</button
          >{:else}<a class="btn" href={AUTHORITY_URL} download>Download ca.pem</a>{/if}
      </div>
    </Panel>

    <Panel>
      {#snippet title()}
        Leader certificate
        <Help label="Leader certificate">
          {#if inForce === 'acme'}
            Ordered from the certificate authority the ACME settings name, and ordered again by this
            leader two thirds of the way through its life.
          {:else if supplied}
            Uploaded by an administrator, so this leader issues none of its own while it is in
            force.
          {:else}
            What this leader serves, signed by the current authority and issued afresh at every
            start.
          {/if}
        </Help>
      {/snippet}

      <div class="setting">
        {@render who(
          certificates.served,
          `reached at ${certificates.served.names.join(', ') || 'no name'}` +
            (inForce === 'acme' && acme?.renewAfterMillis
              ? ` · renews ${on(acme.renewAfterMillis)}`
              : ''),
        )}
      </div>

      <div class="setting">
        <Field label="Source" group>
          <div class="source">
            <Segmented label="Source" options={SOURCES} bind:value={source} marked={inForce} />
          </div>
        </Field>
      </div>

      {#if switching}
        <div class="setting">
          <h3>
            {newRoot ? 'Moving workers to a new root' : `Switch to ${switchingTo} in progress`}
            <Help label="Source switch"
              >Workers are being given the incoming authority; this leader still serves the current
              certificate.</Help
            >
          </h3>
        </div>

        <div class="setting">
          {@render who(switching.incoming, `expires ${on(switching.incoming.notAfterMillis)}`)}
        </div>

        {@render progress(switching)}

        <div class="setting actions">
          <p class="critical">
            Cut over only once every worker holds the incoming authority: a worker without it can no
            longer connect, and enrollment codes minted under the current one are withdrawn.
          </p>
          <div class="buttons">
            <button
              class="danger"
              type="button"
              disabled={rotationBusy}
              onclick={async () => {
                const ok = await ask({
                  title: 'cut over',
                  body: 'A worker that does not hold the incoming authority can no longer connect.',
                  action: 'Cut over now',
                  danger: true,
                });
                if (ok)
                  rotationAct(
                    () => cutOverRotation(),
                    newRoot ? 'serving under the new root' : `switched to ${switchingTo}`,
                  );
              }}
            >
              Cut over now
            </button>
            <button
              type="button"
              disabled={rotationBusy}
              onclick={() => rotationAct(() => abandonRotation(), 'switch abandoned')}
            >
              Abandon switch
            </button>
          </div>
        </div>

        {@render status(rotationError, rotationMessage)}
        {#if source === 'own' || (source === 'acme' && !newRoot)}
          <!-- The form that opened this switch is out of view, so what it said shows here. -->
          {@render status(formError, message)}
        {/if}
      {/if}

      {#if source === 'own' && !switching}
        <div class="setting actions">
          {@render status(formError, message)}
          <div class="buttons">
            {#if certificates.supplied}
              <button
                class="primary"
                type="button"
                disabled={busy}
                onclick={() =>
                  act(() => withdrawCertificate(), switched('back to this leader’s own authority'))}
              >
                Switch to Leader issued
              </button>
            {:else}
              <button
                type="button"
                disabled={busy}
                onclick={async () => {
                  const ok = await ask({
                    title: 'issue certificate',
                    body: 'This leader serves a new certificate under the current authority.',
                    action: 'Issue a new one',
                  });
                  if (ok) act(() => reissueCertificate(), 'a new certificate is in force');
                }}
              >
                Issue a new one
              </button>
            {/if}
          </div>
        </div>
        {#if certificates.rotation}
          <div class="setting">
            <h3>
              Authority rotation in progress
              <Help label="Authority rotation"
                >Workers are being given the incoming authority; this leader still signs under the
                current one.</Help
              >
            </h3>
          </div>

          <div class="setting">
            {@render who(
              certificates.rotation.incoming,
              `expires ${on(certificates.rotation.incoming.notAfterMillis)}`,
            )}
          </div>

          {@render progress(certificates.rotation)}

          <div class="setting actions">
            <p class="critical">
              Cut over only once every worker holds the incoming authority: a worker without it can
              no longer connect, and enrollment codes minted under the current one are withdrawn.
            </p>
            <div class="buttons">
              <button
                class="danger"
                type="button"
                disabled={rotationBusy}
                onclick={async () => {
                  const ok = await ask({
                    title: 'cut over',
                    body: 'A worker that does not hold the incoming authority can no longer connect.',
                    action: 'Cut over now',
                    danger: true,
                  });
                  if (ok)
                    rotationAct(() => cutOverRotation(), 'signing under the incoming authority');
                }}
              >
                Cut over now
              </button>
              <button
                type="button"
                disabled={rotationBusy}
                onclick={() => rotationAct(() => abandonRotation(), 'rotation abandoned')}
              >
                Abandon rotation
              </button>
            </div>
          </div>

          {@render status(rotationError, rotationMessage)}
        {:else if !certificates.supplied}
          <div class="setting">
            <Collapsible open={!!rotationError || !!rotationMessage}>
              {#snippet summary()}Rotate authority{/snippet}
              <div class="rotation-settings">
                <Field label="Authority lifetime" note="A new authority defaults to 10 years.">
                  <select bind:value={authorityValidityDays}>
                    {#each AUTHORITY_VALIDITY_DAYS as days}
                      <option value={days}>{days / 365} {days === 365 ? 'year' : 'years'}</option>
                    {/each}
                  </select>
                </Field>
                <Field label="Wait for the fleet">
                  <div class="wait">
                    <input
                      type="number"
                      min={ROTATION_WAIT_HOURS.min}
                      max={ROTATION_WAIT_HOURS.max}
                      bind:value={waitHours}
                    />
                    <span class="dim"
                      >hours before automatic cutover, up to {ROTATION_WAIT_HOURS.max / 24} days</span
                    >
                  </div>
                </Field>
                <div class="algorithm">
                  <span>Key algorithm</span>
                  <strong>ECDSA P-256</strong>
                </div>
              </div>
              <div class="buttons">
                <button
                  class="primary"
                  type="button"
                  disabled={rotationBusy ||
                    waitHours < ROTATION_WAIT_HOURS.min ||
                    waitHours > ROTATION_WAIT_HOURS.max}
                  onclick={() =>
                    rotationAct(
                      () => beginRotation(waitHours, authorityValidityDays),
                      'a new authority is being published to the fleet',
                    )}
                >
                  Start rotation
                </button>
              </div>
              {@render status(rotationError, rotationMessage)}
            </Collapsible>
          </div>
        {/if}
      {:else if source === 'upload'}
        <div class="setting">
          <Field label="Certificate" note="PEM, carrying {certificates.served.names.join(' and ')}">
            <textarea bind:value={certificate} rows="4" spellcheck="false"></textarea>
          </Field>
          <Field label="Private key" note="PEM">
            <textarea bind:value={privateKey} rows="4" spellcheck="false"></textarea>
          </Field>
          <Field label="Authority" note="PEM, the authority that signed the certificate">
            <textarea bind:value={authority} rows="4" spellcheck="false"></textarea>
          </Field>
          {#if newAuthority}
            <Field
              label="Cut over by"
              note="Workers get the new authority first. This leader switches once they all have it, or at this time, whichever comes first."
            >
              <input
                class="when"
                type="datetime-local"
                bind:value={cutOverBy}
                max={latestCutOver === null ? undefined : local(latestCutOver)}
              />
            </Field>
          {/if}
          {#if switching}
            <p class="dim">
              A switch to {switchingTo} is open. When it cuts over, it replaces this certificate.
            </p>
          {/if}
          {@render status(formError, message)}
          <div class="buttons">
            <button
              class="primary"
              type="button"
              disabled={busy || !certificate || !privateKey || !authority}
              onclick={() =>
                act(
                  () =>
                    putCertificate(
                      certificate,
                      privateKey,
                      authority,
                      newAuthority && cutOverBy ? new Date(cutOverBy).getTime() : null,
                    ),
                  switched('this leader is serving the certificate you supplied'),
                )}
            >
              Serve this certificate
            </button>
          </div>
        </div>
      {:else if source === 'acme' && acme && (!switching || newRoot)}
        <div class="setting">
          {#if acme.lastError && !formError}
            <InlineError>
              The order of {on(acme.lastAttemptMillis ?? Date.now())} failed: {acme.lastError}
              {#if inForce === 'acme'}Tried again every hour.{/if}
            </InlineError>
          {/if}
          <Field label="Directory">
            <select bind:value={preset}>
              {#each ACME_DIRECTORIES as known (known.value)}
                <option value={known.value}>{known.label}</option>
              {/each}
              <option value={OTHER}>Other</option>
            </select>
          </Field>
          {#if preset === OTHER}
            <Field label="Directory URL" note="https://, as the authority publishes it">
              <input type="url" bind:value={directoryUrl} spellcheck="false" />
            </Field>
          {/if}
          <Field
            label="Names"
            group
            note="What the certificate carries, and where the authority reaches this leader."
          >
            <TokenListInput bind:values={nameList} placeholder="leader.example.com" />
          </Field>
          <Field label="Contact email">
            {#snippet help()}Where the certificate authority sends notices about this account.{/snippet}
            <input type="email" bind:value={contact} />
          </Field>
          <label class="agree">
            <input type="checkbox" bind:checked={agreed} />
            <span
              >I agree to the
              {#if terms}
                <a href={terms} target="_blank" rel="noreferrer">terms of service</a>
              {:else}
                authority's terms of service
              {/if}
            </span>
          </label>
          <Advanced modified={advancedChanged}>
            <div class="acme-advanced">
              <Field
                label="Challenge"
                note={challenge === 'http-01'
                  ? `The authority asks on port 80; this leader answers on port ${acme.httpPort}.`
                  : undefined}
              >
                {#snippet help()}How the certificate authority checks this leader holds the names.{/snippet}
                <select bind:value={challenge}>
                  <option value="tls-alpn-01">TLS-ALPN-01</option>
                  <option value="http-01">HTTP-01</option>
                </select>
              </Field>
              <Field
                label="Authority"
                note="Optional. PEM, the root a private authority issues under, where the platform does not trust it."
              >
                <textarea bind:value={acmeAuthority} rows="4" spellcheck="false"></textarea>
              </Field>
              <Field
                label="EAB key ID"
                note="Optional. External account binding, for an authority that requires an account with it."
              >
                <input bind:value={eabKeyId} spellcheck="false" />
              </Field>
              <Field label="EAB HMAC key" note="Optional. Stored encrypted and never shown again.">
                <PasswordField
                  bind:value={eabHmac}
                  ariaLabel="EAB HMAC key"
                  placeholder={acme.eabHmacSet ? 'stored' : ''}
                />
              </Field>
            </div>
          </Advanced>
          {#if inForce !== 'acme'}
            <p class="critical">
              Port {challenge === 'tls-alpn-01' ? 443 : 80} on {nameList.join(', ') ||
                'the names above'} must reach this leader.
            </p>
          {/if}
          {@render status(formError, message)}
          <div class="buttons">
            {#if inForce === 'acme'}
              <button
                class="primary"
                type="button"
                disabled={busy || !orderable}
                onclick={() => save()}
              >
                Save
              </button>
              <button type="button" disabled={busy || !orderable} onclick={() => order()}>
                Renew now
              </button>
            {:else}
              <button
                class="primary"
                type="button"
                disabled={busy || !orderable || !agreed}
                onclick={() => order()}
              >
                Get certificate
              </button>
            {/if}
          </div>
        </div>
      {/if}
    </Panel>
  {/if}
</div>

<style>
  .stack {
    display: grid;
    gap: 1.25rem;
  }

  /* The one card whose head carries a sentence under its title. */
  .panel {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }

  .head {
    padding: 0.85rem 1rem;
    border-bottom: 1px solid var(--border);
  }

  .head h2 {
    margin: 0;
    font-family: var(--font-display);
    font-size: var(--text-label);
    font-weight: 600;
  }

  .head p {
    margin: 0.25rem 0 0;
    font-size: var(--text-note);
  }

  .panel > .setting {
    padding: 0.85rem 1rem;
  }

  .setting {
    padding-bottom: 0.9rem;
    border-bottom: 1px solid var(--border);
    display: grid;
    gap: 0.6rem;
  }

  .setting:last-child {
    border-bottom: none;
    padding-bottom: 0;
  }

  .setting:has(.who) {
    grid-template-columns: 1fr auto;
    align-items: center;
  }

  h3 {
    margin: 0;
    font-size: var(--text-body);
    font-weight: 600;
  }

  p {
    margin: 0;
  }

  .who p {
    margin: 0.2rem 0 0;
    font-size: var(--text-note);
  }

  .mono {
    font-family: var(--mono);
    color: var(--dim);
  }

  .dim {
    color: var(--dim);
  }

  .when {
    max-width: 16rem;
  }

  .countdown strong {
    font-variant-numeric: tabular-nums;
  }

  .workers {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: 0.3rem;
  }

  .workers li {
    display: flex;
    justify-content: space-between;
    max-width: 32rem;
  }

  .holds {
    color: var(--added);
  }

  .soon {
    color: var(--error);
    font-weight: 600;
  }

  textarea,
  select {
    width: 100%;
  }

  /* PEM is a value someone pastes and copies; the lifetime choice is chrome. */
  textarea {
    font-family: var(--mono);
    resize: vertical;
  }

  textarea:focus-visible,
  select:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 1px;
  }

  /* The chooser keeps its own width rather than the field's. */
  .source {
    justify-self: start;
    display: flex;
  }

  .agree {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: var(--text-label);
  }

  .acme-advanced {
    display: grid;
    gap: 0.85rem;
  }

  .rotation-settings {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(12rem, 1fr));
    gap: 0.85rem;
    align-items: start;
  }

  .algorithm {
    display: grid;
    gap: 0.3rem;
    font-size: var(--text-label);
  }

  .algorithm > span {
    font-weight: 600;
  }

  .buttons {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
  }

  .wait {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
  }

  .wait input {
    width: 5rem;
  }
</style>
