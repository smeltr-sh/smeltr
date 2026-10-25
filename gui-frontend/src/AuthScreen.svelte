<!--
  The two screens somebody sees before the app: signing in, and choosing a
  password they were given rather than chose. One component because they are
  one layout and one set of failures -- a wrong password, a throttle, a leader
  that cannot answer -- and splitting them would mean saying each of those
  twice.

  The leader decides which is showing: `mustChangePassword` on the session.
  It refuses every other route until that is false, so this is not a
  suggestion the GUI is making.

  A leader that offers OIDC gets a choice before the password form, because
  most people on such a leader have no password to type.
-->
<script lang="ts">
  import InlineError from './InlineError.svelte';
  import {
    changePassword,
    oidcOffered,
    retryAfter,
    signIn,
    signOut,
    type AuthFailure,
    type SessionDto,
  } from './session';
  import { tick } from 'svelte';
  import Help from './Help.svelte';
  import Field from './Field.svelte';
  import { neverLocked } from './Locked.svelte';

  let {
    mode,
    username = '',
    displayName = '',
    notice = '',
    oncancel,
    onchanged,
  }: {
    mode: 'sign-in' | 'change-password';
    username?: string;
    displayName?: string;
    /** Shown above the form: what the shell has to say about how this
     * screen came to be showing. */
    notice?: string;
    /** Given when the change was asked for rather than required, which is the
     * difference between a way back and a way out. */
    oncancel?: () => void;
    /** A password was chosen. The session is already gone by the time this
     * fires, and this screen with it, so the shell carries the news. */
    onchanged?: () => void;
  } = $props();

  const changing = $derived(mode === 'change-password');

  // The address may name an authoring page this person cannot edit, or there is no person yet. The
  // form that signs them in stays open either way.
  neverLocked();

  // What the leader's redirect back from a failed OIDC sign-in means, by the
  // code it puts in `?oidc=`.
  const OIDC_FAILURES: Record<string, string> = {
    unavailable: 'single sign-on is not available on this leader',
    provider: 'your sign-in provider could not be reached',
    refused: 'single sign-on refused that account',
    expired: 'that sign-in expired; start it again',
  };

  // `null` until the leader answers, so the screen never shows the password
  // form and then swaps it for the choice under somebody's cursor.
  let offered = $state<boolean | null>(null);
  let choosing = $state(true);
  let oidcError = $state('');

  $effect(() => {
    if (changing) return;
    oidcError = takeOidcFailure();
    oidcOffered().then((answer) => (offered = answer));
  });

  /** The message for the failure the leader redirected back with, if any.
   * The code leaves the URL as it is read, or every reload would repeat a
   * failure that already happened. */
  function takeOidcFailure(): string {
    const url = new URL(location.href);
    const code = url.searchParams.get('oidc');
    if (code === null) return '';
    url.searchParams.delete('oidc');
    history.replaceState(history.state, '', url);
    return Object.hasOwn(OIDC_FAILURES, code) ? OIDC_FAILURES[code] : '';
  }

  function chooseUsername() {
    choosing = false;
    // It was about the OIDC attempt, and the form has failures of its own.
    oidcError = '';
  }

  let ssoLink = $state<HTMLAnchorElement | undefined>();

  // Back removes the button that was pressed, so focus is put where the
  // choice starts rather than left to fall to the page.
  async function back() {
    choosing = true;
    error = '';
    await tick();
    ssoLink?.focus();
  }

  // A required change follows a sign-in with the very password it replaces,
  // so only a change asked for proves the current one first.
  const asked = $derived(changing && oncancel !== undefined);

  // Only the sign-in form has a username to type. Changing a password is
  // done by whoever is signed in, so that name comes from the session and is
  // shown rather than asked for.
  let typed = $state('');
  const account = $derived(changing ? username : typed);
  let current = $state('');
  let chosen = $state('');
  let repeated = $state('');
  let error = $state('');
  let busy = $state(false);

  // A throttled caller is told how long to wait, and waits: the button is
  // disabled until then. Never auto-submitted -- a form that fires itself
  // spends the next attempt without anybody deciding to.
  let waitUntil = $state(0);
  let now = $state(Date.now());
  const waiting = $derived(Math.max(0, Math.ceil((waitUntil - now) / 1000)));

  $effect(() => {
    if (waitUntil === 0) return;
    const tick = setInterval(() => (now = Date.now()), 250);
    return () => clearInterval(tick);
  });

  // When the wait runs out, so does what it said. "Try again in 2 seconds"
  // left on screen once those seconds have passed is worse than nothing: it
  // is the form describing a state it is no longer in.
  $effect(() => {
    if (waitUntil !== 0 && waiting === 0) {
      waitUntil = 0;
      error = '';
    }
  });

  function refused(result: AuthFailure) {
    error = result.error;
    if (result.waitSeconds !== undefined) {
      waitUntil = Date.now() + result.waitSeconds * 1000;
      now = Date.now();
    }
  }

  async function submit(event: SubmitEvent) {
    event.preventDefault();
    if (busy || waiting > 0) return;
    error = '';
    oidcError = '';

    if (changing && chosen !== repeated) {
      error = 'those two passwords are not the same';
      return;
    }

    busy = true;
    try {
      if (changing) {
        const result = await changePassword(account, asked ? current : null, chosen);
        if (result !== true) return refused(result);
        // Succeeding signs you out: the session that asked held the old
        // password. `session.ts` has already cleared it, and the shell shows
        // sign-in in place of this screen.
        onchanged?.();
      } else {
        const result = await signIn(account, current);
        if (!('authenticated' in (result as SessionDto))) return refused(result as AuthFailure);
      }
    } catch {
      error = 'the leader could not be reached';
    } finally {
      busy = false;
      current = '';
      chosen = '';
      repeated = '';
    }
  }
</script>

<main>
  <p class="eyebrow">
    {#if !changing}Leader access{:else if oncancel}Your account{:else}Required before you continue{/if}
  </p>
  <h1>
    {changing ? 'Choose your own password' : 'Sign in to smeltr'}
    {#if !changing}
      <Help label="Signing in">An administrator creates accounts and resets passwords.</Help>
    {/if}
  </h1>
  {#if changing && oncancel}
    <p class="critical">
      Save open work first: changing your password signs you out of every session, this one
      included.
    </p>
  {/if}

  {#if notice}<p class="notice" role="status">{notice}</p>{/if}

  {#if !changing && offered === null}
    <!-- Asking the leader whether OIDC is offered. -->
  {:else if !changing && offered && choosing}
    <div class="choice">
      <!-- A navigation, not a fetch: the leader answers with a redirect to
           the provider, and the provider's pages are whole pages. -->
      <a class="btn primary" href="/api/auth/oidc/start" bind:this={ssoLink}>Sign in with SSO</a>
      <InlineError message={oidcError} />
      <button type="button" onclick={chooseUsername}>Sign in with username</button>
    </div>
  {:else}
    <form onsubmit={submit}>
      {#if changing}
        <p class="who">{displayName} · <code>{username}</code></p>
        {#if asked}
          <Field required label="Current password">
            <input type="password" bind:value={current} autocomplete="current-password" required />
          </Field>
        {/if}
        <Field required label="New password">
          <input type="password" bind:value={chosen} autocomplete="new-password" required />
        </Field>
        <Field required label="Confirm new password">
          <input type="password" bind:value={repeated} autocomplete="new-password" required />
        </Field>
      {:else}
        <Field required label="Username">
          <!-- svelte-ignore a11y_autofocus -->
          <input bind:value={typed} autocomplete="username" autofocus required />
        </Field>
        <Field required label="Password">
          <input type="password" bind:value={current} autocomplete="current-password" required />
        </Field>
      {/if}

      {#if waiting > 0}
        <div class="wait" role="status">
          <strong>Wait before trying again</strong>
          <p>
            Too many attempts.{changing ? ' Your password has not changed.' : ''} Try again in {waiting}s.
          </p>
        </div>
      {:else if error || oidcError}
        <!-- An OIDC failure lands here when OIDC is no longer offered, as when it was turned off
             while somebody was at the provider: there is no choice screen to say it on. -->
        <InlineError message={error || oidcError} />
      {/if}

      <button class="primary" type="submit" disabled={busy || waiting > 0}>
        {changing ? 'Change password' : 'Sign in'}
      </button>
    </form>
  {/if}

  {#if !changing && offered && !choosing}
    <button class="quiet" type="button" onclick={back}>Back</button>
  {/if}

  {#if changing}
    {#if oncancel}
      <button class="quiet" type="button" onclick={oncancel}>Cancel</button>
    {:else}
      <button class="quiet" type="button" onclick={() => signOut()}>Sign out</button>
    {/if}
  {/if}
</main>

<style>
  main {
    max-width: 24rem;
    margin-inline: auto;
    padding: 3rem 1.5rem 4rem;
  }

  .eyebrow {
    margin: 0;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  h1 {
    font-family: var(--font-display);
    font-size: 1.6rem;
    font-weight: 600;
    line-height: 1.2;
    margin: 0.4rem 0 1.5rem;
  }

  form {
    display: grid;
    gap: 1rem;
  }

  form p {
    margin: 0;
  }

  .choice {
    display: grid;
    gap: 1rem;
  }

  .choice .btn {
    text-align: center;
  }

  .who {
    color: var(--dim);
  }

  code {
    font-family: var(--mono);
    font-size: var(--text-label);
  }

  input {
    width: 100%;
  }

  input:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 1px;
  }

  .quiet {
    margin-top: 0.75rem;
    border: 1px solid transparent;
    border-radius: var(--radius);
    background: none;
    color: var(--dim);
    font: inherit;
    font-size: var(--text-label);
    padding: 0.3rem 0;
    cursor: pointer;
  }

  .quiet:hover {
    color: var(--ink);
  }

  .notice {
    margin: 0 0 1rem;
    padding: 0.6rem 0.75rem;
    background: var(--panel);
    border-radius: var(--radius);
    font-size: var(--text-body);
  }

  .wait {
    padding: 0.7rem 0.8rem;
    border-left: 3px solid var(--accent);
    background: var(--comment-bg);
    color: var(--comment-ink);
  }

  .wait strong {
    display: block;
    font-size: var(--text-label);
  }

  .wait p {
    margin: 0.2rem 0 0;
    font-size: var(--text-body);
  }
</style>
