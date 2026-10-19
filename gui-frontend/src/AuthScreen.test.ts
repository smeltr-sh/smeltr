// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import AuthScreen from './AuthScreen.svelte';
import { SHARED_SCOPE, setScope } from './api';

let screen: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;

/** Mounts the sign-in screen against a leader that does, or does not, offer
 * OIDC, and waits for it to have asked. */
async function signInScreen(offered: boolean | Error) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string) => {
      expect(input).toBe('/api/auth/oidc');
      if (offered instanceof Error) throw offered;
      return new Response(JSON.stringify({ offered }), {
        headers: { 'content-type': 'application/json' },
      });
    }),
  );
  screen = mount(AuthScreen, { target, props: { mode: 'sign-in' } });
  await vi.waitFor(() => expect(target.querySelector('form, .choice')).not.toBeNull());
}

function button(text: string): HTMLButtonElement | undefined {
  return [...target.querySelectorAll('button')].find((b) => b.textContent?.trim() === text);
}

beforeEach(() => {
  history.replaceState(null, '', '/');
  target = document.createElement('div');
  document.body.append(target);
});

afterEach(() => {
  if (screen) unmount(screen);
  screen = undefined;
  target.remove();
  vi.unstubAllGlobals();
});

describe('signing in where OIDC is offered', () => {
  it('offers a choice first, with OIDC as a navigation to the leader', async () => {
    await signInScreen(true);
    const sso = target.querySelector<HTMLAnchorElement>('a.btn.primary');
    expect(sso?.textContent).toBe('Sign in with SSO');
    expect(sso?.getAttribute('href')).toBe('/api/auth/oidc/start');
    expect(button('Sign in with username')).toBeDefined();
    expect(target.querySelector('form')).toBeNull();
  });

  it('goes to the password form and back', async () => {
    await signInScreen(true);
    button('Sign in with username')!.click();
    flushSync();
    expect(target.querySelector('form input[autocomplete="username"]')).not.toBeNull();
    button('Back')!.click();
    flushSync();
    expect(target.querySelector('form')).toBeNull();
    expect(target.querySelector('.choice')).not.toBeNull();
  });

  /// Said once: a reload must not repeat a failure that already happened.
  it('says why the last OIDC sign-in failed, and takes the code out of the URL', async () => {
    history.replaceState(null, '', '/?oidc=refused#/status');
    await signInScreen(true);
    expect(target.querySelector('.choice .error')?.textContent).toBe(
      'single sign-on refused that account',
    );
    expect(location.search).toBe('');
    expect(location.hash).toBe('#/status');
  });
});

describe('signing in where OIDC is not offered', () => {
  it('goes straight to the password form, with no way back', async () => {
    await signInScreen(false);
    expect(target.querySelector('form')).not.toBeNull();
    expect(target.querySelector('.choice')).toBeNull();
    expect(button('Back')).toBeUndefined();
  });

  it('counts a leader that cannot say as not offering it', async () => {
    await signInScreen(new Error('connection refused'));
    expect(target.querySelector('form')).not.toBeNull();
    expect(target.querySelector('.choice')).toBeNull();
  });
});

// The address keeps the page somebody was on when their session ended, and nobody signed in may
// edit an authoring page.
describe('on an authoring page', () => {
  beforeEach(() => {
    history.replaceState(null, '', '/#/build/sources');
    setScope('payments');
  });

  afterEach(() => setScope(SHARED_SCOPE));

  it('leaves the sign-in form open', async () => {
    await signInScreen(false);
    const inputs = [...target.querySelectorAll('input')];
    expect(inputs).toHaveLength(2);
    for (const input of inputs) expect(input.closest('fieldset[disabled]')).toBeNull();
  });

  it('leaves the change-password form open', () => {
    screen = mount(AuthScreen, {
      target,
      props: { mode: 'change-password', username: 'lee', oncancel: () => {} },
    });
    flushSync();
    const inputs = [...target.querySelectorAll('input')];
    expect(inputs).toHaveLength(3);
    for (const input of inputs) expect(input.closest('fieldset[disabled]')).toBeNull();
  });
});
