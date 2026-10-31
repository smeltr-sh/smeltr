import sessionSource from './session.ts?raw';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  changePassword,
  csrf,
  leaderAnswered,
  load,
  oidcOffered,
  retryAfter,
  session,
  signIn,
  signedOut,
} from './session';
import { listWorkers, revokeWorker } from './api';

const SIGNED_IN = {
  authenticated: true,
  userId: 'u_1',
  displayName: 'Morgan Ellis',
  username: 'morgan',
  administrator: true,
  mustChangePassword: false,
  csrf: 'csrf-value',
};

function answer(body: unknown, init: { status?: number; headers?: Record<string, string> } = {}) {
  return new Response(JSON.stringify(body), {
    status: init.status ?? 200,
    headers: { 'content-type': 'application/json', ...(init.headers ?? {}) },
  });
}

beforeEach(() => signedOut());
afterEach(() => vi.restoreAllMocks());

describe('the session the GUI holds', () => {
  it('takes the CSRF value from a sign-in', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async () => answer(SIGNED_IN));
    await signIn('morgan', 'a password');
    expect(csrf()).toBe('csrf-value');
  });

  /// It belongs to one session, so anywhere that outlives one is the wrong
  /// place for it. Asserted against the source because the guarantee is that
  /// the code never reaches for storage, not that it happens not to today.
  it('keeps the session nowhere that outlives it', () => {
    expect(sessionSource).not.toMatch(/localStorage|sessionStorage|document\.cookie|indexedDB/);
  });

  /// A leader that cannot answer is not a leader saying "you are signed in".
  it('reads an unreachable leader as signed out rather than throwing', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('connection refused'));
    await expect(load()).resolves.toMatchObject({ authenticated: false });
  });

  it('reports a throttled sign-in as a wait rather than a failure', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      answer(
        { ok: false, errors: [{ message: 'too many sign-in attempts' }] },
        { status: 429, headers: { 'retry-after': '45' } },
      ),
    );
    const result = await signIn('morgan', 'wrong');
    expect(result).toEqual({ error: 'too many sign-in attempts', waitSeconds: 45 });
  });

  /// The leader sends `Retry-After`; a minute is what to wait when it does not.
  it('falls back to a minute when the wait is not stated', () => {
    expect(retryAfter(new Response(null, { headers: { 'retry-after': '20' } }))).toBe(20);
    expect(retryAfter(new Response(null))).toBe(60);
    expect(retryAfter(new Response(null, { headers: { 'retry-after': 'soon' } }))).toBe(60);
  });

  /// Choosing a password ends every session holding the old one, this one
  /// included, so the GUI must not keep acting as though it were signed in.
  it('signs out locally when a password change succeeds', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(answer(SIGNED_IN));
    await signIn('morgan', 'a password');
    expect(csrf()).toBe('csrf-value');

    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(answer({ ok: true }));
    await expect(changePassword('morgan', 'a password', 'another one')).resolves.toBe(true);
    expect(session().authenticated).toBe(false);
    expect(csrf()).toBeNull();
  });

  /// A required change can lose to a reset landing meanwhile. The leader
  /// refuses and clears the cookie; asked again, it says nobody is signed in.
  it('re-asks the leader when a password change is refused with a 401', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(answer(SIGNED_IN));
    await signIn('morgan', 'a password');

    vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(
        answer({ ok: false, errors: [{ message: 'your session ended' }] }, { status: 401 }),
      )
      .mockResolvedValueOnce(
        answer({ authenticated: false, administrator: false, mustChangePassword: false }),
      );
    await expect(changePassword('morgan', null, 'another one')).resolves.toEqual({
      error: 'your session ended',
    });
    expect(session().authenticated).toBe(false);
  });

  /// The re-ask can fail to arrive. "Signed out" and "the leader cannot say"
  /// are different facts, and the shell shows a different screen for each.
  it('reports the leader unreachable when the re-ask cannot arrive', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(answer(SIGNED_IN));
    await signIn('morgan', 'a password');
    expect(leaderAnswered()).toBe(true);

    vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(
        answer({ ok: false, errors: [{ message: 'your session ended' }] }, { status: 401 }),
      )
      .mockRejectedValueOnce(new Error('the leader went away'));
    await changePassword('morgan', null, 'another one');
    expect(leaderAnswered()).toBe(false);
  });

  /// A wrong current password is a 401 too, and the leader still knows the
  /// session; asked again, it says so, and the form stays.
  it('keeps the session when the current password was simply wrong', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(answer(SIGNED_IN));
    await signIn('morgan', 'a password');

    vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(
        answer(
          { ok: false, errors: [{ message: 'that is not your current password' }] },
          { status: 401 },
        ),
      )
      .mockResolvedValueOnce(answer(SIGNED_IN));
    await changePassword('morgan', 'wrong', 'another one');
    expect(session().authenticated).toBe(true);
    expect(csrf()).toBe('csrf-value');
  });
});

describe('what every API call carries', () => {
  it('echoes the CSRF value on an unsafe method and not on a read', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async () => answer(SIGNED_IN));
    await signIn('morgan', 'a password');

    const fetched = vi
      .spyOn(globalThis, 'fetch')
      .mockImplementation(async () => answer({ ok: true, workers: [] }));
    // The same spy recorded the sign-in; only what follows is the subject.
    fetched.mockClear();

    await listWorkers();
    const read = new Headers((fetched.mock.calls[0][1] as RequestInit | undefined)?.headers);
    expect(read.get('x-smeltr-csrf')).toBeNull();

    await revokeWorker('w1');
    const wrote = new Headers((fetched.mock.calls[1][1] as RequestInit | undefined)?.headers);
    expect(wrote.get('x-smeltr-csrf')).toBe('csrf-value');
  });

  /// One refusal is one signed-out shell, rather than an error on each of the
  /// thirty-seven screens that might have made the call.
  it('treats a 401 from anywhere as the session ending', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async () => answer(SIGNED_IN));
    await signIn('morgan', 'a password');
    expect(session().authenticated).toBe(true);

    vi.spyOn(globalThis, 'fetch').mockImplementation(async () =>
      answer({ ok: false, errors: [{ message: 'sign in to use this' }] }, { status: 401 }),
    );
    await expect(listWorkers()).rejects.toThrow(/session ended/);
    expect(session().authenticated).toBe(false);
  });

  /// A role can change under a session, and a 403 is where that shows. Calls
  /// refused together re-read it once.
  it('re-reads the session once when calls are refused with a 403', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async () => answer(SIGNED_IN));
    await signIn('morgan', 'a password');

    const viewer = { administrator: false, areas: { shared: 'viewer' } };
    const fetched = vi
      .spyOn(globalThis, 'fetch')
      .mockImplementation(async (input) =>
        String(input) === '/api/auth/session'
          ? answer({ ...SIGNED_IN, administrator: false, access: viewer })
          : answer(
              { ok: false, errors: [{ message: 'this needs an administrator' }] },
              { status: 403 },
            ),
      );
    await Promise.allSettled([listWorkers(), listWorkers()]);
    await vi.waitFor(() => expect(session().access).toEqual(viewer));
    const reads = fetched.mock.calls.filter(([input]) => String(input) === '/api/auth/session');
    expect(reads).toHaveLength(1);
    expect(session().authenticated).toBe(true);
  });
});

describe('whether single sign-on is offered', () => {
  it('takes the leader at its word', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(answer({ offered: true }));
    await expect(oidcOffered()).resolves.toBe(true);
    expect(fetch.mock.calls[0][0]).toBe('/api/auth/oidc');
  });

  it('is not offered when the leader says so', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(answer({ offered: false }));
    await expect(oidcOffered()).resolves.toBe(false);
  });

  /// The password form works without it; a choice that leads nowhere does not.
  it('is not offered when the leader cannot say', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(answer({ ok: false }, { status: 503 }));
    await expect(oidcOffered()).resolves.toBe(false);
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('connection refused'));
    await expect(oidcOffered()).resolves.toBe(false);
  });
});
