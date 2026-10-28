// Who is signed in, as the GUI knows it.
//
// The leader decides; this is a cache of its last answer plus the CSRF value
// every unsafe request must echo. That value is held here and nowhere else --
// never in a cookie, which would ride along with the forgery it exists to
// stop, and never in storage, which outlives the session it belongs to.
//
// `api.ts` reads `csrf()` on every unsafe call and reports a refusal back
// through `signedOut()`, so there is one place that knows the session ended
// and one place that says so.

/** How much a person holds in one area: `viewer` reads, `editor` also saves on branches,
 * captures and previews, `deployer` also approves and deploys. */
export type AccessLevel = 'viewer' | 'editor' | 'deployer';

/** What a person holds: everything, or one level per area -- `shared` or a worker group's id. */
export interface AccessDto {
  administrator: boolean;
  /** Empty for an administrator, who holds every area. */
  areas: Record<string, AccessLevel>;
}

/** What `GET /api/auth/session` answers. Mirrors `auth_api::SessionDto`. */
export interface SessionDto {
  authenticated: boolean;
  userId?: string;
  displayName?: string;
  username?: string;
  administrator: boolean;
  mustChangePassword: boolean;
  /** How this session signed in. An OIDC account holds no password, so it
   * is never asked to confirm with one and has none to change. */
  authSource?: 'local' | 'oidc';
  /** What the provider last called an OIDC account, shown where a local one
   * shows its username. */
  providerLogin?: string;
  /** Absent when signed out. */
  access?: AccessDto;
  /** AI agents are turned on and their model endpoint is set, so the agent's screens are offered. */
  agents?: boolean;
  csrf?: string;
}

export const SIGNED_OUT: SessionDto = {
  authenticated: false,
  administrator: false,
  mustChangePassword: false,
};

/** Raised when the leader stops accepting this session, from wherever the
 * refusal arrived. The shell listens and shows the sign-in screen. */
export const SESSION_EVENT = 'smeltr-session';

/** Raised when the same session comes back holding different access: a role changed under it, or AI agents were
 * turned on or off.
 * Apart from `SESSION_EVENT`, since the person is the same and their unsaved work stays theirs. */
export const ACCESS_EVENT = 'smeltr-access';

let current: SessionDto = SIGNED_OUT;

export function session(): SessionDto {
  return current;
}

/** The header value for an unsafe request, or `null` when there is nothing to
 * send -- which the leader will refuse, and should. */
export function csrf(): string | null {
  return current.csrf ?? null;
}

function adopt(next: SessionDto): SessionDto {
  const was = current.authenticated;
  const wasMustChange = current.mustChangePassword;
  const wasAccess = JSON.stringify([current.access, current.agents ?? false]);
  current = next;
  // Guarded like `api.ts`'s own storage reads: this module is imported by
  // tests that have no DOM, and a session is a fact whether or not there is a
  // window to tell about it.
  const moved = was !== next.authenticated || wasMustChange !== next.mustChangePassword;
  if (typeof window === 'undefined') return next;
  if (moved) window.dispatchEvent(new CustomEvent(SESSION_EVENT));
  else if (
    next.authenticated &&
    wasAccess !== JSON.stringify([next.access, next.agents ?? false])
  ) {
    window.dispatchEvent(new CustomEvent(ACCESS_EVENT));
  }
  return next;
}

let reachable = true;

/** Whether the last answer came from a leader that could speak for itself.
 *
 * "Signed out" and "the leader cannot say" are different facts and the route
 * is careful to distinguish them, answering `503` rather than anonymously when
 * the account store is unavailable. Collapsing them here would send an
 * operator to a sign-in form that cannot work, over and over. */
export function leaderAnswered(): boolean {
  return reachable;
}

/** The leader's own answer, which is the only one that counts. Never throws;
 * an unreachable leader is reported through [`leaderAnswered`]. */
export async function load(): Promise<SessionDto> {
  try {
    const res = await fetch('/api/auth/session', { headers: { accept: 'application/json' } });
    if (!res.ok) {
      reachable = false;
      return adopt(SIGNED_OUT);
    }
    reachable = true;
    return adopt((await res.json()) as SessionDto);
  } catch {
    reachable = false;
    return adopt(SIGNED_OUT);
  }
}

/** What a refused call means: the session this page was holding is over.
 * Called by `api.ts` rather than by every screen. */
export function signedOut(): void {
  adopt(SIGNED_OUT);
}

let rechecking: Promise<SessionDto> | null = null;

/** A leader that refused something is worth re-asking: `403` is also what a
 * must-change session gets for everything but its own password, and what a
 * session gets whose roles changed under it, and the difference is what the
 * session DTO says. Calls refused together share one re-ask, and the re-ask
 * goes around `api.ts`, so it cannot set off another. */
export function recheck(): Promise<SessionDto> {
  rechecking ??= load().finally(() => {
    rechecking = null;
  });
  return rechecking;
}

/** The sentence a refused call carries, or a fallback. Every handler answers
 * JSON with `ok` and `error`, on any status. */
async function errorOf(res: Response, fallback: string): Promise<string> {
  try {
    const body = (await res.json()) as { error?: string };
    return body.error || fallback;
  } catch {
    return fallback;
  }
}

/** How long a throttled caller was told to wait. `Retry-After` is what the
 * leader sends; a minute is the documented fallback when it is missing. */
export function retryAfter(res: Response): number {
  const header = res.headers.get('retry-after');
  const seconds = header === null ? NaN : Number(header);
  return Number.isFinite(seconds) && seconds > 0 ? Math.ceil(seconds) : 60;
}

export type AuthFailure = { error: string; waitSeconds?: number };

/** `POST /api/auth/login`. The leader sets the cookie; what comes back here
 * is what the GUI needs to decide which screen to be on. */
export async function signIn(
  username: string,
  password: string,
): Promise<SessionDto | AuthFailure> {
  const res = await fetch('/api/auth/login', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });
  if (res.status === 429) {
    return { error: await errorOf(res, 'too many attempts'), waitSeconds: retryAfter(res) };
  }
  if (!res.ok) return { error: await errorOf(res, 'that did not work') };
  return adopt((await res.json()) as SessionDto);
}

export async function signOut(): Promise<void> {
  const token = csrf();
  try {
    await fetch('/api/auth/session', {
      method: 'DELETE',
      headers: token === null ? {} : { 'x-smeltr-csrf': token },
    });
  } finally {
    // Whether or not the leader answered: this page is done with the session
    // either way, and a sign-out that reports failure is a sign-out nobody
    // trusts.
    signedOut();
  }
}

/** `PUT /api/auth/password`. Succeeds by signing you out: choosing a
 * password ends every session holding the old one, including this one.
 *
 * `currentPassword` is `null` for a required change: that session was issued
 * by presenting it, and the leader does not ask twice. */
export async function changePassword(
  username: string,
  currentPassword: string | null,
  newPassword: string,
): Promise<true | AuthFailure> {
  const token = csrf();
  const res = await fetch('/api/auth/password', {
    method: 'PUT',
    headers: {
      'content-type': 'application/json',
      ...(token === null ? {} : { 'x-smeltr-csrf': token }),
    },
    body: JSON.stringify(
      currentPassword === null
        ? { username, newPassword }
        : { username, currentPassword, newPassword },
    ),
  });
  if (res.status === 429) {
    return { error: await errorOf(res, 'too many attempts'), waitSeconds: retryAfter(res) };
  }
  if (!res.ok) {
    // A 401 is either a wrong current password, which ends nothing, or an account that moved on under this
    // session, which ended it. The leader's own answer is what tells them apart.
    const failure = { error: await errorOf(res, 'that did not work') };
    if (res.status === 401) await recheck();
    return failure;
  }
  signedOut();
  return true;
}

/** The idle clock restarts when somebody does something, so the leader is
 * told -- after an interaction, never on a timer. A tab left open is not a
 * person working, and a heartbeat would make it look like one. */
export async function reportActivity(): Promise<void> {
  const token = csrf();
  if (token === null) return;
  try {
    const res = await fetch('/api/auth/session/activity', {
      method: 'POST',
      headers: { 'x-smeltr-csrf': token },
    });
    // The one report that is not advisory. Interacting with a page whose
    // session has expired is exactly when somebody should be told, rather than
    // leaving the operator surface on screen until an unrelated call notices.
    if (res.status === 401) signedOut();
  } catch {
    // A report that never arrived says nothing about the session, so it costs
    // a little idle time and nothing else. Interrupting somebody mid-keystroke
    // to say a heartbeat failed would be worse than losing it.
  }
}

/** `GET /api/auth/oidc`: whether the leader offers single sign-on. Asked
 * before anybody is signed in, so it needs no session. Never throws: a
 * leader that cannot say is not offering it, and the password form still
 * works without it. */
export async function oidcOffered(): Promise<boolean> {
  try {
    // Bounded: the sign-in screen waits on this answer, and a leader that never
    // gives it must still leave the password form reachable.
    const res = await fetch('/api/auth/oidc', {
      headers: { accept: 'application/json' },
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) return false;
    const body = (await res.json()) as { offered?: unknown };
    return body.offered === true;
  } catch {
    return false;
  }
}
