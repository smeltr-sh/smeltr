// One running capture as a dialog follows it: its session, its latest status,
// and the poll loop that keeps the status current until nothing about it can
// change. A source's and a destination's capture are followed alike; only how
// each is started and how it ends (saved, or read and closed) differ, and those
// stay with their callers.

import { settled } from './capture';
import {
  errorText,
  captureStatus,
  currentScope,
  discardCapture,
  errorMessage,
  getClusterStatus,
  SHARED_SCOPE,
  startCapture,
  stopCapture,
  type CaptureStatus,
} from './api';

/** The group a capture of the source being edited asks: its scope's own. Under `shared/` the source runs in every
 * group, so it asks the first that has a worker heard from lately, else the first, and `set` takes it once the
 * cluster answers. */
export function groupToAsk(set: (group: string) => void) {
  const scope = currentScope();
  if (scope !== SHARED_SCOPE) {
    set(scope);
    return;
  }
  getClusterStatus()
    .then((c) => {
      const groups = c.workerGroups ?? [];
      const live = (id: string) =>
        c.health?.threads.some(
          (t) => t.groupId === id && t.lastSeenMillisAgo <= c.health!.silentAfterMs,
        );
      set((groups.find((g) => live(g.id)) ?? groups[0])?.id ?? '');
    })
    .catch(() => {});
}

/** Backoff after a network error: doubles each attempt, capped, since a
 * capture is bounded in seconds and a retry must not outlive it. */
export const POLL_RETRY_MS = [1000, 2000, 4000, 8000];

export class CaptureSession {
  /** Empty when there is nothing to stop, save or discard. */
  session = $state('');
  /** `null` until the leader has answered the first poll. */
  status = $state<CaptureStatus | null>(null);

  // The poll loop's own timer, so it can be canceled instead of firing into
  // a dead component or a discarded capture.
  #timer: ReturnType<typeof setTimeout> | undefined;
  /** Moved on by `stopPolling`, so a poll already in flight when polling stops
   * finds it moved and drops its answer. */
  #round = 0;
  #failures = 0;
  /** Set once the owner is destroyed. A poll never starts after it: a stop
   * still in flight at that moment fails into a poll of its own. A capture that
   * arrives after it is discarded, since nothing is left to show it. */
  #destroyed = false;

  #report: (error: string) => void;

  /** `report` takes what went wrong, in words for the dialog. */
  constructor(report: (error: string) => void) {
    this.#report = report;
  }

  /** Ask the leader to open a capture and follow it. Answers whether it is now followed: a
   * refusal is reported, and a session that arrives after the owner is gone is discarded. */
  async start(body: Parameters<typeof startCapture>[0]): Promise<boolean> {
    try {
      const res = await startCapture(body);
      if (!res.ok || !res.session) {
        this.#report(errorText(res.errors) || 'could not start the capture');
        return false;
      }
      return this.follow(res.session);
    } catch (e) {
      this.#report(errorMessage(e));
      return false;
    }
  }

  /** Follow a session the leader just opened. A session that arrives after
   * the owner is gone is discarded, and this answers false. */
  follow(session: string): boolean {
    if (this.#destroyed) {
      discardCapture(session).catch(() => {});
      return false;
    }
    this.stopPolling();
    this.session = session;
    this.status = null;
    this.#failures = 0;
    void this.poll();
    return true;
  }

  stopPolling() {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#round += 1;
  }

  /** Poll until nothing about the capture can change (`settled`). It is done
   * once it holds its total, every expected worker answered, the deadline
   * passed, or it was stopped; a worker that stopped before then may still say
   * why after. A worker still running it when it ended never answers, so a
   * finished capture is polled until it is saved or discarded, or the leader
   * forgets it. On a running capture a network error retries with backoff
   * rather than freezing the dialog on stale progress; it gives up only after
   * several in a row. */
  async poll() {
    if (!this.session || this.#destroyed) return;
    const polling = this.session;
    const round = this.#round;
    try {
      const res = await captureStatus(polling);
      // Discarded, replaced or stopped while in flight.
      if (this.session !== polling || round !== this.#round) return;
      if (!res.ok || !res.capture) {
        // Nothing to stop, save or discard any more.
        this.#report(errorText(res.errors) || 'the capture is gone');
        this.session = '';
        this.status = null;
        return;
      }
      this.#failures = 0;
      this.status = res.capture;
      if (!settled(res.capture)) this.#timer = setTimeout(() => void this.poll(), 1000);
    } catch (e) {
      if (this.session !== polling || round !== this.#round) return;
      // A finished capture still holds what it ended with. Losing the leader
      // costs only the reasons still to come, so polling stops and the dialog
      // stays.
      if (this.status?.done) return;
      if (this.#failures >= POLL_RETRY_MS.length) {
        this.#report(`lost contact with the leader: ${errorMessage(e)}`);
        this.session = '';
        this.status = null;
        return;
      }
      this.#timer = setTimeout(() => void this.poll(), POLL_RETRY_MS[this.#failures]);
      this.#failures += 1;
    }
  }

  /** End the capture where it stands. The dialog then offers what it holds. */
  async stop() {
    if (!this.session) return;
    // Before the request: a poll that lands after the stop's answer would show
    // the capture running again, and poll again.
    this.stopPolling();
    try {
      const res = await stopCapture(this.session);
      if (!res.ok || !res.capture) {
        this.#report(errorText(res.errors) || 'could not stop the capture');
        void this.poll();
        return;
      }
      this.status = res.capture;
      if (!settled(res.capture)) this.#timer = setTimeout(() => void this.poll(), 1000);
    } catch (e) {
      this.#report(errorMessage(e));
      void this.poll();
    }
  }

  /** Let the leader forget the session. Every worker lets go of it on its next
   * beat, not at its deadline. */
  async discard() {
    const session = this.release();
    if (session) await discardCapture(session).catch(() => {});
  }

  /** Stop following the session without discarding it: saved, it is the
   * leader's to forget. Answers the session it held. */
  release(): string {
    this.stopPolling();
    const session = this.session;
    this.session = '';
    this.status = null;
    return session;
  }

  /** The owner is gone: nothing reaches the session any more, so it is discarded. */
  destroy() {
    this.#destroyed = true;
    void this.discard();
  }
}
