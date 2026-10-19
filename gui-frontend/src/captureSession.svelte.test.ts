import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { CaptureStatus } from './api';

const api = vi.hoisted(() => ({
  captureStatus: vi.fn(),
  startCapture: vi.fn(),
  stopCapture: vi.fn(),
  discardCapture: vi.fn(),
}));

vi.mock('./api', () => ({
  ...api,
  errorMessage: (e: unknown) => (e instanceof Error ? e.message : String(e)),
  errorText: (errors: { message: string }[]) => errors.map((e) => e.message).join('; '),
}));

const { CaptureSession, POLL_RETRY_MS } = await import('./captureSession.svelte');

function status(over: Partial<CaptureStatus> = {}): CaptureStatus {
  return {
    session: 's1',
    done: false,
    stopped: false,
    maxMatches: 10,
    workers: [{ worker: 'w-a', state: 'filling' }],
    batches: [],
    ended: [],
    secondsLeft: 20,
    waitingOn: ['w-a'],
    truncated: false,
    ...over,
  };
}

const finished = status({ done: true, workers: [{ worker: 'w-a', state: 'answered' }] });

describe('CaptureSession', () => {
  let errors: string[];
  beforeEach(() => {
    vi.useFakeTimers();
    errors = [];
    api.captureStatus.mockReset();
    api.startCapture.mockReset();
    api.stopCapture.mockReset();
    api.discardCapture.mockReset().mockResolvedValue({ ok: true });
  });
  afterEach(() => vi.useRealTimers());

  it('polls until nothing about the capture can change', async () => {
    api.captureStatus
      .mockResolvedValueOnce({ ok: true, capture: status() })
      .mockResolvedValueOnce({ ok: true, capture: finished });
    const c = new CaptureSession((e) => errors.push(e));
    c.follow('s1');
    await vi.advanceTimersByTimeAsync(0);
    expect(c.status?.done).toBe(false);
    await vi.advanceTimersByTimeAsync(1000);
    expect(c.status).toEqual(finished);
    await vi.advanceTimersByTimeAsync(5000);
    expect(api.captureStatus).toHaveBeenCalledTimes(2);
    expect(errors).toEqual([]);
  });

  it('starts a capture and follows the session the leader opened', async () => {
    api.startCapture.mockResolvedValueOnce({ ok: true, session: 's1' });
    api.captureStatus.mockResolvedValue({ ok: true, capture: finished });
    const c = new CaptureSession((e) => errors.push(e));
    const body = { group: 'east', destination: 'search', filter: 'a == 1' };
    expect(await c.start(body)).toBe(true);
    expect(api.startCapture).toHaveBeenCalledWith(body);
    expect(c.session).toBe('s1');
    expect(errors).toEqual([]);
  });

  it('says why the leader refused a start, and follows nothing', async () => {
    api.startCapture.mockResolvedValueOnce({ ok: false, errors: [{ message: 'bad filter' }] });
    const c = new CaptureSession((e) => errors.push(e));
    expect(await c.start({ group: 'east', destination: 'search' })).toBe(false);
    expect(c.session).toBe('');
    expect(errors).toEqual(['bad filter']);
  });

  it('discards a session that arrives after its owner is gone', async () => {
    let answer!: (v: unknown) => void;
    api.startCapture.mockReturnValueOnce(new Promise((r) => (answer = r)));
    const c = new CaptureSession((e) => errors.push(e));
    const started = c.start({ group: 'east', destination: 'search' });
    c.destroy();
    answer({ ok: true, session: 's1' });
    expect(await started).toBe(false);
    expect(api.discardCapture).toHaveBeenCalledWith('s1');
    expect(api.captureStatus).not.toHaveBeenCalled();
  });

  it('lets go of a session the leader forgot, and says why', async () => {
    api.captureStatus.mockResolvedValueOnce({
      ok: false,
      errors: [{ message: 'no such capture' }],
    });
    const c = new CaptureSession((e) => errors.push(e));
    c.follow('s1');
    await vi.advanceTimersByTimeAsync(0);
    expect(c.session).toBe('');
    expect(c.status).toBeNull();
    expect(errors).toEqual(['no such capture']);
  });

  it('retries a running capture through network errors, then gives up', async () => {
    api.captureStatus.mockRejectedValue(new Error('offline'));
    const c = new CaptureSession((e) => errors.push(e));
    c.follow('s1');
    for (const ms of [0, ...POLL_RETRY_MS]) await vi.advanceTimersByTimeAsync(ms);
    expect(api.captureStatus).toHaveBeenCalledTimes(POLL_RETRY_MS.length + 1);
    expect(c.session).toBe('');
    expect(errors).toEqual(['lost contact with the leader: offline']);
  });

  it('shows what a stop answers, and drops a poll that was in flight', async () => {
    let answer: (v: unknown) => void = () => {};
    api.captureStatus.mockReturnValueOnce(new Promise((r) => (answer = r)));
    api.stopCapture.mockResolvedValueOnce({ ok: true, capture: finished });
    const c = new CaptureSession((e) => errors.push(e));
    c.follow('s1');
    await c.stop();
    answer({ ok: true, capture: status() });
    await vi.advanceTimersByTimeAsync(0);
    expect(c.status).toEqual(finished);
  });

  it('discards the session when its owner goes, and one that arrives after', async () => {
    api.captureStatus.mockResolvedValue({ ok: true, capture: status() });
    const c = new CaptureSession((e) => errors.push(e));
    c.follow('s1');
    c.destroy();
    expect(api.discardCapture).toHaveBeenCalledWith('s1');
    expect(c.follow('s2')).toBe(false);
    expect(api.discardCapture).toHaveBeenCalledWith('s2');
    expect(c.session).toBe('');
  });

  it('keeps a released session: it was saved, so it is not discarded', () => {
    api.captureStatus.mockResolvedValue({ ok: true, capture: status() });
    const c = new CaptureSession((e) => errors.push(e));
    c.follow('s1');
    expect(c.release()).toBe('s1');
    c.destroy();
    expect(api.discardCapture).not.toHaveBeenCalled();
  });
});
