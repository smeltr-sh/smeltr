// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mount, tick, unmount } from 'svelte';
import PipelineEditor from './PipelineEditor.svelte';
import { setBranch, setScope, SHARED_SCOPE } from './api';
import { dropAll } from './edits';
import { load, signedOut } from './session';

let editor: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
let asked: string[];
/** The body of each preview asked for. */
let previews: Record<string, unknown>[];
/** The capture the leader holds for the viewer, if any. */
let mine: unknown;

// happy-dom has no popovers, which the sample menu opens in.
HTMLElement.prototype.showPopover ??= function () {};
HTMLElement.prototype.hidePopover ??= function () {};

beforeEach(async () => {
  setBranch('pipeline-viewer-test');
  dropAll();
  asked = [];
  previews = [];
  mine = undefined;
  target = document.createElement('div');
  document.body.append(target);
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string, init?: RequestInit) => {
      const url = new URL(input, 'http://localhost');
      asked.push(url.pathname);
      let answer: unknown;
      if (url.pathname === '/api/captures/mine') {
        answer = { ok: true, errors: [], capture: mine };
        return new Response(JSON.stringify(answer), { status: 200 });
      }
      if (url.pathname === '/api/captures/c1/download') {
        return new Response('{"_raw":"one"}\n{"_raw":"two"}\n', { status: 200 });
      }
      if (url.pathname === '/api/auth/session') {
        answer = {
          authenticated: true,
          administrator: false,
          mustChangePassword: false,
          access: { administrator: false, areas: { shared: 'viewer' } },
        };
      } else if (url.pathname === '/api/areas/shared') {
        answer = {
          ok: true,
          config: {
            pipelines: [{ id: 'audit', name: 'Audit', steps: [] }],
            lookupTables: [],
          },
          errors: [],
        };
      } else if (url.pathname === '/api/worker-groups') {
        answer = { ok: true, groups: [], errors: [] };
      } else if (url.pathname.endsWith('/connections')) {
        answer = { ok: true, errors: [], upstream: [], downstream: [], groups: [], drawnIn: [] };
      } else if (url.pathname === '/api/areas/shared/pipelines/audit') {
        answer = { ok: true, pipeline: { id: 'audit', name: 'Audit', steps: [] }, errors: [] };
      } else if (url.pathname === '/api/areas/shared/samples') {
        answer = { ok: true, samples: [], errors: [] };
      } else if (url.pathname === '/api/areas/shared/tests') {
        answer = { ok: true, tests: [], errors: [] };
      } else if (url.pathname === '/api/areas/shared/preview/pipeline') {
        previews.push(JSON.parse(init!.body as string));
        answer = { ok: true, stage: null, errors: [], rows: [], parsers: [] };
      } else {
        // What an editor alone may ask for.
        return new Response(JSON.stringify({ ok: false, error: 'this needs editor in shared' }), {
          status: 403,
        });
      }
      return new Response(JSON.stringify(answer), { status: 200 });
    }),
  );
  await load();
  location.hash = '#/build/pipelines/audit';
});

afterEach(async () => {
  if (editor) await unmount(editor);
  editor = undefined;
  target.remove();
  dropAll();
  setScope(SHARED_SCOPE);
  signedOut();
  location.hash = '';
  vi.unstubAllGlobals();
});

describe('a viewer in the pipeline editor', () => {
  it('reads the samples and runs the preview, and is offered nothing that saves', async () => {
    editor = mount(PipelineEditor, { target, props: { id: 'audit' } });
    await vi.waitFor(() => expect(target.querySelector('#pipeline-name')).not.toBeNull());
    await new Promise((resolve) => setTimeout(resolve, 600));
    await tick();
    expect(asked).toContain('/api/areas/shared/samples');
    expect(asked).toContain('/api/areas/shared/preview/pipeline');
    expect(target.querySelector('p.error')).toBeNull();
    for (const saves of ['Run now', 'Lock this as a test', 'Paste or file'])
      expect(target.textContent).not.toContain(saves);
  });

  it('runs over the capture the leader holds for them, unsaved', async () => {
    mine = {
      session: 'c1',
      done: true,
      stopped: false,
      maxMatches: 100,
      workers: [],
      batches: [{ columns: [], rows: [{ raw: 'one' }, { raw: 'two' }] }],
      ended: [],
      secondsLeft: 0,
      waitingOn: [],
      truncated: false,
      avro: [],
    };
    editor = mount(PipelineEditor, { target, props: { id: 'audit' } });
    await vi.waitFor(() => expect(target.querySelector('#pipeline-name')).not.toBeNull());
    const button = (name: string) =>
      [...target.querySelectorAll('button')].find((b) => b.textContent?.includes(name));
    await vi.waitFor(() => expect(button('Sample')).toBeDefined());
    button('Sample')!.click();
    await vi.waitFor(() => expect(button('Capture')).toBeDefined());
    button('Capture')!.click();
    await vi.waitFor(() => expect(button('Your capture')).toBeDefined());
    expect(button('Your capture')!.textContent).toContain('2 records');
    button('Your capture')!.click();
    await vi.waitFor(() => expect(previews.some((p) => p.capture === 'c1')).toBe(true));
    const used = previews.find((p) => p.capture === 'c1')!;
    expect(used.sample).toBeNull();
    expect(asked).toContain('/api/captures/c1/download');
    expect(target.textContent).not.toContain('Lock this as a test');
  });

  it('offers the raw JSON, read and not saved', async () => {
    editor = mount(PipelineEditor, { target, props: { id: 'audit' } });
    await vi.waitFor(() => expect(target.querySelector('#pipeline-name')).not.toBeNull());
    const raw = [...target.querySelectorAll('button')].find(
      (b) => b.textContent?.trim() === 'Raw JSON',
    );
    expect(raw).toBeDefined();
    await unmount(editor);
    location.hash = '#/build/pipelines/audit/json';
    target.replaceChildren();
    editor = mount(PipelineEditor, { target, props: { id: 'audit', showJson: true } });
    await vi.waitFor(() => expect(target.querySelector('.json-editor')).not.toBeNull());
    expect((target.querySelector('.json-editor') as HTMLTextAreaElement).readOnly).toBe(true);
    expect(
      [...target.querySelectorAll('button')].some((b) => b.textContent?.trim() === 'Save'),
    ).toBe(false);
  });
});
