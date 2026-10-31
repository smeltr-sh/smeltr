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

beforeEach(async () => {
  setBranch('pipeline-viewer-test');
  dropAll();
  asked = [];
  target = document.createElement('div');
  document.body.append(target);
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string) => {
      const url = new URL(input, 'http://localhost');
      asked.push(url.pathname);
      let answer: unknown;
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
      } else {
        // What an editor alone may ask for: samples and previews.
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
  it('reads the steps and asks for no sample or preview', async () => {
    editor = mount(PipelineEditor, { target, props: { id: 'audit' } });
    await vi.waitFor(() => expect(target.querySelector('#pipeline-name')).not.toBeNull());
    await new Promise((resolve) => setTimeout(resolve, 600));
    await tick();
    expect(asked.filter((p) => /^\/api\/areas\/[^/]+\/(samples|preview)/.test(p))).toEqual([]);
    expect(target.querySelector('#sample')).toBeNull();
    expect(target.querySelector('p.error')).toBeNull();
    expect(target.textContent).not.toContain('Run now');
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
