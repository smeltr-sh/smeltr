// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mount, unmount } from 'svelte';
import PipelineEditor from './PipelineEditor.svelte';
import { setBranch, setScope, SHARED_SCOPE } from './api';
import { dropAll } from './edits';
import { load, signedOut } from './session';

let editor: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
/** The sample each preview ran over. */
let ranOver: (string | null)[];
/** The route, as App hands it to the one editor it keeps across a change of pipeline. */
const route = $state<{ id: string; sample?: string }>({ id: 'a' });

beforeEach(async () => {
  setBranch('pipeline-sample-test');
  dropAll();
  ranOver = [];
  const kept = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (k: string) => kept.get(k) ?? null,
    setItem: (k: string, v: string) => void kept.set(k, v),
    removeItem: (k: string) => void kept.delete(k),
  });
  target = document.createElement('div');
  document.body.append(target);
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string, init?: RequestInit) => {
      const url = new URL(input, 'http://localhost');
      const path = url.pathname;
      const pipeline = /^\/api\/areas\/shared\/pipelines\/([^/]+)$/.exec(path)?.[1];
      const sample = /^\/api\/areas\/shared\/samples\/([^/]+)$/.exec(path)?.[1];
      let answer: unknown;
      if (path === '/api/auth/session') {
        answer = {
          authenticated: true,
          administrator: true,
          mustChangePassword: false,
          access: { administrator: true, areas: {} },
        };
      } else if (pipeline) {
        answer = { ok: true, pipeline: { id: pipeline, name: pipeline, steps: [] }, errors: [] };
      } else if (path === '/api/areas/shared/samples') {
        answer = {
          ok: true,
          samples: ['s1', 's2', 's3'].map((id) => ({ id, name: id, records: 1 })),
          errors: [],
        };
      } else if (sample) {
        answer = { ok: true, sample: { id: sample, name: sample, records: [{ _raw: sample }] } };
      } else if (path === '/api/areas/shared/preview/pipeline') {
        ranOver.push(JSON.parse(init!.body as string).sample ?? null);
        answer = { ok: true, stage: null, errors: [], rows: [], parsers: [] };
      } else if (path === '/api/areas/shared') {
        answer = { ok: true, config: { pipelines: [], lookupTables: [] }, errors: [] };
      } else if (path === '/api/areas/shared/tests') {
        answer = { ok: true, tests: [], errors: [] };
      } else if (path === '/api/worker-groups') {
        answer = { ok: true, groups: [], errors: [] };
      } else if (path.endsWith('/connections')) {
        answer = { ok: true, errors: [], upstream: [], downstream: [], groups: [], drawnIn: [] };
      } else {
        answer = { ok: false, errors: [] };
      }
      return new Response(JSON.stringify(answer), { status: 200 });
    }),
  );
  await load();
});

afterEach(async () => {
  if (editor) await unmount(editor);
  editor = undefined;
  target.remove();
  dropAll();
  setScope(SHARED_SCOPE);
  signedOut();
  vi.unstubAllGlobals();
});

/** The sample the editor's last preview ran over, once it ran. */
async function lastRan(sample: string | null) {
  await vi.waitFor(() => expect(ranOver.at(-1)).toBe(sample), { timeout: 3000 });
}

describe('the sample a pipeline opens on', () => {
  it('follows the pipeline when the editor is kept across a change of route', async () => {
    Object.assign(route, { id: 'a', sample: 's1' });
    editor = mount(PipelineEditor, {
      target,
      props: {
        get id() {
          return route.id;
        },
        get sample() {
          return route.sample;
        },
      },
    });
    await lastRan('s1');

    // The link's sample, not the one the previous pipeline chose.
    Object.assign(route, { id: 'c', sample: 's3' });
    await lastRan('s3');
    Object.assign(route, { id: 'b', sample: 's2' });
    await lastRan('s2');

    // No sample in the link: the one this pipeline last ran over.
    Object.assign(route, { id: 'c', sample: undefined });
    await lastRan('s3');
    Object.assign(route, { id: 'b', sample: 'gone' });
    await lastRan('s2');

    // Neither: none, so no preview runs over the previous pipeline's sample.
    ranOver = [];
    Object.assign(route, { id: 'd', sample: undefined });
    await vi.waitFor(() => expect(target.querySelector('#pipeline-name')).not.toBeNull());
    await new Promise((resolve) => setTimeout(resolve, 600));
    expect(ranOver.filter((s) => s !== null)).toEqual([]);
  });
});
