// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mount, tick, unmount } from 'svelte';
import PipelineEditor from './PipelineEditor.svelte';
import { setBranch, setScope, SHARED_SCOPE, type ApiError, type PipelineDto } from './api';
import { dropAll, hold } from './edits';
import { load, signedOut } from './session';
import { bareError, errors } from './errors.svelte';

let editor: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
let pipelines: Map<string, PipelineDto>;
let writes: PipelineDto[];
/** Set to refuse every save, with the errors it is given the saved id for. */
let refuse: ((id: string) => ApiError[]) | null;
/** What `shared` holds, answered when the editor's scope is a group and it reads `shared` beside its own. */
let sharedConfig: { pipelines: PipelineDto[]; lookupTables: unknown[] } | null;

beforeEach(async () => {
  errors.entries = [];
  setBranch('pipeline-identity-test');
  dropAll();
  location.hash = '';
  pipelines = new Map();
  writes = [];
  refuse = null;
  sharedConfig = null;
  target = document.createElement('div');
  document.body.append(target);
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string, init?: RequestInit) => {
      const url = new URL(input, 'http://localhost');
      let answer: unknown;
      // The editor offers saving only to who may save here (`access.ts`).
      if (url.pathname === '/api/auth/session') {
        answer = {
          authenticated: true,
          administrator: true,
          mustChangePassword: false,
          access: { administrator: true, areas: {} },
        };
      } else if (url.pathname === `/api/areas/${SHARED_SCOPE}` && sharedConfig) {
        answer = { ok: true, config: sharedConfig, errors: [] };
      } else if (/^\/api\/areas\/[^/]+$/.test(url.pathname)) {
        answer = {
          ok: true,
          config: { pipelines: [...pipelines.values()], lookupTables: [] },
          errors: [],
        };
      } else if (url.pathname === '/api/worker-groups') {
        answer = { ok: true, groups: [], errors: [] };
      } else if (/^\/api\/areas\/[^/]+\/samples$/.test(url.pathname)) {
        answer = { ok: true, samples: [], errors: [] };
      } else if (url.pathname.endsWith('/connections')) {
        answer = { ok: true, errors: [], upstream: [], downstream: [], groups: [], drawnIn: [] };
      } else if (/^\/api\/areas\/[^/]+\/pipelines\//.test(url.pathname)) {
        const id = decodeURIComponent(url.pathname.split('/pipelines/')[1]);
        if (init?.method === 'PUT' && refuse) {
          answer = { ok: false, errors: refuse(id) };
        } else if (init?.method === 'PUT') {
          const pipeline = JSON.parse(init.body as string) as PipelineDto;
          expect(pipeline.id).toBe(id);
          writes.push(pipeline);
          pipelines.set(id, pipeline);
          answer = { ok: true, errors: [], commit: `saved-${writes.length}` };
        } else {
          answer = { ok: true, pipeline: pipelines.get(id), errors: [] };
        }
      } else {
        throw new Error(`unexpected request: ${url.pathname}`);
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

async function open(id: string, showJson = false) {
  if (editor) await unmount(editor);
  target.replaceChildren();
  editor = mount(PipelineEditor, { target, props: { id, showJson } });
  await vi.waitFor(() =>
    expect(target.querySelector(showJson ? '.json-editor' : '#pipeline-name')).not.toBeNull(),
  );
  await tick();
}

async function save() {
  const count = writes.length;
  const button = [...target.querySelectorAll('button')].find(
    (button) => button.textContent?.trim() === 'Save',
  );
  expect(button).toBeDefined();
  button!.click();
  await vi.waitFor(() => expect(writes).toHaveLength(count + 1));
  await tick();
}

/** A new pipeline holding one comment step, put back from a held draft the
 * way a return to the page puts one back. */
async function openNewWithAStep() {
  hold('pipelines/new', {
    value: JSON.stringify({
      pipelineName: 'New pipeline',
      steps: [{ type: 'comment', id: 'c1', text: 'hello' }],
    }),
    signature: 'held',
    baseline: 'New pipeline::[]',
    readAtCommit: null,
  });
  await open('new');
}

function stepError(pipeline: string): ApiError {
  return { ...bareError('step is broken'), pipeline, step: 'c1' };
}

async function refusedSave() {
  const button = [...target.querySelectorAll('button')].find(
    (button) => button.textContent?.trim() === 'Save',
  );
  button!.click();
  await vi.waitFor(() => expect(saveEntry()).toBeDefined());
  await tick();
}

const saveEntry = () =>
  errors.entries.find((e) => e.key.startsWith('PUT /api/areas/') && e.key.includes('/pipelines/'));

/** What the error bar draws for the refused save: what no step takes, and the steps holding the rest. */
function barShows() {
  const entry = saveEntry()!;
  return {
    loose: entry.errors.map((e) => e.message),
    onSteps: (entry.placed?.() ?? []).map((p) => p.message),
  };
}

describe('save errors on a new pipeline', () => {
  it('lands an error naming the id its first save wrote on that step', async () => {
    refuse = (id) => [stepError(id)];
    await openNewWithAStep();
    await refusedSave();
    expect(barShows()).toEqual({ loose: [], onSteps: ['step is broken'] });
    expect(target.querySelector('.step-error-msg')?.textContent).toContain('step is broken');
  });

  it('keeps an error naming a pipeline by its name off the step', async () => {
    refuse = () => [stepError('New pipeline')];
    await openNewWithAStep();
    await refusedSave();
    expect(barShows()).toEqual({ loose: ['step is broken'], onSteps: [] });
    expect(target.querySelector('.step-error-msg')).toBeNull();
  });
});

describe('pipeline identity in the mounted editor', () => {
  it('saves two untouched New pipelines as separate objects and preserves the first', async () => {
    await open('new');
    expect((target.querySelector('#pipeline-name') as HTMLInputElement).value).toBe('New pipeline');
    await save();
    const first = structuredClone(writes[0]);
    await open('new');
    await save();
    expect(writes[1].name).toBe(first.name);
    expect(writes[1].id).not.toBe(first.id);
    expect(pipelines.size).toBe(2);
    expect(pipelines.get(first.id)).toEqual(first);
    expect(writes[1].steps).toEqual([]);
  });

  it('keeps an existing ID when the display name changes', async () => {
    pipelines.set('legacy-id', { id: 'legacy-id', name: 'Original', steps: [] });
    await open('legacy-id');
    const name = target.querySelector('#pipeline-name') as HTMLInputElement;
    name.value = 'Renamed';
    name.dispatchEvent(new Event('input', { bubbles: true }));
    await tick();
    await save();
    expect(writes[0]).toEqual({ id: 'legacy-id', name: 'Renamed', steps: [] });
    expect(pipelines.size).toBe(1);
  });

  it('keeps the existing ID when JSON changes the name or proposes another ID', async () => {
    pipelines.set('legacy-id', { id: 'legacy-id', name: 'Original', steps: [] });
    await open('legacy-id', true);
    const json = target.querySelector('.json-editor') as HTMLTextAreaElement;
    json.value = JSON.stringify({ id: 'different-id', name: 'Renamed in JSON', steps: [] });
    json.dispatchEvent(new Event('input', { bubbles: true }));
    await tick();
    await save();
    expect(writes[0]).toEqual({ id: 'legacy-id', name: 'Renamed in JSON', steps: [] });
    expect(pipelines.size).toBe(1);
  });
});

describe('a group pipeline drawing on shared', () => {
  it('offers shared pipelines and tables, and reads no shared table as unknown', async () => {
    setScope('west');
    sharedConfig = {
      pipelines: [{ id: 'enrich', name: 'Enrich', steps: [] }],
      lookupTables: [
        { id: 'geo', type: 'csv', path: 'lookups/geo.csv', delimiter: ',', headers: [] },
      ],
    };
    pipelines.set('audit', {
      id: 'audit',
      name: 'Audit',
      steps: [
        { id: 'call-enrich', type: 'call', pipeline: 'enrich' },
        { id: 'l', type: 'lookup', input: 'ip', table: 'geo', matchKind: 'exact', outputs: [] },
      ],
    } as PipelineDto);
    const fetched = vi.mocked(fetch);
    await open('audit');
    await vi.waitFor(() =>
      expect(
        fetched.mock.calls.some(([input]) =>
          String(input).startsWith(`/api/areas/${SHARED_SCOPE}?`),
        ),
      ).toBe(true),
    );
    await tick();
    expect(target.textContent).not.toContain('unknown lookup table');
  });
});
