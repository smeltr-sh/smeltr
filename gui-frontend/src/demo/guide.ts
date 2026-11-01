// The demo's guide: the tasks a visitor works through, each ticked when what it asks for is there.
//
// The guide reads the visitor's work as the GUI does, through `/api/*`, which the demo's leader answers in the tab
// (`leader.ts`). It reads the branch in hand: a task is done on the branch the visitor is on.

/** What the guide reads to tick its tasks. */
export interface World {
  /** Whether the visitor has opened Status. */
  sawStatus: boolean;
  branch: string;
  /** The network group's bundle on `branch`. */
  config: {
    routes?: { source?: string; pipeline?: string | null }[];
    pipelines?: { id: string; steps?: { declared?: { name: string; type: unknown }[] }[] }[];
  } | null;
  /** The network group's samples on `branch`. */
  samples: { source?: string | null }[];
  /** The ids of the network group's tests on `branch`, and on `main`. */
  tests: string[];
  mainTests: string[];
  /** The ids of the network group's pipelines on `main`. */
  mainPipelines: string[];
}

/** A control a task is done with: the page it is on, by the start of its hash, and a selector for it. */
export interface Step {
  on: string;
  at: string;
  /** Point at the last match on screen, not the first: the newest row of a list. */
  last?: boolean;
  /** What to do there, shown under the task while it is pointed at, with something to copy. */
  hint?: { text: string; copy: string };
}

export interface Task {
  id: string;
  text: string;
  /** Where in the GUI the task is done, and in which scope when it is a group's. */
  hash: string;
  scope?: string;
  /** Its controls, in the order the visitor reaches them. */
  steps: Step[];
  done: (world: World) => boolean;
}

const MAIN = 'main';
/** The group the two problems are in. */
const GROUP = 'network';
/** The source no route takes, and the pipeline whose declared `proto` the fleet's records break. */
const UNROUTED = 'cisco-asa';
const DRIFTED = 'fortigate-traffic';
const MAP = `#/map?group=${GROUP}&kind=source&id=${UNROUTED}`;
const PIPELINE = `#/build/pipelines/${DRIFTED}`;
const guide = (name: string) => `[data-guide="${name}"]`;

/** The type `DRIFTED` declares `proto` as, or undefined when it declares none. */
function protoType(world: World): unknown {
  const pipeline = world.config?.pipelines?.find((p) => p.id === DRIFTED);
  for (const step of pipeline?.steps ?? []) {
    const proto = step.declared?.find((d) => d.name === 'proto');
    if (proto) return proto.type;
  }
  return undefined;
}

export const TASKS: Task[] = [
  {
    id: 'status',
    text: 'Open Status and find the issues for cisco-asa and fortigate-traffic',
    hash: '#/status/issues',
    steps: [],
    done: (w) => w.sawStatus,
  },
  {
    id: 'branch',
    text: 'Start a branch for your fix',
    hash: '#/changes',
    steps: [
      { on: '#/changes', at: guide('branch-start') },
      { on: '#/changes', at: guide('branch-open'), last: true },
    ],
    done: (w) => w.branch !== MAIN,
  },
  {
    id: 'capture',
    text: `Capture a sample of ${UNROUTED}`,
    hash: MAP,
    scope: GROUP,
    steps: [
      { on: '#/map', at: guide('capture') },
      { on: '#/map', at: guide('capture-start') },
      { on: '#/map', at: guide('capture-stop') },
      { on: '#/map', at: guide('capture-save') },
    ],
    done: (w) => w.branch !== MAIN && w.samples.some((s) => s.source === UNROUTED),
  },
  {
    id: 'pipeline',
    text: `Build a pipeline for ${UNROUTED}`,
    hash: MAP,
    scope: GROUP,
    steps: [
      { on: '#/map', at: guide('add-route') },
      { on: '#/build/routes', at: guide('pipeline-pick') },
      // A new pipeline opens on a capture of its source.
      { on: '#/build/pipelines/new', at: guide('capture-start') },
      { on: '#/build/pipelines/new', at: guide('capture-stop') },
      { on: '#/build/pipelines/new', at: guide('capture-save') },
      { on: '#/build/pipelines/new', at: guide('save') },
    ],
    done: (w) =>
      w.branch !== MAIN && !!w.config?.pipelines?.some((p) => !w.mainPipelines.includes(p.id)),
  },
  {
    id: 'route',
    text: `Route ${UNROUTED} to its pipeline`,
    hash: '#/build/routes',
    scope: GROUP,
    steps: [
      { on: '#/build/routes', at: guide('add-route') },
      { on: '#/build/routes', at: guide('save') },
    ],
    done: (w) =>
      w.branch !== MAIN && !!w.config?.routes?.some((r) => r.source === UNROUTED && !!r.pipeline),
  },
  {
    id: 'proto',
    text: 'Fix the proto field in the FortiGate pipeline',
    hash: PIPELINE,
    scope: GROUP,
    steps: [
      { on: PIPELINE, at: guide('parser') },
      { on: PIPELINE, at: '[aria-label="proto type"]' },
      { on: PIPELINE, at: guide('save') },
      // A text proto breaks what compares it to a number, and Save refuses until that is fixed too.
      {
        on: PIPELINE,
        at: guide('assignment-error'),
        hint: {
          text: 'Singapore sends proto as a name, so compare it as text:',
          copy: 'proto == "6" ? "tcp" : proto == "17" ? "udp" : proto',
        },
      },
    ],
    done: (w) => {
      const type = protoType(w);
      return w.branch !== MAIN && type !== undefined && type !== 'i64';
    },
  },
  {
    id: 'test',
    text: 'Lock the result as a test',
    hash: PIPELINE,
    scope: GROUP,
    steps: [
      { on: PIPELINE, at: guide('samples') },
      { on: PIPELINE, at: guide('lock-test') },
      { on: '#/build/tests/lock', at: guide('lock-name') },
      { on: '#/build/tests/lock', at: guide('lock') },
    ],
    done: (w) => w.branch !== MAIN && w.tests.some((t) => !w.mainTests.includes(t)),
  },
];

/** The control to point at for `task` on the page at `hash`: the furthest of its steps on screen that takes a click,
 * and inside the open dialog when there is one, since nothing behind it does. */
export function stepOnScreen(
  task: Task,
  hash: string,
  doc: Document = document,
): { step: Step; el: Element } | null {
  const front = doc.querySelector('dialog[open]') ?? doc;
  for (const step of [...task.steps].reverse()) {
    if (!hash.startsWith(step.on)) continue;
    const shown = [...front.querySelectorAll(step.at)].filter(
      (el) => el.getClientRects().length > 0 && !el.matches(':disabled'),
    );
    const el = step.last ? shown.at(-1) : shown[0];
    if (el) return { step, el };
  }
  return null;
}

const SEEN_KEY = 'smeltr-demo-guide-status';

/** Whether the visitor has opened Status in this tab; opening it now counts. */
export function sawStatus(hash: string, store: Storage | null): boolean {
  try {
    if (hash.startsWith('#/status')) store?.setItem(SEEN_KEY, '1');
    return store?.getItem(SEEN_KEY) === '1';
  } catch {
    return hash.startsWith('#/status');
  }
}

async function get<T>(path: string, q: Record<string, string>): Promise<T | null> {
  try {
    const res = await fetch(`${path}?${new URLSearchParams(q)}`);
    return res.ok ? ((await res.json()) as T) : null;
  } catch {
    return null;
  }
}

/** What the guide needs of `branch`, read through the API. */
export async function look(branch: string, seen: boolean): Promise<World> {
  const area = `/api/areas/${GROUP}`;
  const [config, mainConfig, samples, tests, mainTests] = await Promise.all([
    get<{ config?: World['config'] }>(area, { branch }),
    get<{ config?: World['config'] }>(area, { branch: MAIN }),
    get<{ samples?: World['samples'] }>(`${area}/samples`, { branch }),
    get<{ tests?: { id: string }[] }>(`${area}/tests`, { branch }),
    get<{ tests?: { id: string }[] }>(`${area}/tests`, { branch: MAIN }),
  ]);
  return {
    sawStatus: seen,
    branch,
    config: config?.config ?? null,
    samples: samples?.samples ?? [],
    tests: (tests?.tests ?? []).map((t) => t.id),
    mainTests: (mainTests?.tests ?? []).map((t) => t.id),
    mainPipelines: (mainConfig?.config?.pipelines ?? []).map((p) => p.id),
  };
}

/** What a write the demo's leader answered tells the guide: the visitor's work may have changed. */
export const CHANGED_EVENT = 'smeltr-demo:changed';
