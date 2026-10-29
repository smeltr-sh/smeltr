// What an administrator does, from a fresh install to rows flowing and back, as moves the journey fuzzer
// (`specs/journey-fuzz.spec.ts`) strings together. Each move is done through the GUI, by role and label, and then
// checked against what is true outside the page: the leader's API, the worker it started, the rows at the sink.
// `World` is what the moves so far have left the leader holding; a move is offered only when it makes sense there.
import fs from 'node:fs';
import path from 'node:path';
import type { Browser, Locator, Page } from '@playwright/test';
import {
  expect,
  goto,
  settled,
  signInThroughGui,
  type Api,
  type Fleet,
  type Leader,
} from '../fleet';
import type { Rng } from '../fuzz';
import { MODEL_CONTEXT, MODEL_KEY, request, say, type FakeModel } from '../model';
import { CLIENT_ID, CLIENT_SECRET, type Provider } from '../oidc';
import type { FakeElasticsearch } from '../sinks';
import type { Authority } from '../acme';
import {
  openAdmin,
  openRowMenu,
  openWorkersTab,
  startWorkerFromLines,
  workerRow,
  workerRowOf,
} from './admin';
import { conversationLink, openAgent, pickModel } from './agent';
import { avro, avroFile, type FakeS3 } from '../buckets';
import {
  acmeNewRoot,
  renewAcme,
  refuseDns01,
  replaceCertificate,
  switchToAcme,
  switchToLeaderIssued,
  switchToUploaded,
  type Pki,
} from './certificates';
import { historyRow } from './change';

/** What one branch or deploy holds of what the moves add: the ids of sources, destinations, lookup tables, grok
 * patterns, schemas and worker groups, the names of pipelines and samples, each eval step as its pipeline and the field
 * it sets, each schema a pipeline produces as its pipeline and the schema's id, and each route by its source, its
 * pipeline's name and the destination it delivers to, if any. */
export type Config = {
  sources: string[];
  destinations: string[];
  pipelines: string[];
  steps: string[];
  routes: { source: string; pipeline: string; to: string | null }[];
  lookups: string[];
  grokPatterns: string[];
  schemas: string[];
  produces: string[];
  samples: string[];
  groups: string[];
};

const EMPTY: Config = {
  sources: [],
  destinations: [],
  pipelines: [],
  steps: [],
  routes: [],
  lookups: [],
  grokPatterns: [],
  schemas: [],
  produces: [],
  samples: [],
  groups: [],
};
const copy = (c: Config): Config => structuredClone(c);

export type World = {
  /** The administrator's own password, once chosen. */
  password: string | null;
  /** The branch the GUI edits on, once a save has named one; `null` on main. */
  branch: string | null;
  /** The branch exists on the leader: a save went onto it. */
  branchSaved: boolean;
  /** A save on the branch is one a deploy puts on main: anything but a sample, which no test names. */
  landing: boolean;
  /** What the branch holds, or main when there is none. */
  editing: Config;
  /** `main`'s commits that deployed something, oldest first, the install's own first. */
  deploys: { subject: string; config: Config }[];
  /** Which of `deploys` the fleet runs. */
  running: number;
  /** A rollback holds the fleet behind main. */
  held: boolean;
  /** The open change's title. */
  change: string | null;
  /** The change was closed, and not reopened. */
  closed: boolean;
  /** Each worker, and the group it was enrolled in. */
  workers: Map<string, { state: 'heartbeating' | 'revoked'; group: string }>;
  /** Each secret's newest version. */
  secrets: Map<string, number>;
  /** Each certificate added to the list, by name. */
  certificates: Map<string, Pki>;
  /** For names nothing has used yet. */
  made: number;
  /** When the administrator signed in, the API's own sign-in among them: the leader allows five a minute. */
  signIns: number[];
  /** Each local account the administrator added, by username, and whether it is enabled. */
  accounts: Map<string, boolean>;
  /** Each role, by the provider group it names. */
  roles: Set<string>;
  /** AI agents are turned on, against the journey's model endpoint. */
  agentsOn: boolean;
  /** The administrator's conversations with their agent, by title. */
  conversations: string[];
  /** Those summarized or cleared with nothing said since, which cannot be again until something is. */
  restarted: string[];
  /** Single sign-on is on, through the journey's provider. */
  sso: boolean;
  /** The provider's person has signed in, so the leader knows their groups. */
  ssoSignedIn: boolean;
  /** An administrator has found the provider's person's refused sign-in in the logs. */
  refusalSeen: boolean;
  /** The idle timeout the Sessions form last saved, in seconds. */
  idle: number | null;
  /** An uploaded certificate is in force, in place of the leader's own. */
  supplied: boolean;
  /** The certificate in force was ordered from the journey's certificate authority. It is supplied too. */
  acme: boolean;
  /** A certificate was ordered over ACME. The leader keeps it, sealed, after a switch away from it. */
  ordered: boolean;
};

export function newWorld(): World {
  return {
    password: null,
    branch: null,
    branchSaved: false,
    landing: false,
    editing: copy(EMPTY),
    deploys: [{ subject: 'worker group default', config: copy(EMPTY) }],
    running: 0,
    held: false,
    change: null,
    closed: false,
    workers: new Map(),
    secrets: new Map(),
    certificates: new Map(),
    made: 0,
    signIns: [],
    accounts: new Map(),
    roles: new Set(),
    agentsOn: false,
    conversations: [],
    restarted: [],
    sso: false,
    ssoSignedIn: false,
    refusalSeen: false,
    idle: null,
    supplied: false,
    acme: false,
    ordered: false,
  };
}

export type Journey = {
  page: Page;
  rng: Rng;
  leader: Leader;
  fleet: Fleet;
  sink: FakeElasticsearch;
  /** The fake S3 that holds `AVRO_OBJECTS`. */
  bucket: FakeS3;
  /** The OpenID provider single sign-on is set up against; it signs in one person of `PROVIDER_GROUP`. */
  provider: Provider;
  /** The model endpoint AI agents are turned on against. */
  model: FakeModel;
  /** The certificate authority the leader's certificate is ordered from over ACME. */
  authority: Authority;
  browser: Browser;
  world: World;
  /** The administrator over the API, for checking what the GUI did. It arranges nothing. */
  truth: () => Promise<Api>;
};

export type Move = {
  name: string;
  /** How often it is drawn here; 0 when it makes no sense. */
  weight: (w: World) => number;
  /** The moves it makes possible. */
  leadsTo?: readonly string[];
  /** Does it through the GUI and checks it. Answers what it drew. */
  run: (j: Journey) => Promise<string>;
};

/** A rate the GUI prints that is above zero. */
const MOVING = /(0\.\d*[1-9]|[1-9][\d.]*) rows\/s/;

/** The metrics sources the fleet runs with no route, which no edit has routed since. */
const streamless = (w: World) => {
  const running = w.deploys[w.running].config;
  return running.sources.filter(
    (s) =>
      s.startsWith('src-') &&
      !running.routes.some((r) => r.source === s) &&
      !w.editing.routes.some((r) => r.source === s),
  );
};

/** The workers heartbeating, in `group` where it is named. The journey's sources run in `default`, so a move that needs
 * a worker to read one needs one there. */
const heartbeating = (w: World, group?: string) =>
  [...w.workers]
    .filter(([, s]) => s.state === 'heartbeating' && (group === undefined || s.group === group))
    .map(([n]) => n);

/** The journey's workers share a host, so a receiver's port goes to one of them and the others keep what they ran.
 * A worker of another group holding it leaves `default`'s behind every deploy, so a journey has receivers or worker
 * groups, never both. */
const receiving = (w: World) =>
  [...w.editing.sources, ...w.deploys[w.running].config.sources].some((s) => s.startsWith('in-'));
const grouping = (w: World) =>
  w.editing.groups.length > 0 || w.deploys[w.running].config.groups.length > 0;

/** What `api` reads on `branch`, as `Config` names it: `shared/`'s, where a new browser edits, and the default
 * group's together. */
async function configOn(api: Api, branch: string): Promise<Config> {
  const c: Config = copy(EMPTY);
  const on = encodeURIComponent(branch);
  const names = new Map<string, string>();
  const routes: { source: string; pipeline: string; deliver?: { to: string[] }[] }[] = [];
  for (const scope of ['shared', 'default']) {
    const res = await api.get(`/api/areas/${scope}?branch=${on}`);
    c.sources.push(...res.config.sources.map((s: { id: string }) => s.id));
    c.destinations.push(...res.config.destinations.map((d: { id: string }) => d.id));
    for (const p of res.config.pipelines as {
      id: string;
      name: string;
      produces?: string[];
      steps: { type: string; assignments?: { target: string }[] }[];
    }[]) {
      c.pipelines.push(p.name);
      names.set(p.id, p.name);
      c.produces.push(...(p.produces ?? []).map((schema) => `${p.name}:${schema}`));
      for (const step of p.steps)
        if (step.type === 'eval') c.steps.push(`${p.name}:${step.assignments![0].target}`);
    }
    routes.push(...res.config.routes);
    c.lookups.push(...(res.config.lookupTables ?? []).map((t: { id: string }) => t.id));
    c.grokPatterns.push(...(res.config.grokPatterns ?? []).map((p: { id: string }) => p.id));
    c.schemas.push(...(res.config.schemas ?? []).map((s: { id: string }) => s.id));
    const samples: { name: string }[] = (await api.get(`/api/areas/${scope}/samples?branch=${on}`))
      .samples;
    c.samples.push(...samples.map((s) => s.name));
  }
  // A route names its pipeline by id; the moves know it by the name they gave it.
  c.routes = routes.map((r) => ({
    source: r.source,
    pipeline: names.get(r.pipeline) ?? r.pipeline,
    to: r.deliver?.[0]?.to?.[0] ?? null,
  }));
  const groups: { id: string }[] = (await api.get(`/api/worker-groups?branch=${on}`)).groups;
  c.groups.push(...groups.map((g) => g.id).filter((id) => id !== 'default'));
  return c;
}

/** `c` in an order of its own, so two that hold the same compare equal. */
function sorted(c: Config): Config {
  const each = Object.fromEntries(
    Object.entries(c).map(([k, v]) => [
      k,
      [...(v as unknown[])].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))),
    ]),
  );
  return each as Config;
}

/** Checks the branch the GUI edits holds what the world says. */
async function editingIsSaved(j: Journey) {
  const on = await configOn(await j.truth(), j.world.branch!);
  expect(sorted(on), `branch ${j.world.branch} holds what was saved`).toEqual(
    sorted(j.world.editing),
  );
}

/** Answers the GUI's ask for a branch, if it asks: an edit made on main names a new one. */
async function nameBranch(j: Journey) {
  const { page, world } = j;
  if (world.branch !== null) return;
  const prompt = page.getByRole('dialog', { name: 'branch required' });
  await expect(prompt).toBeVisible();
  const name = `edit-${world.made++}`;
  await prompt.getByLabel('branch to edit on').fill(name);
  await prompt.getByRole('button', { name: 'Edit on this branch' }).click();
  world.branch = name;
}

/** Clicks `button`, which saves, naming a branch if the GUI asks for one, and waits until `done`. A deploy leaves
 * out a `sample` saved. */
async function saveBy(j: Journey, button: Locator, done: () => Promise<void>, sample = false) {
  await button.click();
  await nameBranch(j);
  await done();
  await settled(j.page);
  j.world.branchSaved = true;
  if (!sample) j.world.landing = true;
}

/** Captures two rows with the capture `form`, long enough for a fleet only now deployed to send them, and saves
 * them as the sample `name`. `seen` checks the capture before it is saved, and `done` that the save landed. */
async function captureTwo(
  j: Journey,
  form: Locator,
  name: string,
  seen: (saving: Locator) => Promise<void>,
  done: (saving: Locator) => Promise<void>,
) {
  await form.getByRole('spinbutton', { name: 'records' }).fill('2');
  await form.getByRole('spinbutton', { name: 'seconds' }).fill('45');
  await form.getByRole('button', { name: 'Start' }).click();
  const saving = j.page.getByRole('dialog', { name: /Save the capture/ });
  await expect(saving).toBeVisible({ timeout: 60_000 });
  await seen(saving);
  await saving.getByLabel('Save as').fill(name);
  await saveBy(j, saving.getByRole('button', { name: 'Save sample' }), () => done(saving), true);
  j.world.editing.samples.push(name);
}

/** Saves what is on the page, naming a branch if the GUI asks for one, and waits until `done`. */
async function save(j: Journey, done: () => Promise<void>) {
  const { page, world, rng } = j;
  const saveButton = page.getByRole('button', { name: 'Save', exact: true });
  // Saying no first, as somebody who did not mean to, saves nothing; the edit stays to be saved.
  if (world.branch === null && rng.chance(4)) {
    await saveButton.click();
    const prompt = page.getByRole('dialog', { name: 'branch required' });
    await prompt.getByRole('button', { name: "Don't save" }).click();
    await expect(prompt).toBeHidden();
    expect((await (await j.truth()).get('/api/branches')).branches).toEqual([]);
  }
  await saveBy(j, saveButton, done);
}

/** Signs in with a username and password, choosing that way first where single sign-on is offered too. */
async function signInLocally(page: Page, world: World, username: string, password: string) {
  if (world.sso) await page.getByRole('button', { name: 'Sign in with username' }).click();
  await signInThroughGui(page, username, password);
}

// A save can take seconds on a busy machine, and the flag stays until it answers.
const unsaved = (page: Page) => () =>
  expect(page.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 30_000 });

const firstStart: Move = {
  name: 'first-start',
  weight: (w) => (w.password === null ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const password = `journey-${rng.below(1_000_000)}-password`;
    await page.goto(leader.url);
    await signInThroughGui(page, leader.username, leader.oneTimePassword);
    let drew = 'first start';
    // Two that differ are refused, and the form stays.
    if (rng.chance(3)) {
      await page.getByLabel('New password', { exact: true }).fill(password);
      await page.getByLabel('Confirm new password').fill(`${password}-not`);
      await page.getByRole('button', { name: 'Change password' }).click();
      await expect(page.getByRole('heading', { name: 'Choose your own password' })).toBeVisible();
      drew += ', a mismatch first';
    }
    await page.getByLabel('New password', { exact: true }).fill(password);
    await page.getByLabel('Confirm new password').fill(password);
    await page.getByRole('button', { name: 'Change password' }).click();
    await expect(page.getByRole('status')).toHaveText(
      'password changed -- sign in with your new one',
    );
    await signInThroughGui(page, leader.username, password);
    await expect(page.getByRole('navigation').first()).toBeVisible();
    world.password = password;
    await j.truth();
    world.signIns.push(Date.now(), Date.now(), Date.now());
    return drew;
  },
};

const signOutIn: Move = {
  name: 'sign-out-in',
  weight: (w) => (w.password ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    // The leader allows four sign-ins a minute. Past that the move waits rather than leaves the draw to the clock.
    const recent = world.signIns.filter((t) => Date.now() - t < 65_000).sort((a, b) => a - b);
    if (recent.length >= 4)
      await page.waitForTimeout(recent[recent.length - 4] + 65_000 - Date.now());
    let drew: string;
    if (rng.chance(2)) {
      await page.getByRole('button', { name: /^signed in as / }).click();
      await page.getByRole('button', { name: 'Sign out' }).click();
      drew = 'sign out and in';
    } else {
      await page.context().clearCookies();
      await page.reload();
      drew = 'lose the session, and sign in';
    }
    await expect(page.getByRole('heading', { name: 'Sign in to smeltr' })).toBeVisible();
    await signInLocally(page, world, leader.username, world.password!);
    world.signIns.push(Date.now());
    await expect(page.getByRole('navigation').first()).toBeVisible();
    // The branch in hand is the browser's, and outlives the session.
    await expect(page.getByRole('button', { name: /^Branch: / })).toHaveAccessibleName(
      new RegExp(`^Branch: ${world.branch ?? 'main'}\\b`),
    );
    return drew;
  },
};

const enrollWorker: Move = {
  name: 'enroll-worker',
  leadsTo: ['revoke-worker', 'rows-arrive'],
  // Up to two, and a third when none is left in `default`, where the journey's sources run.
  weight: (w) =>
    !w.password || heartbeating(w).length >= (heartbeating(w, 'default').length ? 2 : 3)
      ? 0
      : heartbeating(w, 'default').length
        ? 1
        : 3,
  run: async (j) => {
    const { page, fleet, world } = j;
    const name = `w${world.made++}`;
    await openAdmin(page, 'Workers');
    await openWorkersTab(page, 'Enrollment codes');
    await page.getByRole('button', { name: '+ Add enrollment code' }).click();
    const mint = page.getByRole('dialog', { name: 'new enrollment code' });
    await mint.getByRole('button', { name: 'Mint' }).click();
    const lines = await page
      .locator('pre')
      .filter({ hasText: 'SMELTR_WORKER_ENROLLMENT_CODE=' })
      .innerText();
    await page.getByRole('button', { name: 'Done' }).click();
    startWorkerFromLines(fleet, name, lines);
    await openWorkersTab(page, 'Workers');
    await workerRow(page, name, 'Heartbeating');
    world.workers.set(name, { state: 'heartbeating', group: 'default' });
    const workers: { workerId: string; revoked: boolean }[] = await (
      await j.truth()
    )
      .get('/api/workers')
      .then((a) => a.workers);
    expect(workers.find((w) => w.workerId === name)?.revoked).toBe(false);
    return `enroll ${name}`;
  },
};

const revokeWorker: Move = {
  name: 'revoke-worker',
  weight: (w) => (heartbeating(w).length ? 1 : 0),
  run: async (j) => {
    const { page, rng, world } = j;
    const name = rng.pick(heartbeating(world));
    await openAdmin(page, 'Workers');
    const row = await workerRow(page, name, 'Heartbeating');
    await openRowMenu(page, `More actions for ${name}`);
    await page.getByRole('button', { name: 'Revoke', exact: true }).click();
    await page
      .getByRole('dialog', { name: 'revoke worker' })
      .getByRole('button', { name: 'Revoke' })
      .click();
    await expect(row.getByRole('cell', { name: 'Revoked', exact: true })).toBeVisible();
    world.workers.set(name, { ...world.workers.get(name)!, state: 'revoked' });
    const workers: { workerId: string; revoked: boolean }[] = await (
      await j.truth()
    )
      .get('/api/workers')
      .then((a) => a.workers);
    expect(workers.find((w) => w.workerId === name)?.revoked).toBe(true);
    // A revoked worker keeps running what it has, a receiver's port with it, so the administrator shuts it down.
    await j.fleet.stopWorker(name);
    if (!rng.chance(2)) return `revoke ${name}`;

    await openRowMenu(page, `More actions for ${name}`);
    await page.getByRole('button', { name: 'Remove', exact: true }).click();
    await page
      .getByRole('dialog', { name: 'remove worker' })
      .getByRole('button', { name: 'Remove' })
      .click();
    await expect(workerRowOf(page, name)).toHaveCount(0);
    world.workers.delete(name);
    const after: { workerId: string }[] = await (
      await j.truth()
    )
      .get('/api/workers')
      .then((a) => a.workers);
    expect(after.map((w) => w.workerId)).not.toContain(name);
    return `revoke ${name}, and remove it`;
  },
};

const addSource: Move = {
  name: 'add-source',
  leadsTo: ['add-route', 'route-stream'],
  weight: (w) =>
    !w.password || w.editing.sources.length >= 3 ? 0 : w.editing.sources.length ? 1 : 3,
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const id = `src-${world.made++}`;
    const seconds = 1 + rng.below(3);
    await goto(page, leader, '#/build/sources');
    await page.getByRole('button', { name: '+ Add source' }).click();
    await page.getByRole('button', { name: /^Local, each worker/ }).click();
    await page.getByRole('button', { name: 'smeltr metrics', exact: true }).click();
    const form = page.getByRole('dialog', { name: /new smeltr metrics source/ });
    await form.getByRole('textbox', { name: /^id/ }).fill(id);
    await form.getByRole('spinbutton', { name: /^bucket seconds/ }).fill(String(seconds));
    await form.getByRole('button', { name: 'Add', exact: true }).click();
    await save(j, unsaved(page));
    world.editing.sources.push(id);
    await editingIsSaved(j);
    return `add source ${id}, every ${seconds}s`;
  },
};

/** Routes, from the map, what `plus` on `source`'s panel offers: a new pipeline made from a capture, its proposed JSON
 * Parser taken, then the route saved, which delivers to the first destination there is, as `add-route`'s does. Each
 * Save comes back to the page that sent it. */
async function routeFromMap(
  j: Journey,
  source: string,
  plus: (panel: Locator) => Locator,
  sample: string,
  pipeline: string,
  condition?: string,
) {
  const { page, leader, world } = j;
  // The journey's sources run in `default`, so that is where the map marks them.
  const card = page.locator(`a.node[title="${source}"][href*="group=default"]`);
  const panel = page.getByRole('complementary', { name: source });
  // A stream is listed once a worker's heartbeat has carried it to the leader.
  await goto(page, leader, '#/map');
  await expect(async () => {
    // A card drawn once rows arrive comes with a new layout.
    await page.reload();
    await card.click({ timeout: 2_000 });
    await expect(plus(panel)).toBeVisible({ timeout: 2_000 });
  }).toPass({ timeout: 60_000 });
  await plus(panel).click();

  const picker = page.getByRole('combobox', { name: 'pipeline', exact: true });
  await expect(picker).toHaveValue('');
  await picker.selectOption({ label: '+ New pipeline\u2026' });
  const form = page.getByRole('dialog', { name: 'capture a sample' });
  if (condition)
    await expect(form.getByRole('textbox', { name: /^Keep records where/ })).toHaveValue(condition);
  await captureTwo(
    j,
    form,
    sample,
    async () => {},
    // A metrics record is a line of JSON.
    () => expect(page.getByText(/records? read as JSON/)).toBeVisible(),
  );
  await save(j, () => expect(picker.locator('option:checked')).toContainText(pipeline));
  world.editing.pipelines.push(pipeline);
  await save(j, () => expect(page).toHaveURL(/#\/map/));
  world.editing.routes.push({ source, pipeline, to: world.editing.destinations[0] ?? null });
  await editingIsSaved(j);
}

/** What a metrics source files every record under (`metrics::METRICS_TOPIC`), which is its one stream. */
const METRICS_STREAM = '__smeltr_metrics';

const routeStream: Move = {
  name: 'route-stream',
  leadsTo: ['open-change', 'refused-route'],
  // A metrics source the fleet runs with no route, so its rows arrive as a stream no route takes, and nothing
  // since has routed it.
  weight: (w) =>
    heartbeating(w, 'default').length &&
    streamless(w).length &&
    w.editing.routes.length < 3 &&
    !w.closed
      ? 4
      : 0,
  run: async (j) => {
    const { rng, world } = j;
    const source = rng.pick(streamless(world));
    const sample = `streamed ${world.made++}`;
    const condition = `__source.topic == "${METRICS_STREAM}"`;
    await routeFromMap(
      j,
      source,
      (panel) =>
        panel
          .getByRole('listitem')
          .filter({ hasText: METRICS_STREAM })
          .getByRole('button', { name: '+ Add route' }),
      sample,
      `${source}-parse`,
      condition,
    );
    const api = await j.truth();
    const on = encodeURIComponent(world.branch ?? 'main');
    const routes: { source: string; condition?: string }[] = [];
    for (const scope of ['shared', 'default'])
      routes.push(...(await api.get(`/api/areas/${scope}?branch=${on}`)).config.routes);
    expect(routes.find((r) => r.source === source)?.condition).toBe(condition);
    return `route ${source}'s stream ${METRICS_STREAM} from the map, its pipeline from a capture as ${sample}`;
  },
};

const refusedSource: Move = {
  name: 'refused-source',
  weight: (w) => (w.password ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    // An id already there, or none at all.
    const id = world.editing.sources.length && rng.chance(2) ? rng.pick(world.editing.sources) : '';
    const why = id ? `${id} is taken` : 'an id is required';
    const before = world.branchSaved ? await configOn(await j.truth(), world.branch!) : null;
    await goto(page, leader, '#/build/sources');
    await page.getByRole('button', { name: '+ Add source' }).click();
    await page.getByRole('button', { name: /^Local, each worker/ }).click();
    await page.getByRole('button', { name: 'smeltr metrics', exact: true }).click();
    const form = page.getByRole('dialog', { name: /new smeltr metrics source/ });
    await form.getByRole('textbox', { name: /^id/ }).fill(id);
    await expect(form.getByText(why)).toBeVisible();
    await expect(form.getByRole('button', { name: 'Add', exact: true })).toBeDisabled();
    await page.keyboard.press('Escape');
    await expect(form).toBeHidden();
    await expect(page.getByText('Unsaved', { exact: true })).toBeHidden();
    if (before) expect(await configOn(await j.truth(), world.branch!)).toEqual(before);
    return `a source with ${id ? `the id ${id}` : 'no id'} is refused`;
  },
};

/** The bucket the journey's fake S3 (`Journey.bucket`) holds, and its one Avro file of two orders. */
export const AVRO_BUCKET = 'orders';
const ORDER_SCHEMA = {
  type: 'record',
  name: 'Order',
  fields: [
    { name: 'order_id', type: 'long' },
    {
      name: 'customer',
      type: {
        type: 'record',
        name: 'Customer',
        fields: [
          { name: 'email', type: 'string' },
          { name: 'country', type: 'string' },
        ],
      },
    },
    { name: 'total', type: 'double' },
    { name: 'tags', type: { type: 'array', items: 'string' } },
    { name: 'paid', type: 'boolean' },
  ],
};
const order = (id: number) =>
  Buffer.concat([
    avro.long(id),
    avro.string(`c${id}@example.com`),
    avro.string('NO'),
    avro.double(9.5 * id),
    avro.array([avro.string('gift')]),
    avro.boolean(id % 2 === 0),
  ]);
export const AVRO_OBJECTS = new Map([
  [AVRO_BUCKET, new Map([['2026/orders.avro', avroFile(ORDER_SCHEMA, [order(1), order(2)])]])],
]);
/** The fields a fill declares from `ORDER_SCHEMA`, nested ones named with dots. */
const ORDER_FIELDS = [
  { name: 'order_id', type: 'i64' },
  { name: 'customer.email', type: 'str' },
  { name: 'customer.country', type: 'str' },
  { name: 'total', type: 'f64' },
  { name: 'tags', type: 'array<str>' },
  { name: 'paid', type: 'bool' },
];

const fillAvro: Move = {
  name: 'fill-avro',
  leadsTo: ['refused-route'],
  weight: (w) =>
    heartbeating(w, 'default').length && !w.editing.sources.some((s) => s.startsWith('avro-'))
      ? 1
      : 0,
  run: async (j) => {
    const { page, leader, rng, world, bucket } = j;
    const id = `avro-${world.made++}`;
    // A field already declared keeps the type it was given; the fill types only the rest.
    const typed = rng.chance(2);
    await goto(page, leader, '#/build/sources');
    await page.getByRole('button', { name: '+ Add source' }).click();
    await page.getByRole('button', { name: /^Pull, one worker/ }).click();
    await page.getByRole('button', { name: 'S3 listing', exact: true }).click();
    const form = page.getByRole('dialog', { name: /new S3 listing source/ });
    await form.getByRole('textbox', { name: /^id/ }).fill(id);
    await form.getByRole('textbox', { name: /^bucket/ }).fill(AVRO_BUCKET);
    await form.getByRole('combobox', { name: /^format/ }).selectOption('avro');
    if (typed) {
      await form.getByRole('button', { name: '+ field' }).click();
      await form
        .getByRole('textbox', { name: /^field \d+ name$/ })
        .last()
        .fill('total');
      await form
        .getByRole('combobox', { name: /^field \d+ type$/ })
        .last()
        .selectOption('str');
    }
    await form.getByRole('button', { name: /^Advanced(?! TLS)/ }).click();
    await form.getByRole('textbox', { name: /^endpoint/ }).fill(bucket.url);
    await form.getByRole('textbox', { name: /^region/ }).fill('us-east-1');

    await form.getByRole('button', { name: 'Fill from a message' }).click();
    const capture = page.getByRole('dialog', { name: 'Fill the Avro fields from a message' });
    // The capture runs on a group's workers, and `default` is where the journey's are.
    await capture.getByRole('combobox', { name: 'Group' }).selectOption('default');
    await capture.getByRole('button', { name: 'Start' }).click();
    const take = capture.getByRole('button', { name: 'Fill fields' });
    await expect(take).toBeVisible({ timeout: 60_000 });
    await expect(capture).toContainText('c1@example.com');
    await take.click();
    await expect(capture).toBeHidden();
    await expect(form.getByText(/Filled from a message on worker \S+ · 6 fields/)).toBeVisible();
    const fields = ORDER_FIELDS.map((f) =>
      typed && f.name === 'total' ? { ...f, type: 'str' } : f,
    );

    await form.getByRole('button', { name: 'Add', exact: true }).click();
    await save(j, unsaved(page));
    world.editing.sources.push(id);
    await editingIsSaved(j);
    const on = encodeURIComponent(world.branch!);
    const res = await (await j.truth()).get(`/api/areas/shared?branch=${on}`);
    const saved = res.config.sources.find((s: { id: string }) => s.id === id);
    expect(saved.fields, `${id} declares the file's fields`).toEqual(fields);
    return `add Avro source ${id}, its fields filled from a message${typed ? ', total typed str first' : ''}`;
  },
};

/** Fills the add-destination dialog, opened at its type catalog, as an Elasticsearch destination at the sink,
 * and presses Add. */
async function addElasticsearch(j: Journey, id: string, index: string) {
  const { page, sink } = j;
  await page.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  const form = page.getByRole('dialog', { name: /new Elasticsearch destination/ });
  await form.getByRole('textbox', { name: /^id/ }).fill(id);
  await form.getByRole('textbox', { name: 'url' }).fill(sink.url);
  await form.getByRole('textbox', { name: 'default index' }).fill(index);
  await form.getByRole('button', { name: 'Add', exact: true }).click();
  return form;
}

/** The pipelines that compile for `source`. A pipeline made from a capture parses `_raw` as JSON, which an Avro source,
 * reading declared fields, does not have: the leader refuses that route's save. */
const pipelinesFor = (w: World, source: string) =>
  w.editing.pipelines.filter((p) => !(source.startsWith('avro-') && p.endsWith('-parse')));

/** The sources a new route can take, each with a pipeline that compiles for it. */
const routable = (w: World) => w.editing.sources.filter((s) => pipelinesFor(w, s).length);

/** Adds a route on the Routes page and picks its source and pipeline. A source's routes share one card, the new
 * route last on it; the card, which is a line while it holds one route to one destination. */
async function startRoute(j: Journey, source: string, pipeline: string) {
  const { page, leader } = j;
  await goto(page, leader, '#/build/routes');
  await page.getByRole('button', { name: '+ Add route' }).click();
  await page.getByRole('combobox', { name: 'from source' }).last().selectOption(source);
  const card = page.locator(`.flow[data-source="${source}"]`);
  await card.getByRole('combobox', { name: 'pipeline' }).last().selectOption({ label: pipeline });
  return card;
}

const addDestination: Move = {
  name: 'add-destination',
  leadsTo: ['add-route'],
  weight: (w) =>
    !w.password || w.editing.destinations.length >= 2 ? 0 : w.editing.destinations.length ? 1 : 3,
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const id = `dst-${world.made++}`;
    const index = rng.pick(['metrics', 'fleet', `smeltr-${id}`]);
    await goto(page, leader, '#/build/destinations');
    await page.getByRole('button', { name: '+ Add destination' }).click();
    await addElasticsearch(j, id, index);
    await save(j, unsaved(page));
    world.editing.destinations.push(id);
    await editingIsSaved(j);
    return `add destination ${id}, index ${index}`;
  },
};

const addPipeline: Move = {
  name: 'add-pipeline',
  leadsTo: ['add-route', 'add-route-to-new-destination', 'add-step'],
  weight: (w) =>
    !w.password || w.editing.pipelines.length >= 2 ? 0 : w.editing.pipelines.length ? 1 : 3,
  run: async (j) => {
    const { page, leader, world } = j;
    const name = `pipe ${world.made++}`;
    await goto(page, leader, '#/build/pipelines');
    await page.getByRole('link', { name: '+ Add pipeline' }).click();
    await page.getByLabel('pipeline name').fill(name);
    await save(j, () => expect(page.getByText('Saved.')).toBeVisible());
    world.editing.pipelines.push(name);
    await editingIsSaved(j);
    return `add pipeline "${name}"`;
  },
};

const addRoute: Move = {
  name: 'add-route',
  leadsTo: ['open-change', 'blast-radius'],
  weight: (w) =>
    !routable(w).length || w.editing.routes.length >= 3 ? 0 : w.editing.routes.length ? 1 : 4,
  run: async (j) => {
    const { page, rng, world } = j;
    const source = rng.pick(routable(world));
    const pipeline = rng.pick(pipelinesFor(world, source));
    await startRoute(j, source, pipeline);
    await save(j, unsaved(page));
    // A new route delivers to the first destination there is.
    const to = world.editing.destinations[0] ?? null;
    world.editing.routes.push({ source, pipeline, to });
    await editingIsSaved(j);
    return `add route from ${source} through "${pipeline}" to ${to ?? 'nothing'}`;
  },
};

const addRouteToNewDestination: Move = {
  name: 'add-route-to-new-destination',
  leadsTo: ['open-change'],
  weight: (w) =>
    !w.password ||
    !routable(w).length ||
    w.editing.routes.length >= 3 ||
    w.editing.destinations.length >= 2
      ? 0
      : 1,
  run: async (j) => {
    const { page, rng, world } = j;
    const source = rng.pick(routable(world));
    const pipeline = rng.pick(pipelinesFor(world, source));
    const id = `dst-${world.made++}`;
    const card = await startRoute(j, source, pipeline);
    // The destination is made from the card, and saved on Add: a line card's + beside "destination" wires it to
    // the one pipeline; on a card of several routes, the new route's socket list does.
    // The route is seeded with the first destination there is; this one goes to the new one alone.
    const seeded = world.editing.destinations[0];
    const line = await card.evaluate((el) => el.classList.contains('line'));
    if (line) {
      if (seeded) await card.getByRole('button', { name: `stop sending to ${seeded}` }).click();
      await card.getByRole('button', { name: 'add a destination' }).click();
    } else {
      await card
        .locator('.pipe')
        .last()
        .getByRole('button', { name: /drag to a destination/ })
        .click();
      if (seeded) await page.locator('.menu button', { hasText: seeded }).click();
      await page
        .locator('.menu')
        .getByRole('button', { name: /another destination/ })
        .click();
    }
    await page
      .locator('.menu')
      .getByRole('button', { name: /New destination/ })
      .click();
    const form = await addElasticsearch(j, id, 'metrics');
    await nameBranch(j);
    await expect(form).toBeHidden();
    // Made, it is on the card.
    await expect(card.locator('.pill', { hasText: id })).toBeVisible();
    world.editing.destinations.push(id);
    await save(j, unsaved(page));
    world.editing.routes.push({ source, pipeline, to: id });
    await editingIsSaved(j);
    return `add route from ${source} through "${pipeline}" to a new destination ${id}`;
  },
};

/** The routes whose pipeline does not compile for their source, each a source and a pipeline. */
const unroutable = (w: World) =>
  w.editing.sources.flatMap((s) =>
    w.editing.pipelines.filter((p) => !pipelinesFor(w, s).includes(p)).map((p) => [s, p]),
  );

const refusedRoute: Move = {
  name: 'refused-route',
  // On a branch already saved, so the refused save names none.
  weight: (w) => (w.branch !== null && w.branchSaved && unroutable(w).length ? 1 : 0),
  run: async (j) => {
    const { page, rng, world } = j;
    const [source, pipeline] = rng.pick(unroutable(world));
    const before = await configOn(await j.truth(), world.branch!);
    await startRoute(j, source, pipeline);
    await page.getByRole('button', { name: 'Save', exact: true }).click();
    // The leader says why, and the edit stays to be put right or discarded.
    await expect(page.getByRole('alert').filter({ hasText: '`_raw` is a variant' })).toBeVisible();
    await expect(page.getByText('Unsaved', { exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Discard' }).click();
    await unsaved(page)();
    expect(await configOn(await j.truth(), world.branch!)).toEqual(before);
    return `a route from ${source} through "${pipeline}" is refused`;
  },
};

const blastRadius: Move = {
  name: 'blast-radius',
  weight: (w) => (w.editing.routes.some((r) => r.to !== null) ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const route = rng.pick(world.editing.routes.filter((r) => r.to !== null));
    const to = route.to!;
    await goto(page, leader, `#/build/sources/${encodeURIComponent(route.source)}`);
    const row = page.locator(`#source-${encodeURIComponent(route.source)}`);
    await row.getByRole('button', { name: /Connections/ }).click();
    const link = row.getByRole('link', { name: to, exact: true });
    await expect(link).toBeVisible();
    // The leader answers the same, in the scope that declares the source.
    const api = await j.truth();
    const on = encodeURIComponent(world.branch ?? 'main');
    const reached: string[] = [];
    for (const scope of ['shared', 'default']) {
      const source = encodeURIComponent(route.source);
      const res = await api.get(`/api/areas/${scope}/sources/${source}/connections?branch=${on}`);
      reached.push(...res.downstream.map((o: { id: string }) => o.id));
    }
    expect(reached).toContain(to);
    await link.click();
    await expect(page).toHaveURL(new RegExp(`#/build/destinations/${encodeURIComponent(to)}$`));
    return `see that ${route.source} reaches ${to}`;
  },
};

const openChange: Move = {
  name: 'open-change',
  leadsTo: ['comment', 'deploy', 'close-change', 'bring-current'],
  weight: (w) => (w.branchSaved && w.change === null ? 3 : 0),
  run: async (j) => {
    const { page, leader, world } = j;
    const title = `Change ${world.made++}`;
    await goto(page, leader, '#/build');
    await page.getByRole('button', { name: 'Open for review' }).click();
    await page.getByLabel('Title').fill(title);
    await page.getByRole('button', { name: 'Open for review' }).click();
    await expect(page.getByRole('heading', { name: title })).toBeVisible();
    world.change = title;
    const branches: { name: string }[] = (await (await j.truth()).get('/api/branches')).branches;
    expect(JSON.stringify(branches.find((b) => b.name === world.branch))).toContain(title);
    return `open "${title}" for review`;
  },
};

const comment: Move = {
  name: 'comment',
  weight: (w) => (w.change && !w.closed ? 1 : 0),
  run: async (j) => {
    const { page, leader, world } = j;
    const text = `Looked at ${world.change}, note ${world.made++}.`;
    await goto(page, leader, `#/changes/review/${world.branch}`);
    await page.getByRole('textbox', { name: 'Comment', exact: true }).fill(text);
    await page.getByRole('button', { name: 'Submit' }).click();
    await expect(page.getByText(text)).toBeVisible();
    await page.reload();
    await expect(page.getByText(text)).toBeVisible();
    const timeline = await (await j.truth()).get(`/api/branches/${world.branch}/timeline`);
    expect(JSON.stringify(timeline)).toContain(text);
    return `comment on "${world.change}"`;
  },
};

type History = { deployed?: string; held?: object; commits: { commit: string; subject: string }[] };

const deploy: Move = {
  name: 'deploy',
  leadsTo: [
    'rows-arrive',
    'roll-back',
    'enroll-in-group',
    'capture',
    'bring-current',
    'declare-from-production',
    // A source deployed without a route is a stream no route takes.
    'route-stream',
  ],
  weight: (w) => (w.change && !w.closed ? (w.landing ? 3 : 1) : 0),
  run: async (j) => {
    const { page, leader, world } = j;
    const title = world.change!;
    await goto(page, leader, `#/changes/review/${world.branch}`);
    // Held by a rollback, an approval merges and deploys nothing, and says so.
    const verb = world.held ? 'Approve' : 'Approve & deploy';
    await page.getByRole('button', { name: verb, exact: true }).click();
    await page
      .getByRole('dialog', { name: verb.toLowerCase(), exact: true })
      .getByRole('button', { name: verb, exact: true })
      .click();
    if (!world.landing) {
      // Only samples, all left out: main would not change, so the change stays open.
      const refused = page.getByRole('dialog').filter({ hasText: 'there is nothing to deploy' });
      await refused.getByRole('button', { name: 'Close' }).click();
      await expect(page).toHaveURL(/#\/changes\/review/);
      return `"${title}" holds only samples a deploy leaves out, and deploys nothing`;
    }
    // The deploy itself, then the reload it ends with, can outlast the default wait on a busy machine.
    await expect(page).not.toHaveURL(/#\/changes\/review/, { timeout: 30_000 });
    const api = await j.truth();
    const history: History = await api.get('/api/main/history');
    expect(history.commits[0].subject).toBe(title);
    // A deploy leaves out the samples no test names, and the journey writes no tests: main keeps its own.
    const main = world.deploys[world.deploys.length - 1].config;
    world.editing.samples = world.editing.samples.filter((s) => main.samples.includes(s));
    world.deploys.push({ subject: title, config: copy(world.editing) });
    // A rollback's hold outlives an approval: main moves, and the fleet stays where it was held.
    if (world.held) expect(history.deployed).not.toBe(history.commits[0].commit);
    else {
      expect(history.deployed).toBe(history.commits[0].commit);
      world.running = world.deploys.length - 1;
    }
    const branches: { name: string }[] = (await api.get('/api/branches')).branches;
    expect(branches.map((b) => b.name)).not.toContain(world.branch);
    world.branch = null;
    world.branchSaved = false;
    world.landing = false;
    world.change = null;
    await expect(page.getByRole('button', { name: /^Branch: / })).toHaveAccessibleName(
      /^Branch: main\b/,
    );
    return `approve and deploy "${title}"`;
  },
};

/** The routes the fleet runs that carry rows to the sink: from a metrics source, which makes rows of its own,
 * unlike a receiver nothing sends to, into an Elasticsearch destination, which every one the moves add writes. The
 * moves' routes match every row, so only a source's first route takes its rows. */
const flowing = (w: World) => {
  const routes = w.deploys[w.running].config.routes;
  return routes.filter(
    (r) =>
      r.source.startsWith('src-') &&
      r.to?.startsWith('dst-') &&
      routes.find((first) => first.source === r.source) === r,
  );
};

const rowsArrive: Move = {
  name: 'rows-arrive',
  weight: (w) => (flowing(w).length && heartbeating(w, 'default').length ? 6 : 0),
  run: async (j) => {
    const { page, leader, sink, world } = j;
    const before = sink.indexed;
    const route = flowing(world)[0];
    await goto(page, leader, '#/map');
    const band = page.getByRole('region', { name: 'default', exact: true });
    await expect(band.getByRole('link', { name: new RegExp(`^${route.source} `) })).toContainText(
      MOVING,
      {
        timeout: 60_000,
      },
    );
    await expect.poll(() => sink.indexed, { timeout: 60_000 }).toBeGreaterThan(before);
    return `rows flow from ${route.source} to ${route.to}`;
  },
};

const rollBack: Move = {
  name: 'roll-back',
  leadsTo: ['release'],
  weight: (w) => (!w.held && w.running > 0 ? 2 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const to = rng.below(world.running);
    const subject = world.deploys[to].subject;
    await goto(page, leader, '#/changes/history');
    await historyRow(page, subject).getByRole('button', { name: 'Roll back to here' }).click();
    await page
      .getByRole('dialog', { name: 'deploy' })
      .getByRole('button', { name: 'Deploy' })
      .click();
    await expect(page).toHaveURL(/#\/status\?deploying=/);
    const api = await j.truth();
    const history: History = await api.get('/api/main/history');
    expect(history.held).toBeDefined();
    expect(history.commits.find((c) => c.commit === history.deployed)?.subject).toBe(subject);
    // Until a worker has taken the rollback, it still reports the commit a release returns to, and a capture after
    // the release would take that report for the release's own. One that will not run it says why.
    type Row = {
      workerId: string;
      commit: string;
      supervising?: { desiredCommit?: string; failure?: string };
    };
    await expect
      .poll(
        async () => {
          const rows: Row[] = (await api.get('/api/cluster')).health.threads;
          return heartbeating(world).every((w) =>
            rows.some(
              (t) =>
                t.workerId === w &&
                (t.commit === history.deployed ||
                  (t.supervising?.desiredCommit === history.deployed && !!t.supervising.failure)),
            ),
          );
        },
        { timeout: 60_000 },
      )
      .toBe(true);
    world.held = true;
    world.running = to;
    return `roll back to "${subject}"`;
  },
};

const release: Move = {
  name: 'release',
  weight: (w) => (w.held ? 2 : 0),
  run: async (j) => {
    const { page, leader, world } = j;
    await goto(page, leader, '#/changes/history');
    await page.getByRole('button', { name: 'Release' }).click();
    await page
      .getByRole('dialog', { name: 'release' })
      .getByRole('button', { name: 'Release' })
      .click();
    await expect(page).toHaveURL(/#\/status\?deploying=/);
    const history: History = await (await j.truth()).get('/api/main/history');
    expect(history.held).toBeUndefined();
    expect(history.deployed).toBe(history.commits[0].commit);
    world.held = false;
    world.running = world.deploys.length - 1;
    return 'release the hold';
  },
};

type Secret = { name: string; versions: unknown[] };

const addSecret: Move = {
  name: 'add-secret',
  leadsTo: ['rotate-secret'],
  weight: (w) => (w.password && w.secrets.size < 2 ? 1 : 0),
  run: async (j) => {
    const { page, rng, world } = j;
    const name = `secret-${world.made++}`;
    const value = `value-${rng.below(1_000_000_000)}`;
    await openAdmin(page, 'Secrets');
    await page.getByRole('button', { name: '+ Add secret' }).click();
    const add = page.getByRole('dialog', { name: 'new secret' });
    await add.getByRole('textbox', { name: /^Name\b/ }).fill(name);
    await add.getByLabel('value', { exact: true }).fill(value);
    await add.getByRole('button', { name: 'Add' }).click();
    const row = page
      .getByRole('row')
      .filter({ has: page.getByRole('cell', { name, exact: true }) });
    await expect(row.getByRole('cell', { name: 'v1', exact: true })).toBeVisible();
    world.secrets.set(name, 1);
    const secrets: Secret[] = await (await j.truth()).get('/api/secrets').then((a) => a.secrets);
    expect(secrets.find((s) => s.name === name)?.versions).toHaveLength(1);
    expect(JSON.stringify(secrets)).not.toContain(value);
    expect(await page.content()).not.toContain(value);
    return `add secret ${name}`;
  },
};

const rotateSecret: Move = {
  name: 'rotate-secret',
  weight: (w) => (w.secrets.size ? 1 : 0),
  run: async (j) => {
    const { page, rng, world } = j;
    const name = rng.pick([...world.secrets.keys()].sort());
    const version = world.secrets.get(name)! + 1;
    const value = `value-${rng.below(1_000_000_000)}`;
    await openAdmin(page, 'Secrets');
    const row = page
      .getByRole('row')
      .filter({ has: page.getByRole('cell', { name, exact: true }) });
    await row.getByRole('button', { name: 'New value' }).click();
    await page.getByLabel('the new value', { exact: true }).fill(value);
    await page.getByRole('button', { name: 'Store', exact: true }).click();
    // Nothing the moves deploy names a secret, so a new value points nothing anywhere: it needs no branch, and
    // the page says so.
    await expect(page.getByText('Stored. Nothing references it yet.')).toBeVisible();
    await expect(page.getByRole('dialog', { name: 'branch required' })).toHaveCount(0);
    await expect(row.getByRole('cell', { name: `v${version}`, exact: true })).toBeVisible();
    world.secrets.set(name, version);
    const secrets: Secret[] = await (await j.truth()).get('/api/secrets').then((a) => a.secrets);
    expect(secrets.find((s) => s.name === name)?.versions).toHaveLength(version);
    expect(JSON.stringify(secrets)).not.toContain(value);
    return `rotate ${name} to v${version}`;
  },
};

/** Where the journey's leader keeps its secrets encryption key: the default, under its own directory. */
const keyFile = (leader: Leader) =>
  path.join(leader.proc.dir, 'smeltr-leader-secrets', 'sealing.key');

/** The key file is lost, which is not something the GUI does; finding out and regenerating it is. Only while
 * nothing else is sealed: single sign-on and the agents would stop working until entered again, and no move
 * enters them again. */
const regenerateKey: Move = {
  name: 'regenerate-key',
  leadsTo: ['rotate-secret'],
  weight: (w) => (w.password && w.secrets.size ? 3 : 0),
  run: async (j) => {
    const { page, leader, world } = j;
    // The settings it encrypts, each entered again from the link beside it, so a later move finds them working.
    const settings = [
      ...(world.sso
        ? [
            {
              key: 'oidc.client_secret',
              label: 'Single sign-on client secret',
              field: 'client secret',
              value: CLIENT_SECRET,
            },
          ]
        : []),
      ...(world.agentsOn
        ? [
            {
              key: 'agents.api_key',
              label: 'AI agents API key',
              field: 'API key',
              value: MODEL_KEY,
            },
          ]
        : []),
    ];
    fs.rmSync(keyFile(leader));
    await openAdmin(page, 'Secrets');
    await expect(page.getByText(/^The key file is missing/)).toBeVisible();
    await page
      .getByRole('navigation', { name: 'Secrets' })
      .getByRole('link', { name: 'Encryption key', exact: true })
      .click();
    await page.getByRole('button', { name: 'Regenerate key' }).click();
    await page
      .getByRole('dialog', { name: 'regenerate the encryption key' })
      .getByRole('button', { name: 'Regenerate key' })
      .click();
    await expect(page.getByText('To enter again')).toBeVisible();
    for (const name of world.secrets.keys())
      await expect(page.getByText(name, { exact: true })).toBeVisible();
    expect(fs.existsSync(keyFile(leader))).toBe(true);
    const unopenable = async () => {
      const key: { state: string; unopenable: { name: string }[] } = await (
        await j.truth()
      ).get('/api/settings/encryption-key');
      expect(key.state).toBe('held');
      // One in force may be ordered again at once, so it may be sealed or not.
      return key.unopenable
        .map((u) => u.name)
        .filter((name) => !(world.acme && name === 'acme'))
        .sort();
    };
    // An ordered certificate is not entered again: the leader orders it again while ordering is on, and keeps
    // it sealed while ordering is off. The leader's own certificate is renewed at once, so only what an admin
    // entered is left to enter again.
    const ordered = world.ordered && !world.acme ? ['acme'] : [];
    await expect
      .poll(unopenable, { timeout: 30_000 })
      .toEqual(
        [
          ...world.secrets.keys(),
          ...world.certificates.keys(),
          ...ordered,
          ...settings.map((s) => s.key),
        ].sort(),
      );
    // Each certificate is entered again from its link, as it was uploaded.
    for (const [name, pki] of world.certificates) {
      await page
        .getByRole('listitem')
        .filter({ has: page.getByText(name, { exact: true }) })
        .getByRole('link', { name: 'Certificates' })
        .click();
      await expect(page).toHaveURL(/#\/admin\/certificates$/);
      await replaceCertificate(page, name, pki);
      await openAdmin(page, 'Secrets');
      await page
        .getByRole('navigation', { name: 'Secrets' })
        .getByRole('link', { name: 'Encryption key', exact: true })
        .click();
      await expect(page.getByText(name, { exact: true })).toHaveCount(0);
    }
    for (const setting of settings) {
      await page.getByRole('listitem').filter({ hasText: setting.label }).getByRole('link').click();
      // A value the key cannot open reads as none stored, so the page offers an empty input, not Replace.
      await page.getByRole('textbox', { name: setting.field, exact: true }).fill(setting.value);
      await page.getByRole('button', { name: 'Save', exact: true }).click();
      await expect(
        page.getByRole('button', { name: `replace the ${setting.field}` }),
      ).toBeVisible();
      await openAdmin(page, 'Secrets');
      await page
        .getByRole('navigation', { name: 'Secrets' })
        .getByRole('link', { name: 'Encryption key', exact: true })
        .click();
      await expect(page.getByText(setting.label, { exact: true })).toHaveCount(0);
    }
    expect(await unopenable()).toEqual([...world.secrets.keys(), ...ordered].sort());
    const again = [...world.certificates.keys(), ...settings.map((s) => s.label)];
    const entered = again.length ? `, ${again.join(' and ')} entered again` : '';
    return `regenerate the key, ${world.secrets.size} to enter again${entered}`;
  },
};

const addStep: Move = {
  name: 'add-step',
  weight: (w) => (w.editing.pipelines.length ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const pipeline = rng.pick(world.editing.pipelines);
    const field = `journey_${world.made++}`;
    await goto(page, leader, '#/build/pipelines');
    await page.getByRole('link', { name: pipeline, exact: true }).click();
    // At the end, or above a step the pipeline has.
    const above = page.getByRole('button', { name: 'Insert step' });
    await expect(page.getByRole('button', { name: 'Add step' })).toBeVisible();
    const at = rng.below((await above.count()) + 1);
    const end = at === (await above.count());
    await (end ? page.getByRole('button', { name: 'Add step' }) : above.nth(at)).click();
    await page
      .getByRole('dialog', { name: 'add step' })
      .getByRole('button', { name: 'Eval', exact: true })
      .click();
    // The new step is the open one with nothing in it yet. Held by its id, since filling it ends that.
    const id = await page
      .locator('.card.open')
      .filter({ has: page.locator('input[placeholder="target field"]:placeholder-shown') })
      .getAttribute('id');
    const step = page.locator(`[id="${id}"]`);
    await step.getByPlaceholder('target field').fill(field);
    await step.getByPlaceholder('expression').fill(`"${field}"`);
    await save(j, () => expect(page.getByText('Saved.')).toBeVisible());
    world.editing.steps.push(`${pipeline}:${field}`);
    await editingIsSaved(j);
    return `add a step to "${pipeline}" ${end ? 'at the end' : `above step ${at + 1}`} that sets ${field}`;
  },
};

/** The pipelines made from a capture that the fleet runs, which nothing has deleted since: each a JSON Parser production has read. */
const parsedInProduction = (w: World) =>
  w.deploys[w.running].config.routes
    .map((r) => r.pipeline)
    .filter((p) => p.endsWith('-parse') && w.editing.pipelines.includes(p));

const declareFromProduction: Move = {
  name: 'declare-from-production',
  weight: (w) => (heartbeating(w, 'default').length && parsedInProduction(w).length ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const pipeline = rng.pick(parsedInProduction(world));
    await goto(page, leader, '#/build/pipelines');
    await page.getByRole('link', { name: pipeline, exact: true }).click();
    // The editor opens on the steps it opened last, so the Parser may be open already.
    const parser = page.getByRole('button', { name: /^Parser .*as JSON/ });
    // At its start: on a narrow page the step's controls, arriving on hover, cover the head's middle.
    if ((await parser.getAttribute('aria-expanded')) !== 'true')
      await parser.click({ position: { x: 8, y: 8 } });
    const suggested = page.getByRole('group', { name: 'Suggested fields' });
    // Shown once a worker's heartbeat has carried what the parser read to the leader. Suggesting again reads again.
    await expect(async () => {
      await page.getByRole('button', { name: 'Suggest fields' }).click();
      await expect(suggested).toContainText('production', { timeout: 2_000 });
    }).toPass({ timeout: 60_000 });
    const offered = suggested
      .getByRole('group')
      .filter({ has: page.getByRole('button', { name: 'Add', exact: true }) })
      .filter({ hasNotText: 'not in production' });
    const rows = await offered.count();
    if (rows === 0) return `nothing production read of "${pipeline}" is left to declare`;
    const row = offered.nth(rng.below(rows));
    const field = await row.getByRole('textbox').inputValue();
    const type = await row.getByRole('combobox').inputValue();
    await row.getByRole('button', { name: 'Add', exact: true }).click();
    await save(j, () => expect(page.getByText('Saved.')).toBeVisible());
    await editingIsSaved(j);
    const truth = await j.truth();
    const declared: { name: string; type: unknown }[] = [];
    for (const scope of ['shared', 'default']) {
      const res = await truth.get(
        `/api/areas/${scope}?branch=${encodeURIComponent(world.branch!)}`,
      );
      for (const p of res.config.pipelines as {
        name: string;
        steps: { type: string; declared?: { name: string; type: unknown }[] }[];
      }[])
        if (p.name === pipeline)
          for (const step of p.steps) if (step.type === 'parser') declared.push(...step.declared!);
    }
    // The picker names an array `array of str`, which the file spells `{"type": "array", "of": "str"}`.
    const spelled = (t: unknown) =>
      typeof t === 'string' ? t : `array of ${(t as { of: string }).of}`;
    expect(
      spelled(declared.find((d) => d.name === field)?.type),
      `${field} is declared as shown`,
    ).toBe(type);
    return `declare ${field} as ${type} in "${pipeline}", from what production read`;
  },
};

/** The pipelines made from a capture, which nothing has deleted since: each a JSON Parser over `_raw`. */
const parsing = (w: World) => w.editing.pipelines.filter((p) => p.endsWith('-parse'));

const writeInto: Move = {
  name: 'write-into',
  weight: (w) => (parsing(w).length ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const pipeline = rng.pick(parsing(world));
    await goto(page, leader, '#/build/pipelines');
    await page.getByRole('link', { name: pipeline, exact: true }).click();
    // The step's head: once records show, each field's step glyph names the step too.
    const parser = page.getByRole('button', { name: /^Parser .*as JSON/ });
    if ((await parser.getAttribute('aria-expanded')) !== 'true')
      await parser.click({ position: { x: 8, y: 8 } });
    const box = page.getByRole('textbox', { name: 'write into' });
    // Advanced is open where it holds something set.
    if (!(await box.isVisible())) await page.getByRole('button', { name: /^Advanced/ }).click();
    // An object of its own, or the record again: never the one it writes into now, which leaves nothing to save.
    const into = (await box.inputValue()) !== '$' && rng.chance(4) ? '$' : `o${world.made++}`;
    await box.fill(into);
    await expect(parser).toContainText(into === '$' ? /as JSON( \(|$)/ : `as JSON into ${into}`);
    await save(j, () => expect(page.getByText('Saved.')).toBeVisible());
    await editingIsSaved(j);
    const truth = await j.truth();
    const written: string[] = [];
    for (const scope of ['shared', 'default']) {
      const res = await truth.get(
        `/api/areas/${scope}?branch=${encodeURIComponent(world.branch!)}`,
      );
      for (const p of res.config.pipelines as {
        name: string;
        steps: { type: string; into: string }[];
      }[])
        if (p.name === pipeline)
          for (const step of p.steps) if (step.type === 'parser') written.push(step.into);
    }
    expect(written, `${pipeline} writes into ${into}`).toEqual([into]);
    return `parse "${pipeline}" into ${into === '$' ? 'the record' : into}`;
  },
};

const addReceiver: Move = {
  name: 'add-receiver',
  weight: (w) => (w.password && !receiving(w) && !grouping(w) ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const id = `in-${world.made++}`;
    const port = 20_000 + rng.below(20_000);
    await goto(page, leader, '#/build/sources');
    await page.getByRole('button', { name: '+ Add source' }).click();
    await page.getByRole('button', { name: /^Push, all workers/ }).click();
    await page.getByRole('button', { name: 'Syslog receiver', exact: true }).click();
    const form = page.getByRole('dialog', { name: /new Syslog receiver source/ });
    await form.getByRole('textbox', { name: /^id/ }).fill(id);
    await form.getByRole('button', { name: 'Add', exact: true }).click();
    // A receiver admits only the networks it is given; a port of its own keeps it off the others'.
    await page
      .getByRole('spinbutton', { name: /^port\b/ })
      .last()
      .fill(String(port));
    await page
      .getByRole('textbox', { name: /^allow from\b/ })
      .last()
      .fill('127.0.0.0/8');
    await page.keyboard.press('Enter');
    await save(j, unsaved(page));
    world.editing.sources.push(id);
    await editingIsSaved(j);
    return `add syslog receiver ${id} on port ${port}`;
  },
};

const addOtherDestination: Move = {
  name: 'add-other-destination',
  weight: (w) =>
    w.password && w.editing.destinations.filter((d) => d.startsWith('out-')).length < 2 ? 1 : 0,
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const id = `out-${world.made++}`;
    const kind = rng.pick(['HTTP', 'S3'] as const);
    await goto(page, leader, '#/build/destinations');
    await page.getByRole('button', { name: '+ Add destination' }).click();
    await page.getByRole('button', { name: kind, exact: true }).click();
    const form = page.getByRole('dialog', { name: new RegExp(`new ${kind} destination`) });
    await form.getByRole('textbox', { name: /^id/ }).fill(id);
    if (kind === 'HTTP')
      await form.getByRole('textbox', { name: /^url/ }).fill(`http://127.0.0.1:9/${id}`);
    else await form.getByRole('textbox', { name: /^bucket/ }).fill(`bucket-${id}`);
    await form.getByRole('button', { name: 'Add', exact: true }).click();
    await save(j, unsaved(page));
    world.editing.destinations.push(id);
    await editingIsSaved(j);
    return `add ${kind} destination ${id}`;
  },
};

const addLookup: Move = {
  name: 'add-lookup',
  leadsTo: ['bring-current'],
  weight: (w) => (w.password && w.editing.lookups.length < 2 ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const id = `table-${world.made++}`;
    const rows = 1 + rng.below(3);
    await goto(page, leader, '#/build/lookups');
    await page.getByRole('button', { name: '+ Add table' }).click();
    const dialog = page.getByRole('dialog');
    await dialog.getByRole('button', { name: 'Built by hand' }).click();
    await dialog.getByLabel('id').fill(id);
    await dialog.getByRole('button', { name: 'Add' }).click();
    await expect(dialog).toBeHidden();
    for (let r = 1; r <= rows; r++) {
      await page.getByRole('button', { name: '+ row' }).click();
      await page.getByLabel(`row ${r} key`).fill(`k${r}`);
      await page.getByLabel(`row ${r} value`).fill(`v${r}`);
    }
    await save(j, unsaved(page));
    world.editing.lookups.push(id);
    await editingIsSaved(j);
    const res = await (await j.truth()).get(`/api/areas/shared?branch=${world.branch}`);
    const table = res.config.lookupTables.find((t: { id: string }) => t.id === id);
    expect(table.rows).toHaveLength(rows);
    return `add lookup table ${id} of ${rows} row(s)`;
  },
};

const addGrokPattern: Move = {
  name: 'add-grok-pattern',
  leadsTo: ['bring-current'],
  weight: (w) => (w.password && w.editing.grokPatterns.length < 2 ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const id = `PATTERN_${world.made++}`;
    const pattern = rng.pick(['%{WORD:vendor}-%{INT:code}', '%{IP:client} %{NOTSPACE}']);
    await goto(page, leader, '#/build/grok');
    await page.getByRole('button', { name: '+ Add pattern' }).click();
    await page.getByRole('textbox', { name: 'name', exact: true }).last().fill(id);
    await page.getByRole('textbox', { name: `${id} pattern` }).fill(pattern);
    await save(j, unsaved(page));
    world.editing.grokPatterns.push(id);
    await editingIsSaved(j);
    const res = await (await j.truth()).get(`/api/areas/shared?branch=${world.branch}`);
    const saved = res.config.grokPatterns.find((p: { id: string }) => p.id === id);
    expect(saved.pattern).toBe(pattern);
    return `add grok pattern ${id}`;
  },
};

/** A JSON Schema type, and the type the schema's table spells it as. */
const JSON_TYPES = [
  ['string', 'str'],
  ['integer', 'i64'],
  ['number', 'f64'],
  ['boolean', 'bool'],
] as const;

const addSchema: Move = {
  name: 'add-schema',
  leadsTo: ['produce-schema'],
  weight: (w) => (w.password && w.editing.schemas.length < 2 ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const id = `schema-${world.made++}`;
    // Empty, or read from a JSON Schema of a few fields, some required.
    const fields = rng.chance(3)
      ? []
      : Array.from({ length: 1 + rng.below(3) }, (_, i) => {
          const [json, ty] = rng.pick(JSON_TYPES);
          return { field: `f${i}`, json, type: ty, required: rng.chance(2) };
        });
    await goto(page, leader, '#/build/schemas');
    await page.getByRole('button', { name: '+ Add schema' }).click();
    const dialog = page.getByRole('dialog');
    if (fields.length) {
      await dialog.getByRole('button', { name: 'JSON Schema', exact: true }).click();
      await dialog.getByLabel('id', { exact: true }).fill(id);
      const doc = {
        type: 'object',
        required: fields.filter((f) => f.required).map((f) => f.field),
        properties: Object.fromEntries(fields.map((f) => [f.field, { type: f.json }])),
      };
      await dialog.getByRole('textbox', { name: 'JSON Schema' }).fill(JSON.stringify(doc));
    } else {
      await dialog.getByRole('button', { name: 'Empty schema', exact: true }).click();
      await dialog.getByLabel('id', { exact: true }).fill(id);
    }
    await dialog.getByRole('button', { name: 'Add', exact: true }).click();
    await expect(dialog).toBeHidden();
    await save(j, unsaved(page));
    world.editing.schemas.push(id);
    await editingIsSaved(j);
    const res = await (await j.truth()).get(`/api/areas/shared?branch=${world.branch}`);
    const saved = res.config.schemas.find((s: { id: string }) => s.id === id);
    expect(saved.fields).toEqual(
      fields.map((f) =>
        f.required
          ? { field: f.field, type: f.type, required: true }
          : { field: f.field, type: f.type },
      ),
    );
    return `add schema ${id} of ${fields.length} field(s)`;
  },
};

/** Each pipeline and schema the pipeline does not yet produce, as `pipeline:schema`. */
const unproduced = (w: World) =>
  w.editing.pipelines.flatMap((p) =>
    w.editing.schemas.map((s) => `${p}:${s}`).filter((ps) => !w.editing.produces.includes(ps)),
  );

const produceSchema: Move = {
  name: 'produce-schema',
  weight: (w) => (unproduced(w).length ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const pair = rng.pick(unproduced(world));
    const [pipeline, schema] = pair.split(':');
    await goto(page, leader, '#/build/pipelines');
    await page.getByRole('link', { name: pipeline, exact: true }).click();
    const produces = page.locator('.produces');
    await produces.getByRole('combobox', { name: 'Add a schema it produces' }).selectOption(schema);
    await expect(produces.getByRole('link', { name: schema, exact: true })).toBeVisible();
    await save(j, () => expect(page.getByText('Saved.')).toBeVisible());
    world.editing.produces.push(pair);
    await editingIsSaved(j);
    return `name schema ${schema} under what "${pipeline}" produces`;
  },
};

const pasteSample: Move = {
  name: 'paste-sample',
  leadsTo: ['edit-sample', 'delete-sample'],
  weight: (w) => (w.password && w.editing.samples.length < 2 ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const name = `sample ${world.made++}`;
    const records = 1 + rng.below(4);
    await goto(page, leader, '#/build');
    await page.getByRole('button', { name: 'Add sample' }).click();
    await page.getByRole('dialog').getByRole('button', { name: 'Paste' }).click();
    // Found by what it holds: a branch prompt can open beside it.
    const dialog = page.getByRole('dialog').filter({ has: page.getByLabel(/^Records/) });
    await dialog.getByLabel('Name').fill(name);
    await dialog
      .getByLabel(/^Records/)
      .fill(
        Array.from({ length: records }, (_, i) => JSON.stringify({ id: i, total: i * 10 })).join(
          '\n',
        ),
      );
    await expect(dialog.getByText(`${records} record${records === 1 ? '' : 's'}`)).toBeVisible();
    await saveBy(
      j,
      dialog.getByRole('button', { name: 'Save sample' }),
      () => expect(dialog).toBeHidden(),
      true,
    );
    world.editing.samples.push(name);
    await expect(page.getByRole('row', { name: new RegExp(name) })).toBeVisible();
    await editingIsSaved(j);
    return `paste ${name} of ${records} record(s)`;
  },
};

/** The samples pasted on the Samples page, which shows `shared/`'s; a capture's belongs to its group. */
const pasted = (w: World) => w.editing.samples.filter((s) => s.startsWith('sample '));

/** The fields of `record` a cell shows: all but where it was read from and a tag field holding only its failed list. */
function ownFields(record: Record<string, unknown>): string[] {
  const tag = record.smeltr;
  const onlyFailed =
    typeof tag === 'object' && tag !== null && Object.keys(tag).join() === 'mistyped';
  return Object.keys(record).filter((k) => k !== '__source' && !(k === 'smeltr' && onlyFailed));
}

/** Edits a sample on its row, a pasted one or a capture: cuts it down to the ticked records or drops them, removes
 * one, sets a text field and pastes another record, each as the draw falls, and checks the leader holds the records
 * the draft did, each still read from where it was. */
const editSample: Move = {
  name: 'edit-sample',
  weight: (w) => (w.editing.samples.length ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const name = rng.pick(world.editing.samples);
    const api = await j.truth();
    // Read where the GUI edits: main until a save names a branch. A capture made in a group's scope is saved there.
    const on = `branch=${encodeURIComponent(world.branch ?? 'main')}`;
    let scope = 'shared';
    let id: string | undefined;
    for (scope of ['shared', 'default']) {
      const { samples } = await api.get(`/api/areas/${scope}/samples?${on}`);
      id = samples.find((s: { name: string }) => s.name === name)?.id;
      if (id) break;
    }
    const read = async () =>
      (await api.get(`/api/areas/${scope}/samples/${encodeURIComponent(id!)}?${on}`)).sample
        .records as Record<string, unknown>[];
    // Each record by the number the editor gives it, which it keeps while others go.
    let draft = (await read()).map((record, i) => ({ n: i + 1, record }));
    // The editor reads pasted lines as text wherever a record has `_raw`, whatever other fields it shows.
    const raw = draft.some((r) => '_raw' in r.record);
    await goto(page, leader, '#/build');
    if (scope !== 'shared') await switchScope(page, 'shared', scope);
    await page
      .getByRole('row', { name: new RegExp(`^${name}\\b`) })
      .getByRole('button', { name: 'Edit' })
      .click();
    const editor = page.locator('form').filter({ has: page.getByLabel('Search records') });
    await expect(editor.getByLabel(`Select record ${draft[0].n}`)).toBeVisible();
    const done: string[] = [];

    if (draft.length > 1 && rng.chance(2)) {
      const ticked = draft.filter(() => rng.chance(2));
      if (ticked.length === 0) ticked.push(rng.pick(draft));
      const keep = ticked.length === draft.length || rng.chance(2);
      for (const r of ticked) await editor.getByLabel(`Select record ${r.n}`).check();
      await editor
        .getByRole('button', { name: keep ? 'Keep only selected' : 'Remove selected' })
        .click();
      // Keeping every record changes nothing, so it is no edit and leaves nothing to save.
      if (!keep || ticked.length < draft.length)
        done.push(`${keep ? 'keep' : 'remove'} ${ticked.length} ticked`);
      draft = draft.filter((r) => ticked.includes(r) === keep);
    } else if (draft.length > 1 && rng.chance(2)) {
      const r = rng.pick(draft);
      await editor.getByRole('button', { name: `Remove record ${r.n}` }).click();
      draft = draft.filter((d) => d !== r);
      done.push(`remove record ${r.n}`);
    }
    // A text field that did not fail its type: any text is still one.
    const failed = (rec: Record<string, unknown>) =>
      ((rec.smeltr as { mistyped?: string[] } | undefined)?.mistyped ?? []) as string[];
    const texts = draft.flatMap((r) =>
      ownFields(r.record)
        .filter((f) => typeof r.record[f] === 'string' && !failed(r.record).includes(f))
        .map((field) => ({ r, field })),
    );
    if (texts.length && rng.chance(2)) {
      const { r, field } = rng.pick(texts);
      const text = `edited ${world.made++} ü`;
      // Its number, not its middle: the middle of a row can be its remove button.
      await editor
        .getByRole('row')
        .filter({ has: page.getByLabel(`Select record ${r.n}`) })
        .getByRole('cell')
        .nth(1)
        .click();
      const cell = editor.getByLabel(`${field}, record ${r.n}`);
      await cell.fill(text);
      await cell.press('Enter');
      const source = r.record.__source as Record<string, unknown> | undefined;
      r.record = { ...r.record, [field]: text };
      // A byte record is read from its bytes, so they change with its text.
      if (field === '_raw' && typeof source?.bytes === 'string')
        r.record.__source = { ...source, bytes: Buffer.from(text).toString('base64') };
      done.push(`set record ${r.n}'s ${field}`);
    }
    if (done.length === 0 || rng.chance(2)) {
      // A sample of text takes a line a record, and one of fields a JSON object a line.
      const line = raw ? `pasted ${world.made++}` : JSON.stringify({ pasted: world.made++ });
      await editor.getByLabel('Add records').fill(line);
      const record = raw ? { _raw: line } : JSON.parse(line);
      draft.push({ n: 0, record });
      done.push('paste one');
    }

    await saveBy(
      j,
      editor.getByRole('button', { name: 'Save sample' }),
      () => expect(editor).toBeHidden(),
      true,
    );
    expect(await read(), `${name} holds the edited records`).toEqual(draft.map((r) => r.record));
    await editingIsSaved(j);
    if (scope !== 'shared') await switchScope(page, scope, 'shared');
    return `edit ${name}${scope === 'shared' ? '' : ` in ${scope}`}: ${done.join(', ')}`;
  },
};

const deleteSample: Move = {
  name: 'delete-sample',
  weight: (w) => (pasted(w).length ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const name = rng.pick(pasted(world));
    await goto(page, leader, '#/build');
    const row = page.getByRole('row', { name: new RegExp(`^${name}\\b`) });
    await row.getByRole('button', { name: 'Delete' }).click();
    await saveBy(
      j,
      page.getByRole('dialog', { name: 'delete sample' }).getByRole('button', { name: 'Delete' }),
      () => expect(row).toHaveCount(0),
      true,
    );
    world.editing.samples = world.editing.samples.filter((s) => s !== name);
    await editingIsSaved(j);
    return `delete ${name}`;
  },
};

/** Picks scope `to` from the section rail's scope chip, which mounts the page afresh in it. */
async function switchScope(page: Page, from: string, to: string) {
  // A narrow window folds the rail, and the chip with it.
  const unfold = page.getByRole('button', { name: /^Show .* section names$/ });
  if (await unfold.isVisible()) await unfold.click();
  const option = page.getByRole('button', { name: new RegExp(`^${to}\\b`) });
  // The chip lists the groups it reads when it opens; a click before the page is ready opens nothing, and the
  // list can close again before its option is clicked. The reload the click starts is waited for below, not here.
  await expect(async () => {
    if (!(await option.isVisible()))
      await page.getByRole('button', { name: from, exact: true }).click();
    await option.click({ timeout: 2_000, noWaitAfter: true });
  }).toPass({ timeout: 15_000 });
  await expect(page.getByRole('button', { name: to, exact: true })).toBeVisible();
}

const capture: Move = {
  name: 'capture',
  weight: (w) => {
    const live = new Set(flowing(w).map((r) => r.to));
    return heartbeating(w, 'default').length && w.editing.destinations.some((d) => live.has(d))
      ? 3
      : 0;
  },
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const live = new Set(flowing(world).map((r) => r.to));
    const destination = rng.pick(world.editing.destinations.filter((d) => live.has(d)));
    const name = `captured ${world.made++}`;
    // A capture asks the workers running the route, and a deploy reaches them in time. One may never take it:
    // the workers share a host, so only one holds a receiver's port, and the others keep what they ran.
    const api = await j.truth();
    await expect
      .poll(
        async () => {
          const c = await api.get('/api/cluster');
          return c.health.threads.some(
            (t: { workerId: string; groupId: string; commit: string }) =>
              t.groupId === 'default' &&
              heartbeating(world).includes(t.workerId) &&
              t.commit === c.deployedCommit,
          );
        },
        { timeout: 60_000 },
      )
      .toBe(true);
    await goto(page, leader, '#/build/destinations');
    // A capture runs on a group's workers, so it is started from the group's scope.
    await switchScope(page, 'shared', 'default');
    await openRowMenu(page, `More actions for ${destination}`);
    await page
      .getByRole('menuitem', { name: 'Capture' })
      .or(page.getByRole('button', { name: 'Capture' }))
      .first()
      .click();
    await captureTwo(
      j,
      page.getByRole('dialog', { name: new RegExp(destination) }),
      name,
      (saving) => expect(saving).toContainText('__destination.route'),
      (saving) => expect(saving).toBeHidden(),
    );
    await editingIsSaved(j);
    await switchScope(page, 'default', 'shared');
    return `capture 2 rows at ${destination} as ${name}`;
  },
};

const addGroup: Move = {
  name: 'add-group',
  leadsTo: ['enroll-in-group'],
  weight: (w) => (w.password && w.editing.groups.length < 2 ? 1 : 0),
  run: async (j) => {
    const { page, world } = j;
    const name = `Group ${world.made++}`;
    const id = name.toLowerCase().replace(' ', '-');
    await openAdmin(page, 'Worker groups');
    await page.getByRole('button', { name: '+ Add group' }).click();
    const dialog = page.getByRole('dialog', { name: 'new worker group' });
    await dialog.getByRole('textbox', { name: /^Name\b/ }).fill(name);
    // The id is the name's, made fit for a directory.
    await expect(dialog.getByRole('textbox', { name: /^Id\b/ })).toHaveValue(id);
    await saveBy(j, dialog.getByRole('button', { name: 'Add' }), () =>
      expect(page.getByText(`Group ${id} saved on branch ${world.branch}`)).toBeVisible(),
    );
    await page.getByRole('button', { name: 'Done' }).click();
    world.editing.groups.push(id);
    await editingIsSaved(j);
    return `add worker group ${id}`;
  },
};

const enrollInGroup: Move = {
  name: 'enroll-in-group',
  weight: (w) =>
    w.deploys[w.running].config.groups.length && heartbeating(w).length < 3 && !receiving(w)
      ? 1
      : 0,
  run: async (j) => {
    const { page, fleet, rng, world } = j;
    // Only a group the fleet runs takes workers.
    const group = rng.pick(world.deploys[world.running].config.groups);
    const name = `w${world.made++}`;
    await openAdmin(page, 'Workers');
    await openWorkersTab(page, 'Enrollment codes');
    await page.getByRole('button', { name: '+ Add enrollment code' }).click();
    const mint = page.getByRole('dialog', { name: 'new enrollment code' });
    await mint.getByLabel(/^Group/).selectOption({ label: group.replace('group-', 'Group ') });
    await mint.getByRole('button', { name: 'Mint' }).click();
    const lines = await page
      .locator('pre')
      .filter({ hasText: 'SMELTR_WORKER_ENROLLMENT_CODE=' })
      .innerText();
    expect(lines).toContain(`SMELTR_WORKER_GROUP=${group}`);
    await page.getByRole('button', { name: 'Done' }).click();
    startWorkerFromLines(fleet, name, lines);
    await openWorkersTab(page, 'Workers');
    await workerRow(page, name, 'Heartbeating');
    world.workers.set(name, { state: 'heartbeating', group });
    const workers: { workerId: string; revoked: boolean }[] = await (
      await j.truth()
    )
      .get('/api/workers')
      .then((a) => a.workers);
    expect(workers.find((w) => w.workerId === name)?.revoked).toBe(false);
    return `enroll ${name} in ${group}`;
  },
};

type Account = {
  username?: string;
  displayName: string;
  enabled: boolean;
  authSource: string;
  roles: string[];
  groups: string[];
};

const addAccount: Move = {
  name: 'add-account',
  leadsTo: ['disable-account'],
  weight: (w) => (w.password && w.accounts.size < 2 ? 1 : 0),
  run: async (j) => {
    const { page, leader, browser, rng, world } = j;
    const username = `admin${world.made++}`;
    await openAdmin(page, 'Accounts');
    await page.getByRole('button', { name: '+ Add user' }).click();
    const dialog = page.getByRole('dialog', { name: 'new administrator' });
    await dialog.getByRole('textbox', { name: /^Display name\b/ }).fill(`Admin ${username}`);
    await dialog.getByRole('textbox', { name: /^Username\b/ }).fill(username);
    await dialog.getByRole('button', { name: 'Add' }).click();
    const handover = page.locator('section.handover').filter({ hasText: username });
    await expect(handover.getByText('it is shown only once')).toBeVisible();
    const temporary = (await handover.locator('.secret code').innerText()).trim();
    await handover.getByRole('button', { name: 'Done' }).click();
    await expect(handover).toHaveCount(0);
    world.accounts.set(username, true);
    const users: Account[] = await (await j.truth()).get('/api/users').then((a) => a.users);
    expect(users.find((u) => u.username === username)).toMatchObject({
      enabled: true,
      authSource: 'local',
    });
    if (!rng.chance(2)) return `add administrator ${username}`;

    // The one handed the password signs in with it, and chooses their own.
    const context = await browser.newContext({ ignoreHTTPSErrors: true });
    try {
      const theirs = await context.newPage();
      await theirs.goto(leader.url);
      await signInLocally(theirs, world, username, temporary);
      const chosen = `${username}-own-password`;
      await theirs.getByLabel('New password', { exact: true }).fill(chosen);
      await theirs.getByLabel('Confirm new password').fill(chosen);
      await theirs.getByRole('button', { name: 'Change password' }).click();
      await expect(theirs.getByRole('status')).toHaveText(
        'password changed -- sign in with your new one',
      );
      await signInLocally(theirs, world, username, chosen);
      await expect(theirs.getByRole('button', { name: /^signed in as / })).toBeVisible();
    } finally {
      await context.close();
    }
    return `add administrator ${username}, who signs in and chooses a password`;
  },
};

const disableAccount: Move = {
  name: 'disable-account',
  weight: (w) => ([...w.accounts.values()].some((e) => e) ? 1 : 0),
  run: async (j) => {
    const { page, rng, world } = j;
    const username = rng.pick([...world.accounts].filter(([, e]) => e).map(([u]) => u));
    await openAdmin(page, 'Accounts');
    const row = page.getByRole('row').filter({ hasText: username });
    await row.getByRole('button', { name: 'Manage' }).click();
    await page.getByRole('button', { name: 'Disable account' }).click();
    await page
      .getByRole('dialog', { name: 'disable account' })
      .getByRole('button', { name: 'Disable account' })
      .click();
    await expect(page.getByRole('status')).toHaveText('account disabled; its sessions are over');
    world.accounts.set(username, false);
    const users: Account[] = await (await j.truth()).get('/api/users').then((a) => a.users);
    expect(users.find((u) => u.username === username)?.enabled).toBe(false);
    return `disable ${username}`;
  },
};

/** The group the journey's provider puts the one person it signs in into. */
export const PROVIDER_GROUP = 'journey-people';
/** Their other group, which no role names until an administrator makes one from it on the roster. */
export const REVIEW_GROUP = 'journey-reviewers';

const LEVELS = ['viewer', 'editor', 'deployer'] as const;

/** Gives the role whose row is open a level in shared, saves it, and checks the leader holds it. Answers the level. */
async function saveRole(j: Journey, name: string): Promise<string> {
  const { page, rng, world } = j;
  const level = rng.pick(LEVELS);
  await page.getByRole('combobox', { name: 'shared level' }).selectOption(level);
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.getByRole('combobox', { name: 'shared level' })).toHaveCount(0);
  world.roles.add(name);
  const roles: { name: string; administrator: boolean; areas: Record<string, string> }[] = await (
    await j.truth()
  )
    .get('/api/roles')
    .then((a) => a.roles);
  expect(roles.find((r) => r.name === name)).toMatchObject({
    administrator: false,
    areas: { shared: level },
  });
  return level;
}

const addRole: Move = {
  name: 'add-role',
  leadsTo: ['sso-sign-in'],
  weight: (w) => (w.password && w.roles.size < 2 ? 1 : 0),
  run: async (j) => {
    const { page, world } = j;
    // The first names the provider's group, so the person it signs in holds it.
    const name = world.roles.size ? `role-${world.made++}` : PROVIDER_GROUP;
    await openAdmin(page, 'Accounts');
    await page.getByRole('link', { name: 'Roles', exact: true }).click();
    await page.getByRole('button', { name: '+ Add role' }).click();
    const dialog = page.getByRole('dialog', { name: 'new role' });
    await dialog.getByRole('textbox', { name: /^Role\b/ }).fill(name);
    await dialog.getByRole('button', { name: 'Add' }).click();
    return `add role ${name}, ${await saveRole(j, name)} in shared`;
  },
};

const turnOnAgents: Move = {
  name: 'turn-on-agents',
  leadsTo: ['set-agent-permissions', 'ask-agent'],
  weight: (w) => (w.password && !w.agentsOn ? 1 : 0),
  run: async (j) => {
    const { page, leader, model, world } = j;
    await goto(page, leader, '#/admin/system/agents');
    await page.getByRole('switch', { name: 'AI agents enabled' }).check();
    await expect(page.getByRole('combobox', { name: /^Provider\b/ })).toHaveValue('anthropic');
    await page.getByRole('textbox', { name: /^Endpoint URL\b/ }).fill(model.base);
    await expect(page.getByRole('textbox', { name: /^Model\b/ })).toHaveValue('claude-opus-5-5');
    await page.getByRole('textbox', { name: 'API key', exact: true }).fill(MODEL_KEY);
    await page.getByRole('button', { name: 'Save', exact: true }).click();
    await expect(page.getByRole('button', { name: 'replace the API key' })).toBeVisible();
    world.agentsOn = true;
    const truth = await j.truth();
    expect((await truth.get('/api/settings/agent')).settings).toEqual({
      enabled: true,
      provider: 'anthropic',
      endpoint: model.base,
      model: 'claude-opus-5-5',
      apiKeySet: true,
      keepDays: 90,
      contextTokens: MODEL_CONTEXT,
    });
    expect((await truth.get('/api/auth/session')).agents).toBe(true);
    return 'turn AI agents on';
  },
};

const setAgentPermissions: Move = {
  name: 'set-agent-permissions',
  weight: (w) => (w.password && w.agentsOn ? 1 : 0),
  run: async (j) => {
    const { page, rng } = j;
    await page.getByRole('button', { name: /^signed in as / }).click();
    await page.getByRole('button', { name: 'Agent permissions' }).click();
    await expect(page.getByRole('heading', { name: 'Agent permissions' })).toBeVisible();
    let expected: object;
    if (rng.chance(3)) {
      await page.getByRole('button', { name: 'Everything you hold' }).click();
      expected = { kind: 'everything' };
    } else {
      const level = rng.pick(LEVELS);
      const shared = rng.pick(LEVELS);
      await page.getByRole('button', { name: 'Up to a level' }).click();
      await page.getByRole('combobox', { name: /^Every area\b/ }).selectOption(level);
      await page.getByRole('combobox', { name: 'shared level' }).selectOption(shared);
      expected = { kind: 'upto', default: level, areas: { shared } };
    }
    await page.getByRole('button', { name: 'Save', exact: true }).click();
    await expect(page.getByRole('status')).toHaveText('agent permissions saved');
    expect(await (await j.truth()).get('/api/me/agent-permissions')).toEqual({
      ok: true,
      errors: [],
      ...expected,
    });
    return `set my agent's permissions to ${JSON.stringify(expected)}`;
  },
};

/** The leader keeps the administrator's conversations the world says they have. */
async function conversationsAsKept(j: Journey) {
  const listed: { conversations: { title: string }[] } = await (
    await j.truth()
  ).get('/api/agent/conversations');
  expect(listed.conversations.map((c) => c.title).sort()).toEqual(
    [...j.world.conversations].sort(),
  );
}

const askAgent: Move = {
  name: 'ask-agent',
  leadsTo: ['delete-conversation', 'ask-agent-in-panel', 'restart-conversation'],
  weight: (w) => (w.password && w.agentsOn ? 1 : 0),
  run: async (j) => {
    const { page, model, rng, world } = j;
    await openAgent(page);
    const going = world.conversations.length > 0 && rng.chance(2);
    const title = going ? rng.pick(world.conversations) : `Question ${world.made++}`;
    await (
      going ? conversationLink(page, title) : page.getByRole('link', { name: 'New conversation' })
    ).click();
    const answer = `Answer ${world.made++}`;
    model.answer([request('GET', '/api/me/agent-permissions')], [say(answer)]);
    const picked = rng.chance(2) ? await pickModel(page, rng) : null;
    await page.getByRole('textbox', { name: 'Message' }).fill(going ? 'And then?' : title);
    const before = model.asked.length;
    await page.getByRole('button', { name: 'Send', exact: true }).click();
    await expect(page.getByText(answer)).toBeVisible();
    if (picked) {
      const sent = model.asked[before] as { model?: string; output_config?: { effort?: string } };
      expect([sent.model, sent.output_config?.effort]).toEqual([picked.model, picked.effort]);
    }
    await expect(
      page.getByRole('button', { name: /^GET \/api\/me\/agent-permissions/ }).last(),
    ).toContainText('200');
    if (!going) world.conversations.push(title);
    world.restarted = world.restarted.filter((t) => t !== title);
    await conversationsAsKept(j);
    return going ? `ask my agent more in ${title}` : `ask my agent ${title}`;
  },
};

const askAgentInPanel: Move = {
  name: 'ask-agent-in-panel',
  weight: (w) => (w.password && w.agentsOn ? 1 : 0),
  run: async (j) => {
    const { page, leader, model, rng, world } = j;
    await goto(page, leader, rng.pick(['#/build/sources', '#/map', '#/admin/system/agents']));
    const chip = page.getByRole('button', { name: 'Agent panel', exact: true });
    if ((await chip.getAttribute('aria-pressed')) !== 'true') await chip.click();
    const panel = page.getByRole('complementary', { name: 'Agent' });
    const going = world.conversations.length > 0 && rng.chance(2);
    const title = going ? rng.pick(world.conversations) : `Question ${world.made++}`;
    await panel
      .getByRole('combobox', { name: 'Conversation' })
      .selectOption({ label: going ? title : 'New conversation' });
    const answer = `Answer ${world.made++}`;
    model.answer([say(answer)]);
    const box = panel.getByRole('textbox', { name: 'Message' });
    await box.fill(going ? 'And here?' : title);
    await box.press('Enter');
    await expect(panel.getByText(answer)).toBeVisible();
    if (!going) world.conversations.push(title);
    world.restarted = world.restarted.filter((t) => t !== title);
    await conversationsAsKept(j);
    if (rng.chance(2)) {
      await panel.getByRole('button', { name: 'Close the agent panel' }).click();
      await expect(panel).toHaveCount(0);
    }
    return going
      ? `ask my agent more in ${title}, in the panel`
      : `ask my agent ${title}, in the panel`;
  },
};

const restartConversation: Move = {
  name: 'restart-conversation',
  weight: (w) =>
    w.password && w.agentsOn && w.conversations.some((t) => !w.restarted.includes(t)) ? 1 : 0,
  run: async (j) => {
    const { page, model, rng, world } = j;
    const title = rng.pick(world.conversations.filter((t) => !world.restarted.includes(t)));
    await openAgent(page);
    await conversationLink(page, title).click();
    const summarize = rng.chance(2);
    const summary = `Summary ${world.made++}`;
    if (summarize) model.answer([say(summary)]);
    const before = model.asked.length;
    await openRowMenu(page, /^Context \d+% full$/);
    await page
      .getByRole('button', { name: summarize ? 'Summarize now' : 'Clear', exact: true })
      .click();
    if (summarize) {
      await expect(page.getByText(/^Summarized\./)).toBeVisible();
      const asked = JSON.stringify(model.asked[before].messages.at(-1));
      expect(asked).toContain('Summarize the conversation');
    } else {
      await page
        .getByRole('dialog', { name: 'clear context' })
        .getByRole('button', { name: 'Clear', exact: true })
        .click();
      await expect(page.getByText(/^Cleared\./).last()).toBeVisible();
      expect(model.asked.length).toBe(before);
    }
    world.restarted.push(title);
    return summarize ? `summarize ${title} for my agent` : `clear what my agent reads of ${title}`;
  },
};

const deleteConversation: Move = {
  name: 'delete-conversation',
  weight: (w) => (w.password && w.agentsOn && w.conversations.length > 0 ? 1 : 0),
  run: async (j) => {
    const { page, rng, world } = j;
    const title = rng.pick(world.conversations);
    await openAgent(page);
    await conversationLink(page, title).click();
    await page.getByRole('button', { name: 'Delete conversation' }).click();
    await page
      .getByRole('dialog', { name: 'delete conversation' })
      .getByRole('button', { name: 'Delete', exact: true })
      .click();
    world.conversations = world.conversations.filter((t) => t !== title);
    world.restarted = world.restarted.filter((t) => t !== title);
    await expect(conversationLink(page, title)).toHaveCount(0);
    await conversationsAsKept(j);
    return `delete my conversation ${title}`;
  },
};

const setUpSso: Move = {
  name: 'set-up-sso',
  leadsTo: ['sso-sign-in', 'refused-sign-in'],
  weight: (w) => (w.password && !w.sso ? 1 : 0),
  run: async (j) => {
    const { page, leader, provider, world } = j;
    await goto(page, leader, '#/admin/accounts/oidc');
    await page.getByRole('switch', { name: 'single sign-on enabled' }).check();
    await page.getByRole('textbox', { name: /^Issuer URL\b/ }).fill(provider.base);
    await page.getByRole('textbox', { name: /^Client ID\b/ }).fill(CLIENT_ID);
    await page.getByLabel('client secret', { exact: true }).fill(CLIENT_SECRET);
    await page.getByRole('button', { name: 'Save', exact: true }).click();
    // Saved, the secret is held and offers no input to type over, only Replace.
    await expect(page.getByRole('button', { name: 'replace the client secret' })).toBeVisible();
    await expect(page.getByLabel('client secret', { exact: true })).toHaveCount(0);
    world.sso = true;
    const { settings } = await (await j.truth()).get('/api/settings/oidc');
    expect(settings).toMatchObject({
      enabled: true,
      issuer: provider.base,
      clientId: CLIENT_ID,
      clientSecretSet: true,
    });
    expect(JSON.stringify(settings)).not.toContain(CLIENT_SECRET);
    return 'set up single sign-on';
  },
};

const ssoSignIn: Move = {
  name: 'sso-sign-in',
  leadsTo: ['role-from-group'],
  weight: (w) => (w.sso && w.roles.has(PROVIDER_GROUP) ? 1 : 0),
  run: async (j) => {
    const { leader, browser, provider, world } = j;
    const context = await browser.newContext({ ignoreHTTPSErrors: true });
    try {
      const theirs = await context.newPage();
      await theirs.goto(leader.url);
      await theirs.getByRole('link', { name: 'Sign in with SSO' }).click();
      await expect(theirs.getByRole('button', { name: /^signed in as / })).toHaveAccessibleName(
        new RegExp(provider.person.name),
      );
    } finally {
      await context.close();
    }
    world.ssoSignedIn = true;
    const users: Account[] = await (await j.truth()).get('/api/users').then((a) => a.users);
    const person = users.find((u) => u.authSource === 'oidc');
    expect(person).toMatchObject({
      displayName: provider.person.name,
      groups: [PROVIDER_GROUP, REVIEW_GROUP],
    });
    expect([...person!.roles].sort()).toEqual(
      [PROVIDER_GROUP, REVIEW_GROUP].filter((g) => world.roles.has(g)),
    );
    return `${provider.person.name} signs in through the provider`;
  },
};

/** The provider's person is turned away while no role names their groups, and an administrator reads why. */
const refusedSignIn: Move = {
  name: 'refused-sign-in',
  weight: (w) => (w.sso && !w.refusalSeen && !w.roles.has(PROVIDER_GROUP) ? 1 : 0),
  run: async (j) => {
    const { page, leader, browser, provider, world } = j;
    const context = await browser.newContext({ ignoreHTTPSErrors: true });
    try {
      const theirs = await context.newPage();
      await theirs.goto(leader.url);
      await theirs.getByRole('link', { name: 'Sign in with SSO' }).click();
      await expect(theirs.getByText('single sign-on refused that account')).toBeVisible();
    } finally {
      await context.close();
    }
    await openAdmin(page, 'Accounts');
    await page.getByRole('link', { name: 'Roles', exact: true }).click();
    await page.getByRole('button', { name: 'Refused sign-ins' }).click();
    const row = page.getByRole('row').filter({ hasText: 'leader.sign_in_refused' }).first();
    // The leader's own logs reach the page on its next pass, so the page is read again until they do.
    await expect(async () => {
      await page.reload();
      await expect(row).toBeVisible({ timeout: 1000 });
    }).toPass();
    await row.getByRole('button', { name: /^\d+ fields?$/ }).click();
    await expect(row).toContainText(`${PROVIDER_GROUP}, ${REVIEW_GROUP}`);
    world.refusalSeen = true;
    return `${provider.person.name} is refused, and the logs name their groups`;
  },
};

/** A role made from a group the roster shows the person holds and no role names yet. */
const roleFromGroup: Move = {
  name: 'role-from-group',
  leadsTo: ['sso-sign-in'],
  weight: (w) => (w.ssoSignedIn && !w.roles.has(REVIEW_GROUP) ? 1 : 0),
  run: async (j) => {
    const { page } = j;
    await openAdmin(page, 'Accounts');
    await page.getByRole('link', { name: 'Users', exact: true }).click();
    await page.getByRole('button', { name: `+ ${REVIEW_GROUP}` }).click();
    await expect(page.getByRole('link', { name: 'Roles', exact: true })).toHaveAttribute(
      'aria-current',
      'page',
    );
    const level = await saveRole(j, REVIEW_GROUP);
    return `make the role ${REVIEW_GROUP} from the roster, ${level} in shared`;
  },
};

const sessions: Move = {
  name: 'sessions',
  weight: (w) => (w.password ? 1 : 0),
  run: async (j) => {
    const { page, rng, world } = j;
    await openAdmin(page, 'System');
    await page.getByRole('link', { name: 'Sessions', exact: true }).click();
    const idle = page.getByLabel('Idle timeout');
    const maximum = await page.getByLabel('Maximum session age').inputValue();
    const save = page.getByRole('button', { name: 'Save sessions' });
    let drew = '';
    if (rng.chance(2)) {
      // Longer than the maximum age, in whichever unit the form shows each.
      await idle.fill(String(Number(maximum) * 100_000));
      await expect(page.getByRole('alert')).toHaveText(
        'An idle timeout longer than the maximum session age could never be reached.',
      );
      await expect(save).toBeDisabled();
      await page.getByRole('button', { name: 'Discard' }).click();
      drew = ', an impossible one refused first';
    }
    // Never shorter than it was, so the journey's own session does not idle out, and short of the maximum age.
    const before = await (await j.truth()).get('/api/settings/sessions');
    const unit = before.idleSeconds / Number(await idle.inputValue());
    const value = Math.min(
      Number(await idle.inputValue()) + 1 + rng.below(5),
      Math.floor((before.absoluteSeconds - 1) / unit),
    );
    await idle.fill(String(value));
    await save.click();
    await expect(page.getByRole('status')).toHaveText(
      'saved -- these apply to every session on the next request it makes',
    );
    const after = await (await j.truth()).get('/api/settings/sessions');
    expect(after.idleSeconds).toBe(value * unit);
    world.idle = after.idleSeconds;
    return `idle timeout ${after.idleSeconds}s${drew}`;
  },
};

/** The workers keep reporting to the leader through its changed certificate. */
async function fleetStillHeard(j: Journey) {
  const { page, world } = j;
  if (heartbeating(world).length) await openAdmin(page, 'Workers');
  for (const name of heartbeating(world)) await workerRow(page, name, 'Heartbeating');
}

const uploadCertificate: Move = {
  name: 'upload-certificate',
  weight: (w) => (w.password && !w.supplied ? 1 : 0),
  leadsTo: ['withdraw-certificate'],
  run: async (j) => {
    const pki = j.fleet.externalPki();
    j.world.certificates.set(await switchToUploaded(j.page, pki), pki);
    j.world.supplied = true;
    const { supplied, rotation } = await (await j.truth()).get('/api/settings/tls');
    expect({ supplied, rotation }).toEqual({ supplied: true, rotation: null });
    await fleetStillHeard(j);
    return `switch to an uploaded certificate, with ${heartbeating(j.world).length} workers following`;
  },
};

const withdrawCertificate: Move = {
  name: 'withdraw-certificate',
  weight: (w) => (w.supplied ? 1 : 0),
  run: async (j) => {
    await switchToLeaderIssued(j.page);
    j.world.supplied = false;
    j.world.acme = false;
    const { supplied, rotation } = await (await j.truth()).get('/api/settings/tls');
    expect({ supplied, rotation }).toEqual({ supplied: false, rotation: null });
    await fleetStillHeard(j);
    return `switch back to the leader's own certificate, with ${heartbeating(j.world).length} workers following`;
  },
};

/** The name the fleet reaches the leader by, which the authority must reach it by too. */
const LEADER_NAME = '127.0.0.1';

const orderAcme: Move = {
  name: 'order-acme',
  weight: (w) => (w.password && !w.supplied ? 1 : 0),
  leadsTo: ['renew-acme', 'acme-new-root', 'withdraw-certificate'],
  run: async (j) => {
    await switchToAcme(j.page, j.authority, [LEADER_NAME]);
    j.world.supplied = true;
    j.world.acme = true;
    j.world.ordered = true;
    const { source, rotation, served } = await (await j.truth()).get('/api/settings/tls');
    expect({ source, rotation }).toEqual({ source: 'acme', rotation: null });
    expect(served.names).toContain(LEADER_NAME);
    await fleetStillHeard(j);
    return `order a certificate over ACME, with ${heartbeating(j.world).length} workers following`;
  },
};

const renewAcmeCertificate: Move = {
  name: 'renew-acme',
  weight: (w) => (w.acme ? 1 : 0),
  run: async (j) => {
    const challenge = j.rng.pick(['TLS-ALPN-01', 'HTTP-01'] as const);
    const before = await (await j.truth()).get('/api/settings/tls');
    await renewAcme(j.page, challenge);
    const after = await (await j.truth()).get('/api/settings/tls');
    expect(after.source).toBe('acme');
    expect(after.served.fingerprint).not.toBe(before.served.fingerprint);
    expect(after.authority.fingerprint).toBe(before.authority.fingerprint);
    await fleetStillHeard(j);
    return `renew the ACME certificate over ${challenge}`;
  },
};

const acmeDns01Refused: Move = {
  name: 'acme-dns-01-refused',
  weight: (w) => (w.acme ? 1 : 0),
  run: async (j) => {
    const before = await (await j.truth()).get('/api/settings/tls/acme');
    await refuseDns01(j.page, `cf-${j.rng.below(1_000_000)}`, LEADER_NAME);
    // Refused whole: neither the challenge nor the token was stored.
    expect(await (await j.truth()).get('/api/settings/tls/acme')).toEqual(before);
    return `DNS-01 refused for ${LEADER_NAME}, an IP address`;
  },
};

const acmeUnderNewRoot: Move = {
  name: 'acme-new-root',
  weight: (w) => (w.acme ? 1 : 0),
  run: async (j) => {
    const before = await (await j.truth()).get('/api/settings/tls');
    await acmeNewRoot(j.page, j.authority);
    const after = await (await j.truth()).get('/api/settings/tls');
    expect({ source: after.source, rotation: after.rotation }).toEqual({
      source: 'acme',
      rotation: null,
    });
    expect(after.authority.fingerprint).not.toBe(before.authority.fingerprint);
    await fleetStillHeard(j);
    return `the authority issues under a new root, with ${heartbeating(j.world).length} workers following`;
  },
};

const refusedRemote: Move = {
  name: 'refused-remote',
  weight: (w) => (w.password ? 1 : 0),
  run: async (j) => {
    const { page, rng } = j;
    await openAdmin(page, 'System');
    await page.getByRole('link', { name: 'Git remote', exact: true }).click();
    const url = page.getByRole('textbox', { name: /^URL\b/ });
    const tried = rng.pick([
      'git@git.example.com:team/config.git',
      'ftp://git.example.com/team/config.git',
    ]);
    await url.fill(tried);
    await page.getByRole('button', { name: 'Save' }).click();
    await expect(page.getByRole('alert')).toHaveText(
      `"${tried}" is not a whole http:// or https:// URL. SSH is not supported`,
    );
    await url.fill('');
    // Nothing was saved: the leader has no remote.
    const { settings } = await (await j.truth()).get('/api/settings/git-remote');
    expect(settings?.url || null).toBeNull();
    return `the git remote ${tried} is refused`;
  },
};

const closeChange: Move = {
  name: 'close-change',
  leadsTo: ['reopen-change'],
  weight: (w) => (w.change && !w.closed ? 1 : 0),
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const reason = rng.chance(2) ? `Not yet, note ${world.made++}.` : null;
    await goto(page, leader, `#/changes/review/${world.branch}`);
    if (reason) await page.getByRole('textbox', { name: 'Comment', exact: true }).fill(reason);
    await page.getByRole('button', { name: 'Close change' }).click();
    await expect(page.getByText(reason ?? /Closed without a reason/).first()).toBeVisible();
    await expect(page.getByRole('button', { name: 'Reopen change' })).toBeVisible();
    await expect(page.getByRole('button', { name: /^Approve/ })).toBeDisabled();
    world.closed = true;
    const branches: { name: string; change?: { state: string } }[] = (
      await (await j.truth()).get('/api/branches')
    ).branches;
    expect(branches.find((b) => b.name === world.branch)?.change?.state).toBe('closed');
    return `close "${world.change}"${reason ? ' with a reason' : ''}`;
  },
};

const reopenChange: Move = {
  name: 'reopen-change',
  weight: (w) => (w.closed ? 2 : 0),
  run: async (j) => {
    const { page, leader, world } = j;
    await goto(page, leader, `#/changes/review/${world.branch}`);
    await page.getByRole('button', { name: 'Reopen change' }).click();
    await expect(page.getByRole('button', { name: 'Close change' })).toBeVisible();
    world.closed = false;
    const branches: { name: string; change?: { state: string } }[] = (
      await (await j.truth()).get('/api/branches')
    ).branches;
    expect(branches.find((b) => b.name === world.branch)?.change?.state).toBe('open');
    return `reopen "${world.change}"`;
  },
};

/** Picks `name` from the branch chip, which reloads the page onto it. */
async function switchBranch(page: Page, name: string) {
  await page.getByRole('button', { name: /^Branch: / }).click();
  await page.getByRole('button', { name: new RegExp(`^${name} `) }).click();
  await expect(page.getByRole('button', { name: /^Branch: / })).toHaveAccessibleName(
    new RegExp(`^Branch: ${name}\\b`),
  );
}

/** Sets the first value of lookup table `id` on `branch`, which the page edits, and saves it there. */
async function setFirstValue(j: Journey, branch: string, id: string, value: string) {
  const { page, leader } = j;
  await goto(page, leader, '#/build/lookups');
  await page.getByRole('button', { name: new RegExp(`^${id}\\b`) }).click();
  await page.getByLabel('row 1 value').fill(value);
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await unsaved(page)();
  const res = await (await j.truth()).get(`/api/areas/shared?branch=${branch}`);
  const table = res.config.lookupTables.find((t: { id: string }) => t.id === id);
  expect(table.rows[0][1], `${id} on ${branch}`).toBe(value);
}

const bringCurrent: Move = {
  name: 'bring-current',
  // A table main runs and the open change holds both, which another change then edits under it.
  weight: (w) =>
    w.change &&
    !w.closed &&
    w.deploys[w.deploys.length - 1].config.lookups.some((t) => w.editing.lookups.includes(t))
      ? 2
      : 0,
  run: async (j) => {
    const { page, leader, rng, world } = j;
    const mine = world.branch!;
    const id = rng.pick(
      world.deploys[world.deploys.length - 1].config.lookups.filter((t) =>
        world.editing.lookups.includes(t),
      ),
    );
    const n = world.made++;
    await setFirstValue(j, mine, id, `mine-${n}`);

    // Somebody else's change to the same value goes in first.
    const theirs = `other-${n}`;
    const title = `Change ${theirs}`;
    await goto(page, leader, '#/changes');
    await page.getByPlaceholder('a name for new work').fill(theirs);
    await page.getByRole('button', { name: 'Start a branch' }).click();
    // The branch menu lists the branches as they are when it opens, so not before this one is there.
    await expect(page.getByRole('row').filter({ hasText: theirs })).toBeVisible();
    await switchBranch(page, theirs);
    await setFirstValue(j, theirs, id, `theirs-${n}`);
    await goto(page, leader, '#/build');
    await page.getByRole('button', { name: 'Open for review' }).click();
    await page.getByLabel('Title').fill(title);
    await page.getByRole('button', { name: 'Open for review' }).click();
    await expect(page.getByRole('heading', { name: title })).toBeVisible();
    const verb = world.held ? 'Approve' : 'Approve & deploy';
    await page.getByRole('button', { name: verb, exact: true }).click();
    await page
      .getByRole('dialog', { name: verb.toLowerCase(), exact: true })
      .getByRole('button', { name: verb, exact: true })
      .click();
    // The deploy itself, then the reload it ends with, can outlast the default wait on a busy machine.
    await expect(page).not.toHaveURL(/#\/changes\/review/, { timeout: 30_000 });
    world.deploys.push({
      subject: title,
      config: copy(world.deploys[world.deploys.length - 1].config),
    });
    if (!world.held) world.running = world.deploys.length - 1;

    // Back on the change, which is now behind, and whose author settles the conflict.
    await switchBranch(page, mine);
    await goto(page, leader, `#/changes/review/${mine}`);
    await expect(page.getByText('Behind main by 1 commit', { exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: /^Approve/ })).toBeDisabled();
    await page.getByRole('button', { name: 'Bring current' }).click();
    await expect(page.getByRole('heading', { name: 'Conflicts' })).toBeVisible();
    const keep = rng.pick(['main', 'work'] as const);
    await page.getByLabel('Keep').selectOption(keep);
    await page.getByRole('button', { name: 'Apply' }).click();
    await expect(page.getByText('Current with main', { exact: true })).toBeVisible();
    const res = await (await j.truth()).get(`/api/areas/shared?branch=${mine}`);
    const table = res.config.lookupTables.find((t: { id: string }) => t.id === id);
    expect(table.rows[0][1]).toBe(keep === 'main' ? `theirs-${n}` : `mine-${n}`);
    await editingIsSaved(j);
    return `"${title}" edits ${id} under "${world.change}", which keeps ${keep === 'main' ? "main's" : 'its own'}`;
  },
};

/** Seeds a run takes when nothing asks for more: one long journey, which makes every move. */
export const JOURNEY_SEEDS = 1;

/** Every move. */
export const MOVES: readonly Move[] = [
  firstStart,
  signOutIn,
  enrollWorker,
  revokeWorker,
  addSource,
  routeStream,
  refusedSource,
  fillAvro,
  addDestination,
  addPipeline,
  addRoute,
  addRouteToNewDestination,
  refusedRoute,
  blastRadius,
  openChange,
  comment,
  deploy,
  rowsArrive,
  rollBack,
  release,
  addSecret,
  rotateSecret,
  regenerateKey,
  addStep,
  declareFromProduction,
  writeInto,
  addReceiver,
  addOtherDestination,
  addLookup,
  addGrokPattern,
  addSchema,
  produceSchema,
  pasteSample,
  editSample,
  deleteSample,
  capture,
  addGroup,
  enrollInGroup,
  addAccount,
  disableAccount,
  addRole,
  turnOnAgents,
  setAgentPermissions,
  askAgent,
  askAgentInPanel,
  restartConversation,
  deleteConversation,
  setUpSso,
  ssoSignIn,
  refusedSignIn,
  roleFromGroup,
  sessions,
  uploadCertificate,
  withdrawCertificate,
  orderAcme,
  renewAcmeCertificate,
  acmeDns01Refused,
  acmeUnderNewRoot,
  refusedRemote,
  closeChange,
  reopenChange,
  bringCurrent,
];

/** How much more a move the journey has not made yet, or one on the way to it, is drawn than one it has. Both
 * weigh the same, so the long way to rows flowing is taken as readily as a move that is one step away. */
const UNMADE = 16;

/** Draws the next move the world allows, or `null` when none does. A move not in `made` is drawn first, and then
 * one on the way to it, so one journey of a few more moves than there are makes them all. */
export function nextMove(rng: Rng, world: World, made: ReadonlySet<string>): Move | null {
  const byName = new Map(MOVES.map((m) => [m.name, m]));
  const toward = (m: Move, seen = new Set<string>()): boolean =>
    (m.leadsTo ?? []).some((n) => {
      if (seen.has(n)) return false;
      seen.add(n);
      return !made.has(n) || toward(byName.get(n)!, seen);
    });
  const offered = MOVES.flatMap((m) =>
    Array<Move>(m.weight(world) * (!made.has(m.name) || toward(m) ? UNMADE : 1)).fill(m),
  );
  return offered.length ? rng.pick(offered) : null;
}
