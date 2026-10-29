// The shape of the GUI: five sections, what each holds, and the trail that
// says where you are. The nav, the router and the breadcrumb all read this,
// so none of them can disagree about where a page lives.
//
// Sections follow `gui-design.md`: Status is what is wrong now, Map draws
// how the objects are wired and links into the editor for each, Build is
// where they are authored, Branches is review and deploy, Admin is the fleet
// itself.

import { untrack } from 'svelte';
import { canEdit, isAdministrator } from './access';
import { session } from './session';
import { SHARED_SCOPE, type MapNodeKind } from './api';

/** `me` is what belongs to the person signed in, reached from their name chip rather than a tab of its own. */
export type Tab = 'status' | 'map' | 'build' | 'changes' | 'admin' | 'me';

/** The tabs of Admin -> System. Logging is named in the design and has no
 * leader-side settings yet, so it is not offered. */
export type SystemSection = 'general' | 'sessions' | 'certificates' | 'git' | 'agents' | 'license';

export const SYSTEM_SECTIONS: { section: SystemSection; label: string }[] = [
  { section: 'general', label: 'General' },
  { section: 'sessions', label: 'Sessions' },
  { section: 'certificates', label: 'Leader certificate' },
  { section: 'git', label: 'Git remote' },
  { section: 'agents', label: 'AI agents' },
  { section: 'license', label: 'License' },
];

/** The tabs of Admin -> Workers: the fleet's workers, and the enrollment codes that bring them in. */
export type WorkersSection = 'workers' | 'enrollment';

/** The tabs of Admin -> Accounts: the people, the roles their provider's groups give them, and the provider. */
export type AccountsSection = 'users' | 'roles' | 'oidc';

/** The tabs of Admin -> Secrets: the secrets, and the key that encrypts the ones the leader stores. */
export type SecretsSection = 'secrets' | 'key';

export type Route =
  // `deploying` is the commit a deploy just pointed the fleet at, whose
  // rollout the page follows.
  | { tab: 'status'; page: 'status'; deploying?: string }
  // Every condition the leader holds right now, grouped by what kind of
  // condition it is. A row clears the moment its fact does.
  | { tab: 'status'; page: 'issues' }
  // What the fleet's workers have said, newest first. Every filter lives
  // in the hash so a view worth showing someone is a link (design doc,
  // Observability).
  | {
      tab: 'status';
      page: 'logs';
      levels: string[];
      worker?: string;
      group?: string;
      site?: string;
      q?: string;
      /** The pager's position: rows below this id. Set from a previous read's `nextBefore`. */
      before?: number;
      /** The pager's position: rows above this id. Set from a previous read's `nextAfter`. */
      after?: number;
    }
  // The topology map itself: every source, pipeline and destination a
  // route or delivery rule names, or -- when the branch does not compile
  // -- the problems stopping it from drawing one.
  | {
      tab: 'map';
      page: 'graph';
      // The node whose stats are open, carried in the query so opening one is a link and the
      // back button closes it rather than leaving the map.
      // `panel` is which of its tabs is showing; absent is the overview.
      // `stack` says `id` names a stack of `kind` (`map.ts`'s `MapCard`) rather than one object.
      stats?: {
        group: string;
        kind: MapNodeKind;
        id: string;
        panel?: string;
        stack?: boolean;
        /** How far back its charts reach (`chartRanges`); absent is the default. */
        range?: string;
      };
      // What the search starts with, for a link that picks out several nodes at once.
      q?: string;
    }
  // `focus` is a map node's own way in: which card to land on and
  // highlight, distinct from `json`, which replaces the whole page.
  // `only` narrows a list to the objects an editor's Connections linked to (`connections.ts`).
  | { tab: 'build'; page: 'sources'; json: boolean; focus?: string; only?: string[] }
  | { tab: 'build'; page: 'destinations'; json: boolean; focus?: string; only?: string[] }
  | { tab: 'build'; page: 'samples' }
  // `sample` carries the choice from a sample row's "Run in a pipeline"
  // link, so the list can say which sample opening a pipeline will preview.
  | { tab: 'build'; page: 'pipelines'; sample?: string; only?: string[] }
  | { tab: 'build'; page: 'lookups'; json: boolean }
  | { tab: 'build'; page: 'grok'; json: boolean; only?: string[] }
  // `open` is a schema to land on, opened: what a pipeline's Produces links to.
  | { tab: 'build'; page: 'schemas'; json: boolean; open?: string }
  // `sampling` is the Route a sample tab (`ROUTES_TABS`). `make` opens a new route; `chose` puts the pipeline the editor just saved on one being made (`Making`), and
  // `group` is that one's.
  // `back` is the page Save returns to.
  | {
      tab: 'build';
      page: 'routes';
      json: boolean;
      sampling?: boolean;
      only?: string[];
      make?: Making;
      chose?: { route: string; pipeline: string };
      group?: string;
      back?: string;
    }
  | { tab: 'build'; page: 'route-preview' }
  // `sample`, carried the same way, is what the editor previews against on
  // load instead of defaulting to the first sample in the library.
  // `for` starts a new pipeline on what a route being made takes; `back` is where Save returns,
  // told the pipeline saved (`withPipeline`).
  | {
      tab: 'build';
      page: 'pipeline';
      id: string;
      json: boolean;
      sample?: string;
      for?: Making;
      back?: string;
    }
  // `pipeline` absent locks an end-to-end test instead: the sample routed
  // by its source, not one pipeline's output.
  | { tab: 'build'; page: 'lock-test'; pipeline?: string; sample: string }
  // A failing test locked again: its pipeline, sample, name and lock instant
  // are read from the test, so the hash carries only its id.
  | { tab: 'build'; page: 'lock-test'; relock: string }
  // One file's raw text, by its full path in the repository: the way to a file that does not
  // load, which no section editor can open. `back` is the page that sent you.
  | { tab: 'build'; page: 'file'; path: string; back: string }
  | { tab: 'changes'; page: 'changes' }
  | { tab: 'changes'; page: 'review'; branch: string }
  | { tab: 'changes'; page: 'history' }
  | { tab: 'admin'; page: 'workers'; section: WorkersSection }
  | { tab: 'admin'; page: 'groups' }
  | { tab: 'admin'; page: 'secrets'; section: SecretsSection }
  | { tab: 'admin'; page: 'certificates'; adding?: boolean }
  // `section` is which tab of Accounts is showing.
  | { tab: 'admin'; page: 'accounts'; section: AccountsSection }
  // `section` is which tab of System is showing; the others arrive when their
  // backends do, and the URL is already the shape they will need.
  | { tab: 'admin'; page: 'system'; section: SystemSection }
  // What the person signed in lets their own agent use.
  | { tab: 'me'; page: 'agent-permissions' }
  | { tab: 'me'; page: 'agent'; conversation?: string };

/** What a route being made takes: all of `source`, or what `condition` selects of it. `group` is where the map
 * showed the source, which a capture of a shared one needs. */
export type Making = { source: string; condition?: string; group?: string };

/** The Routes page, opening a new route that takes `make`. Save returns to `back`. */
export function newRouteHash(make: Making, back?: string): string {
  const q = new URLSearchParams({ new: make.source });
  if (make.condition) q.set('condition', make.condition);
  if (make.group) q.set('group', make.group);
  if (back) q.set('back', back);
  return `#/build/routes?${q}`;
}

/** The Routes page, showing route `route` alone, where a new pipeline's Save puts it (`withPipeline`). `group` is
 * the route's `Making` group, kept for the pipeline made for it. */
export function routeBackHash(route: string, back?: string, group?: string): string {
  const q = new URLSearchParams({ only: route, route });
  if (group) q.set('group', group);
  if (back) q.set('back', back);
  return `#/build/routes?${q}`;
}

/** A new pipeline for what a route being made takes, in the editor or as raw JSON. Save returns to `back`
 * (`withPipeline`). */
export function newPipelineHash(make: Making, back?: string, json = false): string {
  const q = new URLSearchParams({ source: make.source });
  if (make.condition) q.set('condition', make.condition);
  if (make.group) q.set('group', make.group);
  if (back) q.set('back', back);
  return `#/build/pipelines/new${json ? '/json' : ''}?${q}`;
}

/** `back`, told the pipeline a Save made. */
export function withPipeline(back: string, pipeline: string): string {
  return `${back}${back.includes('?') ? '&' : '?'}${new URLSearchParams({ pipeline })}`;
}

/** The page a link sends back to: only ever a hash of this app's own, since the link is followed, not trusted. */
function backFrom(params: URLSearchParams): { back?: string } {
  const back = params.get('back') ?? '';
  return back.startsWith('#/') ? { back } : {};
}

/** What a route being made takes, from a link. */
function makingFrom(params: URLSearchParams, key: string): Making | undefined {
  const source = params.get(key);
  if (!source) return undefined;
  const [condition, group] = [params.get('condition'), params.get('group')];
  return { source, ...(condition ? { condition } : {}), ...(group ? { group } : {}) };
}

/** Where a tab lands when its own label is clicked. */
export const TAB_HOME: Record<Tab, string> = {
  status: '#/status',
  map: '#/map',
  build: '#/build/routes',
  changes: '#/changes',
  admin: '#/admin',
  me: '#/me/agent-permissions',
};

export const TABS: { tab: Tab; label: string }[] = [
  { tab: 'status', label: 'Status' },
  { tab: 'map', label: 'Map' },
  { tab: 'build', label: 'Build' },
  { tab: 'changes', label: 'Branches' },
  { tab: 'admin', label: 'Admin' },
];

export type SectionItem = { label: string; hash: string };

/** Where a tab lands when its own label is clicked. */
export function tabHome(tab: Tab): string {
  return TAB_HOME[tab];
}

/** Where the person signed in is sent instead of `route`, a page every read of which the leader
 * would refuse them, or `null` to stay. Admin and the fleet's logs are administrators' alone. */
export function sentFrom(route: Route): string | null {
  if ((route.tab === 'admin' || route.page === 'logs') && !isAdministrator()) {
    return TAB_HOME.status;
  }
  if (route.page === 'samples' && !canEdit()) return TAB_HOME.build;
  // The agent's screens wait for AI agents to be turned on (Admin -> System -> AI agents).
  if (!session().agents) {
    if (route.page === 'agent-permissions' || route.page === 'agent') return TAB_HOME.status;
  }
  return null;
}

/** The sibling pages of a tab. A tab with a single page has no section,
 * and its pages carry no section nav. */
const SECTIONS: Partial<Record<Tab, SectionItem[]>> = {
  status: [
    { label: 'Status', hash: '#/status' },
    { label: 'Issues', hash: '#/status/issues' },
    { label: 'Logs', hash: '#/status/logs' },
  ],
  build: [
    { label: 'Routes', hash: '#/build/routes' },
    { label: 'Sources', hash: '#/build/sources' },
    { label: 'Pipelines', hash: '#/build/pipelines' },
    { label: 'Destinations', hash: '#/build/destinations' },
    { label: 'Resources', hash: '#/build' },
  ],
  changes: [
    { label: 'Branches', hash: '#/changes' },
    { label: 'History', hash: '#/changes/history' },
  ],
  admin: [
    { label: 'Workers', hash: '#/admin' },
    { label: 'Worker groups', hash: '#/admin/groups' },
    { label: 'Secrets', hash: '#/admin/secrets' },
    { label: 'Certificates', hash: '#/admin/certificates' },
    { label: 'Accounts', hash: '#/admin/accounts' },
    { label: 'System', hash: '#/admin/system/general' },
  ],
};

/** The pages behind Build's Resources rail entry: not each other's siblings
 * in the rail, but a secondary tab strip (`STYLE_GUIDE.md`, "Secondary
 * tabs") on the page Resources opens, the same way System's own sections
 * are tabs rather than rail entries. */
export const RESOURCES_TABS: SectionItem[] = [
  { label: 'Samples', hash: '#/build' },
  { label: 'Lookup tables', hash: '#/build/lookups' },
  { label: 'Grok patterns', hash: '#/build/grok' },
  { label: 'Schemas', hash: '#/build/schemas' },
];

/** The Resources tabs the person signed in may open: samples are an editor's. */
export function resourcesTabs(): SectionItem[] {
  return canEdit() ? RESOURCES_TABS : RESOURCES_TABS.filter((t) => t.label !== 'Samples');
}

/** The Routes page's tabs: the routes, and a sample run through them as they stand, unsaved edits included. */
export const ROUTES_TABS: SectionItem[] = [
  { label: 'Routes', hash: '#/build/routes' },
  { label: 'Route a sample', hash: '#/build/routes/sample' },
];

/** Admin -> Workers' tabs, in the same strip. */
export const WORKERS_TABS: SectionItem[] = [
  { label: 'Workers', hash: '#/admin' },
  { label: 'Enrollment codes', hash: '#/admin/enrollment' },
];

/** The tabs of Admin -> Accounts. The first is where the bare `#/admin/accounts` lands. */
export const ACCOUNTS_SECTIONS: { section: AccountsSection; label: string }[] = [
  { section: 'users', label: 'Users' },
  { section: 'roles', label: 'Roles' },
  { section: 'oidc', label: 'Single sign-on' },
];

export function accountsHash(section: AccountsSection): string {
  return section === 'users' ? '#/admin/accounts' : `#/admin/accounts/${section}`;
}

/** Admin -> Secrets' tabs. The first is where the bare `#/admin/secrets` lands. */
export const SECRETS_TABS: SectionItem[] = [
  { label: 'Secrets', hash: '#/admin/secrets' },
  { label: 'Encryption key', hash: '#/admin/secrets/key' },
];

export function secretsHash(section: SecretsSection): string {
  return section === 'secrets' ? '#/admin/secrets' : `#/admin/secrets/${section}`;
}

/** Admin -> Accounts' tabs, in the same strip. */
export const ACCOUNTS_TABS: SectionItem[] = ACCOUNTS_SECTIONS.map((s) => ({
  label: s.label,
  hash: accountsHash(s.section),
}));

/** Which entry of its section a page marks. A page opened from a list --
 * a pipeline, a preview, a locked test, a branch under review -- marks
 * the list it came from. */
const SECTION_OF: Record<Route['page'], string | null> = {
  status: 'Status',
  issues: 'Issues',
  logs: 'Logs',
  graph: null,
  sources: 'Sources',
  destinations: 'Destinations',
  samples: 'Resources',
  pipelines: 'Pipelines',
  routes: 'Routes',
  lookups: 'Resources',
  grok: 'Resources',
  schemas: 'Resources',
  'route-preview': 'Routes',
  pipeline: 'Pipelines',
  'lock-test': 'Resources',
  file: null,
  changes: 'Branches',
  review: 'Branches',
  history: 'History',
  workers: 'Workers',
  groups: 'Worker groups',
  secrets: 'Secrets',
  certificates: 'Certificates',
  accounts: 'Accounts',
  system: 'System',
  'agent-permissions': null,
  agent: null,
};

/** The section nav a page shows to the person signed in, or null where the tab has one page.
 * Logs are administrators' alone, and Resources opens on its first page they may read. */
export function section(route: Route): { items: SectionItem[]; here: string } | null {
  const items = SECTIONS[route.tab]
    ?.filter((i) => i.label !== 'Logs' || isAdministrator())
    .map((i) => (i.label === 'Resources' && !canEdit() ? { ...i, hash: '#/build/lookups' } : i));
  const here = SECTION_OF[route.page];
  return items && here ? { items, here } : null;
}

/** Pages that read or write within a scope: authoring, where the branch chip's scope picker (`shared` or
 * one group's own, design doc, Control plane) applies at all. Everything else names no bundle of its own -- Status
 * and the map draw the whole fleet, Branches and Admin act on a branch or the fleet as a whole -- so the
 * chip has nothing to say there. A display rule only: hiding the chip on these pages does not change the
 * stored scope, which still applies to the next authoring page. */
const SCOPED_PAGES: ReadonlySet<Route['page']> = new Set([
  'sources',
  'destinations',
  'samples',
  'pipelines',
  'pipeline',
  'routes',
  'route-preview',
  'lookups',
  'grok',
  'schemas',
  'lock-test',
]);

export function showsScope(route: Route): boolean {
  return SCOPED_PAGES.has(route.page);
}

/** Hashes from the flat one-page-per-file GUI, and where each is now. A
 * bookmark predating the sections still lands on the page it named. */
const MOVED: Record<string, string> = {
  cluster: '#/status',
  pipelines: '#/build/pipelines',
  sources: '#/build/sources',
  destinations: '#/build/destinations',
  routes: '#/build/routes',
  'lookup-tables': '#/build/lookups',
  samples: '#/build',
  'map/sources': '#/build/sources',
  'map/destinations': '#/build/destinations',
  'admin/system/oidc': '#/admin/accounts/oidc',
};

/** The hash a moved route should be rewritten to, or null when it is already
 * a current one. Whatever followed the section is carried over, so
 * `#/sources/json` and `#/pipelines/orders` both keep what they named.
 *
 * Longest key first, since a key can be several segments: `#/map/sources` is a
 * moved route while `#/map` is a current one. */
export function movedTo(hash: string): string | null {
  const parts = hash
    .replace(/^#\/?/, '')
    .split('/')
    .filter((p) => p.length > 0);
  for (let take = Math.min(3, parts.length); take > 0; take--) {
    const to = MOVED[parts.slice(0, take).join('/')];
    if (to) return [to, ...parts.slice(take)].join('/');
  }
  return null;
}

/** A list's `?only=a&only=b`: the ids it is narrowed to, or nothing where it is not. */
function only(search: string): { only?: string[] } {
  const ids = new URLSearchParams(search).getAll('only').filter((id) => id.length > 0);
  return ids.length > 0 ? { only: ids } : {};
}

export function parseRoute(hash: string): Route {
  // Split the query off first: it belongs to the route, not to whichever
  // path segment happened to come last.
  const [path, search = ''] = hash.replace(/^#\/?/, '').split('?');
  const parts = path.split('/').filter((p) => p.length > 0);
  const json = (i: number) => parts[i] === 'json';

  switch (parts[0]) {
    case 'map': {
      const params = new URLSearchParams(search);
      const group = params.get('group');
      const kind = params.get('kind');
      const id = params.get('id');
      const isKind = (k: string | null): k is MapNodeKind =>
        k === 'source' || k === 'pipeline' || k === 'destination';
      const q = params.get('q');
      return {
        tab: 'map',
        page: 'graph',
        ...(q ? { q } : {}),
        stats:
          group && id && isKind(kind)
            ? {
                group,
                kind,
                id,
                panel: params.get('panel') ?? undefined,
                ...(params.get('stack') === '1' ? { stack: true } : {}),
                ...(params.get('range') ? { range: params.get('range')! } : {}),
              }
            : undefined,
      };
    }
    case 'status': {
      if (parts[1] === 'issues') return { tab: 'status', page: 'issues' };
      if (parts[1] !== 'logs') {
        const deploying = new URLSearchParams(search).get('deploying') || undefined;
        return { tab: 'status', page: 'status', ...(deploying ? { deploying } : {}) };
      }
      // Filters ride the query string rather than path segments: they are
      // optional, unordered and several, which is what a query string is for.
      const params = new URLSearchParams(search);
      const one = (name: string) => params.get(name) || undefined;
      // A malformed or missing cursor is the same as none: nothing is lost by landing on the head
      // instead of a page that cannot be found.
      const cursor = (name: string) => {
        const n = Number(params.get(name));
        return params.has(name) && Number.isFinite(n) ? n : undefined;
      };
      return {
        tab: 'status',
        page: 'logs',
        levels: (params.get('level') ?? '').split(',').filter((l) => l.length > 0),
        worker: one('worker'),
        group: one('group'),
        site: one('site'),
        q: one('q'),
        before: cursor('before'),
        after: cursor('after'),
      };
    }
    case 'build':
      if (parts[1] === 'file') {
        const params = new URLSearchParams(search);
        const back = params.get('back') ?? '';
        return {
          tab: 'build',
          page: 'file',
          path: params.get('path') ?? '',
          // Only ever a hash of this app's own: the link is followed, not trusted.
          back: back.startsWith('#/') ? back : TAB_HOME.build,
        };
      }
      // `json` is a reserved word at this slot (as it is for every other
      // raw-JSON page); anything else there is a map node's focus id.
      if (parts[1] === 'sources' || parts[1] === 'destinations') {
        const page = parts[1];
        if (parts[2] === 'json') return { tab: 'build', page, json: true };
        if (parts[2])
          return { tab: 'build', page, json: false, focus: decodeURIComponent(parts[2]) };
        return { tab: 'build', page, json: false, ...only(search) };
      }
      if (parts[1] === 'lookups') return { tab: 'build', page: 'lookups', json: json(2) };
      if (parts[1] === 'grok')
        return { tab: 'build', page: 'grok', json: json(2), ...only(search) };
      if (parts[1] === 'schemas') {
        const open = new URLSearchParams(search).get('open');
        return { tab: 'build', page: 'schemas', json: json(2), ...(open ? { open } : {}) };
      }
      if (parts[1] === 'routes') {
        if (parts[2] === 'preview') return { tab: 'build', page: 'route-preview' };
        const params = new URLSearchParams(search);
        const make = makingFrom(params, 'new');
        const [route, pipeline] = [params.get('route'), params.get('pipeline')];
        return {
          tab: 'build',
          page: 'routes',
          json: json(2),
          ...(parts[2] === 'sample' ? { sampling: true } : {}),
          ...only(search),
          ...(make ? { make } : {}),
          ...(route && pipeline ? { chose: { route, pipeline } } : {}),
          ...(params.get('group') ? { group: params.get('group')! } : {}),
          ...backFrom(params),
        };
      }
      // The pipeline (or, for an end-to-end lock, nothing) and the sample
      // are given, not chosen here, so both travel in the hash rather than
      // being typed into the screen. An end-to-end lock needs its own
      // marker word: `parts[3]` is read unconditionally as the pipeline
      // below, so a sample alone at that position would be misread as one.
      if (parts[1] === 'tests' && parts[2] === 'lock-e2e' && parts[3]) {
        return { tab: 'build', page: 'lock-test', sample: decodeURIComponent(parts[3]) };
      }
      if (parts[1] === 'tests' && parts[2] === 'relock' && parts[3]) {
        return { tab: 'build', page: 'lock-test', relock: decodeURIComponent(parts[3]) };
      }
      if (parts[1] === 'tests' && parts[2] === 'lock' && parts[3] && parts[4]) {
        return {
          tab: 'build',
          page: 'lock-test',
          pipeline: decodeURIComponent(parts[3]),
          sample: decodeURIComponent(parts[4]),
        };
      }
      if (parts[1] === 'pipelines') {
        if (!parts[2]) return { tab: 'build', page: 'pipelines', ...only(search) };
        // `for` is reserved the same way `json` is: it says a sample
        // follows rather than that this segment is a pipeline id. Checked
        // ahead of the plain id/json case below (parts[3], not parts[2])
        // so a pipeline actually named `for`, opened with a sample, still
        // reads as that pipeline rather than as the list route.
        if (parts[3] === 'for' && parts[4]) {
          return {
            tab: 'build',
            page: 'pipeline',
            id: decodeURIComponent(parts[2]),
            json: false,
            sample: decodeURIComponent(parts[4]),
          };
        }
        // List carrying a sample, no id: `for/<sample>`. `json` stays
        // reserved here too, so a pipeline named `for` viewed as raw JSON
        // (`pipelines/for/json`, valid before this route existed) still
        // opens that pipeline instead of being read as a sample named json.
        if (parts[2] === 'for' && parts[3] && parts[3] !== 'json') {
          return { tab: 'build', page: 'pipelines', sample: decodeURIComponent(parts[3]) };
        }
        const params = new URLSearchParams(search);
        const make = parts[2] === 'new' ? makingFrom(params, 'source') : undefined;
        return {
          tab: 'build',
          page: 'pipeline',
          id: decodeURIComponent(parts[2]),
          json: json(3),
          ...(make ? { for: make } : {}),
          ...backFrom(params),
        };
      }
      return { tab: 'build', page: 'samples' };
    case 'changes':
      // `#/changes/branches` is an old link; it lands on the branch list.
      if (parts[1] === 'branches') return { tab: 'changes', page: 'changes' };
      if (parts[1] === 'history') return { tab: 'changes', page: 'history' };
      // A branch name is free text, so it lives under a segment of its own
      // rather than beside the words this section reserves.
      if (parts[1] === 'review' && parts[2]) {
        return {
          tab: 'changes',
          page: 'review',
          branch: decodeURIComponent(parts.slice(2).join('/')),
        };
      }
      return { tab: 'changes', page: 'changes' };
    case 'admin':
      if (parts[1] === 'groups') return { tab: 'admin', page: 'groups' };
      if (parts[1] === 'secrets') {
        return { tab: 'admin', page: 'secrets', section: parts[2] === 'key' ? 'key' : 'secrets' };
      }
      if (parts[1] === 'certificates')
        return parts[2] === 'new'
          ? { tab: 'admin', page: 'certificates', adding: true }
          : { tab: 'admin', page: 'certificates' };
      if (parts[1] === 'accounts') {
        const named = ACCOUNTS_SECTIONS.find((s) => s.section === parts[2]);
        return { tab: 'admin', page: 'accounts', section: named?.section ?? 'users' };
      }
      if (parts[1] === 'system') {
        const named = SYSTEM_SECTIONS.find((s) => s.section === parts[2]);
        return { tab: 'admin', page: 'system', section: named?.section ?? 'general' };
      }
      return {
        tab: 'admin',
        page: 'workers',
        section: parts[1] === 'enrollment' ? 'enrollment' : 'workers',
      };
    case 'me':
      if (parts[1] === 'agent') {
        return { tab: 'me', page: 'agent', ...(parts[2] ? { conversation: parts[2] } : {}) };
      }
      return { tab: 'me', page: 'agent-permissions' };
    default:
      return { tab: 'status', page: 'status' };
  }
}

export type Crumb = { label: string; hash?: string };

/** Pipeline names by id, for the trail: a page drilled into a pipeline is
 * named by its URL's id, which nobody should have to read. */
export type PipelineNames = Record<string, string>;

/** Raised with a `PipelineNames` whenever a page learns pipeline names. The
 * shell keeps them for `trail`, the way it draws its unsaved list from
 * `EDITS_EVENT`. */
export const PIPELINE_NAMES_EVENT = 'smeltr:pipeline-names';

/** Tells the shell what these pipelines are called. Safe to call from an
 * effect: the shell's listener reads and writes its own state, and run
 * inside the caller's effect those reads would make it depend on its write. */
export function announcePipelineNames(names: PipelineNames): void {
  if (typeof window === 'undefined') return;
  untrack(() => window.dispatchEvent(new CustomEvent(PIPELINE_NAMES_EVENT, { detail: names })));
}

/** A pipeline's name for the trail. A pipeline not saved yet reads as the
 * editor's own default name, and one whose name has not arrived as the word
 * alone. */
function pipelineCrumb(id: string, names: PipelineNames): string {
  return names[id] ?? (id === 'new' ? 'New pipeline' : 'pipeline');
}

/** The specific thing a page drilled into names -- a branch, a pipeline, a
 * locked test -- beyond the section item it belongs to. Empty for a page
 * that is simply at its section, which `trail` covers on its own -- except
 * Resources, whose pages share that one section crumb and so name
 * themselves here, the same way a `RESOURCES_TABS` entry names itself in
 * the tab strip. */
function drilledInto(route: Route, names: PipelineNames): Crumb[] {
  const [samplesTab, lookupsTab, grokTab, schemasTab] = RESOURCES_TABS;
  switch (route.page) {
    case 'samples':
      return [{ label: samplesTab.label }];
    case 'lookups':
      return [{ label: lookupsTab.label }];
    case 'grok':
      return [{ label: grokTab.label }];
    case 'schemas':
      return [{ label: schemasTab.label }];
    case 'route-preview':
      return [{ label: 'route preview' }];
    case 'pipeline':
      return [{ label: pipelineCrumb(route.id, names) }];
    case 'lock-test':
      if ('relock' in route) return [{ label: 're-lock a test' }];
      return route.pipeline
        ? [
            {
              label: pipelineCrumb(route.pipeline, names),
              hash: `#/build/pipelines/${encodeURIComponent(route.pipeline)}`,
            },
            { label: 'lock a test' },
          ]
        : [
            { label: 'routes', hash: '#/build/routes' },
            { label: 'route preview', hash: '#/build/routes/preview' },
            { label: 'lock a test' },
          ];
    case 'review':
      return [{ label: route.branch }];
    case 'file':
      return [{ label: route.path }];
    default:
      return [];
  }
}

/** Where this route sits, said the same way on every screen: the tab, then
 * -- where the tab has a rail and its own label differs from the tab's --
 * the section item it belongs to, then -- for a page reached by drilling
 * into one -- what was drilled into (`drilledInto`). Shown on every page,
 * even one that is simply at its section and whose rail already marks it,
 * since a rail scrolled out of view or narrower than `SectionShell` shows
 * at all still leaves the trail as the one place that says where a page
 * sits. Status and Branches name their own landing page the same as their
 * tab, so there the section crumb would repeat the tab's and is dropped.
 * Every crumb but the last links back to where it names; the last is
 * always the page itself. A pipeline is named from `names`. */
export function trail(route: Route, names: PipelineNames = {}): Crumb[] {
  // What belongs to the person signed in sits under no tab, so its trail is the page alone.
  if (route.tab === 'me')
    return [{ label: route.page === 'agent' ? 'Agent' : 'Agent permissions' }];
  const tabLabel = TABS.find((t) => t.tab === route.tab)!.label;
  const sectionLabel = SECTION_OF[route.page];
  const crumbs: Crumb[] = [
    { label: tabLabel, hash: TAB_HOME[route.tab] },
    ...(sectionLabel && sectionLabel !== tabLabel
      ? [
          {
            label: sectionLabel,
            hash: SECTIONS[route.tab]?.find((i) => i.label === sectionLabel)?.hash,
          },
        ]
      : []),
    ...drilledInto(route, names),
  ];
  crumbs[crumbs.length - 1].hash = undefined;
  return crumbs;
}

/** Where a page drilled into one thing goes back to, or `undefined` on a
 * page that is not drilled into, which the rail already places. That is the
 * trail's parent crumb, except a file, which goes back to the page that sent
 * you to it. A re-lock is opened from a change's tests, which no crumb names,
 * so it offers none and leaves the way back to its own Cancel. */
export function exitOf(route: Route, crumbs: Crumb[]): string | undefined {
  if (route.page === 'file') return route.back;
  if (route.page === 'lock-test' && 'relock' in route) return undefined;
  if (!DRILL_INS.has(route.page)) return undefined;
  return crumbs.at(-2)?.hash;
}

const DRILL_INS = new Set<Route['page']>(['pipeline', 'review', 'route-preview', 'lock-test']);

/** The five section files, by the Build page that edits each. */
/** Where an unsaved edit `edits.ts` is holding gets edited. Its names are the
 * page's, not the bundle's, so this is the short way round that `editorFor`
 * takes the long way. Both halves matter: landing on the right editor in the
 * wrong group is the mistake the rail exists to catch. */
export function editorForHeld(file: string): string | null {
  const pipeline = /^pipelines\/(.+)$/.exec(file);
  if (pipeline) return `#/build/pipelines/${encodeURIComponent(pipeline[1])}`;
  const page = HELD_PAGES[file];
  return page ? `#/build/${page}` : null;
}

/** The pages `edits.ts` names a file after. */
const HELD_PAGES: Record<string, string> = {
  sources: 'sources',
  destinations: 'destinations',
  routes: 'routes',
  manifest: 'routes',
  lookups: 'lookups',
  grok: 'grok',
  schemas: 'schemas',
};

const SECTION_PAGES: Record<string, string> = {
  'sources.json': 'sources',
  'destinations.json': 'destinations',
  'routes.json': 'routes',
  'lookup-tables.json': 'lookups',
  'grok-patterns.json': 'grok',
};

/** Where a bundle path is edited: the scope holding it, and the page holding
 * the objects it carries. The scope matters as much as the page -- landing
 * on the right editor in the wrong group is the mistake the rail exists to
 * catch, so a caller sets both or neither.
 *
 * `system.json` is a group's manifest, which no Build page edits, so it maps
 * to nothing rather than to a page that does not hold it. */
export function editorFor(path: string): { scope: string; hash: string } | null {
  const group = /^groups\/([^/]+)\//.exec(path);
  const scope = group ? group[1] : SHARED_SCOPE;
  const file = path.replace(/^groups\/[^/]+\//, '').replace(/^shared\//, '');

  const pipeline = /^pipelines\/(.+)\.json$/.exec(file);
  if (pipeline) return { scope, hash: `#/build/pipelines/${encodeURIComponent(pipeline[1])}` };
  if (/^schemas\/.+\.json$/.test(file)) return { scope, hash: '#/build/schemas' };

  const page = SECTION_PAGES[file];
  return page ? { scope, hash: `#/build/${page}` } : null;
}
