import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  TABS,
  TAB_HOME,
  editorFor,
  exitOf,
  movedTo,
  parseRoute,
  section,
  sentFrom,
  newPipelineHash,
  newRouteHash,
  routeBackHash,
  tabHome,
  withPipeline,
  trail,
  type Route,
} from './nav';
import { load, signedOut } from './session';

describe('parseRoute', () => {
  it('reads each section and the pages under it', () => {
    expect(parseRoute('#/status')).toEqual({ tab: 'status', page: 'status' });
    expect(parseRoute('#/status?deploying=c0ffee')).toEqual({
      tab: 'status',
      page: 'status',
      deploying: 'c0ffee',
    });
    expect(parseRoute('#/map')).toEqual({ tab: 'map', page: 'graph' });
    expect(parseRoute('#/build')).toEqual({ tab: 'build', page: 'samples' });
    expect(parseRoute('#/build/sources')).toEqual({ tab: 'build', page: 'sources', json: false });
    expect(parseRoute('#/build/destinations/json')).toEqual({
      tab: 'build',
      page: 'destinations',
      json: true,
    });
    expect(parseRoute('#/build/pipelines')).toEqual({ tab: 'build', page: 'pipelines' });
    expect(parseRoute('#/build/routes')).toEqual({ tab: 'build', page: 'routes', json: false });
    expect(parseRoute('#/build/routes/sample')).toEqual({
      tab: 'build',
      page: 'routes',
      json: false,
      sampling: true,
    });
    expect(parseRoute('#/build/routes/preview')).toEqual({ tab: 'build', page: 'route-preview' });
    expect(parseRoute('#/build/identities')).toEqual({
      tab: 'build',
      page: 'identities',
      json: false,
    });
    expect(parseRoute('#/build/identities/json')).toEqual({
      tab: 'build',
      page: 'identities',
      json: true,
    });
    expect(parseRoute('#/build/lookups/json')).toEqual({
      tab: 'build',
      page: 'lookups',
      json: true,
    });
    expect(parseRoute('#/build/grok')).toEqual({ tab: 'build', page: 'grok', json: false });
    expect(parseRoute('#/changes')).toEqual({ tab: 'changes', page: 'changes' });
    expect(parseRoute('#/changes/branches')).toEqual({ tab: 'changes', page: 'changes' });
    expect(parseRoute('#/changes/history')).toEqual({ tab: 'changes', page: 'history' });
    expect(parseRoute('#/admin')).toEqual({ tab: 'admin', page: 'workers', section: 'workers' });
    expect(parseRoute('#/admin/enrollment')).toEqual({
      tab: 'admin',
      page: 'workers',
      section: 'enrollment',
    });
    expect(parseRoute('#/admin/secrets')).toEqual({ tab: 'admin', page: 'secrets' });
    expect(parseRoute('#/admin/certificates')).toEqual({ tab: 'admin', page: 'certificates' });
    expect(parseRoute('#/admin/certificates/new')).toEqual({
      tab: 'admin',
      page: 'certificates',
      adding: true,
    });
    expect(parseRoute('#/admin/accounts')).toEqual({
      tab: 'admin',
      page: 'accounts',
      section: 'users',
    });
    expect(parseRoute('#/admin/accounts/roles')).toEqual({
      tab: 'admin',
      page: 'accounts',
      section: 'roles',
    });
    expect(parseRoute('#/me/agent-permissions')).toEqual({
      tab: 'me',
      page: 'agent-permissions',
    });
    expect(parseRoute('#/me/agent')).toEqual({ tab: 'me', page: 'agent' });
    expect(parseRoute('#/me/agent/c1')).toEqual({ tab: 'me', page: 'agent', conversation: 'c1' });
    expect(parseRoute('#/admin/accounts/oidc')).toEqual({
      tab: 'admin',
      page: 'accounts',
      section: 'oidc',
    });
  });

  it('reads a list narrowed to some of its objects', () => {
    expect(parseRoute('#/build/destinations?only=a&only=b%2Cc+d')).toEqual({
      tab: 'build',
      page: 'destinations',
      json: false,
      only: ['a', 'b,c d'],
    });
    expect(parseRoute('#/build/routes?only=')).toEqual({
      tab: 'build',
      page: 'routes',
      json: false,
    });
    expect(parseRoute('#/build/pipelines?only=p')).toEqual({
      tab: 'build',
      page: 'pipelines',
      only: ['p'],
    });
  });

  it('reads a branch name under review, slashes and all', () => {
    expect(parseRoute('#/changes/review/al%2Fvendor-discount')).toEqual({
      tab: 'changes',
      page: 'review',
      branch: 'al/vendor-discount',
    });
    expect(parseRoute('#/changes/review/feature/two')).toEqual({
      tab: 'changes',
      page: 'review',
      branch: 'feature/two',
    });
  });

  it('reserves its own words, so a branch called history is still reachable', () => {
    expect(parseRoute('#/changes/history')).toEqual({ tab: 'changes', page: 'history' });
    expect(parseRoute('#/changes/review/history')).toEqual({
      tab: 'changes',
      page: 'review',
      branch: 'history',
    });
  });

  it('decodes a pipeline id and keeps its json flag', () => {
    expect(parseRoute('#/build/pipelines/orders%2Fwest/json')).toEqual({
      tab: 'build',
      page: 'pipeline',
      id: 'orders/west',
      json: true,
    });
  });

  it('reads a pipeline id that looks like the sample marker as a plain id', () => {
    expect(parseRoute('#/build/pipelines/for')).toEqual({
      tab: 'build',
      page: 'pipeline',
      id: 'for',
      json: false,
    });
    expect(parseRoute('#/build/pipelines/for/json')).toEqual({
      tab: 'build',
      page: 'pipeline',
      id: 'for',
      json: true,
    });
    // A pipeline literally named `for`, opened with a carried sample: the
    // id/for/sample check runs first, so this is still that pipeline.
    expect(parseRoute('#/build/pipelines/for/for/orders-west')).toEqual({
      tab: 'build',
      page: 'pipeline',
      id: 'for',
      json: false,
      sample: 'orders-west',
    });
  });

  it('round-trips a sample carried onto the pipelines list or a pipeline', () => {
    const sample = 'orders/west, ordinary hour';
    expect(parseRoute(`#/build/pipelines/for/${encodeURIComponent(sample)}`)).toEqual({
      tab: 'build',
      page: 'pipelines',
      sample,
    });
    expect(
      parseRoute(`#/build/pipelines/orders-normalize/for/${encodeURIComponent(sample)}`),
    ).toEqual({
      tab: 'build',
      page: 'pipeline',
      id: 'orders-normalize',
      json: false,
      sample,
    });
  });

  it('reads the pipeline and sample a test locks, both encoded', () => {
    expect(
      parseRoute('#/build/tests/lock/orders-normalize/orders-west%2C%20ordinary%20hour'),
    ).toEqual({
      tab: 'build',
      page: 'lock-test',
      pipeline: 'orders-normalize',
      sample: 'orders-west, ordinary hour',
    });
  });

  it('falls back to samples when the lock route is missing its sample', () => {
    expect(parseRoute('#/build/tests/lock/orders-normalize')).toEqual({
      tab: 'build',
      page: 'samples',
    });
    expect(parseRoute('#/build/tests/lock/orders-normalize/')).toEqual({
      tab: 'build',
      page: 'samples',
    });
  });

  it('reads the sample an end-to-end test locks, naming no pipeline', () => {
    expect(parseRoute('#/build/tests/lock-e2e/orders-west%2C%20ordinary%20hour')).toEqual({
      tab: 'build',
      page: 'lock-test',
      sample: 'orders-west, ordinary hour',
    });
  });

  it('reads the test a re-lock locks again, encoded', () => {
    expect(parseRoute('#/build/tests/relock/orders%20normal%20hour')).toEqual({
      tab: 'build',
      page: 'lock-test',
      relock: 'orders normal hour',
    });
    expect(parseRoute('#/build/tests/relock')).toEqual({ tab: 'build', page: 'samples' });
  });

  it('names a re-lock in the trail', () => {
    expect(trail({ tab: 'build', page: 'lock-test', relock: 't' }).at(-1)!.label).toBe(
      're-lock a test',
    );
  });

  it('falls back to samples when the end-to-end lock route is missing its sample', () => {
    expect(parseRoute('#/build/tests/lock-e2e')).toEqual({ tab: 'build', page: 'samples' });
    expect(parseRoute('#/build/tests/lock-e2e/')).toEqual({ tab: 'build', page: 'samples' });
  });

  it('lands on Status for an empty or unknown hash', () => {
    expect(parseRoute('')).toEqual({ tab: 'status', page: 'status' });
    expect(parseRoute('#/nothing-here')).toEqual({ tab: 'status', page: 'status' });
  });

  it('reads a section with no page named as that section is landing page', () => {
    expect(parseRoute('#/build/nothing-here')).toEqual({ tab: 'build', page: 'samples' });
  });

  it('reads a node focus id under sources or destinations, distinct from json', () => {
    expect(parseRoute('#/build/sources/orders-west')).toEqual({
      tab: 'build',
      page: 'sources',
      json: false,
      focus: 'orders-west',
    });
    expect(parseRoute('#/build/destinations/es-prod')).toEqual({
      tab: 'build',
      page: 'destinations',
      json: false,
      focus: 'es-prod',
    });
    expect(parseRoute('#/build/sources/json')).toEqual({
      tab: 'build',
      page: 'sources',
      json: true,
    });
  });

  it('reads the map as one page, whatever follows it', () => {
    for (const hash of ['#/map', '#/map/sources', '#/map/anything']) {
      expect(parseRoute(hash)).toEqual({ tab: 'map', page: 'graph' });
    }
  });

  it('reads a node stats are open for, carried in the map hash so the back button closes it', () => {
    expect(parseRoute('#/map?group=west&kind=source&id=orders-west')).toEqual({
      tab: 'map',
      page: 'graph',
      stats: { group: 'west', kind: 'source', id: 'orders-west' },
    });
  });

  it('reads which tab of the panel is showing', () => {
    expect(parseRoute('#/map?group=west&kind=destination&id=es&panel=buffers')).toEqual({
      tab: 'map',
      page: 'graph',
      stats: { group: 'west', kind: 'destination', id: 'es', panel: 'buffers' },
    });
  });

  it('opens no stats for a map hash missing a piece or naming an unknown kind', () => {
    for (const hash of [
      '#/map',
      '#/map?kind=source&id=orders-west',
      '#/map?group=west&id=orders-west',
      '#/map?group=west&kind=source',
      '#/map?group=west&kind=nonsense&id=orders-west',
    ]) {
      expect(parseRoute(hash)).toEqual({ tab: 'map', page: 'graph' });
    }
  });
});

describe('movedTo', () => {
  it('rewrites every hash the flat GUI used', () => {
    expect(movedTo('#/cluster')).toBe('#/status');
    expect(movedTo('#/pipelines')).toBe('#/build/pipelines');
    expect(movedTo('#/sources')).toBe('#/build/sources');
    expect(movedTo('#/destinations')).toBe('#/build/destinations');
    expect(movedTo('#/routes')).toBe('#/build/routes');
    expect(movedTo('#/lookup-tables')).toBe('#/build/lookups');
    expect(movedTo('#/samples')).toBe('#/build');
    expect(movedTo('#/admin/system/oidc')).toBe('#/admin/accounts/oidc');
  });

  it('carries over whatever followed the section', () => {
    expect(movedTo('#/sources/json')).toBe('#/build/sources/json');
    expect(movedTo('#/lookup-tables/json')).toBe('#/build/lookups/json');
    expect(movedTo('#/pipelines/orders-normalize')).toBe('#/build/pipelines/orders-normalize');
    expect(movedTo('#/pipelines/orders-normalize/json')).toBe(
      '#/build/pipelines/orders-normalize/json',
    );
  });

  it('leaves a current hash alone, so a rewrite cannot loop', () => {
    for (const hash of [
      '',
      '#/status',
      '#/map',
      '#/build',
      '#/build/routes',
      '#/build/sources',
      '#/build/destinations',
    ]) {
      expect(movedTo(hash)).toBeNull();
    }
  });

  it('rewrites the second hop these two pages have taken, focus id and all', () => {
    expect(movedTo('#/map/sources')).toBe('#/build/sources');
    expect(movedTo('#/map/destinations')).toBe('#/build/destinations');
    expect(movedTo('#/map/sources/orders-west')).toBe('#/build/sources/orders-west');
    expect(movedTo('#/map/sources/json')).toBe('#/build/sources/json');
  });
});

describe('a route made from another page', () => {
  const make = { source: 'fw/hec', condition: '__source.sourcetype == "okta:system"' };

  it('opens a new route taking what the link says, and Save goes back', () => {
    expect(parseRoute(newRouteHash(make, '#/map?group=edge'))).toEqual({
      tab: 'build',
      page: 'routes',
      json: false,
      make,
      back: '#/map?group=edge',
    });
    expect(parseRoute(newRouteHash({ source: 'hec' }))).toEqual({
      tab: 'build',
      page: 'routes',
      json: false,
      make: { source: 'hec' },
    });
  });

  it("puts a new pipeline's id on the route it was made for, and keeps where Save goes", () => {
    const back = routeBackHash('route-a', '#/map');
    const editor = parseRoute(newPipelineHash(make, back));
    expect(editor).toEqual({
      tab: 'build',
      page: 'pipeline',
      id: 'new',
      json: false,
      for: make,
      back,
    });
    expect(parseRoute(withPipeline(back, 'pipeline-x'))).toEqual({
      tab: 'build',
      page: 'routes',
      json: false,
      only: ['route-a'],
      chose: { route: 'route-a', pipeline: 'pipeline-x' },
      back: '#/map',
    });
  });

  it('keeps the group the map showed the source in, through the route to its pipeline, raw JSON included', () => {
    const shown = { ...make, group: 'edge' };
    expect(parseRoute(newRouteHash(shown))).toMatchObject({ make: shown });
    const back = routeBackHash('route-a', '#/map', 'edge');
    expect(parseRoute(back)).toMatchObject({ page: 'routes', group: 'edge' });
    expect(parseRoute(newPipelineHash(shown, back, true))).toEqual({
      tab: 'build',
      page: 'pipeline',
      id: 'new',
      json: true,
      for: shown,
      back,
    });
  });

  it('follows only a link back into the app', () => {
    expect(parseRoute('#/build/routes?back=https%3A%2F%2Fexample.com')).toEqual({
      tab: 'build',
      page: 'routes',
      json: false,
    });
  });
});

describe('trail', () => {
  it('ends at the page itself, which never links, behind its tab and section', () => {
    const crumbs = trail(
      { tab: 'build', page: 'pipeline', id: 'pipeline-x7k2m9q1', json: false },
      { 'pipeline-x7k2m9q1': 'orders normalize' },
    );
    expect(crumbs.map((c) => c.label)).toEqual(['Build', 'Pipelines', 'orders normalize']);
    expect(crumbs.map((c) => c.hash)).toEqual(['#/build/routes', '#/build/pipelines', undefined]);
  });

  it('never names a pipeline by its id', () => {
    const label = (id: string) =>
      trail({ tab: 'build', page: 'pipeline', id, json: false }).at(-1)!.label;
    expect(label('new')).toBe('New pipeline');
    expect(label('pipeline-x7k2m9q1')).toBe('pipeline');
  });

  it('links every crumb but the last, however deep', () => {
    const crumbs = trail({ tab: 'build', page: 'route-preview' });
    expect(crumbs.map((c) => c.label)).toEqual(['Build', 'Routes', 'route preview']);
    expect(crumbs.map((c) => c.hash)).toEqual(['#/build/routes', '#/build/routes', undefined]);
  });

  it('names the pipeline a locked test belongs to, behind its tab and own section', () => {
    const crumbs = trail(
      {
        tab: 'build',
        page: 'lock-test',
        pipeline: 'pipeline-x7k2m9q1',
        sample: 'orders-west, ordinary hour',
      },
      { 'pipeline-x7k2m9q1': 'orders normalize' },
    );
    expect(crumbs.map((c) => c.label)).toEqual([
      'Build',
      'Resources',
      'orders normalize',
      'lock a test',
    ]);
    expect(crumbs.map((c) => c.hash)).toEqual([
      '#/build/routes',
      '#/build',
      '#/build/pipelines/pipeline-x7k2m9q1',
      undefined,
    ]);
  });

  it('routes back through route preview for an end-to-end locked test', () => {
    const crumbs = trail({
      tab: 'build',
      page: 'lock-test',
      sample: 'orders-west, ordinary hour',
    });
    expect(crumbs.map((c) => c.label)).toEqual([
      'Build',
      'Resources',
      'routes',
      'route preview',
      'lock a test',
    ]);
    expect(crumbs.map((c) => c.hash)).toEqual([
      '#/build/routes',
      '#/build',
      '#/build/routes',
      '#/build/routes/preview',
      undefined,
    ]);
  });

  it("names which of Resources' three pages it is on, not just the shared section", () => {
    expect(trail({ tab: 'build', page: 'samples' }).map((c) => c.label)).toEqual([
      'Build',
      'Resources',
      'Samples',
    ]);
    expect(trail({ tab: 'build', page: 'lookups', json: false }).map((c) => c.label)).toEqual([
      'Build',
      'Resources',
      'Lookup tables',
    ]);
    expect(trail({ tab: 'build', page: 'grok', json: false }).map((c) => c.label)).toEqual([
      'Build',
      'Resources',
      'Grok patterns',
    ]);
    expect(trail({ tab: 'build', page: 'identities', json: true }).map((c) => c.label)).toEqual([
      'Build',
      'Resources',
      'Connection identities',
    ]);
    // The page itself never links, same as every other trail's last crumb.
    expect(trail({ tab: 'build', page: 'lookups', json: false }).map((c) => c.hash)).toEqual([
      '#/build/routes',
      '#/build',
      undefined,
    ]);
  });

  it('is just the tab for the one page on a tab with no section nav of its own', () => {
    expect(trail({ tab: 'map', page: 'graph' })).toEqual([{ label: 'Map', hash: undefined }]);
  });

  it('drops the section crumb where its label only repeats the tab, on a landing page', () => {
    expect(trail({ tab: 'status', page: 'status' })).toEqual([
      { label: 'Status', hash: undefined },
    ]);
    expect(trail({ tab: 'changes', page: 'changes' })).toEqual([
      { label: 'Branches', hash: undefined },
    ]);
  });

  it('names a page that is simply at its section too, unlinked -- shown even though the rail already says it', () => {
    expect(trail({ tab: 'build', page: 'sources', json: false })).toEqual([
      { label: 'Build', hash: '#/build/routes' },
      { label: 'Sources', hash: undefined },
    ]);
    expect(trail({ tab: 'changes', page: 'history' })).toEqual([
      { label: 'Branches', hash: '#/changes' },
      { label: 'History', hash: undefined },
    ]);
    expect(trail({ tab: 'admin', page: 'secrets' })).toEqual([
      { label: 'Admin', hash: '#/admin' },
      { label: 'Secrets', hash: undefined },
    ]);
  });
});

describe('section', () => {
  it('marks the entry a nav hash lands on', () => {
    for (const { tab } of TABS) {
      const here = section(parseRoute(TAB_HOME[tab]));
      if (!here) continue;
      for (const item of here.items) {
        expect(section(parseRoute(item.hash))?.here).toBe(item.label);
      }
    }
  });

  it('gives a tab of one page no section nav', () => {
    expect(section({ tab: 'map', page: 'graph' })).toBeNull();
  });

  it('marks the list a page was opened from', () => {
    expect(section({ tab: 'build', page: 'route-preview' })?.here).toBe('Routes');
    expect(section({ tab: 'build', page: 'pipeline', id: 'x', json: false })?.here).toBe(
      'Pipelines',
    );
    expect(section({ tab: 'build', page: 'lock-test', sample: 's' })?.here).toBe('Resources');
    expect(section({ tab: 'changes', page: 'review', branch: 'b' })?.here).toBe('Branches');
  });

  it('gives Resources one rail entry for its three pages, Samples included', () => {
    expect(section({ tab: 'build', page: 'samples' })?.here).toBe('Resources');
    expect(section({ tab: 'build', page: 'lookups', json: false })?.here).toBe('Resources');
    expect(section({ tab: 'build', page: 'identities', json: false })?.here).toBe('Resources');
  });
});

describe('editorFor', () => {
  it('sends a section file to its page, in the shared scope', () => {
    expect(editorFor('shared/sources.json')).toEqual({
      scope: 'shared',
      hash: '#/build/sources',
    });
  });

  it('carries the group a file belongs to', () => {
    expect(editorFor('groups/prod-west/routes.json')).toEqual({
      scope: 'prod-west',
      hash: '#/build/routes',
    });
  });

  it('knows the lookup page is not named after its file', () => {
    expect(editorFor('groups/west/lookup-tables.json')?.hash).toBe('#/build/lookups');
  });

  it('sends an identities file to the identities page', () => {
    expect(editorFor('shared/identities.json')).toEqual({
      scope: 'shared',
      hash: '#/build/identities',
    });
    expect(editorFor('groups/west/identities.json')?.hash).toBe('#/build/identities');
  });

  it('opens a pipeline on its own page', () => {
    expect(editorFor('groups/west/pipelines/enrich-orders.json')).toEqual({
      scope: 'west',
      hash: '#/build/pipelines/enrich-orders',
    });
  });

  it('escapes a slug that would otherwise change the route', () => {
    expect(editorFor('shared/pipelines/a b.json')?.hash).toBe('#/build/pipelines/a%20b');
  });

  it('has no page for a group manifest', () => {
    expect(editorFor('groups/west/system.json')).toBeNull();
    expect(editorFor('shared/system.json')).toBeNull();
  });

  it('has no page for a file it does not know', () => {
    expect(editorFor('groups/west/lookups/geoip.mmdb')).toBeNull();
  });
});

describe('exitOf', () => {
  const exit = (route: Route) => exitOf(route, trail(route));

  it('goes back to the list a drilled-into page sits in', () => {
    expect(exit({ tab: 'build', page: 'pipeline', id: 'p', json: false })).toBe(
      '#/build/pipelines',
    );
    expect(exit({ tab: 'changes', page: 'review', branch: 'work' })).toBe('#/changes');
    expect(exit({ tab: 'build', page: 'route-preview' })).toBe('#/build/routes');
  });

  it('goes back to the pipeline a test is being locked from', () => {
    expect(exit({ tab: 'build', page: 'lock-test', pipeline: 'p', sample: 's' })).toBe(
      '#/build/pipelines/p',
    );
  });

  it('offers no way out of a re-lock, whose Cancel goes back to the change', () => {
    expect(exit({ tab: 'build', page: 'lock-test', relock: 't' })).toBeUndefined();
  });

  it('sends a file back to the page that sent you to it', () => {
    expect(exit({ tab: 'build', page: 'file', path: 'shared/system.json', back: '#/map' })).toBe(
      '#/map',
    );
  });

  it('offers no way out of a page the rail already places', () => {
    expect(exit({ tab: 'build', page: 'pipelines' })).toBeUndefined();
    expect(exit({ tab: 'changes', page: 'changes' })).toBeUndefined();
    expect(exit({ tab: 'build', page: 'samples' })).toBeUndefined();
  });
});

describe('what the person signed in is offered', () => {
  function signInAs(
    access: { administrator: boolean; areas: Record<string, string> },
    agents = false,
  ) {
    vi.spyOn(globalThis, 'fetch').mockImplementation(
      async () =>
        new Response(
          JSON.stringify({
            authenticated: true,
            administrator: access.administrator,
            mustChangePassword: false,
            access,
            agents,
          }),
        ),
    );
    return load();
  }

  afterEach(() => {
    signedOut();
    vi.restoreAllMocks();
  });

  it('lets anyone set their own agent permissions once AI agents are on', async () => {
    await signInAs({ administrator: false, areas: { shared: 'viewer' } }, true);
    expect(sentFrom(parseRoute('#/me/agent-permissions'))).toBeNull();
    expect(section(parseRoute('#/me/agent-permissions'))).toBeNull();
    expect(trail(parseRoute('#/me/agent-permissions'))).toEqual([{ label: 'Agent permissions' }]);
  });

  it('lets anyone talk to their own agent once AI agents are on', async () => {
    await signInAs({ administrator: false, areas: { shared: 'viewer' } }, true);
    expect(sentFrom(parseRoute('#/me/agent/c1'))).toBeNull();
    expect(section(parseRoute('#/me/agent'))).toBeNull();
    expect(trail(parseRoute('#/me/agent/c1'))).toEqual([{ label: 'Agent' }]);
  });

  it('offers no agent screen while AI agents are off', async () => {
    await signInAs({ administrator: true, areas: {} });
    expect(sentFrom(parseRoute('#/me/agent-permissions'))).toBe('#/status');
    expect(sentFrom(parseRoute('#/me/agent'))).toBe('#/status');
  });

  it('lands a viewer on a Build page they may read, never on samples', async () => {
    await signInAs({ administrator: false, areas: { shared: 'viewer' } });
    expect(tabHome('build')).toBe('#/build/routes');
    expect(sentFrom(parseRoute('#/build'))).toBe('#/build/routes');
    expect(
      section(parseRoute('#/build/sources'))?.items.find((i) => i.label === 'Resources')?.hash,
    ).toBe('#/build/lookups');
  });

  it('lands an editor on Routes, first in the rail', async () => {
    await signInAs({ administrator: false, areas: { shared: 'editor' } });
    expect(tabHome('build')).toBe('#/build/routes');
    expect(sentFrom(parseRoute('#/build'))).toBeNull();
    expect(section(parseRoute('#/build/routes'))?.items[0]).toEqual({
      label: 'Routes',
      hash: '#/build/routes',
    });
  });

  it('offers Logs and Admin to an administrator alone', async () => {
    await signInAs({ administrator: false, areas: { shared: 'deployer' } });
    const labels = () => section(parseRoute('#/status'))?.items.map((i) => i.label);
    expect(labels()).toEqual(['Status', 'Issues']);
    expect(sentFrom(parseRoute('#/status/logs?level=error'))).toBe(TAB_HOME.status);
    expect(sentFrom(parseRoute('#/admin/groups'))).toBe(TAB_HOME.status);

    await signInAs({ administrator: true, areas: {} });
    expect(labels()).toEqual(['Status', 'Issues', 'Logs']);
    expect(sentFrom(parseRoute('#/status/logs'))).toBeNull();
    expect(sentFrom(parseRoute('#/admin/groups'))).toBeNull();
  });
});
