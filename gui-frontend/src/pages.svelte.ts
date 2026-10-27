import type { Component } from 'svelte';
import type { Route } from './nav';

/** Each route's page, in a chunk of its own. Keyed by route, so a route
 * without a page fails to compile. */
const LOAD = {
  status: () => import('./ClusterPage.svelte'),
  issues: () => import('./IssuesPage.svelte'),
  logs: () => import('./LogsPage.svelte'),
  graph: () => import('./MapPage.svelte'),
  sources: () => import('./SourcesPage.svelte'),
  destinations: () => import('./DestinationsPage.svelte'),
  samples: () => import('./SamplesPage.svelte'),
  pipelines: () => import('./PipelinesListPage.svelte'),
  lookups: () => import('./LookupTablesPage.svelte'),
  grok: () => import('./GrokPatternsPage.svelte'),
  routes: () => import('./RoutesPage.svelte'),
  'route-preview': () => import('./RoutePreviewPage.svelte'),
  pipeline: () => import('./PipelineEditor.svelte'),
  'lock-test': () => import('./LockTestPage.svelte'),
  file: () => import('./FilePage.svelte'),
  changes: () => import('./BranchesPage.svelte'),
  review: () => import('./ChangeDetailPage.svelte'),
  history: () => import('./HistoryPage.svelte'),
  workers: () => import('./AdminPage.svelte'),
  groups: () => import('./AdminPage.svelte'),
  secrets: () => import('./SecretsPage.svelte'),
  certificates: () => import('./CertificatesPage.svelte'),
  accounts: () => import('./AccountsPage.svelte'),
  system: () => import('./SystemPage.svelte'),
  'agent-permissions': () => import('./AgentPermissionsPage.svelte'),
  agent: () => import('./AgentPage.svelte'),
} satisfies Record<Route['page'], () => Promise<{ default: Component<never> }>>;

type Page = keyof typeof LOAD;

/** The pages whose chunks have arrived. The shell draws a page from here, in
 * the same frame as everything else; awaiting the import there instead paints
 * the shell over an empty page for a frame or two each time. */
export const pages: { [P in Page]?: Awaited<ReturnType<(typeof LOAD)[P]>>['default'] } = $state({});

/** Fetches every page's chunk. The returned promise settles once `current` has
 * arrived, so the first paint can wait for it while the rest load behind it. */
export function preload(current: Page): Promise<void> {
  const arrive = (page: Page) =>
    LOAD[page]()
      .then(({ default: component }) => {
        (pages as Record<Page, unknown>)[page] = component;
      })
      // A chunk that fails to arrive leaves its page blank, as a failed import always did.
      .catch(() => {});
  const ready = arrive(current);
  for (const page of Object.keys(LOAD) as Page[]) if (page !== current) void arrive(page);
  return ready;
}
