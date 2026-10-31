<!--
  The shell: five sections, a hash router dispatching to each one's page, the
  branch every config call is made against, and the theme. `nav.ts` owns the
  shape; this draws it. The `:root` block below is the source of truth for the
  design tokens STYLE_GUIDE.md describes.
-->
<script lang="ts">
  import {
    TABS,
    TAB_HOME,
    PIPELINE_NAMES_EVENT,
    exitOf,
    movedTo,
    parseRoute,
    sentFrom,
    showsScope,
    tabHome,
    trail,
    type PipelineNames,
    type Route,
  } from './nav';
  import AuthScreen from './AuthScreen.svelte';
  import CrossIcon from './CrossIcon.svelte';
  import { store, stored } from './stored';
  import ErrorBar from './ErrorBar.svelte';
  import { heldAreas, isAdministrator, levelIn, settleScope } from './access';
  import { pages, preload } from './pages.svelte';
  import Brand from './Brand.svelte';
  import {
    ACCESS_EVENT,
    SESSION_EVENT,
    SIGNED_OUT,
    leaderAnswered,
    load as loadSession,
    reportActivity,
    session as currentSession,
    signOut,
    type SessionDto,
  } from './session';
  import BranchPrompt from './BranchPrompt.svelte';
  import Confirm from './Confirm.svelte';
  import { confirmContextSwitch } from './unsavedGuard';
  import ChipPicker from './ChipPicker.svelte';
  import LevelMark from './LevelMark.svelte';
  import BranchBar from './BranchBar.svelte';
  import DemoBar from './DemoBar.svelte';
  import Locked from './Locked.svelte';
  import HeldWork from './HeldWork.svelte';
  import Version from './Version.svelte';
  import AgentPanel from './AgentPanel.svelte';
  import ChatIcon from './ChatIcon.svelte';
  import PersonIcon from './PersonIcon.svelte';
  import {
    currentBranch,
    listBranches,
    setBranch,
    currentScope,
    SCOPE_EVENT,
    leaveScope,
    listWorkerGroups,
    SHARED_SCOPE,
    onBranchNeeded,
    MAIN_BRANCH,
    getSystemInfo,
    type BranchDto,
    type SystemInfoDto,
    type WorkerGroupDto,
  } from './api';

  /** A hash from the flat GUI is rewritten before it is parsed, so an old
   * bookmark lands on the page it named rather than on Status. */
  function readRoute(): Route {
    const moved = movedTo(location.hash);
    if (moved) location.replace(moved);
    const read = parseRoute(moved ?? location.hash);
    // A link to a page whose every read would be refused lands somewhere the person may read
    // instead of on a page of refusals.
    const to = currentSession().authenticated ? sentFrom(read) : null;
    if (to) {
      location.replace(to);
      return parseRoute(to);
    }
    return read;
  }

  let route: Route = $state(readRoute());
  if (typeof window !== 'undefined') {
    window.addEventListener('hashchange', () => {
      route = readRoute();
    });
  }

  // -- who is signed in --------------------------------------------------
  //
  // Nothing renders until the leader has answered. Drawing the app first and
  // replacing it a moment later shows the operator surface to somebody who may
  // not be entitled to it, and reads as a flicker rather than as a decision.

  let who = $state<SessionDto>(SIGNED_OUT);
  let asked = $state(false);
  /// Whether the leader could speak for itself. Not the same as being signed
  /// out, and the route is careful to answer them differently: a sign-in form
  /// in front of an unavailable account store is one nobody can get past.
  let reachable = $state(true);
  /// Asked for rather than required. Not a route: it is something you do
  /// once and leave, not a place the app can be at.
  let changingPassword = $state(false);
  // Why the sign-in screen is showing, when there is a reason worth giving.
  // Choosing a password ends the session that asked, and the screen that
  // asked with it, so the news outlives it here.
  let signInNotice = $state('');
  const CHANGED = 'password changed -- sign in with your new one';

  async function askWhoIsSignedIn() {
    const page = preload(route.page);
    who = await loadSession();
    reachable = leaderAnswered();
    // Before the first page reads with them.
    if (who.authenticated) {
      settleScope();
      route = readRoute();
    }
    // The shell first paints with its page; the screens before it show none.
    if (reachable && who.authenticated && !who.mustChangePassword) await page;
    asked = true;
  }

  if (typeof window !== 'undefined') {
    void askWhoIsSignedIn();
    // Raised wherever a call was refused, so one `401` anywhere is one
    // signed-out shell rather than an error on each page that made a call.
    window.addEventListener(SESSION_EVENT, () => {
      who = currentSession();
      // Both facts move together. A re-ask that could not reach the leader
      // must show that, rather than a sign-in form which cannot work either.
      reachable = leaderAnswered();
      if (who.authenticated) {
        signInNotice = '';
        settleScope();
        route = readRoute();
      } else changingPassword = false;
    });
    // A role changed under the session: what the shell offers follows it.
    window.addEventListener(ACCESS_EVENT, () => {
      who = currentSession();
      settleScope();
      route = readRoute();
    });
    // The idle clock restarts when somebody does something. Per interaction,
    // never on a timer: a tab left open is not a person working.
    for (const signal of ['pointerdown', 'keydown']) {
      window.addEventListener(signal, () => void reportActivity(), { passive: true });
    }
  }

  // Admin is administrators' alone, so nobody else is shown the way there.
  let administrator = $derived(who.authenticated && isAdministrator());
  // Read from `who`, so a role that changes under the session redraws them.
  let tabs = $derived(
    TABS.filter((t) => t.tab !== 'admin' || administrator).map((t) => ({
      ...t,
      hash: who.authenticated ? tabHome(t.tab) : '',
    })),
  );

  /** Which smeltr this leader runs, for the band. Without an answer the band shows none, and whatever the
   * person was doing reports the failure itself. */
  let systemInfo = $state<SystemInfoDto | null>(null);
  $effect(() => {
    if (!shellShown) return;
    getSystemInfo()
      .then((next) => (systemInfo = next))
      .catch(() => {});
  });

  /** Pipeline names the pages have announced, so a crumb names a pipeline rather than its id. */
  let pipelineNames = $state<PipelineNames>({});
  window.addEventListener(PIPELINE_NAMES_EVENT, (e) => {
    pipelineNames = { ...pipelineNames, ...(e as CustomEvent<PipelineNames>).detail };
  });
  let crumbs = $derived(trail(route, pipelineNames));
  /** The app itself is on screen, rather than signing in or changing a password. */
  let shellShown = $derived(
    asked && reachable && who.authenticated && !who.mustChangePassword && !changingPassword,
  );
  // Behind a sign-in screen the page is not showing, so there is nothing for
  // Escape to leave, and leaving would lose the link that was followed.
  let exit = $derived(shellShown ? exitOf(route, crumbs) : undefined);

  /** Escape leaves a drilled-into page, unless something else took the key:
   * a field, a dialog, a popover, or a menu that closed on it. Read after
   * every other listener has run, since a menu that closes on Escape may
   * be listening on the window too. */
  function leaveOnEscape(e: KeyboardEvent) {
    if (e.key !== 'Escape' || !exit) return;
    if ((e.target as Element).closest('input, textarea, select, [contenteditable]')) return;
    if (document.querySelector('dialog[open], :popover-open')) return;
    const to = exit;
    setTimeout(() => {
      if (!e.defaultPrevented && exit === to) location.hash = to;
    });
  }

  // -- the scope ---------------------------------------------------------
  // Unlike the branch, a scope switch keeps the tab: unsaved work in another
  // group lives only in its memory (`edits.ts`). A scoped page is mounted afresh
  // instead.

  let scopeNow = $state(currentScope());
  $effect(() => {
    const moved = () => (scopeNow = currentScope());
    window.addEventListener(SCOPE_EVENT, moved);
    return () => window.removeEventListener(SCOPE_EVENT, moved);
  });

  // -- the branch --------------------------------------------------------
  // Every config call carries it (`api.ts`), so switching is a reload: the
  // pages hold what they read, and what they read was branch-scoped.

  let branch = $state(currentBranch());
  let picking = $state(false);
  let branches = $state<BranchDto[]>([]);

  // main is never edited directly (W1): a write aimed at it stops and asks
  // which branch to go on, so nobody lands somewhere they did not choose.
  // `api.ts` waits on this answer, so the write is held until it comes.
  let branchAsk = $state<{ suggested: string; resolve: (b: string | null) => void } | null>(null);

  onBranchNeeded(
    (suggested) =>
      new Promise<string | null>((resolve) => {
        branchAsk = { suggested, resolve };
      }),
  );

  /** An existing branch may hold edits this page never read -- it was showing `main`. Landing the
   * write there sight unseen risks overwriting them, the same way any other switch onto a branch
   * with unseen state would. So an existing branch gets the ordinary switch, a reload, and the
   * write is not resumed; the operator sees what is actually there and redoes it once satisfied.
   * A branch this names for the first time carries nothing to miss, so the write it started for
   * goes on straight away. */
  async function answerBranch(chosen: string | null) {
    // The request this answer belongs to. Canceling (Escape, "Don't save") clears `branchAsk`
    // before the lookup below can return, and the write it would have resumed must stay canceled
    // -- not get resolved a second time, or acted on, by a check that is already stale.
    const asked = branchAsk;
    if (!chosen) {
      asked?.resolve(null);
      branchAsk = null;
      return;
    }
    // `listBranches` can throw outright -- unreachable, a non-2xx response, a body that is not
    // JSON -- rather than answering `{ ok: false }`; either way leaves `asked` unresolved unless
    // caught here.
    let res: Awaited<ReturnType<typeof listBranches>> | undefined;
    try {
      res = await listBranches();
    } catch {
      // fall through with res left undefined
    }
    if (branchAsk !== asked) return;
    // Unknown whether `chosen` is a branch already carrying edits this page never read -- so, the
    // same as one confirmed to be that, the write does not resume; the operator sees the error and
    // tries again once branches can be listed.
    if (!res?.ok) {
      asked?.resolve(null);
      branchAsk = null;
      return;
    }
    const exists = res.branches.some((b) => b.name === chosen);
    asked?.resolve(exists ? null : chosen);
    branchAsk = null;
    if (exists) {
      // The same guard an operator-driven branch switch already gets: reloading is what makes
      // landing on the existing branch honest, but it is still a reload, and one still holding a
      // draft the pipeline editor hasn't saved deserves the chance to say so first.
      if (!(await confirmContextSwitch())) return;
      setBranch(chosen);
      location.reload();
      return;
    }
    // The chip names the branch every config call is made against, and this
    // answer is what changed it.
    branch = chosen;
  }

  async function loadBranches() {
    const res = await listBranches();
    branches = res.ok ? res.branches : [];
  }

  async function switchTo(name: string) {
    if (name === branch) {
      picking = false;
      return;
    }
    if (!(await confirmContextSwitch())) return;
    setBranch(name);
    location.reload();
  }

  let identityPicking = $state(false);
  /** Whether the agent panel is folded up, for this browser tab. */
  let agentPanel = $state(stored('smeltr-agent-panel', 'session') === 'open');
  $effect(() => store('smeltr-agent-panel', agentPanel ? 'open' : '', 'session'));

  // -- theme -------------------------------------------------------------
  // Light/dark and the palette, each remembered across visits. The root
  // always carries a resolved `data-theme`, so the palettes' CSS needs no
  // media query; `index.html` sets both before first paint.

  type Theme = 'system' | 'light' | 'dark';
  const THEME_KEY = 'smeltr-theme';

  /** What the theme menu offers, and what each choice means. */
  const THEMES: { value: Theme; label: string; says: string }[] = [
    { value: 'system', label: 'System', says: 'whatever this device is set to' },
    { value: 'light', label: 'Light', says: 'always light' },
    { value: 'dark', label: 'Dark', says: 'always dark' },
  ];

  type Palette = 'foundry' | 'ember' | 'frost' | 'verdigris' | 'amethyst';
  const PALETTE_KEY = 'smeltr-palette';

  /** The palettes, the default first. Each is a `[data-palette]` block below. */
  const PALETTES: { value: Palette; label: string }[] = [
    { value: 'foundry', label: 'Foundry' },
    { value: 'ember', label: 'Ember' },
    { value: 'frost', label: 'Frost' },
    { value: 'verdigris', label: 'Verdigris' },
    { value: 'amethyst', label: 'Amethyst' },
  ];

  function loadTheme(): Theme {
    const saved = stored(THEME_KEY);
    return saved === 'light' || saved === 'dark' ? saved : 'system';
  }

  function loadPalette(): Palette {
    const saved = stored(PALETTE_KEY);
    return PALETTES.find((p) => p.value === saved)?.value ?? 'foundry';
  }

  let theme: Theme = $state(loadTheme());
  let palette: Palette = $state(loadPalette());

  $effect(() => {
    if (typeof document === 'undefined') return;
    const root = document.documentElement;
    store(THEME_KEY, theme);
    if (theme !== 'system') {
      root.setAttribute('data-theme', theme);
      return;
    }
    const system = window.matchMedia('(prefers-color-scheme: dark)');
    const follow = () => root.setAttribute('data-theme', system.matches ? 'dark' : 'light');
    follow();
    system.addEventListener('change', follow);
    return () => system.removeEventListener('change', follow);
  });

  $effect(() => {
    if (typeof document === 'undefined') return;
    // Foundry is the bare `:root`, so the default carries no attribute.
    if (palette === 'foundry') document.documentElement.removeAttribute('data-palette');
    else document.documentElement.setAttribute('data-palette', palette);
    store(PALETTE_KEY, palette);
  });

  // The pipeline editor is a two-pane workspace with its own draggable
  // divider, not prose or a form with a reading measure to protect -- so
  // unlike every other page it claims the full width. On the root, the same
  // way `ScopePicker` sets `--scope-h`, so the nav row and breadcrumb widen
  // with it (`app.css`) rather than the page alone.
  $effect(() => {
    const root = document.documentElement.style;
    if (route.page === 'pipeline') root.setProperty('--measure', '100%');
    else root.removeProperty('--measure');
  });
</script>

<svelte:window onkeydown={leaveOnEscape} />

{#if shellShown}
  <nav class="top-nav">
    <div class="nav-row nav-identity">
      <Brand />

      <div class="nav-end">
        <ChipPicker
          bind:open={picking}
          kind="branch"
          title={branch}
          ariaLabel="Branch: {branch}{branch === 'main' ? ' · read-only' : ''}"
          onopen={loadBranches}
        >
          {#snippet face()}
            <span class="dim">Branch</span>
            {branch}{#if branch === 'main'}<span class="branch-note"> &middot; read-only</span>{/if}
          {/snippet}
          <button
            type="button"
            class="pick"
            class:on={branch === 'main'}
            onclick={() => switchTo('main')}
          >
            <span class="mono">main</span>
            <span class="dim">what the fleet runs &mdash; read-only</span>
          </button>
          {#each branches as b (b.name)}
            <button
              type="button"
              class="pick"
              class:on={branch === b.name}
              onclick={() => switchTo(b.name)}
            >
              <span class="mono">{b.name}</span>
              <span class="dim">{b.commits} ahead{b.behind > 0 ? `, ${b.behind} behind` : ''}</span>
            </button>
          {/each}
          {#if branch !== 'main' && !branches.some((b) => b.name === branch)}
            <button type="button" class="pick on">
              <span class="mono">{branch}</span>
              <span class="dim">nothing saved on it yet, so it reads as main</span>
            </button>
          {/if}
        </ChipPicker>

        <HeldWork />

        {#if who.agents && route.page !== 'agent'}
          <button
            type="button"
            class="agent-chip"
            class:on={agentPanel}
            aria-label="Agent panel"
            aria-pressed={agentPanel}
            onclick={() => (agentPanel = !agentPanel)}><ChatIcon /><span>Agent</span></button
          >
        {/if}

        <!-- Who is signed in, and what belongs to them: their name, the way
           out, and the theme -- chosen once and theirs, so it lives here
           rather than taking a place on every screen. -->
        <ChipPicker
          bind:open={identityPicking}
          kind="identity"
          ariaLabel="signed in as {who.displayName}"
        >
          {#snippet face()}<PersonIcon /><span class="who-face">{who.displayName}</span
            >{#if administrator}<span class="dim"><LevelMark level="administrator" /></span
              >{/if}<span class="caret" aria-hidden="true">&#9662;</span>{/snippet}

          <div class="who">
            <div class="who-name">{who.displayName}</div>
            <div class="dim">
              {[
                who.authSource === 'oidc' ? who.providerLogin : who.username,
                administrator ? 'administrator' : '',
              ]
                .filter(Boolean)
                .join(' · ')}
            </div>
            {#if !administrator}
              <div class="held">
                <span class="dim held-head">Groups</span>
                {#each heldAreas() as area (area)}
                  {@const level = levelIn(area)}
                  {#if level}<span class="mono">{area}</span><span class="dim"
                      ><LevelMark {level} /></span
                    >{/if}
                {/each}
              </div>
            {/if}
          </div>
          <!-- An OIDC account holds no password here; its provider owns it. -->
          {#if who.authSource !== 'oidc'}
            <button
              type="button"
              class="pick"
              onclick={() => {
                identityPicking = false;
                changingPassword = true;
              }}
            >
              <span>Change password</span>
            </button>
          {/if}
          {#if who.agents}
            <button
              type="button"
              class="pick"
              onclick={() => {
                identityPicking = false;
                location.hash = '#/me/agent';
              }}
            >
              <span>Agent</span>
            </button>
            <button
              type="button"
              class="pick"
              onclick={() => {
                identityPicking = false;
                location.hash = TAB_HOME.me;
              }}
            >
              <span>Agent permissions</span>
            </button>
          {/if}
          <button
            type="button"
            class="pick"
            onclick={async () => {
              identityPicking = false;
              // Signing out unmounts whatever was open without a navigation, so
              // the editor's own guard never fires and the work would go
              // silently. Switching branches asks the same question.
              if (!(await confirmContextSwitch())) return;
              void signOut();
            }}
          >
            <span>Sign out</span>
          </button>

          <div class="pick-seam"></div>
          {#each THEMES as option (option.value)}
            <button
              type="button"
              class="pick"
              class:on={theme === option.value}
              onclick={() => {
                theme = option.value;
                identityPicking = false;
              }}
            >
              <span>{option.label}</span>
              <span class="dim">{option.says}</span>
            </button>
          {/each}

          <div class="pick-seam"></div>
          {#each PALETTES as option (option.value)}
            <button
              type="button"
              class="pick"
              class:on={palette === option.value}
              onclick={() => {
                palette = option.value;
                identityPicking = false;
              }}
            >
              <span>{option.label}</span>
            </button>
          {/each}
        </ChipPicker>
      </div>
    </div>

    <div class="nav-row nav-tabs">
      <div class="tabs">
        {#each tabs as item}
          <a href={item.hash} class:active={route.tab === item.tab}>{item.label}</a>
        {/each}
      </div>
      {#if systemInfo}
        <span class="nav-version">
          <!-- A newer release is the administrators' to act on, where the check for it is set. -->
          <Version
            info={systemInfo}
            newerHref={administrator ? '#/admin/system/general' : undefined}
          />
        </span>
      {/if}
    </div>
  </nav>

  {#if import.meta.env.MODE === 'demo'}<DemoBar />{/if}
  {#if branch !== MAIN_BRANCH}<BranchBar {branch} />{/if}
  <ErrorBar />

  <BranchPrompt
    open={branchAsk !== null}
    suggested={branchAsk?.suggested ?? ''}
    onanswer={answerBranch}
  />

  <!-- The app's one confirm dialog; pages reach it through `ask()`. -->
  <Confirm />

  {#if crumbs.length > 0}
    <!-- On a drilled-into page the trail is the way back out, so it says so. -->
    <nav class="crumbs" class:exit aria-label="Where you are">
      {#if exit}<a class="btn back" href={exit} aria-label="Back">&larr;</a>{/if}
      {#each crumbs as crumb, i}
        {#if i > 0}<span class="sep">/</span>{/if}
        {#if crumb.hash}<a href={crumb.hash}>{crumb.label}</a>{:else}<span>{crumb.label}</span>{/if}
      {/each}
      {#if exit}
        <kbd class="key">Esc</kbd>
        <a class="btn close" href={exit}><CrossIcon /> Close</a>
      {/if}
    </nav>
  {/if}

  <!-- A scoped page holds what it read under the scope it opened in. A new scope mounts it afresh. -->
  {#key showsScope(route) ? scopeNow : ''}
    {#if route.page === 'status'}
      {#if pages.status}
        {@const ClusterPage = pages.status}
        <ClusterPage deploying={route.deploying} />
      {/if}
    {:else if route.page === 'issues'}
      {#if pages.issues}
        {@const IssuesPage = pages.issues}
        <IssuesPage />
      {/if}
    {:else if route.page === 'logs'}
      {#if pages.logs}
        {@const LogsPage = pages.logs}
        <LogsPage
          filters={{
            levels: route.levels,
            worker: route.worker,
            group: route.group,
            site: route.site,
            q: route.q,
            before: route.before,
            after: route.after,
          }}
        />
      {/if}
    {:else if route.page === 'graph'}
      {#if pages.graph}
        {@const MapPage = pages.graph}
        <MapPage stats={route.stats} q={route.q} />
      {/if}
    {:else if route.page === 'sources'}
      {#if pages.sources}
        {@const SourcesPage = pages.sources}
        <SourcesPage showJson={route.json} focus={route.focus} only={route.only} />
      {/if}
    {:else if route.page === 'destinations'}
      {#if pages.destinations}
        {@const DestinationsPage = pages.destinations}
        <DestinationsPage showJson={route.json} focus={route.focus} only={route.only} />
      {/if}
    {:else if route.page === 'samples'}
      {#if pages.samples}
        {@const SamplesPage = pages.samples}
        <SamplesPage />
      {/if}
    {:else if route.page === 'pipelines'}
      {#if pages.pipelines}
        {@const PipelinesListPage = pages.pipelines}
        <PipelinesListPage sample={route.sample} only={route.only} />
      {/if}
    {:else if route.page === 'grok'}
      {#if pages.grok}
        {@const GrokPatternsPage = pages.grok}
        <GrokPatternsPage showJson={route.json} only={route.only} />
      {/if}
    {:else if route.page === 'schemas'}
      {#if pages.schemas}
        {@const SchemasPage = pages.schemas}
        <SchemasPage showJson={route.json} open={route.open} />
      {/if}
    {:else if route.page === 'lookups'}
      {#if pages.lookups}
        {@const LookupTablesPage = pages.lookups}
        <LookupTablesPage showJson={route.json} />
      {/if}
    {:else if route.page === 'routes'}
      {#if pages.routes}
        {@const RoutesPage = pages.routes}
        <RoutesPage
          showJson={route.json}
          sampling={route.sampling}
          only={route.only}
          make={route.make}
          chose={route.chose}
          group={route.group}
          back={route.back}
        />
      {/if}
    {:else if route.page === 'route-preview'}
      {#if pages['route-preview']}
        {@const RoutePreviewPage = pages['route-preview']}
        <RoutePreviewPage />
      {/if}
    {:else if route.page === 'pipeline'}
      {#if pages.pipeline}
        {@const PipelineEditor = pages.pipeline}
        <PipelineEditor
          id={route.id}
          showJson={route.json}
          sample={route.sample}
          for={route.for}
          back={route.back}
        />
      {/if}
    {:else if route.page === 'lock-test'}
      {#if pages['lock-test']}
        {@const LockTestPage = pages['lock-test']}
        {#if 'relock' in route}
          <LockTestPage relock={route.relock} />
        {:else}
          <LockTestPage pipeline={route.pipeline} sample={route.sample} />
        {/if}
      {/if}
    {:else if route.page === 'file'}
      {#if pages.file}
        {@const FilePage = pages.file}
        <FilePage path={route.path} back={route.back} />
      {/if}
    {:else if route.page === 'changes'}
      {#if pages.changes}
        {@const BranchesPage = pages.changes}
        <BranchesPage />
      {/if}
    {:else if route.page === 'review'}
      {#if pages.review}
        {@const ChangeDetailPage = pages.review}
        <ChangeDetailPage branch={route.branch} />
      {/if}
    {:else if route.page === 'history'}
      {#if pages.history}
        {@const HistoryPage = pages.history}
        <HistoryPage />
      {/if}
    {:else if route.page === 'workers'}
      {#if pages.workers}
        {@const AdminPage = pages.workers}
        <Locked refusedInDemo><AdminPage page="workers" section={route.section} /></Locked>
      {/if}
    {:else if route.page === 'groups'}
      {#if pages.groups}
        {@const AdminPage = pages.groups}
        <Locked refusedInDemo><AdminPage page="groups" /></Locked>
      {/if}
    {:else if route.page === 'certificates'}
      {#if pages.certificates}
        {@const CertificatesPage = pages.certificates}
        <Locked refusedInDemo><CertificatesPage adding={route.adding ?? false} /></Locked>
      {/if}
    {:else if route.page === 'secrets'}
      {#if pages.secrets}
        {@const SecretsPage = pages.secrets}
        <Locked refusedInDemo><SecretsPage section={route.section} /></Locked>
      {/if}
    {:else if route.page === 'accounts'}
      {#if pages.accounts}
        {@const AccountsPage = pages.accounts}
        <Locked refusedInDemo><AccountsPage section={route.section} /></Locked>
      {/if}
    {:else if route.page === 'agent'}
      {#if pages['agent']}
        {@const AgentPage = pages['agent']}
        <AgentPage conversation={route.conversation} />
      {/if}
    {:else if route.page === 'agent-permissions'}
      {#if pages['agent-permissions']}
        {@const AgentPermissionsPage = pages['agent-permissions']}
        <AgentPermissionsPage />
      {/if}
    {:else if route.page === 'system'}
      {#if pages.system}
        {@const SystemPage = pages.system}
        <Locked refusedInDemo><SystemPage section={route.section} /></Locked>
      {/if}
    {/if}
  {/key}
  {#if who.agents && agentPanel && route.page !== 'agent'}
    <AgentPanel onclose={() => (agentPanel = false)} />
  {/if}
{:else}
  <!-- The app's band, from the first paint through signing in. -->
  <header class="top-nav"><div class="nav-row"><Brand /></div></header>

  {#if !asked}
    <p class="waiting">Asking the leader who you are…</p>
  {:else if !reachable}
    <div class="waiting">
      <p>This leader cannot say who you are.</p>
      <p>
        Its account store is unavailable, so signing in would not work either. It answers again once
        the store does.
      </p>
      <button type="button" onclick={() => void askWhoIsSignedIn()}>Try again</button>
    </div>
  {:else if !who.authenticated}
    <AuthScreen mode="sign-in" notice={signInNotice} />
  {:else if who.mustChangePassword}
    <AuthScreen
      mode="change-password"
      username={who.username ?? ''}
      displayName={who.displayName ?? ''}
      onchanged={() => (signInNotice = CHANGED)}
    />
  {:else}
    <!-- Asked for rather than required, so it offers a way back. -->
    <AuthScreen
      mode="change-password"
      username={who.username ?? ''}
      displayName={who.displayName ?? ''}
      oncancel={() => (changingPassword = false)}
      onchanged={() => (signInNotice = CHANGED)}
    />
  {/if}
{/if}

<style>
  .agent-chip {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.2rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--bg);
    color: var(--ink);
    font-family: inherit;
    font-size: var(--text-note);
    cursor: pointer;
  }
  .agent-chip.on {
    border-color: var(--accent);
    color: var(--accent);
  }

  .who {
    padding: 0.4rem 0.6rem 0.55rem;
  }

  /* Each group the person holds and their level there, in two columns so the glyphs line up. */
  .held {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.25rem 0.8rem;
    align-items: baseline;
    margin-top: 0.45rem;
  }

  .held-head {
    grid-column: 1 / -1;
  }

  .pick-seam {
    border-top: 1px solid var(--border);
    margin: 0.35rem 0;
  }

  /* The chip's name: held short so a long one leaves the branch chip room. */
  .who-face {
    max-width: 10rem;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .who-name {
    font-size: var(--text-label);
    font-weight: 600;
  }

  .who .dim {
    color: var(--dim);
    font-size: var(--text-micro);
  }

  .waiting button {
    margin-top: 0.75rem;
  }

  .waiting {
    max-width: var(--width-prose);
    margin-inline: auto;
    padding: 4rem 1.5rem;
    color: var(--dim);
    font-size: var(--text-body);
  }

  /* The Foundry palette is `website/src/lib/foundry/palette.css`, shared
     with the website. Here are what only the app has: its dense type scale,
     the group-hue wheel's accent hue, and each other palette as a
     `[data-palette]` block (STYLE_GUIDE.md, "Color"). `data-theme` is always
     set, resolved from the system when the reader chose "system"
     (`index.html`, and the effect above), so each palette needs one light
     and one dark block and nothing else. */
  :root {
    /* One size per role (STYLE_GUIDE.md, "Text"). A component picks a
       token, never a number, so nothing can land between two roles. */
    --text-title: 1.25rem;
    --text-section: 0.95rem;
    --text-body: 0.85rem;
    --text-label: 0.8rem;
    --text-note: 0.75rem;
    --text-micro: 0.7rem;
    /* The accent's hue alone, for the group-hue wheel's mark. */
    --accent-h: 45;
    /* A group's hue is the accent's own lightness and chroma, turned. */
    --scope-l: 60%;
    --scope-c: 0.17;
  }
  :root[data-theme='dark'] {
    --accent-h: 48;
    --scope-l: 70%;
    --scope-c: 0.17;
  }

  /* Ember: oxblood chrome, steel-blue actions, a teal second color, sand notes. */
  :root[data-palette='ember'] {
    --bg: oklch(97.5% 0.004 35);
    --panel: oklch(94% 0.006 35);
    --field: oklch(99.5% 0 35);
    --ink: oklch(20% 0.012 35);
    --dim: oklch(47% 0.012 35);
    --border: oklch(85% 0.008 35);
    --band-bg: oklch(24% 0.06 35);
    --band-ink: oklch(96% 0.01 35);
    --band-dim: oklch(76% 0.03 35);
    --band-line: oklch(33% 0.06 35);
    --accent: oklch(50% 0.13 250);
    --accent-h: 250;
    --scope-l: 50%;
    --scope-c: 0.13;
    --band-accent: oklch(64% 0.13 250);
    --on-accent: oklch(99% 0.005 90);
    --band-on-accent: oklch(16% 0.02 250);
    --counter: oklch(55% 0.09 195);
    --comment-bg: oklch(94% 0.03 70);
    --comment-border: oklch(76% 0.07 70);
    --comment-ink: oklch(42% 0.08 70);
  }
  :root[data-palette='ember'][data-theme='dark'] {
    --bg: oklch(17% 0.016 35);
    --panel: oklch(22% 0.022 35);
    --field: oklch(14% 0.014 35);
    --ink: oklch(94% 0.008 35);
    --dim: oklch(70% 0.018 35);
    --border: oklch(32% 0.028 35);
    --band-bg: oklch(12% 0.03 35);
    --band-line: oklch(26% 0.035 35);
    --band-ink: var(--ink);
    --band-dim: var(--dim);
    --accent: oklch(72% 0.12 245);
    --accent-h: 245;
    --scope-l: 72%;
    --scope-c: 0.12;
    --on-accent: oklch(16% 0.02 250);
    --counter: oklch(72% 0.12 195);
    --comment-bg: oklch(25% 0.04 70);
    --comment-border: oklch(45% 0.07 70);
    --comment-ink: oklch(80% 0.08 70);
  }
  /* Frost: ice-blue chrome, aurora-pink actions, a lavender second color, pale-blue notes. */
  :root[data-palette='frost'] {
    --bg: oklch(97.5% 0.004 235);
    --panel: oklch(94% 0.006 235);
    --field: oklch(99.5% 0 235);
    --ink: oklch(20% 0.012 235);
    --dim: oklch(47% 0.012 235);
    --border: oklch(85% 0.008 235);
    --band-bg: oklch(24% 0.06 235);
    --band-ink: oklch(96% 0.01 235);
    --band-dim: oklch(76% 0.03 235);
    --band-line: oklch(33% 0.06 235);
    --accent: oklch(55% 0.18 350);
    --accent-h: 350;
    --scope-l: 55%;
    --scope-c: 0.18;
    --band-accent: oklch(69% 0.18 350);
    --on-accent: oklch(99% 0.005 90);
    --band-on-accent: oklch(16% 0.02 250);
    --counter: oklch(55% 0.13 290);
    --comment-bg: oklch(94% 0.03 230);
    --comment-border: oklch(76% 0.07 230);
    --comment-ink: oklch(42% 0.08 230);
  }
  :root[data-palette='frost'][data-theme='dark'] {
    --bg: oklch(17% 0.016 235);
    --panel: oklch(22% 0.022 235);
    --field: oklch(14% 0.014 235);
    --ink: oklch(94% 0.008 235);
    --dim: oklch(70% 0.018 235);
    --border: oklch(32% 0.028 235);
    --band-bg: oklch(12% 0.03 235);
    --band-line: oklch(26% 0.035 235);
    --band-ink: var(--ink);
    --band-dim: var(--dim);
    --accent: oklch(74% 0.15 350);
    --accent-h: 350;
    --scope-l: 74%;
    --scope-c: 0.15;
    --on-accent: oklch(16% 0.02 250);
    --counter: oklch(72% 0.13 290);
    --comment-bg: oklch(25% 0.04 230);
    --comment-border: oklch(45% 0.07 230);
    --comment-ink: oklch(80% 0.08 230);
  }
  /* Verdigris: patina-green chrome, violet actions, a blue second color, moss notes. */
  :root[data-palette='verdigris'] {
    --bg: oklch(97.5% 0.004 165);
    --panel: oklch(94% 0.006 165);
    --field: oklch(99.5% 0 165);
    --ink: oklch(20% 0.012 165);
    --dim: oklch(47% 0.012 165);
    --border: oklch(85% 0.008 165);
    --band-bg: oklch(24% 0.06 165);
    --band-ink: oklch(96% 0.01 165);
    --band-dim: oklch(76% 0.03 165);
    --band-line: oklch(33% 0.06 165);
    --accent: oklch(50% 0.17 310);
    --accent-h: 310;
    --scope-l: 50%;
    --scope-c: 0.17;
    --band-accent: oklch(64% 0.17 310);
    --on-accent: oklch(99% 0.005 90);
    --band-on-accent: oklch(16% 0.02 250);
    --counter: oklch(55% 0.13 250);
    --comment-bg: oklch(94% 0.03 120);
    --comment-border: oklch(76% 0.07 120);
    --comment-ink: oklch(42% 0.08 120);
  }
  :root[data-palette='verdigris'][data-theme='dark'] {
    --bg: oklch(17% 0.016 165);
    --panel: oklch(22% 0.022 165);
    --field: oklch(14% 0.014 165);
    --ink: oklch(94% 0.008 165);
    --dim: oklch(70% 0.018 165);
    --border: oklch(32% 0.028 165);
    --band-bg: oklch(12% 0.03 165);
    --band-line: oklch(26% 0.035 165);
    --band-ink: var(--ink);
    --band-dim: var(--dim);
    --accent: oklch(74% 0.14 310);
    --accent-h: 310;
    --scope-l: 74%;
    --scope-c: 0.14;
    --on-accent: oklch(16% 0.02 250);
    --counter: oklch(72% 0.13 250);
    --comment-bg: oklch(25% 0.04 120);
    --comment-border: oklch(45% 0.07 120);
    --comment-ink: oklch(80% 0.08 120);
  }
  /* Amethyst: violet chrome, teal actions, a pink second color, lilac notes. */
  :root[data-palette='amethyst'] {
    --bg: oklch(97.5% 0.004 300);
    --panel: oklch(94% 0.006 300);
    --field: oklch(99.5% 0 300);
    --ink: oklch(20% 0.012 300);
    --dim: oklch(47% 0.012 300);
    --border: oklch(85% 0.008 300);
    --band-bg: oklch(24% 0.06 300);
    --band-ink: oklch(96% 0.01 300);
    --band-dim: oklch(76% 0.03 300);
    --band-line: oklch(33% 0.06 300);
    --accent: oklch(52% 0.085 195);
    --accent-h: 195;
    --scope-l: 52%;
    --scope-c: 0.085;
    --band-accent: oklch(66% 0.1 195);
    --on-accent: oklch(99% 0.005 90);
    --band-on-accent: oklch(16% 0.02 250);
    --counter: oklch(55% 0.13 350);
    --comment-bg: oklch(94% 0.03 310);
    --comment-border: oklch(76% 0.07 310);
    --comment-ink: oklch(42% 0.08 310);
  }
  :root[data-palette='amethyst'][data-theme='dark'] {
    --bg: oklch(17% 0.016 300);
    --panel: oklch(22% 0.022 300);
    --field: oklch(14% 0.014 300);
    --ink: oklch(94% 0.008 300);
    --dim: oklch(70% 0.018 300);
    --border: oklch(32% 0.028 300);
    --band-bg: oklch(12% 0.03 300);
    --band-line: oklch(26% 0.035 300);
    --band-ink: var(--ink);
    --band-dim: var(--dim);
    --accent: oklch(78% 0.11 190);
    --accent-h: 190;
    --scope-l: 78%;
    --scope-c: 0.11;
    --on-accent: oklch(16% 0.02 250);
    --counter: oklch(72% 0.13 350);
    --comment-bg: oklch(25% 0.04 310);
    --comment-border: oklch(45% 0.07 310);
    --comment-ink: oklch(80% 0.08 310);
  }

  :global(body) {
    background: var(--bg);
    color: var(--ink);
    font-family: var(--font-body);
    /* Dense by default (STYLE_GUIDE.md, "Principles"). Without this every
       element that never named a size lands on the browser's 16px, which
       made undeclared prose the largest text on a page. */
    font-size: var(--text-body);
  }

  /* A link in prose is ink and underlined: the accent is the primary
     action's (STYLE_GUIDE.md, "Restraint"). A link drawn as a control sets
     its own. */
  :global(a) {
    color: var(--ink);
    text-underline-offset: 2px;
  }

  /* A placeholder asks for a value; it is not one, so it is the body face
     even where the value it asks for is mono. */
  :global(::placeholder) {
    font-family: var(--font-body);
  }

  /* The display face: the wordmark and every page's title. */
  :global(h1) {
    font-family: var(--font-display);
    font-weight: 600;
    letter-spacing: -0.025em;
  }

  /* Full-bleed: the border must span the window even though its rows do
     not. Rows carry their own inline padding instead, so the hairline
     still reaches both edges. */
  .top-nav {
    --accent: var(--band-accent);
    --on-accent: var(--band-on-accent);
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    padding-block: 0.8rem;
    background: var(--band-bg);
    border-bottom: 1px solid var(--band-line);
  }

  /* The chrome is a band above the work. Its own pieces take the band's
     surface, ink, dim and hairline under the page's names, so a chip or a
     button drawn for the page reads right on it. The pickers those chips
     open are page surfaces, so the swap stops at the chip. */
  .tabs,
  .nav-version,
  .nav-end > :global(:where(:not(.chip-picker))),
  .nav-end :global(.chip) {
    --bg: var(--band-bg);
    --ink: var(--band-ink);
    --dim: var(--band-dim);
    --border: var(--band-line);
    color: var(--ink);
  }

  /* A row of the shell, held to the page measure and centered like the
     column it sits above -- the wordmark, the tabs and a page's content
     then start at the same edge. Each also wraps its own children rather
     than the whole nav overflowing: identity items (row 1) drop to a
     second line if a branch name pushes past the edge; tabs and the
     toggle (row 2) do the same, tabs first since reaching every tab
     matters more than the toggle sharing its line. */
  .nav-row {
    display: flex;
    align-items: center;
    gap: 1rem;
    width: 100%;
    max-width: var(--measure, var(--width-page));
    margin-inline: auto;
    padding-inline: 1.5rem;
  }

  .nav-identity {
    justify-content: space-between;
    flex-wrap: wrap;
    row-gap: 0.4rem;
  }

  .nav-end {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 1rem;
    row-gap: 0.4rem;
  }

  .nav-tabs {
    flex-wrap: wrap;
    row-gap: 0.4rem;
  }

  /* On the tabs' line rather than centered on their underline, and ending where the identity chip's text above it
     does, inside that chip's padding and border. */
  .nav-version {
    margin-left: auto;
    align-self: baseline;
    padding-right: calc(0.6rem + 1px);
    color: var(--dim);
    font-size: var(--text-note);
  }

  /* The branch chip says on what before it says how -- "main" reads as
     read-only from the words themselves, not from a separate control that
     could disagree with the chip beside it. */
  .branch-note {
    color: var(--dim);
    font-family: inherit;
  }

  .tabs {
    display: flex;
    gap: 1.4rem;
    flex: 1 1 auto;
    /* Below this, tabs scroll instead of shrinking their labels or
       forcing the row wider than the viewport. */
    min-width: 10rem;
    overflow-x: auto;
  }

  .tabs a {
    flex-shrink: 0;
    white-space: nowrap;
    color: var(--dim);
    text-decoration: none;
    font-size: var(--text-body);
    font-weight: 600;
    padding: 0.25rem 0;
    border-bottom: 3px solid transparent;
  }

  .tabs a:hover {
    color: var(--ink);
  }

  /* Ink and the accent underline: on a page with no primary action this is
     the page's one accent. */
  .tabs a.active {
    color: var(--ink);
    font-weight: 700;
    border-bottom-color: var(--accent);
  }

  /* Told, not asked (W1): a mutation on main moved the editor, and this
     says where. Not a button -- there is nothing to click, it just fades. */

  /* Hover says the row responds, so it tints toward ink over the panel it
     sits on: the accent is spent on the page's one action. Toward `--bg` it would take the color of the page
     behind the menu, which reads as a hole rather than as emphasis. */
  .pick:hover,
  .pick:focus-visible {
    background: color-mix(in oklab, var(--ink) 10%, var(--panel));
  }

  /* The ring sits inside the row: the panel clips at its own rounded edge,
     and an outline outside the border box would be cut on the first and
     last rows. */
  .pick:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: -2px;
  }

  /* The row already chosen does nothing, which the cursor says; a hover
     background would say the opposite. Focus still shows, because arriving
     on a row has to be visible whether or not pressing it does anything. */
  .pick.on:hover {
    background: none;
  }

  .pick .mono {
    font-family: var(--mono);
    flex-shrink: 0;
  }

  .pick .dim {
    color: var(--dim);
    font-size: var(--text-micro);
    margin-left: auto;
    text-align: right;
  }

  .crumbs {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    row-gap: 0.2rem;
    gap: 0.4rem;
    max-width: var(--measure, var(--width-page));
    margin-inline: auto;
    /* Even top and bottom: this row carries the scope's tint, so anything
       in it sitting against an edge of that band reads as misaligned. */
    padding: 0.3rem 1.5rem;
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .crumbs a {
    color: var(--dim);
    text-decoration: none;
  }

  .crumbs a:hover {
    color: var(--ink);
  }

  .crumbs .sep {
    opacity: 0.5;
  }

  .crumbs.exit {
    gap: 0.6rem;
    padding-block: 0.6rem;
    border-bottom: 1px solid var(--border);
    font-size: var(--text-section);
    font-weight: 500;
    color: var(--ink);
  }

  .crumbs.exit a:not(.btn) {
    color: var(--accent);
  }

  .crumbs.exit a:not(.btn):hover {
    text-decoration: underline;
  }

  .crumbs.exit > span:last-of-type {
    font-weight: 600;
  }

  .crumbs .key {
    margin-left: auto;
    font-family: var(--font-body);
    font-size: var(--text-micro);
    color: var(--dim);
  }

  .caret {
    font-size: 0.7em;
    opacity: 0.7;
  }
</style>
