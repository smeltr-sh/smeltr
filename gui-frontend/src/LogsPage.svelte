<!--
  What the fleet's workers have said, newest first, from `GET /api/logs`
  (`leader::get_logs`) -- the same events a smeltr logs source ships, held
  by the leader instead of routed to a destination.

  Filtered on the server rather than in the browser, unlike the map: the
  table is capped at `series::LOGS_KEPT` rows, which is far more
  than a page should carry and far too many to narrow in memory.

  Every filter lives in the hash, so a view worth showing someone is a
  link and the back button walks the searches actually made. The page
  reads its filters from the route and writes them by navigating, which
  keeps one copy of them rather than two that can disagree.

  The page position is a cursor on `id` (`nextBefore`/`nextAfter`), in the
  hash alongside the filters. The cursor survives pruning: `LOGS_KEPT`
  deletes from the oldest end, and an id cannot move, where an offset
  would shift under a reader. Older replaces the page rather than
  appending it -- one page is one link, and there is only ever one to be
  on.
-->
<script lang="ts">
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Help from './Help.svelte';
  import {
    getLogs,
    getClusterStatus,
    type LogRowDto,
    type WorkerCredentialDto,
    type WorkerGroupDto,
  } from './api';
  import { clockTime } from './time';
  import {
    LEVELS,
    fields,
    isFiltered,
    resetCursor,
    toHash,
    toQuery,
    toggleLevel,
    wasTruncated,
    type Filters,
  } from './logs';

  let { filters }: { filters: Filters } = $props();

  /** How many rows one read asks for. The server clamps its own maximum;
   * this is what a screen can usefully show at once. */
  const PAGE = 200;

  /** How long typing pauses before a search box's edit becomes a read.
   * Short enough that a pause between words already fires it, long
   * enough that a fast typist causes one request rather than one per
   * key. */
  const SEARCH_DEBOUNCE_MS = 300;

  let rows: LogRowDto[] = $state([]);
  let messageLimit = $state(0);
  let nextBefore: number | null = $state(null);
  let nextAfter: number | null = $state(null);
  let loading = $state(true);
  let loadError = $state('');
  /** Which rows have their fields open. Keyed by row id, which is the
   * leader's own arrival order and unique for the life of the table. */
  let opened: Record<number, boolean> = $state({});

  /** The fleet's known workers and groups, to pick a filter from rather than
   * type one: `worker_id`/`group_id` match a log row exactly (`series::logs`),
   * so a free-text box could only ever be right by copying an id from
   * elsewhere. A worker is never deleted, only revoked, so every id a log
   * row could carry is here once it has loaded. */
  let workers: WorkerCredentialDto[] = $state([]);
  let groups: WorkerGroupDto[] = $state([]);
  getClusterStatus()
    .then((status) => {
      workers = [...(status.workers ?? [])].sort((a, b) => a.workerId.localeCompare(b.workerId));
      groups = [...(status.workerGroups ?? [])].sort((a, b) => a.name.localeCompare(b.name));
    })
    .catch(() => {
      // The picker degrades to "any worker"/"any group" plus whatever the
      // route already named; the table itself still reads without this.
    });

  /** A group can be deleted (`deleteWorkerGroup`) after it logged something;
   * without this its id would drop off the list and the filter would look
   * cleared instead of naming a group that is simply gone. */
  let groupOptions = $derived(
    filters.group && !groups.some((g) => g.id === filters.group)
      ? [...groups, { id: filters.group, name: filters.group, deployed: false }]
      : groups,
  );

  /** The same gap `groupOptions` closes, but reached differently: `workers`
   * starts empty and fills once `getClusterStatus` answers, so a routed
   * `?worker=` opened before then -- or a roster read that fails outright --
   * would otherwise have no matching `<option>`. With none selected, the
   * browser silently picks the first (`""`), and the next `fromForm` read
   * drops the filter rather than keeping it. */
  let workerOptions = $derived(
    filters.worker && !workers.some((w) => w.workerId === filters.worker)
      ? [...workers, { workerId: filters.worker, revoked: false }]
      : workers,
  );

  // Re-reads whenever the route's filters (position included) change,
  // which is the only way they change: every control below navigates
  // rather than assigning.
  $effect(() => {
    const query = toQuery(filters, PAGE);
    let live = true;
    loading = true;
    loadError = '';
    getLogs(query)
      .then((answer) => {
        if (!live) return;
        rows = answer.rows;
        messageLimit = answer.messageLimit;
        nextBefore = answer.nextBefore;
        nextAfter = answer.nextAfter;
      })
      .catch((e) => {
        if (live) loadError = e instanceof Error ? e.message : String(e);
      })
      .finally(() => {
        if (live) loading = false;
      });
    // A filter changed while a slower read was in flight: its answer is
    // for a question nobody is asking any more.
    return () => {
      live = false;
    };
  });

  function go(next: Filters) {
    location.hash = toHash(next);
  }

  function fromForm(form: HTMLFormElement): Filters {
    const data = new FormData(form);
    const text = (name: string) => String(data.get(name) ?? '').trim() || undefined;
    // A fresh object rather than `{ ...filters, ... }`: a new search reads a
    // different set of rows, so a page position from the old one goes with it.
    return {
      levels: filters.levels,
      worker: text('worker'),
      group: text('group'),
      site: text('site'),
      q: text('q'),
    };
  }

  let debounceHandle: ReturnType<typeof setTimeout> | undefined;

  function clearDebounce() {
    if (debounceHandle !== undefined) {
      clearTimeout(debounceHandle);
      debounceHandle = undefined;
    }
  }

  // A search box applies as it is typed; submitting the form (Enter, or the
  // button) skips the wait.
  function onType(event: Event) {
    const form = (event.currentTarget as HTMLInputElement).form;
    if (!form) return;
    clearDebounce();
    debounceHandle = setTimeout(() => go(fromForm(form)), SEARCH_DEBOUNCE_MS);
  }

  // A picked option is a whole answer, not a keystroke to wait out.
  function onSelect(event: Event) {
    const form = (event.currentTarget as HTMLSelectElement).form;
    if (!form) return;
    clearDebounce();
    go(fromForm(form));
  }

  function submit(event: SubmitEvent) {
    event.preventDefault();
    clearDebounce();
    go(fromForm(event.target as HTMLFormElement));
  }

  $effect(() => clearDebounce);

  /** Which bound to page toward: `before` reads further into the past,
   * `after` back toward the current head. Only ever one cursor at a time. */
  function older() {
    if (nextBefore == null) return;
    go({ ...filters, before: nextBefore, after: undefined });
  }
  function newer() {
    if (nextAfter == null) return;
    go({ ...filters, after: nextAfter, before: undefined });
  }
</script>

<SectionShell>
  <PageHeader title="Logs">
    {#snippet help()}
      Worker logs, sent with each heartbeat.
    {/snippet}
  </PageHeader>

  <div class="filters">
    <div class="levels">
      {#each LEVELS as level (level)}
        <button
          type="button"
          class="chip {level}"
          class:on={filters.levels.includes(level)}
          onclick={() =>
            go({ ...resetCursor(filters), levels: toggleLevel(filters.levels, level) })}
        >
          {level}
        </button>
      {/each}
      <span class="dim">
        {filters.levels.length === 0 ? 'every severity' : 'these severities'}
        <Help label="severity">
          A severity is not a minimum: <code>warn</code> shows warnings only, so pick
          <code>error</code> too to see both.
        </Help>
      </span>
    </div>

    <form class="text" onsubmit={submit}>
      <select name="worker" value={filters.worker ?? ''} onchange={onSelect}>
        <option value="">worker</option>
        {#each workerOptions as w (w.workerId)}
          <option value={w.workerId}>{w.workerId}{w.revoked ? ' (revoked)' : ''}</option>
        {/each}
      </select>
      <select name="group" value={filters.group ?? ''} onchange={onSelect}>
        <option value="">group</option>
        {#each groupOptions as g (g.id)}
          <option value={g.id}>{g.name}</option>
        {/each}
      </select>
      <input
        name="site"
        placeholder="site, e.g. worker."
        value={filters.site ?? ''}
        oninput={onType}
      />
      <input name="q" placeholder="in the message" value={filters.q ?? ''} oninput={onType} />
      <button type="submit">Search</button>
      {#if isFiltered(filters)}
        <button type="button" onclick={() => go({ levels: [] })}>Clear</button>
      {/if}
    </form>
  </div>

  {#if loadError}
    <p class="notice">the logs could not be read &mdash; {loadError}</p>
  {:else if loading && rows.length === 0}
    <p class="dim">Reading…</p>
  {:else if rows.length === 0}
    <p class="dim">
      {isFiltered(filters)
        ? 'Nothing matches. A message is cut when it is delivered, so a search cannot find what was cut.'
        : 'Nothing has been delivered yet. A worker sends what it has said on its next heartbeat.'}
    </p>
  {:else}
    <table>
      <thead>
        <tr>
          <th>when</th>
          <th>severity</th>
          <th>site</th>
          <th>message</th>
          <th>worker</th>
        </tr>
      </thead>
      <tbody>
        {#each rows as row (row.id)}
          {@const detail = fields(row)}
          <tr class:open={opened[row.id]}>
            <td class="mono when">{clockTime(row.atMillis)}</td>
            <td><span class="chip {row.level} on">{row.level}</span></td>
            <td class="mono site">
              <button
                type="button"
                class="link"
                onclick={() => go({ ...resetCursor(filters), site: row.site })}
              >
                {row.site}
              </button>
            </td>
            <td>
              {row.message}{#if wasTruncated(row.message, messageLimit)}<span
                  class="dim"
                  title="cut to {messageLimit} characters on delivery">…</span
                >{/if}
              {#if detail.length > 0}
                <button
                  type="button"
                  class="link fields"
                  onclick={() => (opened[row.id] = !opened[row.id])}
                >
                  {opened[row.id]
                    ? 'hide'
                    : `${detail.length} field${detail.length === 1 ? '' : 's'}`}
                </button>
              {/if}
              {#if opened[row.id]}
                <dl class="detail">
                  {#each detail as field (field.name)}
                    <dt class="mono">{field.name}</dt>
                    <dd class="mono">{field.value}</dd>
                  {/each}
                </dl>
              {/if}
            </td>
            <td class="mono who">
              <button
                type="button"
                class="link"
                onclick={() => go({ ...resetCursor(filters), worker: row.workerId })}
              >
                {row.workerId}
              </button>
              {#if row.group}
                <button
                  type="button"
                  class="link dim"
                  onclick={() => go({ ...resetCursor(filters), group: row.group ?? undefined })}
                >
                  {row.group}
                </button>
              {/if}
              {#if row.thread}
                <span class="dim">{row.thread}</span>
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
    {#if nextAfter != null || nextBefore != null}
      <div class="pager">
        <button type="button" onclick={newer} disabled={nextAfter == null}> Newer </button>
        <button type="button" onclick={older} disabled={nextBefore == null}> Older </button>
      </div>
    {/if}
  {/if}
</SectionShell>

<style>
  .filters {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    margin-bottom: 1rem;
  }
  .levels,
  .text {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    align-items: center;
  }
  .text input,
  .text select {
    min-width: 8rem;
  }
  .chip {
    border: 1px solid var(--border);
    background: none;
    border-radius: 999px;
    padding: 0.15rem 0.6rem;
    font-size: 0.85em;
    cursor: pointer;
    color: var(--ink);
  }
  .chip.on.error {
    background: color-mix(in srgb, var(--error) 15%, transparent);
    border-color: var(--error);
    color: var(--error);
  }
  .chip.on.warn {
    background: color-mix(in srgb, var(--warn) 18%, transparent);
    border-color: var(--warn);
    color: var(--warn);
  }
  .chip.on.info,
  .chip.on.debug,
  .chip.on.trace {
    background: color-mix(in srgb, var(--dim) 20%, transparent);
    border-color: var(--dim);
    /* A chosen level reads as chosen: ink and bold, not dimmer than the rest. */
    color: var(--ink);
    font-weight: 600;
  }
  table {
    width: 100%;
    border-collapse: collapse;
  }
  th,
  td {
    text-align: left;
    padding: 0.35rem 0.5rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }
  th {
    font-weight: 600;
    font-size: 0.85em;
  }
  .when,
  .who {
    white-space: nowrap;
  }
  .site {
    /* Wide enough that an ordinary dotted site is one piece: `anywhere` broke
       `worker.enroll_failed` after the `faile`, which is unreadable. A site is
       cut at `MAX_TEXT_LEN`, so a pathological one still has to wrap somewhere. */
    max-width: 22rem;
    overflow-wrap: break-word;
  }
  .mono {
    font-family: ui-monospace, monospace;
    font-size: 0.9em;
  }
  .dim {
    opacity: 0.65;
  }
  .pager {
    display: flex;
    gap: 0.4rem;
    margin-top: 0.75rem;
  }
  .link {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    color: inherit;
    text-decoration: underline dotted;
    cursor: pointer;
  }
  .fields {
    margin-left: 0.4rem;
    font-size: 0.85em;
    opacity: 0.7;
  }
  .detail {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.1rem 0.75rem;
    margin: 0.4rem 0 0;
  }
  .detail dt {
    opacity: 0.7;
  }
  .detail dd {
    margin: 0;
    overflow-wrap: anywhere;
  }
  .notice {
    color: #a33;
  }
</style>
