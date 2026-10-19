<!--
  What a change did to its files between two commits: each file and its
  line counts, and the unified diff of the whole range or of one file. Over
  the whole change, a line of the new side takes a comment by its number and
  shows its threads under it. A thread on a line the diff does not show is
  listed above its file, and one on a file the diff does not hold above them all.
-->
<script lang="ts">
  import { commitDiff, setScope, type CommentAnchor, type FileChangeDto } from './api';
  import { count, shortPath } from './time';
  import { editorFor } from './nav';
  import CommentComposer from './CommentComposer.svelte';
  import ThreadView from './ThreadView.svelte';
  import { threadsOnLine, type Thread } from './threads';
  import { parseUnifiedDiff } from './unifiedDiff';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';

  let {
    branch,
    from,
    to,
    whole,
    threads,
    onchanged,
  }: {
    branch: string;
    from: string;
    to: string;
    /** The range is the whole change, so its new side is the branch head
     * that line comments are numbered against. */
    whole: boolean;
    threads: Thread[];
    /** A comment, reply, resolve or reopen landed: the timeline has it. */
    onchanged: () => Promise<void> | void;
  } = $props();

  let files = $state<FileChangeDto[]>([]);
  let selectedPath = $state<string | null>(null);
  let diffText = $state('');
  let diffLoading = $state(false);
  let diffFailed = $state(false);

  /** Which load is newest, so a slower earlier one cannot land over it. */
  let loads = 0;

  /** `path === null` is the whole range; only that call updates `files`, so
   * narrowing to one file never shrinks the table it was picked from. */
  async function loadDiff(path: string | null) {
    const mine = ++loads;
    diffLoading = true;
    diffFailed = false;
    try {
      const res = await commitDiff(from, to, path ?? undefined);
      if (mine !== loads) return;
      if (path === null) files = res.files;
      diffText = res.diff;
      if (!res.ok) diffFailed = true;
    } catch (e) {
      if (mine === loads) diffFailed = true;
    } finally {
      if (mine === loads) diffLoading = false;
    }
  }

  $effect(() => {
    void from;
    void to;
    selectedPath = null;
    void loadDiff(null);
  });

  function selectFile(path: string | null) {
    selectedPath = path;
    void loadDiff(path);
  }

  function statusWord(status: string): string {
    if (status === 'A') return 'Added';
    if (status === 'D') return 'Deleted';
    return 'Modified';
  }

  const fileSort = new TableSort<'file' | 'status' | 'change'>();
  let sortedFiles = $derived(
    fileSort.apply(files, {
      file: (f) => f.path,
      status: (f) => statusWord(f.status),
      change: (f) => f.added + f.removed,
    }),
  );

  let diffFiles = $derived(parseUnifiedDiff(diffText));

  /** Each shown file's threads on lines its diff does not show; every one of
   * them when the range is not the whole change, whose numbers they are not. */
  function outside(path: string, shown: Set<number>): Thread[] {
    return threadsOnLine(threads, path).filter(
      (t) => !whole || (t.anchor?.kind === 'line' && !shown.has(t.anchor.line)),
    );
  }

  /** Threads on files the diff does not hold, while it shows every file. */
  let away = $derived.by(() => {
    if (selectedPath !== null) return [];
    const paths = new Set(files.map((f) => f.path));
    return threads.filter((t) => t.anchor?.kind === 'line' && !paths.has(t.anchor.path));
  });

  /** The one composer open, as `path:line`. */
  let composing = $state<string | null>(null);

  function lineAnchor(path: string, line: number): CommentAnchor {
    return { kind: 'line', path, line };
  }

  async function posted() {
    composing = null;
    await onchanged();
  }

  /** Open the page that edits a file, in the group that holds it. Setting
   * the scope is half the navigation: the same file name lives in every
   * group, and only one of them is the one under review. */
  function openEditor(target: { scope: string; hash: string }) {
    setScope(target.scope, target.hash);
  }
</script>

{#snippet placed(t: Thread)}
  <div class="inline placed">
    {#if t.anchor?.kind === 'line'}
      <span class="dim mono where">{t.anchor.path} &middot; line {t.anchor.line}</span>
    {/if}
    <ThreadView {branch} thread={t} {onchanged} />
  </div>
{/snippet}

<div class="card">
  <div class="row card-head">
    <h2>Files</h2>
    <span class="dim count">{count(files.length, 'file')}</span>
    {#if selectedPath}
      <button type="button" onclick={() => selectFile(null)}>Whole change</button>
    {/if}
  </div>
  {#if files.length === 0}
    <div class="card-body"><p class="dim">No files changed.</p></div>
  {:else}
    <table>
      <thead>
        <tr>
          <th aria-sort={fileSort.aria('file')}
            ><SortButton sort={fileSort} key="file">File</SortButton></th
          >
          <th aria-sort={fileSort.aria('status')}
            ><SortButton sort={fileSort} key="status">Status</SortButton></th
          >
          <th aria-sort={fileSort.aria('change')}
            ><SortButton sort={fileSort} key="change">Change</SortButton></th
          >
          <th><span class="visually-hidden">Edit</span></th>
        </tr>
      </thead>
      <tbody>
        {#each sortedFiles as f (f.path)}
          {@const target = editorFor(f.path)}
          <tr class:selected={selectedPath === f.path}>
            <td>
              <button
                type="button"
                class="file-name"
                title={f.path}
                onclick={() => selectFile(f.path)}
              >
                {shortPath(f.path)}
              </button>
            </td>
            <td class="dim">{statusWord(f.status)}</td>
            <td class="mono stat">
              <span class="added">+{f.added}</span>
              <span class="removed">&minus;{f.removed}</span>
            </td>
            <td class="edit-cell">
              {#if target}
                <button type="button" onclick={() => openEditor(target)}> Edit </button>
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
</div>

<div class="card">
  <div class="row card-head">
    <h2>Diff</h2>
    <span class="dim mono scope">{selectedPath ? shortPath(selectedPath) : 'whole change'}</span>
  </div>
  {#if diffLoading}
    <div class="card-body"><p class="dim">Loading&hellip;</p></div>
  {:else if diffFailed}
    <!-- The error bar says why. -->
  {:else}
    {#each away as t (t.id)}
      {@render placed(t)}
    {/each}
    {#if diffFiles.length === 0}
      <div class="card-body"><p class="dim">No differences.</p></div>
    {:else}
      <div class="diff-scroll">
        <table class="diff">
          <tbody>
            {#each diffFiles as file, fi (fi)}
              {@const shown = new Set(file.rows.flatMap((r) => r.newLine ?? []))}
              {#each outside(file.path, shown) as t (t.id)}
                <tr class="thread">
                  <td colspan="2">{@render placed(t)}</td>
                </tr>
              {/each}
              {#each file.rows as row, ri (ri)}
                {@const n = row.newLine}
                {@const key = `${file.path}:${n}`}
                <tr class={row.kind}>
                  <td class="n">
                    {#if n !== undefined && whole}
                      <button
                        type="button"
                        class="ln"
                        aria-label="Comment on line {n}"
                        aria-expanded={composing === key}
                        onclick={() => (composing = composing === key ? null : key)}>{n}</button
                      >
                    {:else if n !== undefined}
                      {n}
                    {/if}
                  </td>
                  <td class="code">{row.text}</td>
                </tr>
                {#if n !== undefined && whole}
                  {@const here = threadsOnLine(threads, file.path, n)}
                  {#if here.length > 0 || composing === key}
                    <tr class="thread">
                      <td colspan="2">
                        <div class="inline">
                          {#each here as t (t.id)}
                            <ThreadView {branch} thread={t} {onchanged} />
                          {/each}
                          {#if composing === key}
                            <CommentComposer
                              {branch}
                              anchor={lineAnchor(file.path, n)}
                              oncancel={() => (composing = null)}
                              onposted={posted}
                            />
                          {/if}
                        </div>
                      </td>
                    </tr>
                  {/if}
                {/if}
              {/each}
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  {/if}
</div>

<style>
  h2 {
    margin: 0;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }
  .dim {
    color: var(--dim);
  }
  .mono {
    font-family: var(--mono);
  }
  .card {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    margin-bottom: 1.25rem;
    overflow-x: auto;
  }
  .card-head {
    background: var(--bg);
    border-radius: var(--radius) var(--radius) 0 0;
  }
  .card-body {
    padding: 0.7rem 0.9rem;
  }
  .row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.55rem 0.9rem;
    border-bottom: 1px solid var(--border);
  }
  .row:last-child {
    border-bottom: 0;
  }
  .card-head h2 {
    flex-grow: 1;
  }
  .count,
  .scope {
    font-size: var(--text-micro);
  }
  .edit-cell {
    text-align: right;
    white-space: nowrap;
  }

  /* Names a column whose header would otherwise be an empty cell. */
  .visually-hidden {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-label);
  }
  th,
  td {
    text-align: left;
    padding: 0.4rem 0.9rem;
    border-bottom: 1px solid var(--border);
  }
  th {
    color: var(--dim);
    font-weight: 700;
    font-size: var(--text-micro);
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  tbody tr:last-child td {
    border-bottom: 0;
  }
  tr.selected {
    background: color-mix(in oklch, var(--ink) 8%, transparent);
  }
  .file-name {
    border: 0;
    background: none;
    padding: 0;
    color: var(--ink);
    text-decoration: underline;
    text-underline-offset: 2px;
    font-family: var(--mono);
    font-size: var(--text-label);
    cursor: pointer;
  }
  tr.selected .file-name {
    font-weight: 600;
  }
  .stat {
    white-space: nowrap;
  }
  .added {
    color: var(--added);
  }
  .removed {
    color: var(--removed);
  }

  .diff-scroll {
    overflow-x: auto;
  }
  .diff {
    font-family: var(--mono);
    font-size: var(--text-note);
    line-height: 1.5;
  }
  .diff td {
    padding: 0 0.9rem 0 0;
    border-bottom: 0;
    vertical-align: top;
  }
  .diff td.code {
    white-space: pre;
    color: var(--ink);
  }
  .diff tr.added {
    background: color-mix(in oklch, var(--added) 13%, transparent);
  }
  .diff tr.removed {
    background: color-mix(in oklch, var(--removed) 13%, transparent);
  }
  .diff tr.added td.code {
    color: var(--added);
  }
  .diff tr.removed td.code {
    color: var(--removed);
  }
  .diff tr.hunk td.code,
  .diff tr.meta td.code {
    color: var(--dim);
  }
  .diff td.n {
    width: 1%;
    padding: 0 0.4rem 0 0.9rem;
    text-align: right;
    color: var(--dim);
    user-select: none;
  }
  /* A new-side line number is the control that comments on its line. */
  .ln {
    border: 0;
    border-radius: var(--radius);
    background: none;
    padding: 0 0.2rem;
    color: var(--dim);
    font-family: var(--mono);
    font-size: var(--text-note);
    line-height: 1.5;
    cursor: pointer;
  }
  .ln:hover,
  .ln:focus-visible,
  .ln[aria-expanded='true'] {
    color: var(--ink);
    background: color-mix(in oklch, var(--ink) 10%, transparent);
  }
  .diff tr.thread td {
    padding: 0.6rem 0.9rem;
    font-family: var(--font-body);
    background: var(--panel);
    border-block: 1px solid var(--border);
  }
  .inline {
    display: grid;
    gap: 0.6rem;
    max-width: 46rem;
  }
  .placed {
    gap: 0.4rem;
  }
  .card > .placed {
    padding: 0.6rem 0.9rem;
    border-bottom: 1px solid var(--border);
  }
  .where {
    font-size: var(--text-note);
  }
</style>
