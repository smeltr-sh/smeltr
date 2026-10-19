<!--
  What is waiting on the branch you are on, and the one thing to do about it.

  It lives in the branch bar because that is what it is about -- not the
  page, which is why an editor is the wrong home for it: work saved from
  Sources and work saved from a pipeline are one change, and either can be
  the last edit before it is ready. Being in the shell also means the count
  is visible from Status and Map, where nothing else would say a branch has
  unshipped work on it.

  The bar names the branch, so these words do not: they say what is waiting,
  and the bar says on what. It renders no wrapper of its own, so they sit
  directly in the bar's flex.
-->
<script lang="ts">
  import { CHANGE_EVENT, SAVED_EVENT, listBranches, type BranchDto } from './api';
  import { count } from './time';

  let {
    branch,
    opening,
    onopen,
  }: {
    branch: string;
    /** The form that opens this branch is showing, so the button that shows it is not. */
    opening: boolean;
    /** Open for review was pressed: the caller shows the form that opens it. */
    onopen: (entry: BranchDto) => void;
  } = $props();

  let here = $state<BranchDto | null>(null);
  /** The latest load. One started earlier may answer later with what the branch was. */
  let loads = 0;

  async function load() {
    const mine = ++loads;
    if (branch === 'main') {
      here = null;
      return;
    }
    const res = await listBranches();
    if (mine !== loads) return;
    here = res.ok ? (res.branches.find((b) => b.name === branch) ?? null) : null;
  }

  $effect(() => {
    // Reading `branch` here is what subscribes this to a branch switch.
    void branch;
    void load();
  });

  $effect(() => {
    const again = () => void load();
    addEventListener(SAVED_EVENT, again);
    addEventListener(CHANGE_EVENT, again);
    // A deploy deletes the branch and moves to History, so arriving
    // anywhere is how this hears of it.
    addEventListener('hashchange', again);
    return () => {
      removeEventListener(SAVED_EVENT, again);
      removeEventListener(CHANGE_EVENT, again);
      removeEventListener('hashchange', again);
    };
  });

  let open_ = $derived(here?.change?.state === 'open');
  let show = $derived(here !== null && here.commits > 0);
</script>

{#if show && here}
  {#if open_}
    <span class="says" role="group" aria-label="this branch">
      open for review &mdash; {count(here.touches.length, 'file')}
    </span>
    <a class="act" href={`#/changes/review/${encodeURIComponent(here.name)}`}>Review</a>
  {:else if opening}
    <span class="says" role="group" aria-label="this branch">{count(here.commits, 'save')}</span>
  {:else}
    <span class="says" role="group" aria-label="this branch">
      {count(here.commits, 'save')}, not open for review yet
    </span>
    <button type="button" class="act primary" onclick={() => here && onopen(here)}>
      Open for review
    </button>
  {/if}
{/if}

<style>
  /* Prose, beside chips that are names. */
  .says {
    font-family: var(--font-body);
    font-size: var(--text-note);
    color: var(--ink);
  }

  .act {
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--bg);
    color: var(--ink);
    padding: 0.15rem 0.6rem;
    font: 500 var(--text-note) var(--font-body);
    text-decoration: none;
    cursor: pointer;
  }

  .act.primary {
    border-color: var(--accent);
    background: var(--accent);
    color: var(--on-accent);
  }

  .act:disabled {
    opacity: 0.6;
    cursor: default;
  }
</style>
