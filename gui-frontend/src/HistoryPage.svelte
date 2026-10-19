<!--
  Branches / history: `main`'s commit log, newest first, and which commit the
  fleet runs. Rolling back is deploying an earlier commit -- `deployCommit` and
  nothing else (W7) -- and holds the fleet there until `main`'s head is
  deployed again, which is the Release button.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import Locked from './Locked.svelte';
  import { history, deployCommit, type CommitDto, type HeldDto } from './api';
  import { ago, shortCommit, pathSummary } from './time';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import { ask } from './Confirm.svelte';
  import Help from './Help.svelte';

  const START_LIMIT = 50;
  const MORE_LIMIT = 200;

  let commits = $state<CommitDto[]>([]);
  let deployed = $state<string | null>(null);
  let held = $state<HeldDto | null>(null);
  // Commits listed above the deployed one are newer: deploying one moves the
  // fleet forward, not back. One older than every listed commit leaves all of
  // them newer.
  let deployedAt = $derived.by(() => {
    const at = commits.findIndex((c) => c.commit === deployed);
    return at === -1 ? commits.length : at;
  });
  let limit = $state(START_LIMIT);
  let loading = $state(true);
  let loadFailed = $state(false);

  // The commit a rollback is in flight for, or null. Every rollback button
  // disables while any one of them is running, so two can't race.
  let rollingBack = $state<string | null>(null);

  let canShowMore = $derived(commits.length >= limit && limit < MORE_LIMIT);

  async function load() {
    loading = true;
    loadFailed = false;
    try {
      const res = await history(limit);
      commits = res.commits;
      deployed = res.deployed ?? null;
      held = res.held ?? null;
    } catch (e) {
      loadFailed = true;
      reported(e);
    } finally {
      loading = false;
    }
  }
  load();

  function showMore() {
    limit = MORE_LIMIT;
    load();
  }

  async function rollback(commit: string, release = false) {
    const ok = await ask(
      release
        ? {
            title: 'release',
            body: `Deploy ${shortCommit(commit)}, main's head, and end the hold. This moves what the whole fleet runs.`,
            action: 'Release',
          }
        : {
            title: 'deploy',
            body: `Deploy ${shortCommit(commit)} and hold the fleet there until it is released. This moves what the whole fleet runs.`,
            action: 'Deploy',
          },
    );
    if (!ok) return;
    rollingBack = commit;
    try {
      let res = await deployCommit(commit);
      if (!res.ok && res.doesNotLoad?.length) {
        // Declining is the answer to the refusal, so it is not shown again as an error.
        if (!(await deployAnyway(commit, res.doesNotLoad))) return;
        res = await deployCommit(commit, true);
      }
      if (res.ok) {
        location.hash = `#/status?deploying=${encodeURIComponent(commit)}`;
      }
    } catch (e) {
      reported(e);
    } finally {
      rollingBack = null;
    }
  }

  // A commit where a group does not load is still a way back, so it is offered,
  // but only past two questions: which groups, and what their workers will do.
  async function deployAnyway(commit: string, broken: { group: string; why: string }[]) {
    const groups = broken.map((b) => b.group).join(', ');
    const first = await ask({
      title: 'does not load',
      body: `At ${shortCommit(commit)}, ${broken.length === 1 ? 'this group does' : 'these groups do'} not load: ${broken.map((b) => `${b.group}: ${b.why}`).join('; ')}.`,
      action: 'Continue',
      danger: true,
    });
    if (!first) return false;
    return !!(await ask({
      title: 'deploy anyway',
      body: `The workers in ${groups} will refuse ${shortCommit(commit)} and keep running what they run now. Fix it on a branch and deploy that to move them.`,
      action: 'Deploy anyway',
      danger: true,
    }));
  }
</script>

<SectionShell>
  <PageHeader title="History">
    {#snippet beside()}
      <span class="main-tag">main</span>
    {/snippet}
    {#snippet help()}
      The history of <code>main</code>, and which commit of it the fleet runs, whichever branch you
      have open.
    {/snippet}
  </PageHeader>
  {#if held && deployed}
    <div class="held">
      <span>
        The fleet is held at <span class="mono">{shortCommit(deployed)}</span>, deployed there by
        {held.by}
        {ago(held.since)}. The {held.behind} commit{held.behind === 1 ? '' : 's'} on main since then
        {held.behind === 1 ? 'is' : 'are'} not deployed.
      </span>
      <Locked refusedInDemo>
        <button
          class="compact"
          onclick={() => rollback(held!.head, true)}
          disabled={rollingBack !== null}
        >
          {rollingBack === held.head ? 'Releasing…' : 'Release'}
        </button>
      </Locked>
    </div>
  {/if}

  {#if loading}
    <p class="dim">Loading&hellip;</p>
  {:else if loadFailed}
    <!-- The error bar says why. -->
  {:else}
    <div class="card">
      <table>
        <thead>
          <tr>
            <th>Commit</th>
            <th>Change</th>
            <th>
              Author<Help label="Author"
                ><span class="mono">smeltr-leader</span> marks a commit no person asked for, and every
                commit from before smeltr recorded authors.</Help
              >
            </th>
            <th>Deployed by</th>
            <th>When</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each commits as c, i (c.commit)}
            <tr class:current-row={c.commit === deployed}>
              <td class="mono">
                <span class="commit">
                  {shortCommit(c.commit)}
                  {#if c.commit === deployed}
                    <span class="tag accent">running now</span>
                    {#if held}<span class="tag warn-tag">held</span>{/if}
                  {/if}
                </span>
              </td>
              <td class="change">
                <strong>{c.subject}</strong>
                <div class="dim small">{pathSummary(c.files)}</div>
                {#if c.body.trim()}
                  <div class="dim small">{c.body}</div>
                {/if}
              </td>
              <!-- Each names its column, so the author and the deployer are told apart by name rather
                   than by where they sit. -->
              <td class="who" aria-label="Author: {c.author}">{c.author}</td>
              <td class="who" aria-label="Deployed by: {c.deployer || 'nobody'}">
                <!-- Only a deploy has one, and it is the same person whenever
                     somebody released their own change. -->
                {#if c.deployer}{c.deployer}{:else}<span class="dim">&mdash;</span>{/if}
              </td>
              <td class="dim nowrap">{ago(c.time)}</td>
              <td class="right nowrap">
                {#if c.commit !== deployed}
                  <Locked refusedInDemo
                    ><button
                      class="compact"
                      onclick={() => rollback(c.commit, c.commit === held?.head)}
                      disabled={rollingBack !== null}
                    >
                      {#if i < deployedAt}
                        {rollingBack === c.commit ? 'Deploying…' : 'Deploy this'}
                      {:else}
                        {rollingBack === c.commit ? 'Rolling back…' : 'Roll back to here'}
                      {/if}
                    </button></Locked
                  >
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
      {#if canShowMore}
        <div class="row">
          <button class="compact" onclick={showMore} disabled={rollingBack !== null}
            >Show more</button
          >
        </div>
      {/if}
    </div>
  {/if}
</SectionShell>

<style>
  .dim {
    color: var(--dim);
  }
  .small {
    font-size: var(--text-micro);
  }
  .mono {
    font-family: var(--mono);
  }
  .nowrap {
    white-space: nowrap;
  }
  .main-tag {
    padding: 0.08rem 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--dim);
    font-size: var(--text-micro);
  }

  .card {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    overflow-x: auto;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-label);
  }
  th,
  td {
    text-align: left;
    padding: 0.4rem 0.75rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }
  th {
    color: var(--dim);
    font-weight: 700;
    font-size: var(--text-micro);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    white-space: nowrap;
  }
  tbody tr:last-child td {
    border-bottom: 0;
  }
  .change {
    min-width: 22rem;
  }
  @media (max-width: 640px) {
    .who {
      font-size: var(--text-note);
      white-space: nowrap;
    }
  }
  .change strong {
    display: block;
  }
  .current-row {
    background: color-mix(in oklch, var(--ink) 6%, transparent);
  }
  .right {
    text-align: right;
  }
  /* The tag wraps under the commit at a narrow width, flush with it. */
  .commit {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.2rem 0.35rem;
  }
  .tag {
    display: inline-flex;
    align-items: center;
    padding: 0.08rem 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    font-family: var(--font-body);
    font-size: var(--text-micro);
    color: var(--dim);
    white-space: nowrap;
  }
  .tag.warn-tag {
    border-color: var(--warn);
    color: var(--warn);
  }
  .held {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem 1rem;
    margin-bottom: 0.75rem;
    padding: 0.5rem 0.75rem;
    border: 1px solid var(--warn);
    border-radius: var(--radius-card);
    color: var(--ink);
    font-size: var(--text-label);
  }
  .tag.accent {
    border-color: var(--ink);
    color: var(--ink);
    font-weight: 600;
  }
  .row {
    display: flex;
    justify-content: center;
    padding: 0.5rem 0.75rem;
    border-top: 1px solid var(--border);
  }
</style>
