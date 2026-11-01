<!--
  Admin: fleet management, not fleet health -- adding worker groups, minting
  enrollment codes, revoking credentials. What a worker is doing lives on
  the Status page; this is what an operator can do to the roster.
-->

<script lang="ts">
  import WorkerGroupIcon from './WorkerGroupIcon.svelte';
  import { slugify } from './slugify';
  import { bareError, errors, reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import { onDestroy, untrack } from 'svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Help from './Help.svelte';
  import Field from './Field.svelte';
  import { ask } from './Confirm.svelte';
  import { dropScope } from './edits';
  import Dialog from './Dialog.svelte';
  import HueWheel from './HueWheel.svelte';
  import MoreIcon from './MoreIcon.svelte';
  import ChipPicker from './ChipPicker.svelte';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';
  import { scopeHue } from './scope';
  import {
    type AdminActionResult,
    listWorkerGroups,
    createWorkerGroup,
    updateWorkerGroup,
    deleteWorkerGroup,
    updateGroupHue,
    mintEnrollmentCode,
    listEnrollmentCodes,
    withdrawEnrollmentCode,
    listWorkers,
    revokeWorker,
    restartWorker,
    shutdownWorker,
    removeWorker,
    getWorkerSettings,
    putWorkerSettings,
    getClusterStatus,
    currentBranch,
    leaveScope,
    errorText,
    errorMessage,
    SHARED_SCOPE,
    type EnrollmentCodeDto,
    type MintedEnrollmentCode,
    type MintEnrollmentCodeRequest,
    type WorkerKind,
    type WorkerSettingsDto,
    type WorkerGroupDto,
    type WorkerCredentialDto,
    type ThreadHealth,
  } from './api';
  import { canAsk, canRemove, rollupWorkers, type WorkerStatus } from './workers';
  import { ago, expiry } from './time';
  import { WORKERS_TABS, type WorkersSection } from './nav';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import Advanced from './Advanced.svelte';

  let {
    page = 'workers',
    section = 'workers',
  }: { page?: 'workers' | 'groups'; section?: WorkersSection } = $props();

  let groups: WorkerGroupDto[] = $state([]);
  /** Groups an enrollment code can be minted into: this branch's own listing, restricted to the
   * ones `main` also holds. A worker validates against `main`, so a group only the branch has --
   * created and not yet deployed, or deleted here but still live on `main` -- would enroll fine
   * and then fail to run. Minting stops at the same line the worker's own validation would. */
  let mintableGroups = $derived(groups.filter((g) => g.deployed));
  let sharedHue: number | null = $state(null);
  let credentials: WorkerCredentialDto[] = $state([]);
  let codes = $state<EnrollmentCodeDto[]>([]);
  let threads: ThreadHealth[] = $state([]);
  let silentAfterMs = $state(0);
  let workerSettings = $state<WorkerSettingsDto | null>(null);
  let loading = $state(true);
  /** The leader's store could not be read: an answer, with the rest of the cluster beside it. */
  let storeError = $state('');

  async function load() {
    loading = true;
    try {
      const [g, c, cluster, outstanding, settings] = await Promise.all([
        listWorkerGroups(true),
        listWorkers(),
        getClusterStatus(),
        listEnrollmentCodes(),
        getWorkerSettings(),
      ]);
      groups = g.groups;
      sharedHue = g.ok ? (g.sharedHue ?? null) : null;
      credentials = c;
      threads = cluster.health?.threads ?? [];
      silentAfterMs = cluster.health?.silentAfterMs ?? 0;
      storeError = cluster.error ?? '';
      codes = outstanding.codes;
      if (settings.settings) adoptWorkerSettings(settings.settings);
    } catch (e) {
      reported(e);
    } finally {
      loading = false;
    }
  }
  // The page stays mounted across its tabs, so opening one is the moment to read the roster again.
  $effect(() => {
    void section;
    untrack(load);
  });

  let workerRows = $derived.by(() => rollupWorkers(credentials, threads, silentAfterMs));

  /** Only a worker currently reporting a group counts toward it -- group
   * membership isn't stored, so a silent worker's group is unknown. */
  let groupCounts = $derived.by(() => {
    const counts: Record<string, number> = {};
    for (const w of workerRows) {
      if (w.groupId) counts[w.groupId] = (counts[w.groupId] ?? 0) + 1;
    }
    return counts;
  });

  const STATE_LABEL: Record<WorkerStatus, string> = {
    online: 'Heartbeating',
    silent: 'Silent',
    neverHeartbeated: 'Never heartbeated',
    revoked: 'Revoked',
    stopped: 'Stopped',
  };

  const workerSort = new TableSort<'worker' | 'group' | 'kind' | 'state' | 'lastSeen'>();
  const sortedWorkers = $derived(
    workerSort.apply(workerRows, {
      worker: (w) => w.workerId,
      group: (w) => w.groupId,
      kind: (w) => w.kind,
      state: (w) => (w.asked ? ASKED_LABEL[w.asked] : STATE_LABEL[w.status]),
      // When it was last seen, so ascending reads oldest first like any other time.
      lastSeen: (w) => (w.lastSeenMillisAgo === null ? null : -w.lastSeenMillisAgo),
    }),
  );

  // Shared is not a group and keeps its place above them; only the groups sort.
  const groupSort = new TableSort<'name' | 'id' | 'state' | 'color' | 'workers'>();
  const sortedGroups = $derived(
    groupSort.apply(groups, {
      name: (g) => g.name,
      id: (g) => g.id,
      state: (g) => (g.deployed ? 'Live' : 'Pending'),
      color: (g) => scopeHue(g.id, groups, sharedHue),
      workers: (g) => groupCounts[g.id] ?? 0,
    }),
  );

  // -- add a group ----------------------------------------------------------

  let addingGroup = $state(false);
  let newGroupName = $state('');
  let newGroupId = $state('');
  let idHandEdited = $state(false); // once true, typing the name stops overwriting the id
  let groupSaving = $state(false);
  // Set on a successful create, rename or delete, naming the branch it
  // landed on. A group write is not live until a change carrying it
  // deploys (design doc, Control plane). So this stands until dismissed,
  // not a toast the operator might miss.
  let groupSaved: { id: string; branch: string; kind: 'created' | 'renamed' | 'deleted' } | null =
    $state(null);

  // -- a group's name -------------------------------------------------------
  // A config write, so it lands on the edit branch and waits for a deploy -- the same notice a
  // newly created group gets. The leader holds the rule that a name is not blank
  // (`config_file::is_safe_group_name`); refusing one here saves a round trip to be told so.
  let renaming: string | null = $state(null);
  let renameValue = $state('');
  let renameSaving = $state(false);
  let renameError = $state('');

  // The row whose actions menu (Rename, Delete) is open. One at a time, the
  // same rule `renaming` and `hueEditing` already keep per row.
  let actionsOpenFor: string | null = $state(null);

  function startRename(id: string, name: string) {
    renaming = id;
    renameValue = name;
    renameError = '';
  }

  async function saveRename(id: string) {
    const name = renameValue.trim();
    if (!name) {
      renameError = 'a group needs a name';
      return;
    }
    renameSaving = true;
    renameError = '';
    try {
      const res = await updateWorkerGroup(id, name);
      if (!res.ok) {
        renameError = errorText(res.errors) || 'could not rename the group';
        return;
      }
      renaming = null;
      groupSaved = { id, branch: currentBranch(), kind: 'renamed' };
      await load();
    } catch (e) {
      renameError = unreported(e);
    } finally {
      renameSaving = false;
    }
  }

  // -- delete a group --------------------------------------------------------
  // A config write like a rename. It lands on the edit branch and waits for
  // a deploy, and the commit can be reverted like any other. It does not
  // touch workers: once it deploys, one still heartbeating under this id
  // has nothing to run and stops its pipelines until someone moves or shuts
  // it down (docs/status.md, "Deleting a group does not look at its workers").
  // Live or not, it does not block the deploy.
  let deletingId = $state('');

  async function deleteGroup(id: string, name: string) {
    // An enrollment code is unbranched, sqlite state (like a hue), so withdrawing one is immediate
    // and has nothing to do with the branch this delete lands on. One still usable outlives the
    // group's manifest: nothing else here revalidates an enrollment code's group against what
    // `main` runs.
    const outstandingCodes = codes.filter((c) => c.groupId === id && usable(c));
    const codeWarning = outstandingCodes.length
      ? ` It withdraws ${outstandingCodes.length} outstanding enrollment code${outstandingCodes.length === 1 ? '' : 's'} for it too.`
      : '';
    const ok = await ask({
      title: 'delete group',
      body:
        `Delete the group ${name} (${id}).${codeWarning} Once deployed, its workers stop all their ` +
        'pipelines until they are moved or shut down.',
      action: 'Delete',
      danger: true,
    });
    if (!ok) return;
    deletingId = id;
    try {
      // Delete first. An enrollment code withdrawn cannot be restored, so it waits until the delete
      // itself has actually landed -- a refused delete (a stale base commit, say) then leaves both
      // the group and its enrollment codes untouched, instead of enrollment codes gone for a group
      // that is still there.
      const res = await deleteWorkerGroup(id);
      if (!res.ok) return;
      // The scope chip holds what a Build page reads and where its saves land -- deleting the
      // group it names leaves a save nothing to write to, and the next one would recreate the
      // group the delete just removed. Same treatment `leaveBranch` gives a branch that's gone.
      // Done as soon as the delete itself has landed, whatever happens to the enrollment codes
      // below: a failed withdrawal must not leave the scope pointed at a group that is already
      // gone. `res.commit` is kept as the base across the switch -- it is the branch's own tip
      // after this delete, valid for the next write whatever scope is active, and leaving must
      // not trade it for no base at all.
      leaveScope(id, res.commit);
      dropScope(id);
      // The group is already gone at this point, so retrying Delete cannot pick a failed
      // withdrawal back up -- attempt every one, rather than stopping at the first, or an
      // enrollment code that failed for its own reason (already withdrawn by someone else, say)
      // would strand every one queued behind it as still outstanding.
      const failedCodes: string[] = [];
      for (const code of outstandingCodes) {
        try {
          // Quiet: what failed is said once below, with the delete it followed.
          const wr = await withdrawEnrollmentCode(code.id, 'quiet');
          if (!wr.ok) failedCodes.push(errorText(wr.errors) || code.id);
        } catch (e) {
          failedCodes.push(errorMessage(e));
        }
      }
      if (failedCodes.length > 0) {
        const n = failedCodes.length;
        errors.report(errors.call('DELETE', `/api/worker-groups/${id}/enrollment-codes`, 'bar'), [
          bareError(
            `Group deleted, but could not withdraw ${n} enrollment code${n === 1 ? '' : 's'}: ${failedCodes.join('; ')}.`,
          ),
        ]);
      } else {
        groupSaved = { id, branch: currentBranch(), kind: 'deleted' };
      }
      await load();
    } catch (e) {
      reported(e);
    } finally {
      deletingId = '';
    }
  }

  // -- a group's color ----------------------------------------------------
  // Immediate: unlike a group's name, a color is not a config write, so
  // saving one needs no branch and is live at once. `shared`'s color is the
  // same kind of write, addressed by `SHARED_SCOPE` rather than a group id.
  let hueEditing: string | null = $state(null);
  let hueValue = $state(0);
  let hueSaving = $state(false);
  let hueError = $state('');

  function editHue(id: string, currentHue: number | null) {
    hueEditing = id;
    hueValue = currentHue ?? 0;
    hueError = '';
  }

  async function saveHue(id: string) {
    hueSaving = true;
    hueError = '';
    try {
      const res = await updateGroupHue(id, hueValue);
      if (!res.ok) {
        hueError = errorText(res.errors) || 'could not save the color';
        return;
      }
      hueEditing = null;
      await load();
    } catch (e) {
      hueError = unreported(e);
    } finally {
      hueSaving = false;
    }
  }

  /** Why the new group's id cannot be added, if it cannot -- under the field, before Add. A blank
   * field only holds Add back: a fresh form shows no error. */
  const groupIdProblem = $derived.by(() => {
    const id = newGroupId.trim();
    if (!id && newGroupName.trim())
      return 'a group needs an id -- adjust the name so one can be derived, or type one';
    if (!id) return null;
    // An answer without a round trip. `groups` holds this branch's, so the leader stays the one
    // that decides: it refuses an id already taken on the branch being written either way.
    if (groups.some((g) => g.id === id)) return `there is already a worker group ${id}`;
    return null;
  });

  function cancelGroup() {
    addingGroup = false;
    newGroupName = '';
    newGroupId = '';
    idHandEdited = false;
  }

  function onGroupNameInput(value: string) {
    newGroupName = value;
    if (!idHandEdited) newGroupId = slugify(value);
  }

  function onGroupIdInput(value: string) {
    newGroupId = value;
    idHandEdited = true;
  }

  async function addGroup() {
    if (groupSaving || !newGroupName.trim() || !newGroupId.trim() || groupIdProblem) return;
    const name = newGroupName.trim();
    const id = newGroupId.trim();
    groupSaving = true;
    try {
      const res = await createWorkerGroup(id, name);
      if (!res.ok) return;
      cancelGroup();
      groupSaved = { id, branch: currentBranch(), kind: 'created' };
      await load();
    } catch (e) {
      reported(e);
    } finally {
      groupSaving = false;
    }
  }

  // -- enrollment codes -------------------------------------------------------

  let minting = $state(false);
  let minted = $state<MintedEnrollmentCode | null>(null);
  let copied = $state(false);
  let withdrawingCode = $state('');

  let mintGroup = $state('');
  let mintKind = $state<WorkerKind>('permanent');
  let mintUses = $state(1);
  let mintHours = $state(24);

  // The select has no blank option, so its bound value has to track a real, live group --
  // otherwise the browser shows a group selected that mintGroup disagrees with.
  $effect(() => {
    if (!mintableGroups.some((group) => group.id === mintGroup)) {
      mintGroup = mintableGroups[0]?.id ?? '';
    }
  });

  /** Whether the form says something the leader would accept. A cleared number
   * input binds as `undefined` and `JSON.stringify` drops the field, so a Mint
   * pressed over an empty box would take the leader's default and say nothing:
   * an operator who cleared "50" to retype it gets a one-worker enrollment code.
   * An ephemeral one has no cap and no expiry, so it asks for neither. */
  const mintReady = $derived(
    mintGroup !== '' &&
      (mintKind === 'ephemeral' ||
        (Number.isInteger(mintUses) &&
          mintUses >= 1 &&
          Number.isInteger(mintHours) &&
          mintHours >= 1)),
  );
  let mintOpen = $state(false);
  // The page stays mounted across its tabs, so a dialog left open by Back would reopen on return.
  $effect(() => {
    if (section !== 'enrollment') mintOpen = false;
  });

  // The address this browser reached the leader at. The leader cannot know what
  // reaches it from somewhere else -- a loopback bind and a proxy both make it a
  // guess -- so this is a starting point the operator corrects, not an answer.
  let leaderUrl = $state(typeof window === 'undefined' ? '' : window.location.origin);

  /** Everything a new worker needs, and nothing else. */
  const enrollCommand = $derived(
    minted
      ? [
          // A process runs no role unless it is told to, so the block that is meant to bring a worker up has to
          // say so. Without this line the paste is complete and the process still exits having done nothing.
          `SMELTR_WORKER_ENABLED=1`,
          `SMELTR_LEADER_URL=${leaderUrl}`,
          ...(minted.groupId ? [`SMELTR_WORKER_GROUP=${minted.groupId}`] : []),
          `SMELTR_WORKER_ENROLLMENT_CODE=${minted.value}`,
        ].join('\n')
      : '',
  );

  async function mintCode() {
    if (minting || !mintReady) return;
    minting = true;
    minted = null;
    copied = false;
    try {
      const groupId = mintGroup.trim();
      const request: MintEnrollmentCodeRequest =
        mintKind === 'ephemeral'
          ? { groupId, kind: 'ephemeral' }
          : { groupId, kind: 'permanent', maxUses: mintUses, lifetimeHours: mintHours };
      const res = await mintEnrollmentCode(request);
      if (!res.ok || !res.code) return;
      minted = res.code;
      mintOpen = false;
      await load();
    } catch (e) {
      reported(e);
    } finally {
      minting = false;
    }
  }

  async function copyCode() {
    if (!enrollCommand) return;
    try {
      await navigator.clipboard.writeText(enrollCommand);
      copied = true;
    } catch {
      // No clipboard access: the value is still on screen to select by hand.
    }
  }

  /** Whether it can still bring anybody in, which is the whole of what there is
   * to withdraw. The leader sends the dates and counts; this reads them. */
  function usable(code: EnrollmentCodeDto): boolean {
    return (
      code.withdrawnAtMillis === null &&
      (code.maxUses === null || code.uses < code.maxUses) &&
      (code.expiresAtMillis === null || code.expiresAtMillis > Date.now())
    );
  }

  /** What an enrollment code is now, in one phrase. A withdrawn one's expiry says
   * nothing, and a spent-out one's is not why it is finished. */
  function codeState(code: EnrollmentCodeDto): string {
    if (code.withdrawnAtMillis !== null) return `withdrawn ${ago(code.withdrawnAtMillis / 1000)}`;
    if (code.maxUses !== null && code.uses >= code.maxUses) return 'spent out';
    if (code.expiresAtMillis === null) return 'until withdrawn';
    return expiry(code.expiresAtMillis);
  }

  const codeSort = new TableSort<'group' | 'kind' | 'mintedBy' | 'used' | 'state'>();
  const sortedCodes = $derived(
    codeSort.apply(codes, {
      group: (c) => c.groupId,
      kind: (c) => c.kind,
      mintedBy: (c) => c.mintedBy,
      used: (c) => c.uses,
      state: (c) => codeState(c),
    }),
  );

  async function withdrawCode(code: EnrollmentCodeDto) {
    const yes = await ask({
      title: 'withdraw enrollment code',
      body:
        code.uses > 0
          ? `It has already brought in ${code.usedBy.join(', ')}. Those workers stay enrolled; ` +
            'this only stops it bringing in anybody else.'
          : 'Anybody holding it will no longer be able to use it.',
      action: 'Withdraw',
      danger: true,
    });
    if (!yes) return;
    withdrawingCode = code.id;
    try {
      // This handler answers `200` with `ok: false` where it refuses, so the
      // result is the only place a refused withdrawal is reported.
      const result = await withdrawEnrollmentCode(code.id);
      if (!result.ok) return;
      await load();
    } catch (e) {
      reported(e);
    } finally {
      withdrawingCode = '';
    }
  }

  // -- how long an ephemeral worker may be silent ------------------------------

  // The leader keeps seconds; the form offers minutes, which is the scale this
  // is set on. Exact either way: a stored 90 seconds reads as 1.5 minutes.
  let forgetMinutes = $state(10);
  let forgetSaving = $state(false);
  let forgetSaved = $state(false);

  function adoptWorkerSettings(next: WorkerSettingsDto) {
    workerSettings = next;
    forgetMinutes = next.ephemeralForgetAfterSeconds / 60;
  }

  const forgetSeconds = $derived(Math.round(forgetMinutes * 60));
  const forgetReady = $derived(
    workerSettings !== null &&
      Number.isFinite(forgetMinutes) &&
      forgetSeconds >= workerSettings.minSeconds &&
      forgetSeconds <= workerSettings.maxSeconds &&
      forgetSeconds !== workerSettings.ephemeralForgetAfterSeconds,
  );

  async function saveForgetAfter(event: SubmitEvent) {
    event.preventDefault();
    if (!forgetReady) return;
    forgetSaving = true;
    forgetSaved = false;
    try {
      const result = await putWorkerSettings(forgetSeconds);
      if (!result.ok || !result.settings) return;
      adoptWorkerSettings(result.settings);
      forgetSaved = true;
    } catch (e) {
      reported(e);
    } finally {
      forgetSaving = false;
    }
  }

  // -- a worker's actions -----------------------------------------------------
  // Restart and shut down reach a worker on its next heartbeat; its State
  // cell says so until it has taken them, so the table looks again until then.

  let workerActionsFor: string | null = $state(null);
  let actingOn = $state('');
  let workerError = $state('');
  let lookingAgain: ReturnType<typeof setTimeout> | undefined;

  const ASKED_LABEL = { restart: 'Restarting…', shutdown: 'Shutting down…' } as const;

  async function act(
    workerId: string,
    question: { title: string; body: string; action: string; danger?: boolean },
    call: (id: string) => Promise<AdminActionResult>,
  ) {
    workerActionsFor = null;
    const ok = await ask(question);
    if (!ok) return;
    actingOn = workerId;
    workerError = '';
    try {
      const res = await call(workerId);
      if (!res.ok) {
        workerError =
          errorText(res.errors) || `could not ${question.action.toLowerCase()} the worker`;
        return;
      }
      await load();
      lookAgainWhileAsked(20);
    } catch (e) {
      workerError = unreported(e);
    } finally {
      actingOn = '';
    }
  }

  function lookAgainWhileAsked(tries: number) {
    clearTimeout(lookingAgain);
    if (tries <= 0 || !workerRows.some((w) => w.asked)) return;
    lookingAgain = setTimeout(async () => {
      await load();
      lookAgainWhileAsked(tries - 1);
    }, 2000);
  }
  onDestroy(() => clearTimeout(lookingAgain));

  const restart = (id: string) =>
    act(id, { title: 'restart worker', body: `Restart ${id}?`, action: 'Restart' }, restartWorker);
  const shutDown = (id: string) =>
    act(
      id,
      {
        title: 'shut down worker',
        body: `Shut down ${id}. Its deployment drains and the process exits.`,
        action: 'Shut down',
        danger: true,
      },
      shutdownWorker,
    );
  const revoke = (id: string) =>
    act(
      id,
      {
        title: 'revoke worker',
        body: `Revoke ${id}. This refuses its next request — a process already running on it keeps running.`,
        action: 'Revoke',
        danger: true,
      },
      revokeWorker,
    );
  const remove = (id: string) =>
    act(
      id,
      {
        title: 'remove worker',
        body: `Remove ${id}. Its credential is revoked for good and it leaves every list.`,
        action: 'Remove',
        danger: true,
      },
      removeWorker,
    );
</script>

<SectionShell>
  <PageHeader title={page === 'groups' ? 'Worker groups' : 'Workers'}>
    {#snippet help()}
      {#if page === 'groups'}
        A worker group is a set of workers that run the same configuration.
      {:else}
        A worker joins by presenting an enrollment code.
      {/if}
    {/snippet}
  </PageHeader>

  {#if page === 'workers'}
    <SecondaryTabs
      tabs={WORKERS_TABS}
      active={section === 'enrollment' ? '#/admin/enrollment' : '#/admin'}
      ariaLabel="Workers"
    />
  {/if}

  {#if page === 'groups' || section === 'enrollment'}
    <div class="toolbar">
      {#if page === 'groups'}
        <button
          type="button"
          class="primary"
          onclick={() => {
            cancelGroup();
            addingGroup = true;
            groupSaved = null;
          }}
        >
          + Add group
        </button>
      {:else if section === 'enrollment'}
        <button
          type="button"
          class="primary"
          onclick={() => {
            mintOpen = true;
            minted = null;
          }}
        >
          + Add enrollment code
        </button>
      {/if}
    </div>
  {/if}

  <InlineError message={storeError} />

  {#if page === 'workers' && section === 'workers'}
    <section>
      <InlineError message={workerError} />
      {#if loading}
        <p>Loading…</p>
      {:else if workerRows.length === 0}
        <p>No worker has ever enrolled.</p>
      {:else}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th aria-sort={workerSort.aria('worker')}
                  ><SortButton sort={workerSort} key="worker">Worker</SortButton></th
                >
                <th aria-sort={workerSort.aria('group')}
                  ><SortButton sort={workerSort} key="group"
                    ><span class="glyphed"><WorkerGroupIcon />Group</span></SortButton
                  ></th
                >
                <th aria-sort={workerSort.aria('kind')}
                  ><SortButton sort={workerSort} key="kind">Kind</SortButton></th
                >
                <th aria-sort={workerSort.aria('state')}
                  ><SortButton sort={workerSort} key="state">State</SortButton></th
                >
                <th aria-sort={workerSort.aria('lastSeen')}
                  ><SortButton sort={workerSort} key="lastSeen">Last seen</SortButton></th
                >
                <th></th>
              </tr>
            </thead>
            <tbody>
              {#each sortedWorkers as w (w.workerId)}
                <tr
                  class:revoked={w.status === 'revoked'}
                  class:attention={w.status === 'silent' || w.status === 'neverHeartbeated'}
                >
                  <td class="mono">{w.workerId}</td>
                  <td class="mono dim">{w.groupId ?? '—'}</td>
                  <td class="dim">{w.kind ?? '—'}</td>
                  <td class="state-{w.status}"
                    >{w.asked ? ASKED_LABEL[w.asked] : STATE_LABEL[w.status]}</td
                  >
                  <td class="dim">
                    {w.lastSeenMillisAgo === null
                      ? 'never'
                      : ago(Date.now() / 1000 - w.lastSeenMillisAgo / 1000)}
                  </td>
                  <td class="right">
                    <ChipPicker
                      kind="row"
                      align="right"
                      compact
                      ariaLabel="More actions for {w.workerId}"
                      bind:open={
                        () => workerActionsFor === w.workerId,
                        (v) => (workerActionsFor = v ? w.workerId : null)
                      }
                    >
                      {#snippet face()}
                        <MoreIcon />
                      {/snippet}
                      {#if canAsk(w, 'restart')}
                        <button
                          type="button"
                          class="pick"
                          disabled={actingOn === w.workerId}
                          onclick={() => restart(w.workerId)}>Restart</button
                        >
                      {/if}
                      {#if canAsk(w, 'shutdown')}
                        <button
                          type="button"
                          class="pick"
                          disabled={actingOn === w.workerId}
                          onclick={() => shutDown(w.workerId)}>Shut down</button
                        >
                      {/if}
                      {#if w.status !== 'revoked'}
                        <button
                          type="button"
                          class="pick remove"
                          disabled={actingOn === w.workerId}
                          onclick={() => revoke(w.workerId)}>Revoke</button
                        >
                      {/if}
                      {#if canRemove(w)}
                        <button
                          type="button"
                          class="pick remove"
                          disabled={actingOn === w.workerId}
                          onclick={() => remove(w.workerId)}>Remove</button
                        >
                      {/if}
                    </ChipPicker>
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </section>
    {#if workerSettings}
      <section>
        <Advanced
          label="Ephemeral workers"
          modified={forgetSeconds !== workerSettings.defaultSeconds}
        >
          <form onsubmit={saveForgetAfter}>
            <div class="mintrow">
              <Field required label="Forget one after it has been silent for (minutes)">
                {#snippet help()}
                  How long an ephemeral worker that stops heartbeating without saying it is stopping
                  stays listed.
                {/snippet}
                <input
                  type="number"
                  min={workerSettings.minSeconds / 60}
                  max={workerSettings.maxSeconds / 60}
                  bind:value={forgetMinutes}
                />
              </Field>
              <button type="submit" class="primary" disabled={forgetSaving || !forgetReady}>
                {forgetSaving ? 'Saving…' : 'Save'}
              </button>
            </div>
            {#if forgetSaved}<p>Saved. The next look for gone workers uses it.</p>{/if}
          </form>
        </Advanced>
      </section>
    {/if}
  {:else if page === 'workers'}
    <Dialog
      open={mintOpen}
      title="new enrollment code"
      oncancel={() => (mintOpen = false)}
      onsubmit={mintCode}
    >
      <div class="mintrow">
        <Field
          label="Group"
          help="Only live groups can receive new workers."
          note={mintableGroups.length === 0
            ? 'Add a worker group on Worker groups and deploy it, then mint.'
            : undefined}
        >
          <select bind:value={mintGroup} disabled={mintableGroups.length === 0}>
            {#if mintableGroups.length === 0}
              <option value="">No live group</option>
            {/if}
            {#each mintableGroups as group (group.id)}
              <option value={group.id}>{group.name}</option>
            {/each}
          </select>
        </Field>
        <Field label="Workers it brings in">
          {#snippet help()}
            <strong>Permanent</strong>: kept until you remove it, with a warning when it goes
            silent. Use for servers, VMs and containers with persistent storage.<br />
            <strong>Ephemeral</strong>: forgotten once it stops or goes silent. Use for Kubernetes
            and other autoscalers that add and remove workers on demand.
          {/snippet}
          <select bind:value={mintKind}>
            <option value="permanent">Permanent</option>
            <option value="ephemeral">Ephemeral</option>
          </select>
        </Field>
        {#if mintKind === 'permanent'}
          <Field required label="Workers">
            <input type="number" min="1" bind:value={mintUses} />
          </Field>
          <Field required label="Valid for (hours)">
            <input type="number" min="1" bind:value={mintHours} />
          </Field>
        {/if}
      </div>
      {#snippet actions()}
        <button type="button" onclick={() => (mintOpen = false)}>Cancel</button>
        <button type="submit" class="primary" disabled={minting || !mintReady}>
          {minting ? 'Minting…' : 'Mint'}
        </button>
      {/snippet}
    </Dialog>

    {#if minted}
      <div class="card mint">
        <p class="critical">
          Copy this enrollment code now: it is shown only once.
          {#if minted.kind === 'ephemeral'}
            Store it where every worker the autoscaler starts can read it, such as a Kubernetes
            Secret.
          {/if}
        </p>
        <Field required label="The address this worker will reach the leader at">
          <input class="mono" bind:value={leaderUrl} />
        </Field>
        <pre class="mono code-value enrollblock">{enrollCommand}</pre>
        <div class="actions">
          <button type="button" onclick={copyCode}>{copied ? 'Copied' : 'Copy'}</button>
          <span class="spacer"></span>
          <button type="button" onclick={() => (minted = null)}>Done</button>
        </div>
      </div>
    {/if}

    <section>
      {#if loading}
        <p>Loading…</p>
      {:else if codes.length === 0}
        <p>No enrollment code is waiting to be used, and none has brought a worker in.</p>
      {:else}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th aria-sort={codeSort.aria('group')}
                  ><SortButton sort={codeSort} key="group"
                    ><span class="glyphed"><WorkerGroupIcon />Group</span></SortButton
                  ></th
                >
                <th aria-sort={codeSort.aria('kind')}
                  ><SortButton sort={codeSort} key="kind">Kind</SortButton></th
                >
                <th aria-sort={codeSort.aria('mintedBy')}
                  ><SortButton sort={codeSort} key="mintedBy">Minted by</SortButton></th
                >
                <th aria-sort={codeSort.aria('used')}
                  ><SortButton sort={codeSort} key="used">Used</SortButton></th
                >
                <th aria-sort={codeSort.aria('state')}
                  ><SortButton sort={codeSort} key="state">State</SortButton></th
                >
                <th></th>
              </tr>
            </thead>
            <tbody>
              {#each sortedCodes as code (code.id)}
                <tr>
                  <td>{code.groupId || '—'}</td>
                  <td>{code.kind}</td>
                  <td>{code.mintedBy}</td>
                  <td>
                    {code.maxUses === null ? code.uses : `${code.uses} of ${code.maxUses}`}
                    {#if code.usedBy.length > 0}
                      <span class="dim">· {code.usedBy.join(', ')}</span>
                    {/if}
                  </td>
                  <td>{codeState(code)}</td>
                  <td class="right">
                    {#if usable(code)}
                      <button
                        type="button"
                        onclick={() => withdrawCode(code)}
                        disabled={withdrawingCode === code.id}
                      >
                        {withdrawingCode === code.id ? 'Withdrawing…' : 'Withdraw'}
                      </button>
                    {/if}
                  </td>
                </tr>
                {#if usable(code) && code.namesAuthority === 'untilCutover'}
                  <tr>
                    <td colspan="6" class="warnrow">
                      This enrollment code names the certificate authority this leader is rotating
                      away from. It still brings a worker in, until the cutover withdraws it. Spend
                      it before then, or mint another once the rotation is over.
                    </td>
                  </tr>
                {:else if usable(code) && code.namesAuthority === 'replaced'}
                  <tr>
                    <td colspan="6" class="warnrow">
                      This enrollment code names a certificate authority this leader no longer
                      serves under. A worker given it will refuse to enroll. Withdraw it and mint
                      another.
                    </td>
                  </tr>
                {/if}
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </section>
  {:else}
    {#if groupSaved}
      <div class="card mint">
        <p class="warn">
          {#if groupSaved.kind === 'deleted'}
            Group <code class="mono">{groupSaved.id}</code> removed on branch
            <code class="mono">{groupSaved.branch}</code>. It stays live until a change carrying
            this deploys -- workers keep running it until then, and the commit can be reverted
            before or after.
          {:else}
            Group <code class="mono">{groupSaved.id}</code> saved on branch
            <code class="mono">{groupSaved.branch}</code>. It is not live until a change carrying it
            deploys -- workers do not run it until then.
          {/if}
        </p>
        <div class="actions">
          <a class="btn primary" href="#/changes">Open for review</a>
          <span class="spacer"></span>
          <button type="button" onclick={() => (groupSaved = null)}>Done</button>
        </div>
      </div>
    {/if}

    <Dialog
      open={addingGroup}
      title="new worker group"
      oncancel={() => groupSaving || cancelGroup()}
      onsubmit={addGroup}
    >
      <div class="mintrow">
        <Field required label="Name">
          <input
            value={newGroupName}
            oninput={(e) => onGroupNameInput(e.currentTarget.value)}
            placeholder="prod west"
          />
        </Field>
        <Field required label="Id" error={groupIdProblem}>
          {#snippet help()}
            The group's directory in the config repository: letters, digits, - _ and ., not starting
            with a dot. It is fixed once saved.
          {/snippet}
          <input
            value={newGroupId}
            oninput={(e) => onGroupIdInput(e.currentTarget.value)}
            placeholder="prod-west"
            class="mono"
          />
        </Field>
      </div>
      {#snippet actions()}
        <button type="button" onclick={cancelGroup} disabled={groupSaving}>Cancel</button>
        <button
          type="submit"
          class="primary"
          disabled={groupSaving ||
            !newGroupName.trim() ||
            !newGroupId.trim() ||
            groupIdProblem !== null}
        >
          {groupSaving ? 'Saving…' : 'Add'}
        </button>
      {/snippet}
    </Dialog>

    <section>
      <h2>Groups</h2>
      {#if loading}
        <p>Loading…</p>
      {:else}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th aria-sort={groupSort.aria('name')}
                  ><SortButton sort={groupSort} key="name">Name</SortButton></th
                >
                <th aria-sort={groupSort.aria('id')}
                  ><SortButton sort={groupSort} key="id">Id</SortButton></th
                >
                <th aria-sort={groupSort.aria('state')}
                  ><SortButton sort={groupSort} key="state">State</SortButton></th
                >
                <th aria-sort={groupSort.aria('color')}
                  ><SortButton sort={groupSort} key="color">Color</SortButton></th
                >
                <th class="num" aria-sort={groupSort.aria('workers')}
                  ><SortButton sort={groupSort} key="workers">Workers</SortButton></th
                >
                <th class="right"></th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td
                  >Shared<Help label="Shared"
                    >Config every group runs. Each worker runs its own group's config merged with
                    Shared, so a change here reaches every group. An id may be defined in Shared or
                    in a group, not both. Shared has no workers of its own.</Help
                  ></td
                >
                <td class="mono dim">{SHARED_SCOPE}</td>
                <td class="dim">&mdash;</td>
                <td>
                  <button
                    type="button"
                    class="swatch"
                    onclick={() => editHue(SHARED_SCOPE, sharedHue)}
                  >
                    <span
                      class="dot"
                      style="background: oklch(var(--scope-l) var(--scope-c) {sharedHue ?? 0});"
                    ></span>
                    <span class="mono">{sharedHue ?? '--'}&deg;</span>
                  </button>
                </td>
                <td class="num dim">&mdash;</td>
                <td></td>
              </tr>
              {#if hueEditing === SHARED_SCOPE}
                <tr>
                  <td colspan="6">
                    <div class="hue-edit">
                      <HueWheel bind:value={hueValue} label="Shared" />
                      <div class="actions">
                        <button
                          type="button"
                          class="primary"
                          disabled={hueSaving}
                          onclick={() => saveHue(SHARED_SCOPE)}
                          >{hueSaving ? 'Saving…' : 'Save color'}</button
                        >
                        <button type="button" onclick={() => (hueEditing = null)}>Cancel</button>
                      </div>
                      <InlineError message={hueError} />
                    </div>
                  </td>
                </tr>
              {/if}
              {#each sortedGroups as g (g.id)}
                <tr>
                  <td>{g.name}</td>
                  <td class="mono dim">{g.id}</td>
                  <td class="state-{g.deployed ? 'live' : 'pending'}"
                    >{g.deployed ? 'Live' : 'Pending'}</td
                  >
                  <td>
                    <button
                      type="button"
                      class="swatch"
                      onclick={() => editHue(g.id, scopeHue(g.id, groups, sharedHue))}
                    >
                      <span
                        class="dot"
                        style="background: oklch(var(--scope-l) var(--scope-c) {scopeHue(
                          g.id,
                          groups,
                          sharedHue,
                        ) ?? 0});"
                      ></span>
                      <span class="mono">{scopeHue(g.id, groups, sharedHue) ?? '--'}&deg;</span>
                    </button>
                  </td>
                  <td class="num">{groupCounts[g.id] ?? 0}</td>
                  <td class="right">
                    <ChipPicker
                      kind="row"
                      align="right"
                      compact
                      ariaLabel="More actions for {g.name}"
                      bind:open={
                        () => actionsOpenFor === g.id, (v) => (actionsOpenFor = v ? g.id : null)
                      }
                    >
                      {#snippet face()}
                        <MoreIcon />
                      {/snippet}
                      <button
                        type="button"
                        class="pick"
                        onclick={() => {
                          actionsOpenFor = null;
                          startRename(g.id, g.name);
                        }}
                      >
                        Rename
                      </button>
                      <button
                        type="button"
                        class="pick remove"
                        disabled={deletingId === g.id}
                        onclick={() => {
                          actionsOpenFor = null;
                          deleteGroup(g.id, g.name);
                        }}
                      >
                        {deletingId === g.id ? 'Deleting…' : 'Delete'}
                      </button>
                    </ChipPicker>
                  </td>
                </tr>
                {#if renaming === g.id}
                  <tr>
                    <td colspan="6">
                      <div class="hue-edit">
                        <Field required label="Name">
                          <!-- svelte-ignore a11y_autofocus -->
                          <input bind:value={renameValue} autofocus />
                        </Field>
                        <div class="actions">
                          <button
                            type="button"
                            class="primary"
                            disabled={renameSaving || !renameValue.trim()}
                            onclick={() => saveRename(g.id)}
                            >{renameSaving ? 'Saving…' : 'Save name'}</button
                          >
                          <button type="button" onclick={() => (renaming = null)}>Cancel</button>
                        </div>
                        <InlineError message={renameError} />
                      </div>
                    </td>
                  </tr>
                {/if}
                {#if hueEditing === g.id}
                  <tr>
                    <td colspan="5">
                      <div class="hue-edit">
                        <HueWheel bind:value={hueValue} label={g.name} />
                        <div class="actions">
                          <button
                            type="button"
                            class="primary"
                            disabled={hueSaving}
                            onclick={() => saveHue(g.id)}
                            >{hueSaving ? 'Saving…' : 'Save color'}</button
                          >
                          <button type="button" onclick={() => (hueEditing = null)}>Cancel</button>
                        </div>
                        <InlineError message={hueError} />
                      </div>
                    </td>
                  </tr>
                {/if}
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </section>
  {/if}
</SectionShell>

<style>
  .glyphed {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
  }

  /* A group's color, opened from its row. */
  .toolbar {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin: 1.2rem 0;
  }

  .swatch {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: none;
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0.15rem 0.5rem 0.15rem 0.25rem;
    color: var(--dim);
    font: inherit;
    font-size: var(--text-note);
    cursor: pointer;
  }

  .swatch:hover {
    border-color: var(--ink);
    color: var(--ink);
  }

  .dot {
    width: 12px;
    height: 12px;
    border-radius: 999px;
  }

  .hue-edit input {
    min-width: 16rem;
  }

  .hue-edit {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 1rem;
    padding: 0.7rem 0.2rem;
  }

  h2 {
    font-size: var(--text-section);
    margin: 0 0 0.6rem;
  }

  section {
    margin-bottom: 1.75rem;
  }

  /* A paragraph is a sentence someone reads, so it is ink; an id or a dash in a row stays dim. */
  .mono {
    font-family: var(--mono);
  }

  .warn {
    color: var(--warn);
    font-size: var(--text-body);
  }

  .card p {
    margin: 0;
  }

  .mintrow {
    display: grid;
    gap: 0.7rem;
  }

  .mintrow > button {
    justify-self: start;
  }

  .card {
    display: grid;
    gap: 0.7rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 0.9rem;
    margin-bottom: 1.25rem;
  }

  .card.mint {
    border-color: var(--warn);
  }

  .code-value {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.35rem 0.6rem;
    font-size: var(--text-section);
    user-select: all;
    overflow-wrap: anywhere;
  }

  /* An enrollment code is one long unbroken value on a line of its own. `pre` keeps the
     line breaks between the settings, and would otherwise keep the overflow
     too, running the value off the panel. `code-value`'s `user-select: all`
     is right for a single-value field but would force a drag anywhere in
     this multi-line block to select the whole thing; undo it here so a
     worker can still select just the enrollment code line. */
  .enrollblock {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    margin: 0;
    user-select: text;
  }

  input,
  select {
    width: 100%;
    box-sizing: border-box;
  }

  .actions {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .spacer {
    flex-grow: 1;
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-body);
  }

  th {
    text-align: left;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
    padding: 0.4rem 0.6rem;
  }

  td {
    padding: 0.5rem 0.6rem;
    border-bottom: 1px solid var(--border);
  }

  tbody tr:last-child td {
    border-bottom: none;
  }

  .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .right {
    text-align: right;
  }

  tr.attention {
    background: color-mix(in oklch, var(--warn) 8%, transparent);
  }

  tr.revoked td {
    opacity: 0.6;
  }

  .state-online {
    color: var(--added);
  }

  .state-silent,
  .state-neverHeartbeated {
    color: var(--warn);
    font-weight: 600;
  }

  .state-stopped {
    color: var(--dim);
  }

  .state-revoked {
    color: var(--error);
    font-weight: 600;
  }

  .state-live {
    color: var(--added);
  }

  .state-pending {
    color: var(--warn);
    font-weight: 600;
  }
</style>
