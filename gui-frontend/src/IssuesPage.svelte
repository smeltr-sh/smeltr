<!--
  Every condition the leader holds right now (`GET /api/warnings`, same
  read `ClusterPage`'s ribbon makes), grouped by condition rather than by
  subject: an operator with six workers silent from one bad rollout wants
  to see "six workers, one condition", not six unrelated rows.

  Nothing here is stored. A group with a cleared condition simply stops
  appearing on the next poll, the same as a single warning does today.
-->
<script lang="ts">
  import { Busy } from './busy.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Collapsible from './Collapsible.svelte';
  import Chips from './Chips.svelte';
  import Button, { BUSY } from './Button.svelte';
  import { ask } from './Confirm.svelte';
  import { holds } from './access';
  import {
    listWarnings,
    lowerSourceRows,
    resetSourceHistory,
    setScope,
    conditionLabel,
    type WarningCondition,
    type WarningDto,
    type WarningSeverity,
  } from './api';
  import { ago } from './time';
  import StateMark from './StateMark.svelte';
  import { severityState } from './stateGlyphs';

  const POLL_MS = 2000;

  let warnings: WarningDto[] = $state([]);
  let loading = $state(true);
  let loadFailed = $state(false);
  // Null is All.
  let severityFilter: WarningSeverity | null = $state(null);
  // A condition seen for the first time opens by default -- a fleet with a
  // handful of them should show all of them; one that grows past a screen
  // can be folded by hand, and stays folded across polls since this only
  // ever adds keys, never resets one a reader already chose.
  let expanded: Partial<Record<WarningCondition, boolean>> = $state({});

  function refresh() {
    listWarnings({ info: true })
      .then((w) => {
        warnings = w;
        loadFailed = false;
      })
      .catch((e) => {
        loadFailed = true;
      })
      .finally(() => {
        loading = false;
      });
  }

  /** Whether `w` is a source's rows against its normal that the reader may mark: an editor in its group. */
  const markable = (w: WarningDto) =>
    (w.condition === 'sourceRowsBelow' || w.condition === 'sourceRowsAbove') &&
    w.group !== null &&
    holds(w.group, 'editor');

  const marking = new Busy();

  async function mark(call: () => Promise<unknown>) {
    await marking.run(async () => {
      await call();
      refresh();
    });
  }

  async function resetHistory(w: WarningDto, group: string) {
    const ok = await ask({
      title: 'reset history',
      body: `${w.subjectId} forgets its history and learns its normal again from now. Until it has 3 days of rows, it raises neither Rows below normal nor Rows above normal.`,
      action: 'Reset history',
    });
    if (ok) await mark(() => resetSourceHistory(group, w.subjectId));
  }

  $effect(() => {
    refresh();
    const id = setInterval(refresh, POLL_MS);
    return () => clearInterval(id);
  });

  let failedCount = $derived(warnings.filter((w) => w.severity === 'failed').length);
  let warningCount = $derived(warnings.filter((w) => w.severity === 'warning').length);
  let infoCount = $derived(warnings.filter((w) => w.severity === 'info').length);

  /** Failed, then warning, then info: info never sits above trouble. */
  const RANK: Record<WarningSeverity, number> = { failed: 0, warning: 1, info: 2 };
  const worst = (rows: WarningDto[]) => Math.min(...rows.map((r) => RANK[r.severity]));
  let filtered = $derived(
    severityFilter === null ? warnings : warnings.filter((w) => w.severity === severityFilter),
  );

  type Group = {
    condition: WarningCondition;
    failed: number;
    warning: number;
    info: number;
    rows: WarningDto[];
  };

  /** One group per condition, worst-hit first: any failure beats none, then
   * the larger group. A condition with only warnings never buries one that
   * has even a single failure. */
  let groups = $derived.by((): Group[] => {
    const byCondition = new Map<WarningCondition, WarningDto[]>();
    for (const w of filtered) {
      byCondition.set(w.condition, [...(byCondition.get(w.condition) ?? []), w]);
    }
    const groups = [...byCondition.entries()].map(([condition, rows]) => ({
      condition,
      failed: rows.filter((r) => r.severity === 'failed').length,
      warning: rows.filter((r) => r.severity === 'warning').length,
      info: rows.filter((r) => r.severity === 'info').length,
      rows: rows.toSorted((a, b) => RANK[a.severity] - RANK[b.severity]),
    }));
    return groups.toSorted(
      (a, b) => worst(a.rows) - worst(b.rows) || b.rows.length - a.rows.length,
    );
  });

  $effect(() => {
    for (const g of groups) {
      if (!(g.condition in expanded)) expanded[g.condition] = true;
    }
  });
</script>

<SectionShell>
  <PageHeader title="Issues">
    {#snippet help()}
      Conditions the leader sees right now. A row clears by itself the moment its cause is gone.
    {/snippet}
    {#if warnings.length > 0}
      <span class="totals" aria-label="Issue totals">
        {#if failedCount > 0}<span class="total failed"
            ><StateMark state="error" /> <b>{failedCount}</b> failed</span
          >{/if}
        {#if warningCount > 0}<span class="total warning"
            ><StateMark state="warning" /> <b>{warningCount}</b>
            {warningCount === 1 ? 'warning' : 'warnings'}</span
          >{/if}
        {#if infoCount > 0}<span class="total info"
            ><StateMark state="info" /> <b>{infoCount}</b> info</span
          >{/if}
      </span>
    {/if}
  </PageHeader>

  {#if loading}
    <p class="dim">Loading…</p>
  {:else if loadFailed}
    <!-- The error bar says why. -->
  {:else if warnings.length === 0}
    <p class="empty">No active issues.</p>
  {:else}
    <div class="filters">
      <Chips
        label="Filter by severity"
        allCount={warnings.length}
        items={[
          { key: 'failed', label: 'Failed', count: failedCount },
          { key: 'warning', label: 'Warning', count: warningCount },
          ...(infoCount > 0 ? [{ key: 'info' as const, label: 'Info', count: infoCount }] : []),
        ]}
        bind:value={severityFilter}
      />
    </div>

    {#each groups as group (group.condition)}
      <Collapsible bind:open={expanded[group.condition]}>
        {#snippet summary()}
          <span class="group-title">{conditionLabel(group.condition)}</span>
        {/snippet}
        {#snippet actions()}
          {#if group.failed > 0}<span class="count failed"
              ><StateMark state="error" label="failed" /> {group.failed}</span
            >{/if}
          {#if group.warning > 0}<span class="count warning"
              ><StateMark state="warning" label="warning" /> {group.warning}</span
            >{/if}
          {#if group.info > 0}<span class="count info"
              ><StateMark state="info" label="info" /> {group.info}</span
            >{/if}
        {/snippet}
        <div class="issues" role="list" aria-label={conditionLabel(group.condition)}>
          {#each group.rows as warning (`${warning.subjectKind}|${warning.subjectId}|${warning.group ?? ''}|${warning.class ?? ''}|${warning.type ?? ''}`)}
            <div class="issue-row" role="listitem">
              <span class="sev"
                ><StateMark
                  state={severityState(warning.severity)}
                  label={warning.severity}
                /></span
              >
              <span class="issue-subject">
                {#if warning.subjectKind === 'worker'}
                  <a
                    href={`#/status/logs?worker=${encodeURIComponent(warning.subjectId)}&level=warn,error`}
                    >{warning.subjectId}</a
                  >
                {:else if warning.subjectKind === 'secret'}
                  <a href="#/admin/secrets">{warning.subjectId}</a>
                {:else if warning.subjectKind === 'certificate'}
                  <a href="#/admin/certificates">{warning.subjectId}</a>
                {:else if warning.condition === 'secretsKeyUnusable'}
                  <a href="#/admin/secrets/key">{warning.subjectId}</a>
                {:else if ['acmeRenewalFailing', 'leaderCertificateRunningOut', 'authorityAging'].includes(warning.condition)}
                  <a href="#/admin/system/certificates">{warning.subjectId}</a>
                {:else if warning.subjectKind === 'pipeline' && warning.area}
                  {@const scope = warning.area}
                  <a
                    href={`#/build/pipelines/${encodeURIComponent(warning.subjectId)}`}
                    onclick={() => setScope(scope)}>{warning.subjectId}</a
                  >
                {:else}
                  <strong>{warning.subjectId}</strong>
                {/if}
                {#if warning.group}<span class="group-name">{warning.group}</span>{/if}
              </span>
              <span class="issue-message">{warning.message}</span>
              <span class="issue-since"
                >{warning.sinceMillis === null
                  ? 'since unknown'
                  : ago(warning.sinceMillis / 1000)}</span
              >
              <span class="issue-actions">
                {#if markable(warning) && warning.group !== null}
                  {@const group = warning.group}
                  {#if warning.severity === 'warning'}
                    <Button
                      class="compact"
                      disabled={marking.on ? BUSY : ''}
                      onclick={() => mark(() => lowerSourceRows(group, warning.subjectId))}
                      >Lower to info</Button
                    >
                  {/if}
                  <Button
                    class="compact"
                    disabled={marking.on ? BUSY : ''}
                    onclick={() => resetHistory(warning, group)}>Reset history</Button
                  >
                {/if}
              </span>
            </div>
          {/each}
        </div>
      </Collapsible>
    {:else}
      <p class="empty">Nothing matches this filter.</p>
    {/each}
  {/if}
</SectionShell>

<style>
  /* An empty state is a sentence someone reads, so it is ink. */
  .empty {
    color: var(--ink);
  }

  .totals {
    display: flex;
    gap: 1rem;
    font-size: var(--text-note);
    color: var(--dim);
  }
  .total b {
    font-family: var(--mono);
    font-weight: 700;
  }
  .total.failed {
    color: var(--error);
  }
  .total.warning {
    color: var(--warn);
  }
  .total.info {
    color: var(--dim);
  }

  .filters {
    margin-bottom: 0.9rem;
  }

  .group-title {
    font-weight: 600;
  }
  .count {
    font-size: var(--text-micro);
    font-family: var(--mono);
    font-weight: 700;
  }
  .count.failed {
    color: var(--error);
  }
  .count.warning {
    color: var(--warn);
  }
  .count.info {
    color: var(--dim);
  }

  /* A row stacks its parts while the list is narrower than 48rem. The list's own width decides whether a row fits on
     one line, not the window's: beside the sidebar, a window wider than a phone can still leave the list narrow. */
  .issues {
    container: issues / inline-size;
    display: flex;
    flex-direction: column;
    gap: inherit;
  }
  .issue-row {
    display: grid;
    grid-template-columns: 0.9rem minmax(9rem, 14rem) 1fr auto auto;
    align-items: start;
    gap: 0.75rem;
    font-size: var(--text-note);
  }
  .sev {
    display: flex;
    margin-top: 0.15rem;
  }
  .issue-subject {
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .issue-subject a {
    color: var(--ink);
  }
  .group-name {
    display: block;
    color: var(--dim);
    font-size: var(--text-micro);
  }
  .issue-message {
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .issue-actions {
    display: flex;
    gap: 0.4rem;
    justify-content: flex-end;
  }
  .issue-since {
    color: var(--dim);
    font-size: var(--text-micro);
    font-family: var(--mono);
    white-space: nowrap;
  }

  @container issues (max-width: 48rem) {
    .issue-row {
      grid-template-columns: 0.9rem 1fr;
    }
    /* Under the subject, in its column: the narrow first column holds only the severity mark. */
    .issue-message,
    .issue-since,
    .issue-actions {
      grid-column: 2;
    }
    .issue-actions {
      justify-content: flex-start;
    }
  }
</style>
