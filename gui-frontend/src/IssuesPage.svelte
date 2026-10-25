<!--
  Every condition the leader holds right now (`GET /api/warnings`, same
  read `ClusterPage`'s ribbon makes), grouped by condition rather than by
  subject: an operator with six workers silent from one bad rollout wants
  to see "six workers, one condition", not six unrelated rows.

  Nothing here is stored. A group with a cleared condition simply stops
  appearing on the next poll, the same as a single warning does today.
-->
<script lang="ts">
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Collapsible from './Collapsible.svelte';
  import Chips from './Chips.svelte';
  import {
    listWarnings,
    setScope,
    conditionLabel,
    type WarningCondition,
    type WarningDto,
    type WarningSeverity,
  } from './api';
  import { ago } from './time';

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

  $effect(() => {
    refresh();
    const id = setInterval(refresh, POLL_MS);
    return () => clearInterval(id);
  });

  let failedCount = $derived(warnings.filter((w) => w.severity === 'failed').length);
  let warningCount = $derived(warnings.filter((w) => w.severity === 'warning').length);
  let infoCount = $derived(warnings.filter((w) => w.severity === 'info').length);

  /** Failed, then warning, then info: a notice never sits above trouble. */
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
        {#if failedCount > 0}<span class="total failed">&#9650; <b>{failedCount}</b> failed</span
          >{/if}
        {#if warningCount > 0}<span class="total warning"
            >&#9650; <b>{warningCount}</b> warning</span
          >{/if}
        {#if infoCount > 0}<span class="total info">&#9679; <b>{infoCount}</b> info</span>{/if}
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
          <span class="group-cond mono">{group.condition}</span>
        {/snippet}
        {#snippet actions()}
          {#if group.failed > 0}<span class="count failed">&#9650; {group.failed}</span>{/if}
          {#if group.warning > 0}<span class="count warning">&#9650; {group.warning}</span>{/if}
          {#if group.info > 0}<span class="count info">&#9679; {group.info}</span>{/if}
        {/snippet}
        <div class="issues" role="list" aria-label={conditionLabel(group.condition)}>
          {#each group.rows as warning (`${warning.subjectKind}|${warning.subjectId}|${warning.group ?? ''}|${warning.site ?? ''}`)}
            <div class="issue-row" role="listitem">
              <i class="sev-dot {warning.severity}"></i>
              <span class="issue-subject">
                {#if warning.subjectKind === 'worker'}
                  <a
                    href={`#/status/logs?worker=${encodeURIComponent(warning.subjectId)}&level=warn,error`}
                    >{warning.subjectId}</a
                  >
                {:else if warning.subjectKind === 'secret'}
                  <a href="#/admin/secrets">{warning.subjectId}</a>
                {:else if warning.condition === 'secretsKeyUnusable'}
                  <a href="#/admin/secrets/key">{warning.subjectId}</a>
                {:else if ['acmeRenewalFailing', 'leaderCertificateRunningOut', 'authorityAging'].includes(warning.condition)}
                  <a href="#/admin/system/certificates">{warning.subjectId}</a>
                {:else if warning.subjectKind === 'pipeline' && warning.scope}
                  {@const scope = warning.scope}
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
  .dim {
    color: var(--dim);
  }
  /* An empty state is a sentence someone reads, so it is ink. */
  .empty {
    color: var(--ink);
  }
  .mono {
    font-family: var(--mono);
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
  .group-cond {
    color: var(--dim);
    font-size: var(--text-micro);
    margin-left: 0.5rem;
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

  /* The list names a condition's rows; Collapsible's body spaces them. */
  .issues {
    display: contents;
  }
  .issue-row {
    display: grid;
    grid-template-columns: 0.9rem minmax(9rem, 14rem) 1fr auto;
    align-items: start;
    gap: 0.75rem;
    font-size: var(--text-note);
  }
  .sev-dot {
    width: 0.6rem;
    height: 0.6rem;
    margin-top: 0.35rem;
    border-radius: 50%;
    background: var(--dim);
    flex: none;
  }
  .sev-dot.failed {
    background: var(--error);
  }
  .sev-dot.warning {
    background: var(--warn);
  }
  .sev-dot.info {
    background: var(--dim);
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
  .issue-since {
    color: var(--dim);
    font-size: var(--text-micro);
    font-family: var(--mono);
    white-space: nowrap;
  }

  @media (max-width: 40rem) {
    .issue-row {
      grid-template-columns: 0.9rem 1fr;
    }
    .issue-since {
      grid-column: 2;
    }
  }
</style>
