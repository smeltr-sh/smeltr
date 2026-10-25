<!--
  An SQL source's own fields, PostgreSQL's or MySQL's: where it connects, the query, and Run. Run
  is a capture of the source as it stands on the page, read by a worker in its group, which answers with the query's
  columns, its first rows and what the database plans for a poll. How rows are found and which
  columns become fields follow from that answer, so a source that has neither shows them only once
  Run has answered. The statement smeltr runs is the leader's, built as a poll builds it.
-->
<script lang="ts">
  import { unreported } from './errors.svelte';
  import { onDestroy } from 'svelte';
  import Field from './Field.svelte';
  import Nested from './Nested.svelte';
  import Advanced from './Advanced.svelte';
  import IdentityPicker from './IdentityPicker.svelte';
  import SecretRefInput from './SecretRefInput.svelte';
  import ValidatedField from './ValidatedField.svelte';
  import { CaptureSession, groupToAsk } from './captureSession.svelte';
  import { cellText, settled } from './capture';
  import { highlight } from '../../website/src/lib/highlight/highlight';
  import { mintSampleId } from './ids';
  import {
    errorText,
    LIMITS,
    FIELD_TYPES,
    saveCapture,
    sqlStatement,
    type IdentityDto,
    type SecretDto,
    type SourceDto,
    type SqlColumn,
    type SqlStatementsResult,
    type SqlVerdict,
  } from './api';
  import {
    bindNotes,
    canOrder,
    DEFAULT_SETTLE_SECS,
    afterRun,
    highlightSql,
    uniqueInResult,
    MODES,
    positionFor,
    roleOf,
    suggestMode,
    switchMode,
    tieBreak,
    type ModeType,
  } from './sql';
  import { numberOrUnset } from './page';

  type SqlSource = Extract<SourceDto, { type: 'postgres' | 'mysql' }>;

  let {
    source,
    identities,
    secrets,
    readAt,
    onsaved,
  }: {
    source: SqlSource;
    identities: IdentityDto[];
    secrets: SecretDto[];
    /** The commit the page read its sources at. A sample is saved against it, so its commit follows that one. */
    readAt?: string | null;
    /** Told the sample's commit, which the page's next save is made against: nothing else moved the branch. */
    onsaved?: (commit: string) => void;
  } = $props();
  /** The port the database listens on when none is named. */
  const defaultPort = $derived(source.type === 'mysql' ? 3306 : 5432);

  /** How many rows Run reads. */
  const RUN_ROWS = 20;
  /** How many of them the columns table shows. */
  const SHOWN_ROWS = 3;
  /** How long Run waits for a worker before it gives up. */
  const RUN_SECS = 30;

  let identity = $derived(
    source.identity === undefined ? undefined : identities.find((i) => i.id === source.identity),
  );

  // The statement follows every edit. Asked again once typing pauses, and an answer that arrives after a later
  // question is dropped.
  let statements: SqlStatementsResult | null = $state(null);
  let asked = 0;
  $effect(() => {
    const body = JSON.stringify(source);
    const at = ++asked;
    const timer = setTimeout(() => {
      sqlStatement(JSON.parse(body) as SourceDto, identity)
        .then((res) => at === asked && (statements = res))
        .catch(() => at === asked && (statements = null));
    }, 300);
    return () => clearTimeout(timer);
  });

  let group = $state('');
  groupToAsk((g) => (group = g));

  let runError = $state('');
  const run = new CaptureSession((e) => (runError = e));
  onDestroy(() => run.destroy());

  /** What the last Run answered. Its capture is kept, so its rows can be saved as a sample, until the next Run. */
  let columns: SqlColumn[] = $state([]);
  let ran = $state<{
    verdict?: SqlVerdict;
    rows: unknown[][];
    worker: string;
    ms: number;
    /** The query, mode and connection it ran: the verdict is that poll's only while they stand. */
    with: string;
  } | null>(null);
  let started = 0;
  let startedWith = '';
  /** The session whose answer was read: a status polled again is not read twice. */
  let read = '';

  const runKey = () =>
    JSON.stringify([
      source.query,
      source.mode,
      source.host,
      source.port,
      source.database,
      source.user,
      source.tls,
      source.identity,
    ]);

  async function startRun() {
    runError = '';
    // What an earlier Run found says nothing about this one's query.
    ran = null;
    columns = [];
    failedRun = null;
    sampled = '';
    await run.discard();
    if (!group) {
      runError = 'No worker group to run it: Run asks a worker in the source’s group.';
      return;
    }
    started = performance.now();
    startedWith = runKey();
    await run.start({
      group,
      source: $state.snapshot(source) as SourceDto,
      identity: identity ? ($state.snapshot(identity) as IdentityDto) : undefined,
      maxMatches: RUN_ROWS,
      deadlineSecs: RUN_SECS,
    });
  }

  $effect(() => {
    const status = run.status;
    if (!status || !settled(status) || read === run.session) return;
    read = run.session;
    const ms = performance.now() - started;
    const worker = status.workers[0]?.worker ?? '';
    const failed = status.ended.map(([, end]) => end).find((end) => typeof end !== 'string');
    if (failed) {
      runError = failed.failed.trim();
      failedRun = { worker, ms };
    } else if (!status.sql) runError = 'No worker in the group answered.';
    if (status.sql) adopt(status.sql.columns, status, worker, ms);
  });

  /** Where a Run that failed ran, for the line beside Run. */
  let failedRun = $state<{ worker: string; ms: number } | null>(null);

  /** Take what Run found: the columns, its rows, and the fields and mode they leave the source with (`afterRun`). */
  function adopt(
    found: SqlColumn[],
    status: NonNullable<typeof run.status>,
    worker: string,
    ms: number,
  ) {
    found = uniqueInResult(found);
    columns = found;
    const rows: unknown[][] = [];
    for (const batch of status.batches) {
      const at = found.map((c) => batch.columns.findIndex((b) => b.name === c.name));
      for (const row of batch.rows) rows.push(at.map((i) => (i < 0 ? undefined : row.values[i])));
    }
    ran = {
      verdict: status.sql?.verdict,
      rows: rows.slice(0, RUN_ROWS),
      worker,
      ms,
      with: startedWith,
    };
    const next = afterRun(source.fields, source.mode, found);
    if (next.fields !== source.fields) {
      source.fields = next.fields;
      source.mode = next.mode;
    }
  }

  let running = $derived(run.session !== '' && !(run.status && settled(run.status)));
  /** Whether the choices that follow from Run are shown: Run has answered, or the source already has them. */
  let opened = $derived(columns.length > 0 || source.fields.length > 0);
  let positional = $derived(
    source.mode.type === 'incrementing' || source.mode.type === 'updatedAt',
  );

  /** The columns of the result as the table shows them: Run's, or before any Run, the declared fields. */
  let shown: SqlColumn[] = $derived(
    columns.length > 0
      ? columns
      : source.fields.map((f) => ({
          name: f.name,
          databaseType: '',
          suggested: f.type,
          unique: false,
        })),
  );
  /** Whether the result reads more than one table, so a column says which it comes from. */
  let joined = $derived(new Set(columns.flatMap((c) => (c.table ? [c.table] : []))).size > 1);
  let suggested = $derived(columns.length > 0 ? suggestMode(columns) : undefined);

  const typeOf = (name: string) => source.fields.find((f) => f.name === name)?.type;

  /** Declare `name` as `type`, or stop reading it. A field keeps its place in the result's order. */
  function setType(name: string, type: string) {
    const rest = source.fields.filter((f) => f.name !== name);
    if (type === 'off') {
      source.fields = rest;
      return;
    }
    const order = shown.map((c) => c.name);
    source.fields = [...rest, { name, type }].sort(
      (a, b) => order.indexOf(a.name) - order.indexOf(b.name),
    );
  }

  function pickMode(type: ModeType) {
    if (type !== source.mode.type) source.mode = switchMode(source.mode, type, shown);
  }

  let orderable = $derived(shown.filter(canOrder));
  let positionColumn = $derived(
    positional && 'position' in source.mode ? source.mode.position[0] : undefined,
  );
  let positionIsTime = $derived(
    shown.find((c) => c.name === positionColumn)?.suggested === 'datetime',
  );
  /** Whether the position needs a column that tells apart rows sharing its value: when Run found it not unique,
   * or before any Run, when the source names one. */
  let needsTie = $derived.by(() => {
    if (positionColumn === undefined) return false;
    const found = columns.find((c) => c.name === positionColumn);
    return found ? !found.unique : 'position' in source.mode && source.mode.position.length > 1;
  });

  /** The last row Run read, by column: what a poll after it would bind. */
  let lastRow = $derived.by(() => {
    const row = ran?.rows.at(-1);
    return row ? Object.fromEntries(columns.map((c, i) => [c.name, row[i]])) : undefined;
  });
  let startText = $derived(
    'start' in source.mode
      ? source.mode.start === 'from'
        ? String(source.mode.startFrom ?? '')
        : (source.mode.start ?? 'now')
      : undefined,
  );

  /** The record the pipeline gets from Run's first row: the declared fields, as Run read them. */
  let firstRecord = $derived.by(() => {
    const row = ran?.rows[0];
    if (!row) return undefined;
    const record: Record<string, unknown> = {};
    columns.forEach((c, i) => {
      if (typeOf(c.name) !== undefined) record[c.name] = row[i];
    });
    return record;
  });

  /** Whether the verdict is the plan of the poll the page shows: nothing it depends on changed since Run. */
  let checked = $derived(ran !== null && ran.with === runKey());

  let sampled = $state('');
  let saving = $state(false);
  async function saveSample() {
    if (!run.session) return;
    saving = true;
    runError = '';
    try {
      const res = await saveCapture(run.session, mintSampleId(), source.id, source.id, readAt);
      if (!res.ok) runError = errorText(res.errors) || 'Could not save the rows.';
      else {
        // Without a read commit the save was checked against nothing, so it says nothing of what else moved.
        if (res.commit && readAt) onsaved?.(res.commit);
        run.release();
        sampled = source.id;
      }
    } catch (e) {
      runError = unreported(e);
    } finally {
      saving = false;
    }
  }

  const seconds = (ms: number) =>
    ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;
</script>

<div class="pair">
  <Field required field="host" label="host">
    <input type="text" bind:value={source.host} placeholder="db.internal" />
  </Field>
  <Field optional field="port" label="port">
    <input
      type="number"
      min="1"
      max="65535"
      value={source.port ?? defaultPort}
      oninput={(e) => {
        const n = numberOrUnset(e.currentTarget);
        source.port = n === undefined || n === defaultPort ? undefined : Math.round(n);
      }}
    />
  </Field>
</div>
<div class="pair">
  <Field required field="database" label="database">
    <input type="text" bind:value={source.database} />
  </Field>
  <Field required field="user" label="user">
    {#snippet help()}
      A user that can only read. smeltr runs only a <code>SELECT</code> or a <code>WITH</code>, in a
      read-only transaction, but the database is what enforces it.
    {/snippet}
    <input type="text" bind:value={source.user} />
  </Field>
</div>
<Field optional field="password" label="password" group>
  <SecretRefInput
    value={source.password}
    {secrets}
    label="password"
    onchange={(v) => (source.password = v === '' ? undefined : v)}
  />
</Field>
<Field field="tls" label="TLS">
  <select
    value={source.tls ?? 'require'}
    onchange={(e) => {
      const v = e.currentTarget.value as 'require' | 'disable';
      source.tls = v === 'require' ? undefined : v;
      if (v === 'disable') source.identity = undefined;
    }}
  >
    <option value="require">require &mdash; encrypted, the server verified</option>
    <option value="disable">disable &mdash; everything in the clear, the password too</option>
  </select>
</Field>
{#if (source.tls ?? 'require') === 'require'}
  <Nested>
    <IdentityPicker
      value={source.identity}
      {identities}
      farEnd="source"
      onchange={(v) => (source.identity = v)}
    />
  </Nested>
{/if}

<Field required field="query" label="query" group>
  {#snippet help()}
    Yours, run as written inside smeltr's own statement. Leave out <code>ORDER BY</code> and
    <code>LIMIT</code>: the statement orders by the position and sets the batch size. A
    <code>SELECT</code> or a <code>WITH</code>, as the database accepts it.
  {/snippet}
  <ValidatedField
    bind:value={source.query}
    multiline
    rows={4}
    ariaLabel="query"
    required
    paint={(text: string) => highlightSql(text, source.type)}
    placeholder="SELECT event_id, occurred_at, user_name FROM auth.events"
  />
</Field>

{#snippet statement()}
  {#if statements && !statements.ok}
    <p class="banner bad">{errorText(statements.errors)}</p>
  {:else if !statements?.first}
    <p class="dim">Type a query.</p>
  {:else if !opened}
    {#if statements.run}<pre class="statement">{@html highlightSql(
          statements.run.sql,
          source.type,
        )}</pre>{/if}
    <p class="dim">
      Your query, unchanged, inside smeltr's, limited to {RUN_ROWS} rows. Once you choose how new rows
      are found, the position test and the order join it here.
    </p>
  {:else}
    {@const poll = statements.later ?? statements.first}
    <div class="statement-box">
      <pre class="statement">{@html highlightSql(poll.sql, source.type)}</pre>
      {#if poll.binds.length > 0}
        <p class="bound">
          Bound {statements.later
            ? 'on each poll, from the last row sent'
            : 'from where the source starts'}:
          {bindNotes(poll, lastRow, startText, source.type).join(
            ', ',
          )}.{#if statements.later && statements.first.binds.length > 0}{' '}Before the first row,
            the start from value.{/if}
        </p>
      {/if}
      {#if statements.later}
        <details>
          <summary>The first poll, before any row is sent</summary>
          <pre class="statement">{@html highlightSql(statements.first.sql, source.type)}</pre>
          {#if statements.first.binds.length > 0}
            <p class="bound">
              Bound: {bindNotes(statements.first, undefined, startText, source.type).join(', ')}.
            </p>
          {/if}
        </details>
      {/if}
    </div>
    <p class="dim">
      Your query, unchanged, inside smeltr's. It follows the choices above. Run reads the result
      from the beginning, {RUN_ROWS} rows, and asks the database for the plan of a poll.
    </p>
  {/if}
{/snippet}

{#if !opened}
  <Field label="what smeltr runs instead" group>
    {@render statement()}
  </Field>
{/if}

<div class="run">
  <button type="button" class:primary={!opened} onclick={() => void startRun()} disabled={running}>
    {running ? 'Running…' : 'Run'}
  </button>
  <span class="dim">
    {#if running}
      Asking a worker in {group}…
    {:else if ran}
      Ran on worker {ran.worker || '?'} in group {group} · {ran.rows.length} rows · {seconds(
        ran.ms,
      )}
    {:else if failedRun && runError}
      Ran on worker {failedRun.worker || '?'} in group {group} · failed after {seconds(
        failedRun.ms,
      )}
    {:else}
      Runs that, on a worker in {group ? `group ${group}` : 'the group'}.
    {/if}
  </span>
</div>
{#if runError}
  <div class="banner bad" role="alert">
    <strong>Run failed.</strong>
    {runError}{#if failedRun?.worker}{' '}(worker {failedRun.worker}{source.user
        ? `, as ${source.user}`
        : ''}){/if}. The query and the connection stay as they were; fix and run again.
  </div>
{/if}

{#if opened}
  <Field field="mode" label="how rows are found" group>
    {#snippet help()}
      Cheapest first: each one down costs the database and the leader more, so the first that fits
      your data is the one to pick.
    {/snippet}
    <span class="cheapest">cheapest first</span>
    <div class="modes" role="radiogroup">
      {#each MODES as m (m.type)}
        {@const on = source.mode.type === m.type}
        <div class="mode" class:on>
          <label class="mode-head">
            <input
              type="radio"
              name="mode-{source.id}"
              checked={on}
              onchange={() => pickMode(m.type)}
            />
            <span class="mode-name">
              {m.name}
              {#if suggested?.type === m.type}<span class="suggested"
                  >suggested from the result</span
                >{/if}
              <span class="meter" title="cost {m.level} of 4" data-level={m.level}>
                {#each [1, 2, 3, 4] as step (step)}<i class:lit={step <= m.level}></i>{/each}
              </span>
            </span>
            <span class="mode-what">{m.what}</span>
            <span class="mode-cost">{m.cost}</span>
          </label>
          {#if on}
            <div class="mode-body">
              {#if source.mode.type === 'incrementing' || source.mode.type === 'updatedAt'}
                {@const mode = source.mode}
                <Field
                  field="mode.position"
                  label={mode.type === 'incrementing' ? 'incrementing column' : 'updated at column'}
                  help={suggested &&
                  'position' in suggested &&
                  suggested.position[0] === mode.position[0]
                    ? 'Picked for you from the columns the query returned.'
                    : undefined}
                >
                  <select
                    value={mode.position[0] ?? ''}
                    onchange={(e) => (mode.position = positionFor(shown, e.currentTarget.value))}
                  >
                    {#if mode.position[0] === undefined}<option value="">choose a column</option
                      >{/if}
                    {#each orderable as c (c.name)}<option value={c.name}>{c.name}</option>{/each}
                  </select>
                </Field>
                {#if needsTie}
                  {@const tie = tieBreak(shown, mode.position[0])}
                  <Field
                    field="mode.position"
                    label="{mode.position[0]} repeats: tell rows apart by"
                  >
                    {#snippet help()}
                      A batch can end between two rows with the same <code>{mode.position[0]}</code
                      >. This column puts them in a fixed order, so the next poll resumes exactly
                      there. This is needed because {mode.position[0]} can repeat: it has no unique index.
                    {/snippet}
                    <span class="controls">
                      <select
                        value={mode.position[1] ?? ''}
                        onchange={(e) =>
                          (mode.position = [mode.position[0], e.currentTarget.value].filter(
                            Boolean,
                          ))}
                      >
                        <option value="">choose a column</option>
                        {#each shown.filter((c) => c.name !== mode.position[0]) as c (c.name)}
                          <option value={c.name}>{c.name}</option>
                        {/each}
                      </select>
                      {#if mode.position[1] === undefined && tie}
                        <button
                          type="button"
                          class="use"
                          onclick={() => (mode.position = [mode.position[0], tie])}
                          >Use {tie}</button
                        >
                      {/if}
                    </span>
                  </Field>
                {/if}
                {#if positionIsTime}
                  <Field optional field="mode.settleSecs" label="wait for late commits (seconds)">
                    {#snippet help()}
                      A poll reads only rows at least this far behind the database's clock, so a
                      transaction that commits late is still read.
                    {/snippet}
                    <input
                      type="number"
                      min="0"
                      value={mode.settleSecs ?? DEFAULT_SETTLE_SECS}
                      oninput={(e) => {
                        const n = numberOrUnset(e.currentTarget);
                        mode.settleSecs =
                          n === undefined || n === DEFAULT_SETTLE_SECS ? undefined : Math.round(n);
                      }}
                    />
                  </Field>
                {/if}
                <Field field="mode.start" label="start from">
                  {#snippet help()}
                    Where a source with no progress begins: now, the beginning, or a value you give.
                    A source with progress resumes from it.
                    {#if mode.start === 'from'}
                      {positionIsTime
                        ? 'The value is a time with its offset.'
                        : 'The value is an integer.'}
                    {/if}
                  {/snippet}
                  <span class="controls">
                    <select
                      value={mode.start ?? 'now'}
                      onchange={(e) => {
                        const v = e.currentTarget.value as 'now' | 'beginning' | 'from';
                        mode.start = v === 'now' ? undefined : v;
                        if (v !== 'from') mode.startFrom = undefined;
                      }}
                    >
                      <option value="now">now</option>
                      <option value="beginning">the beginning</option>
                      <option value="from">a value</option>
                    </select>
                    {#if mode.start === 'from'}
                      <input
                        type="text"
                        aria-label="start from value"
                        value={mode.startFrom ?? ''}
                        placeholder={positionIsTime ? '2026-10-01T00:00:00Z' : '0'}
                        oninput={(e) => {
                          const v = e.currentTarget.value.trim();
                          mode.startFrom =
                            v === ''
                              ? undefined
                              : !positionIsTime &&
                                  /^-?\d+$/.test(v) &&
                                  Number.isSafeInteger(Number(v))
                                ? Number(v)
                                : v;
                        }}
                      />
                    {/if}
                  </span>
                </Field>
              {:else if source.mode.type === 'comparing'}
                {@const mode = source.mode}
                <Field field="mode.key" label="key">
                  {#snippet help()}Tells rows apart: a column no two rows share.{/snippet}
                  <select
                    value={mode.key[0] ?? ''}
                    onchange={(e) => (mode.key = [e.currentTarget.value])}
                  >
                    {#if mode.key[0] === undefined}<option value="">choose a column</option>{/if}
                    {#each shown as c (c.name)}<option value={c.name}>{c.name}</option>{/each}
                  </select>
                </Field>
              {/if}
              <Field optional field="pollIntervalSecs" label="poll every (seconds)">
                <input
                  type="number"
                  min={LIMITS.pollIntervalSecs.min}
                  value={source.pollIntervalSecs ?? LIMITS.pollIntervalSecs.default}
                  oninput={(e) => {
                    const n = numberOrUnset(e.currentTarget);
                    source.pollIntervalSecs =
                      n === undefined || n === LIMITS.pollIntervalSecs.default
                        ? undefined
                        : Math.max(LIMITS.pollIntervalSecs.min, Math.round(n));
                  }}
                />
              </Field>
            </div>
          {/if}
        </div>
      {/each}
    </div>
  </Field>

  <Advanced label="Advanced" modified={source.batch !== undefined}>
    <Field optional field="batch" label="batch">
      {#snippet help()}
        The most rows one poll reads by a position, and the most a poll of every row may return.
      {/snippet}
      <input
        type="number"
        min="1"
        max="100000"
        value={source.batch ?? 5000}
        oninput={(e) => {
          const n = numberOrUnset(e.currentTarget);
          source.batch = n === undefined || n === 5000 ? undefined : Math.round(n);
        }}
      />
    </Field>
  </Advanced>

  <Field field="fields" label="columns, as fields of the record" group>
    {#snippet help()}Names and types come from the database. ▾ changes a type or turns a column off.{/snippet}
    <div class="table">
      <table>
        <thead>
          <tr>
            {#each shown as c (c.name)}
              {@const type = typeOf(c.name)}
              {@const role = roleOf(source.mode, c.name)}
              <th class:off={type === undefined}>
                <span class="name">{c.name}</span>
                {#if c.databaseType}<span class="dbtype"
                    >{c.databaseType}{joined && c.table ? ` · ${c.table}` : ''}</span
                  >{/if}
                <span class="chips">
                  <select
                    class="chip"
                    aria-label="{c.name} type"
                    value={type ?? 'off'}
                    onchange={(e) => setType(c.name, e.currentTarget.value)}
                  >
                    {#each FIELD_TYPES as t (t)}<option value={t}>{t}</option>{/each}
                    <option value="off">off</option>
                  </select>
                  {#if role}<span class="role">{role}</span>{/if}
                </span>
              </th>
            {/each}
          </tr>
        </thead>
        {#if ran && columns.length > 0}
          <tbody>
            {#each ran.rows.slice(0, SHOWN_ROWS) as row, r (r)}
              <tr>
                {#each row as value, i (i)}
                  <td class:off={typeOf(shown[i].name) === undefined} class:null={value === null}
                    >{value === null || value === undefined ? 'null' : cellText(value)}</td
                  >
                {/each}
              </tr>
            {/each}
          </tbody>
        {/if}
      </table>
    </div>
  </Field>

  <Field label="what smeltr runs instead, on Run and on every poll" group>
    {@render statement()}
  </Field>

  {#if positional}
    <div class="run">
      <button type="button" onclick={() => void startRun()} disabled={running}>Run again</button>
      <span class="dim">
        {checked ? "ran: the plan below is this poll's" : "checks this poll's plan; not run yet"}
      </span>
    </div>
    {#if !checked || !ran?.verdict}
      <p class="banner">
        This poll is not checked yet. Run again checks its plan and shows what it would read.
      </p>
    {:else if ran.verdict.type === 'indexed'}
      <p class="banner good">
        Each poll reads through {ran.verdict.indexes.length === 1 ? 'the index' : 'the indexes'}
        {#each ran.verdict.indexes as index, i (index)}{i > 0 ? ', ' : ''}<code>{index}</code
          >{/each}
        and reads only the rows past the last one sent.
      </p>
    {:else}
      <p class="banner warn">
        <strong>Each poll scans all of <code>{ran.verdict.relation}</code></strong>, about
        {ran.verdict.rows.toLocaleString()} rows. Index the position, or filter inside your query with
        <code>:last.{positionColumn}</code>.
      </p>
    {/if}
  {/if}

  {#if firstRecord}
    <Field label="the pipeline sees, for the first row" group>
      <pre class="statement">{@html highlight(JSON.stringify(firstRecord, null, 2))}</pre>
    </Field>
  {/if}

  {#if ran && run.session}
    <div class="run">
      <button type="button" onclick={() => void saveSample()} disabled={saving}
        >Save rows as sample</button
      >
      <span class="dim">Keeps Run's {ran.rows.length} rows, to test the pipeline with.</span>
    </div>
  {:else if sampled}
    <p class="dim">Run's rows are saved as the sample <code>{sampled}</code>.</p>
  {/if}
{/if}

<style>
  .pair {
    display: flex;
    gap: 0.8rem;
    align-items: flex-start;
  }

  .pair > :global(*) {
    flex: 1;
  }

  input,
  select,
  .statement,
  td,
  .name {
    font-family: var(--mono);
  }

  .statement {
    margin: 0;
    padding: 0.5rem 0.65rem;
    background: var(--field);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    font-size: var(--text-note);
    overflow-x: auto;
    white-space: pre;
  }

  .statement-box {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }

  .statement :global(.tok-keyword) {
    color: var(--tok-keyword);
  }
  .statement :global(.tok-string) {
    color: var(--tok-string);
  }
  .statement :global(.tok-number) {
    color: var(--tok-number);
  }
  .statement :global(.tok-literal) {
    color: var(--tok-literal);
  }
  .statement :global(.tok-comment) {
    color: var(--tok-comment);
  }
  .statement :global(.tok-key) {
    color: var(--tok-key);
  }

  .bound {
    margin: 0;
    font-size: var(--text-note);
    color: var(--dim);
  }

  details summary {
    cursor: pointer;
    font-size: var(--text-note);
    font-weight: 600;
  }

  details .statement {
    margin-top: 0.35rem;
  }

  .run {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    flex-wrap: wrap;
    font-size: var(--text-note);
  }

  .dim {
    margin: 0;
    font-size: var(--text-note);
    color: var(--dim);
  }

  .banner {
    margin: 0;
    padding: 0.5rem 0.75rem;
    border-left: 3px solid var(--border);
    border-radius: var(--radius);
    background: var(--field);
    font-size: var(--text-label);
  }

  .banner.good {
    border-left-color: var(--added);
    background: color-mix(in srgb, var(--added) 10%, var(--field));
  }

  .banner.warn {
    border-left-color: var(--warn);
    background: color-mix(in srgb, var(--warn) 14%, var(--field));
  }

  .banner.bad {
    border-left-color: var(--error);
    background: color-mix(in srgb, var(--error) 10%, var(--field));
  }

  .cheapest {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  .modes {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--field);
  }

  .mode + .mode {
    border-top: 1px solid var(--border);
  }

  .mode-head {
    display: grid;
    grid-template-columns: 1rem minmax(10rem, 15rem) 1fr;
    gap: 0.15rem 0.6rem;
    align-items: baseline;
    padding: 0.5rem 0.65rem;
    cursor: pointer;
  }

  .mode-name {
    grid-row: span 2;
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    font-weight: 600;
    font-size: var(--text-label);
  }

  .suggested {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  .meter {
    display: flex;
    gap: 2px;
  }

  .meter i {
    width: 0.5rem;
    height: 0.4rem;
    background: var(--border);
  }

  .meter[data-level='1'] i.lit {
    background: var(--added);
  }
  .meter[data-level='2'] i.lit,
  .meter[data-level='3'] i.lit {
    background: var(--warn);
  }
  .meter[data-level='4'] i.lit {
    background: var(--error);
  }

  .mode-what {
    font-size: var(--text-note);
  }

  .mode-cost {
    grid-column: 3;
    font-size: var(--text-note);
    color: var(--dim);
  }

  .mode-body {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr));
    gap: 0.3rem 0.8rem;
    align-items: start;
    padding: 0.6rem 0.65rem 0.75rem 2.25rem;
    background: var(--panel);
    border-top: 1px solid var(--border);
  }

  /* Each field takes three rows of the body's grid, its name, its control and its note, so names that wrap to two
     lines do not push one field's control below its neighbours'. */
  .mode-body > :global(.field) {
    grid-row: span 3;
    grid-template-rows: subgrid;
  }

  .mode-body > :global(.field > label) {
    display: contents;
  }

  .mode-body > :global(.field .name) {
    align-self: end;
  }

  .controls {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }

  .use {
    align-self: start;
    font-size: var(--text-note);
  }

  @media (max-width: 40rem) {
    .mode-head {
      grid-template-columns: 1rem 1fr;
    }

    .mode-name {
      grid-row: auto;
    }

    .mode-what,
    .mode-cost {
      grid-column: 2;
    }
  }

  .table {
    overflow-x: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--field);
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-note);
    font-variant-numeric: tabular-nums;
  }

  th,
  td {
    text-align: left;
    padding: 0.35rem 0.6rem;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
    vertical-align: top;
  }

  tr:last-child td {
    border-bottom: 0;
  }

  th {
    font-weight: 400;
  }

  th .name {
    display: block;
    font-weight: 500;
  }

  th .dbtype {
    display: block;
    color: var(--dim);
    font-size: var(--text-micro);
  }

  .chips {
    display: flex;
    gap: 0.4rem;
    align-items: center;
    margin-top: 0.2rem;
  }

  .chip {
    padding: 0 0.2rem;
    font-size: var(--text-micro);
    width: auto;
  }

  th .role {
    font-size: var(--text-micro);
    font-weight: 700;
    color: var(--accent);
  }

  th.off .name,
  td.off {
    color: var(--dim);
    text-decoration: line-through;
  }

  td.null {
    color: var(--dim);
    font-style: italic;
  }
</style>
