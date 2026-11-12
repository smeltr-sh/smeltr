<!--
  Importing an export into shared or a group, which the importer chooses: the leader the export came from may not
  have the same groups. Choosing both lists what the file holds, each object new, the same as one here, or taken:
  its id holds something else here. A taken id is answered by keeping one side or renaming the
  imported object, and each secret and certificate the file names is matched to one here. Import saves once every
  one has an answer, against the commit the list was read at.
-->
<script lang="ts">
  import { Busy } from './busy.svelte';
  import Button, { BUSY } from './Button.svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import {
    SHARED_SCOPE,
    importExport,
    listCertificates,
    listSecrets,
    listWorkerGroups,
    type WorkerGroupDto,
    type ImportAnswers,
    type ImportCredential,
    type ImportListed,
    type ImportPart,
  } from './api';
  import { diffObjects, formatDiffValue } from './diff';
  import { reported } from './errors.svelte';

  let {
    open,
    oncancel,
    onimported,
  }: { open: boolean; oncancel: () => void; onimported: () => void } = $props();

  const PARTS: Record<ImportPart, string> = {
    pipeline: 'Pipeline',
    lookupTable: 'Lookup table',
    grokPattern: 'Grok pattern',
    schema: 'Schema',
    sample: 'Sample',
    test: 'Test',
    route: 'Route',
    source: 'Source',
    destination: 'Destination',
  };
  const STANDINGS = { new: 'New', same: 'Same as here', taken: 'Taken' };

  /** Shared or a group's id; '' until one is chosen. */
  let into = $state('');
  let groups = $state<WorkerGroupDto[]>([]);
  let file = $state<File | null>(null);
  let listed = $state<ImportListed[]>([]);
  let credentials = $state<ImportCredential[]>([]);
  let at = $state<string | undefined>(undefined);
  const busy = new Busy();
  /** Each taken id's answer, by `key`: a side, or `renamed` with the new id in `renames`. */
  let keeps = $state<Record<string, '' | 'imported' | 'here' | 'renamed'>>({});
  let renames = $state<Record<string, string>>({});
  /** The secret or certificate here each one the file names is matched to, by `credKey`. */
  let matches = $state<Record<string, string>>({});
  /** The objects not to import, by `key`. Every object is imported until it is unticked. */
  let leftOut = $state<Record<string, boolean>>({});
  let secretsHere = $state<string[]>([]);
  let certificatesHere = $state<string[]>([]);

  /** Each top-level field a taken id's two versions differ in, from the one here to the imported one. */
  function differs(l: ImportListed) {
    const parsed = (text: string | undefined) =>
      text === undefined ? undefined : JSON.parse(text);
    return diffObjects(parsed(l.here), parsed(l.imported)).filter((e) => e.kind !== 'unchanged');
  }

  const key = (l: ImportListed) => `${l.part}\u0000${l.id}`;
  const credKey = (c: ImportCredential) => `${c.certificate}\u0000${c.name}`;
  let kept = $derived(listed.filter((l) => !leftOut[key(l)]));
  let taken = $derived(kept.filter((l) => l.standing === 'taken'));
  /** The secrets and certificates a source or destination being imported names. */
  let needed = $derived.by(() => {
    const by = new Set(kept.map((l) => `${PARTS[l.part]} ${l.id}`));
    return credentials.filter((c) => c.namedBy.some((n) => by.has(n)));
  });

  $effect(() => {
    if (!open) return;
    into = '';
    file = null;
    reset();
    listWorkerGroups()
      .then((res) => (groups = res.ok ? res.groups : []))
      .catch(reported);
  });

  function reset() {
    listed = [];
    credentials = [];
    at = undefined;
    keeps = {};
    renames = {};
    matches = {};
    leftOut = {};
  }

  function choose(input: HTMLInputElement) {
    file = input.files?.[0] ?? null;
    list();
  }

  /** Lists the file against the scope chosen, once both are; each new choice lists it again. */
  async function list() {
    reset();
    const [chosen, scope] = [file, into];
    if (!chosen || !scope) return;
    await busy.run(async () => {
      const [res, secrets, certificates] = await Promise.all([
        importExport(chosen, scope),
        listSecrets(),
        listCertificates(),
      ]);
      secretsHere = secrets.map((s) => s.name);
      certificatesHere = certificates.map((c) => c.name);
      if (!res.ok || file !== chosen || into !== scope) return;
      listed = res.listed ?? [];
      credentials = res.credentials ?? [];
      at = res.at;
      // A name this leader already lists is chosen as the match.
      for (const c of credentials) {
        const here = c.certificate ? certificatesHere : secretsHere;
        matches[credKey(c)] = here.includes(c.name) ? c.name : '';
      }
      for (const l of listed.filter((l) => l.standing === 'taken')) keeps[key(l)] = '';
    });
  }

  /** Why Import is disabled, or '' when every taken id and credential has an answer. */
  let unanswered = $derived.by(() => {
    if (!into) return 'Choose where to import into first.';
    if (!file || listed.length === 0) return 'Choose an export file first.';
    if (kept.length === 0) return 'Tick something to import first.';
    for (const l of taken) {
      const keep = keeps[key(l)];
      if (!keep) return 'Choose what to keep for every taken id first.';
      if (keep === 'renamed' && !renames[key(l)]?.trim())
        return 'Give every renamed object a new id first.';
    }
    if (needed.some((c) => !matches[credKey(c)]))
      return 'Match every secret and certificate to one here first.';
    return '';
  });

  async function save() {
    if (!file) return;
    const answers: ImportAnswers = {
      taken: taken.map((l) => {
        const keep = keeps[key(l)];
        const base = { part: l.part, id: l.id };
        return keep === 'renamed'
          ? { ...base, keep, as: renames[key(l)].trim() }
          : { ...base, keep: keep as 'imported' | 'here' };
      }),
      secrets: Object.fromEntries(
        needed.filter((c) => !c.certificate).map((c) => [c.name, matches[credKey(c)]]),
      ),
      certificates: Object.fromEntries(
        needed.filter((c) => c.certificate).map((c) => [c.name, matches[credKey(c)]]),
      ),
      leftOut: listed.filter((l) => leftOut[key(l)]).map((l) => ({ part: l.part, id: l.id })),
    };
    const chosen = file;
    await busy.run(async () => {
      const res = await importExport(chosen, into, answers, at);
      if (res.ok) onimported();
    });
  }
</script>

<Dialog {open} title="import pipeline" wide {oncancel}>
  <div class="body">
    <Field label="Import into" required>
      <select bind:value={into} disabled={busy.on} onchange={list}>
        <option value="" disabled>Choose</option>
        <option value={SHARED_SCOPE}>Shared</option>
        {#each groups as g (g.id)}
          <option value={g.id}>Worker group {g.id}</option>
        {/each}
      </select>
    </Field>
    <Field label="Export file" required>
      <input
        type="file"
        accept=".zip"
        disabled={busy.on}
        onchange={(e) => choose(e.currentTarget)}
      />
    </Field>
    {#if busy.on && listed.length === 0 && file}
      <p class="dim">Reading…</p>
    {/if}
    {#if listed.length > 0}
      <table>
        <thead>
          <tr><th>Kind</th><th>Name</th><th></th><th>Import</th></tr>
        </thead>
        <tbody>
          {#each listed as l (key(l))}
            <tr>
              <td>{PARTS[l.part]}</td>
              <td class="mono">{l.name ?? l.id}</td>
              <td class:dim={l.standing === 'same'} class:taken={l.standing === 'taken'}
                >{STANDINGS[l.standing]}</td
              >
              <td>
                <input
                  type="checkbox"
                  aria-label="Import {PARTS[l.part]} {l.name ?? l.id}"
                  checked={!leftOut[key(l)]}
                  disabled={busy.on}
                  onchange={(e) => (leftOut[key(l)] = !e.currentTarget.checked)}
                />
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
    {#if taken.length > 0}
      <h3>Taken ids</h3>
      {#each taken as l, i (key(l))}
        <section class="taken-id" aria-label="{PARTS[l.part]} {l.id}">
          <h4>{PARTS[l.part]} {l.id}</h4>
          {#if l.inShared}
            <p class="dim">
              Shared holds this one, so an import into a group can only choose Here, or Both, rename
              imported.
            </p>
          {/if}
          <h5 id="differs-{i}">What differs, here → imported</h5>
          <ul class="differs" aria-labelledby="differs-{i}">
            {#each differs(l) as entry (entry.key)}
              <li class="mono">
                <span class="diff-key">{entry.key}:</span>
                {#if entry.kind !== 'added'}
                  <span class="removed">{formatDiffValue(entry.before)}</span>
                {/if}
                {#if entry.kind === 'changed'}
                  <span class="dim">&rarr;</span>
                {/if}
                {#if entry.kind !== 'removed'}
                  <span class="added">{formatDiffValue(entry.after)}</span>
                {/if}
              </li>
            {:else}
              <li class="dim">Only the contents of the lookup file differ.</li>
            {/each}
          </ul>
          <details>
            <summary>Both versions</summary>
            <div class="versions">
              {#each [['Here', l.here], ['Imported', l.imported]] as [label, text] (label)}
                <div role="group" aria-label={label}>
                  <h5>{label}</h5>
                  <pre>{text}</pre>
                </div>
              {/each}
            </div>
          </details>
          <div class="answer">
            <Field label="Keep">
              <select bind:value={keeps[key(l)]} disabled={busy.on}>
                <option value="" disabled>Choose</option>
                {#if !l.inShared}
                  <option value="imported">Imported</option>
                {/if}
                <option value="here">Here</option>
                <option value="renamed">Both, rename imported</option>
              </select>
            </Field>
            {#if keeps[key(l)] === 'renamed'}
              <Field label="New id" required>
                <input class="mono" bind:value={renames[key(l)]} disabled={busy.on} />
              </Field>
            {/if}
          </div>
        </section>
      {/each}
    {/if}
    {#if needed.length > 0}
      <h3>Secrets and certificates</h3>
      <table>
        <thead>
          <tr><th>In the file</th><th>Named by</th><th>Here</th></tr>
        </thead>
        <tbody>
          {#each needed as c (credKey(c))}
            <tr>
              <td class="mono">{c.name}</td>
              <td>{c.namedBy.join(', ')}</td>
              <td>
                <Field label="Match for {c.name}" bare>
                  <select bind:value={matches[credKey(c)]} disabled={busy.on}>
                    <option value="" disabled
                      >{c.certificate ? 'Choose a certificate' : 'Choose a secret'}</option
                    >
                    {#each c.certificate ? certificatesHere : secretsHere as name (name)}
                      <option value={name}>{name}</option>
                    {/each}
                  </select>
                </Field>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
  {#snippet actions()}
    <Button type="button" onclick={oncancel}>Cancel</Button>
    <Button type="button" class="primary" disabled={busy.on ? BUSY : unanswered} onclick={save}
      >Import</Button
    >
  {/snippet}
</Dialog>

<style>
  .body {
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }
  .mono,
  pre {
    font-family: var(--mono);
  }
  .taken {
    color: var(--warn);
    font-weight: 600;
  }
  h3,
  h4,
  h5,
  .taken-id p {
    margin: 0;
  }
  .taken-id {
    display: grid;
    gap: 0.6rem;
  }
  .taken-id + .taken-id {
    border-top: 1px solid var(--border);
    padding-top: 0.9rem;
  }
  .differs {
    margin: 0;
    padding: 0;
    list-style: none;
    overflow-wrap: anywhere;
  }
  .diff-key {
    color: var(--dim);
    margin-right: 0.4rem;
  }
  .added {
    color: var(--added);
  }
  .removed {
    color: var(--removed);
  }
  summary {
    cursor: pointer;
  }
  .versions {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.75rem;
  }
  pre {
    margin: 0;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    max-height: 20rem;
    overflow: auto;
  }
  .answer {
    display: flex;
    gap: 1rem;
    align-items: flex-end;
  }
  th {
    text-align: left;
  }
  @media (max-width: 800px) {
    .versions {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
