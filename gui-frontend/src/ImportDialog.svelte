<!--
  Importing an export into this area. Choosing the file lists what it holds, each object new, the same as one
  here, or taken: its id holds something else here. A taken id is answered by keeping one side or renaming the
  imported object, and each secret and certificate the file names is matched to one here. Import saves once every
  one has an answer, against the commit the list was read at.
-->
<script lang="ts">
  import Button, { BUSY } from './Button.svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import {
    importExport,
    listCertificates,
    listSecrets,
    type ImportAnswers,
    type ImportCredential,
    type ImportListed,
    type ImportPart,
  } from './api';
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

  let file = $state<File | null>(null);
  let listed = $state<ImportListed[]>([]);
  let credentials = $state<ImportCredential[]>([]);
  let at = $state<string | undefined>(undefined);
  let busy = $state(false);
  /** Each taken id's answer, by `key`: a side, or `renamed` with the new id in `renames`. */
  let keeps = $state<Record<string, '' | 'imported' | 'here' | 'renamed'>>({});
  let renames = $state<Record<string, string>>({});
  /** The secret or certificate here each one the file names is matched to, by `credKey`. */
  let matches = $state<Record<string, string>>({});
  let secretsHere = $state<string[]>([]);
  let certificatesHere = $state<string[]>([]);

  const key = (l: ImportListed) => `${l.part}\u0000${l.id}`;
  const credKey = (c: ImportCredential) => `${c.certificate}\u0000${c.name}`;
  let taken = $derived(listed.filter((l) => l.standing === 'taken'));

  $effect(() => {
    if (open) reset();
  });

  function reset() {
    file = null;
    listed = [];
    credentials = [];
    at = undefined;
    keeps = {};
    renames = {};
    matches = {};
  }

  async function choose(input: HTMLInputElement) {
    const chosen = input.files?.[0];
    if (!chosen) return;
    reset();
    file = chosen;
    busy = true;
    try {
      const [res, secrets, certificates] = await Promise.all([
        importExport(chosen),
        listSecrets(),
        listCertificates(),
      ]);
      secretsHere = secrets.map((s) => s.name);
      certificatesHere = certificates.map((c) => c.name);
      if (!res.ok || file !== chosen) return;
      listed = res.listed ?? [];
      credentials = res.credentials ?? [];
      at = res.at;
      // A name this leader already lists is chosen as the match.
      for (const c of credentials) {
        const here = c.certificate ? certificatesHere : secretsHere;
        matches[credKey(c)] = here.includes(c.name) ? c.name : '';
      }
      for (const l of listed.filter((l) => l.standing === 'taken')) keeps[key(l)] = '';
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }

  /** Why Import is disabled, or '' when every taken id and credential has an answer. */
  let unanswered = $derived.by(() => {
    if (!file || listed.length === 0) return 'Choose an export file first.';
    for (const l of taken) {
      const keep = keeps[key(l)];
      if (!keep) return 'Choose what to keep for every taken id first.';
      if (keep === 'renamed' && !renames[key(l)]?.trim())
        return 'Give every renamed object a new id first.';
    }
    if (credentials.some((c) => !matches[credKey(c)]))
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
        credentials.filter((c) => !c.certificate).map((c) => [c.name, matches[credKey(c)]]),
      ),
      certificates: Object.fromEntries(
        credentials.filter((c) => c.certificate).map((c) => [c.name, matches[credKey(c)]]),
      ),
    };
    busy = true;
    try {
      const res = await importExport(file, answers, at);
      if (res.ok) onimported();
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }
</script>

<Dialog {open} title="import pipeline" wide {oncancel}>
  <div class="body">
    <Field label="Export file" required>
      <input type="file" accept=".zip" disabled={busy} onchange={(e) => choose(e.currentTarget)} />
    </Field>
    {#if busy && listed.length === 0 && file}
      <p class="dim">Reading…</p>
    {/if}
    {#if listed.length > 0}
      <table>
        <thead>
          <tr><th>Kind</th><th>Id</th><th></th></tr>
        </thead>
        <tbody>
          {#each listed as l (key(l))}
            <tr>
              <td>{PARTS[l.part]}</td>
              <td class="mono">{l.id}</td>
              <td class:dim={l.standing === 'same'} class:taken={l.standing === 'taken'}
                >{STANDINGS[l.standing]}</td
              >
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
    {#if taken.length > 0}
      <h3>Taken ids</h3>
      {#each taken as l (key(l))}
        <section class="taken-id" aria-label="{PARTS[l.part]} {l.id}">
          <h4>{PARTS[l.part]} {l.id}</h4>
          <div class="versions">
            {#each [['Imported', l.imported], ['Here', l.here]] as [label, text] (label)}
              <div role="group" aria-label={label}>
                <h5>{label}</h5>
                <pre>{text}</pre>
              </div>
            {/each}
          </div>
          <div class="answer">
            <Field label="Keep">
              <select bind:value={keeps[key(l)]} disabled={busy}>
                <option value="" disabled>Choose</option>
                <option value="imported">Imported</option>
                <option value="here">Here</option>
                <option value="renamed">Both, rename imported</option>
              </select>
            </Field>
            {#if keeps[key(l)] === 'renamed'}
              <Field label="New id" required>
                <input class="mono" bind:value={renames[key(l)]} disabled={busy} />
              </Field>
            {/if}
          </div>
        </section>
      {/each}
    {/if}
    {#if credentials.length > 0}
      <h3>Secrets and certificates</h3>
      <table>
        <thead>
          <tr><th>In the file</th><th>Named by</th><th>Here</th></tr>
        </thead>
        <tbody>
          {#each credentials as c (credKey(c))}
            <tr>
              <td class="mono">{c.name}</td>
              <td>{c.namedBy.join(', ')}</td>
              <td>
                <Field label="Match for {c.name}" bare>
                  <select bind:value={matches[credKey(c)]} disabled={busy}>
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
    <Button type="button" class="primary" disabled={busy ? BUSY : unanswered} onclick={save}
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
  h5 {
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
