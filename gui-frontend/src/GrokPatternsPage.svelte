<!--
  Grok patterns page: `grok-patterns.json`, read through `GET /api/config`
  and written with `PUT /api/grok-patterns`, plus the raw-JSON page. A
  pattern is declared once for the bundle and named as `%{NAME}` from any
  grok in its group, the way a lookup table is named by id.

  One table, one row a pattern, searched as one: the group's own, editable;
  `shared/`'s, which a group's grok may name too and which are changed in
  `shared/`; and the built-ins every grok may name, which no one changes.
-->
<script lang="ts">
  import Connections from './Connections.svelte';
  import EditInShared from './EditInShared.svelte';
  import OnlyNotice from './OnlyNotice.svelte';
  import { Narrowing } from './narrowing.svelte';
  import { untrack } from 'svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import SaveBar from './SaveBar.svelte';
  import ValidationItem from './ValidationItem.svelte';
  import { provideValidation } from './validation.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import HeldEdit from './HeldEdit.svelte';
  import { SectionEditor } from './sectionEditor.svelte';
  import Field from './Field.svelte';
  import ValidatedField from './ValidatedField.svelte';
  import JsonEditPage from './JsonEditPage.svelte';
  import { resourcesTabs } from './nav';
  import { sharedConfig, putGrokPatterns, type GrokPatternDto } from './api';
  import TrashIcon from './TrashIcon.svelte';
  import { count } from './time';
  import {
    builtinRegex,
    GROK_BUILTIN_PATTERNS,
    grokNameProblem,
    sharedGrokSuggestions,
  } from './grokPatterns';
  import { editable } from './access';

  let { showJson = false, only }: { showJson?: boolean; only?: string[] } = $props();

  const readOnly = !editable();

  let patterns: GrokPatternDto[] = $state([]);
  /** What `shared/` declares, which this group's grok may name as well. Never saved from here. */
  let sharedPatterns: GrokPatternDto[] = $state([]);
  const validation = provideValidation('grokPatterns');
  const editor = new SectionEditor<GrokPatternDto[]>('grok', {
    get: () => patterns,
    set: (value) => (patterns = value),
    pick: (config) => config.grokPatterns ?? [],
    put: putGrokPatterns,
    what: 'grok patterns',
    validation,
    onLoaded: loadShared,
  });

  async function loadShared() {
    sharedPatterns = (await sharedConfig())?.grokPatterns ?? [];
  }

  void editor.load();

  /** Every name `patterns[n]` may not take: the others of this scope, and `shared/`'s. */
  function taken(n: number): string[] {
    return [...patterns.filter((_, i) => i !== n), ...sharedPatterns].map((p) => p.id);
  }

  /** Every pattern but `patterns[n]`, for its autocomplete: a pattern may name another. */
  function others(n: number): GrokPatternDto[] {
    return [...sharedPatterns, ...patterns.filter((_, i) => i !== n)];
  }

  function add() {
    q = '';
    narrowing.widen();
    patterns.push({ id: '', pattern: '' });
  }

  const builtins = GROK_BUILTIN_PATTERNS.map((b) => ({
    id: b.name,
    pattern: builtinRegex(b),
    about: b.description,
  }));

  let q = $state('');
  /** The row last focused, whose Connections show under it: one per page, as one is edited at a time. */
  let focused = $state<GrokPatternDto | null>(null);
  const narrowing = new Narrowing(() => only);
  /** Whether a row is shown under the search, and among `only` where the list is narrowed: its name or its
   * definition holds it. */
  function shown(id: string, pattern: string | null): boolean {
    if (!narrowing.keeps(id)) return false;
    const needle = q.trim().toLowerCase();
    return (
      !needle || id.toLowerCase().includes(needle) || !!pattern?.toLowerCase().includes(needle)
    );
  }
  /** Which of the group's own rows the search shows, decided when the search changes and not as a row is edited,
   * so a row being typed in stays put. The rest stay mounted, hidden, so a save error on one can still open it. */
  let ownShown = $derived.by(() => {
    void q;
    void narrowing.ids;
    void patterns.length;
    return untrack(() => patterns.map((p) => shown(p.id, p.pattern)));
  });
  let total = $derived(patterns.length + sharedPatterns.length + builtins.length);
</script>

{#if editor.loading}
  <SectionShell><p class="dim">Loading…</p></SectionShell>
{:else if editor.loadFailed}
  <SectionShell />
{:else if showJson}
  <JsonEditPage
    title="Edit JSON — Grok patterns"
    initialText={JSON.stringify(patterns, null, 2)}
    backHash="#/build/grok"
    onSave={(text) => editor.saveFromJson(text)}
  />
{:else}
  <SectionShell>
    <PageHeader title="Grok patterns">
      {#snippet help()}
        A named pattern any grok step in this group can use as <code>%{'{'}NAME{'}'}</code>.
      {/snippet}
    </PageHeader>
    <SecondaryTabs tabs={resourcesTabs()} active="#/build/grok" ariaLabel="Resources" />

    <div>
      <UnreadableFiles problems={editor.bundleProblems} onchanged={() => editor.load()} />

      {#if editor.moved}
        <HeldEdit
          what="grok patterns"
          rows={editor.moved.rows}
          onrestore={() => editor.restoreHeld()}
          ondiscard={() => editor.dropHeld()}
        />
      {/if}

      {#if editor.conflict}
        <SaveConflictPanel
          conflict={editor.conflict}
          onDiscard={() => location.reload()}
          onRetry={() => editor.save()}
        />
      {/if}

      <OnlyNotice {narrowing} ids={[...patterns, ...sharedPatterns].map((p) => p.id)} />

      <div class="toolbar">
        <input
          type="search"
          placeholder="Search {count(total, 'pattern')}"
          aria-label="Search patterns"
          bind:value={q}
        />
        <span class="grow"></span>
        {#if !readOnly}
          <button type="button" class="primary" onclick={add}>+ Add pattern</button>
          <button type="button" onclick={() => (location.hash = '#/build/grok/json')}>
            Edit JSON&hellip;
          </button>
        {/if}
      </div>

      <table>
        <thead>
          <tr>
            <th class="name">Name</th>
            <th>Pattern</th>
            <th class="act"><span class="hidden">Remove</span></th>
          </tr>
        </thead>
        <tbody>
          {#if sharedPatterns.length > 0}
            <tr class="band"
              ><th colspan="3">from shared/ &mdash; every group may name these</th></tr
            >
            {#each sharedPatterns as p (p.id)}
              {#if shown(p.id, p.pattern)}
                <tr class="fixed fromshared">
                  <td class="name mono">{p.id}</td>
                  <td class="mono">{p.pattern}</td>
                  <td class="act">
                    <EditInShared hash="#/build/grok" />
                  </td>
                </tr>
              {/if}
            {/each}
            <tr class="band"><th colspan="3">this group</th></tr>
          {/if}

          {#each patterns as p, n (n)}
            <ValidationItem
              id={p.id}
              open={() => {
                q = '';
                narrowing.widen();
              }}
            >
              <tr hidden={ownShown[n] === false} onfocusin={() => (focused = p)}>
                <td class="name">
                  <Field
                    required
                    bare
                    field="id"
                    label="name"
                    error={grokNameProblem(p.id, taken(n))}
                  >
                    <input type="text" placeholder="NAME" bind:value={p.id} />
                  </Field>
                </td>
                <td>
                  <Field required bare field="pattern" label="pattern">
                    <ValidatedField
                      ariaLabel={(p.id || 'unnamed') + ' pattern'}
                      placeholder={'%{WORD:vendor},%{GREEDYDATA:rest}'}
                      bind:value={p.pattern}
                      autocomplete="grok"
                      grokNamed={sharedGrokSuggestions(others(n))}
                      wrap
                    />
                  </Field>
                </td>
                <td class="act">
                  {#if !readOnly}
                    <button
                      type="button"
                      class="icon"
                      aria-label="Remove {p.id || 'unnamed'}"
                      onclick={() => patterns.splice(n, 1)}><TrashIcon /></button
                    >
                  {/if}
                </td>
              </tr>
              {#if ownShown[n] !== false && p.id && (focused === p || (narrowing.ids?.length === 1 && narrowing.ids[0] === p.id))}
                <tr class="connections">
                  <td colspan="3"><Connections kind="grokPattern" id={p.id} /></td>
                </tr>
              {/if}
            </ValidationItem>
          {/each}
          {#if patterns.length === 0}
            <tr><td colspan="3" class="dim">No grok patterns of this group's own yet.</td></tr>
          {/if}

          {#if !narrowing.ids}<tr class="band"><th colspan="3">built-in</th></tr>{/if}
          {#each builtins as b (b.id)}
            {#if shown(b.id, b.pattern)}
              <tr class="fixed" title={b.about}>
                <td class="name mono">{b.id}</td>
                {#if b.pattern}
                  <td class="mono">{b.pattern}</td>
                {:else}
                  <td class="dim">{b.about}</td>
                {/if}
                <td class="act"></td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
      <SaveBar
        dirty={editor.isDirty}
        saving={editor.saving}
        onsave={() => void editor.save()}
        ondiscard={() => editor.discard()}
      />
    </div>
  </SectionShell>
{/if}

<style>
  table {
    width: 100%;
    border-collapse: collapse;
  }

  th,
  td {
    padding: 0.25rem 0.5rem;
    border-bottom: 1px solid var(--border);
    text-align: left;
    vertical-align: middle;
  }

  thead th,
  .band th {
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }

  .band th {
    padding-top: 1rem;
  }

  .name {
    width: 14rem;
  }

  .act {
    width: 1%;
    white-space: nowrap;
    text-align: right;
  }

  /* A row no one edits here reads as text in the field's own type, not as a control. */
  .fixed td {
    padding: 0.35rem 0.5rem;
    font-size: var(--text-note);
    overflow-wrap: anywhere;
  }

  .fixed .name {
    font-weight: 600;
  }

  /* `shared/`'s own: shown, because a group's grok may name them, and not editable here. */
  .fromshared td:first-child {
    border-left: 3px solid oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  .fromshared td {
    color: var(--dim);
  }

  input {
    width: 100%;
  }

  .toolbar {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin: 1.2rem 0;
  }

  .toolbar input {
    width: 16rem;
  }

  .grow {
    flex: 1;
  }

  /* A bare glyph that turns red only on hover. */
  .icon {
    display: inline-grid;
    padding: 0;
    border-color: transparent;
    background: none;
    color: var(--dim);
  }

  .icon:hover {
    color: var(--error);
  }

  /* Names a column whose head would otherwise be an empty cell. */
  .hidden {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }

  .mono {
    font-family: var(--mono);
  }

  .dim {
    color: var(--dim);
  }
</style>
