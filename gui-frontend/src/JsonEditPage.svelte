<!--
  The raw-JSON page every config section gets: every page is sugar over a
  JSON file, which stays directly editable and validated on save. Parsing
  and saving are the caller's (`onSave`).
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import { editable } from './access';
  import PageHeader from './PageHeader.svelte';

  let {
    title,
    help = '',
    initialText,
    backHash,
    onSave,
    readOnly = !editable(),
  }: {
    title: string;
    help?: string;
    initialText: string;
    backHash: string;
    /** `errors` is what the text itself got wrong; the error bar says what the leader refused. */
    onSave: (text: string) => Promise<{ ok: boolean; errors: string }>;
    /** Shown to read and not saved. The page's own scope decides, unless the text belongs to another. */
    readOnly?: boolean;
  } = $props();

  // A one-time snapshot: a new starting draft always comes with a fresh
  // mount.
  // svelte-ignore state_referenced_locally
  let draft = $state(initialText);
  // svelte-ignore state_referenced_locally
  let savedText = $state(initialText);
  let isDirty = $derived(draft !== savedText);
  let saving = $state(false);
  let saveError = $state('');
  let saveMessage = $state('');

  $effect(() => {
    function onBeforeUnload(e: BeforeUnloadEvent) {
      if (!isDirty) return;
      e.preventDefault();
      e.returnValue = '';
    }
    window.addEventListener('beforeunload', onBeforeUnload);
    return () => window.removeEventListener('beforeunload', onBeforeUnload);
  });

  function back() {
    location.hash = backHash;
  }

  async function save() {
    saving = true;
    saveError = '';
    saveMessage = '';
    try {
      const result = await onSave(draft);
      if (result.ok) {
        saveMessage = 'Saved.';
        savedText = draft;
      } else {
        saveError = result.errors;
      }
    } catch (e) {
      reported(e);
    } finally {
      saving = false;
    }
  }
</script>

{#snippet helpText()}<p>{help}</p>{/snippet}

<main class="column json-page">
  <PageHeader {title} help={help ? helpText : undefined}>
    <button type="button" class="back-link" onclick={back}>&larr; Back</button>
  </PageHeader>
  <!-- Read, not changed, where the scope is not the person's to change (`access.ts`). -->
  <textarea class="json-editor" bind:value={draft} spellcheck="false" readonly={readOnly}
  ></textarea>
  {#if !readOnly}
    <div class="run-row">
      <button class="primary" onclick={save} disabled={saving}
        >{saving ? 'Validating…' : 'Save'}</button
      >
      {#if saveMessage}<span class="save-message">{saveMessage}</span>{/if}
    </div>
  {/if}
  <InlineError message={saveError} />
</main>

<style>
  .back-link {
    background: none;
    border: none;
    color: var(--ink);
    font-size: var(--text-body);
    padding: 0;
    cursor: pointer;
  }

  .json-editor {
    width: 100%;
    box-sizing: border-box;
    height: 60vh;
    min-height: 300px;
    background: var(--panel);
    border-radius: var(--radius-card);
    padding: 0.6rem;
    font-family: var(--mono);
    resize: vertical;
  }

  .run-row {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin-top: 1rem;
  }

  .save-message {
    color: var(--ink);
    font-size: var(--text-body);
  }
</style>
