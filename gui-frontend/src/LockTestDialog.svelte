<!--
  Locks the preview on screen as a test: its name, and the fields and rows the grid shows.
-->
<script lang="ts">
  import Button, { BUSY, INCOMPLETE } from './Button.svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import InlineError from './InlineError.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import { errorText, type MutationResult, type SaveConflict } from './api';
  import { Busy } from './busy.svelte';
  import { count } from './time';

  let {
    open,
    relocking,
    name: given = '',
    fields,
    rows,
    onlock,
    oncancel,
  }: {
    open: boolean;
    relocking: boolean;
    /** The test's name, when it is locked again. */
    name?: string;
    fields: number;
    rows: number;
    /** Locks the test; the dialog shows why a lock was refused, and offers a refused save again on another branch. */
    onlock: (name: string) => Promise<MutationResult>;
    oncancel: () => void;
  } = $props();

  let name = $state('');
  let error = $state('');
  let conflict = $state<SaveConflict | null>(null);
  const busy = new Busy();
  $effect(() => {
    if (!open) return;
    name = given;
    error = '';
    conflict = null;
  });

  async function lock() {
    if (!name.trim() || busy.on) return;
    await busy.run(async () => {
      const res = await onlock(name.trim());
      conflict = res.conflict ?? null;
      error = res.ok || res.conflict ? '' : errorText(res.errors);
    });
  }
</script>

<Dialog
  {open}
  title={relocking ? 'lock this test again' : 'lock this as a test'}
  oncancel={() => busy.on || oncancel()}
  onsubmit={conflict ? undefined : lock}
>
  <div class="form-body">
    <Field required label="Name">
      <input bind:value={name} placeholder="what this test checks" data-guide="lock-name" />
    </Field>
    <span class="dim">{count(fields, 'field')} on {count(rows, 'row')}</span>
    <InlineError message={error} />
    {#if conflict}
      <SaveConflictPanel
        {conflict}
        onDiscard={() => location.reload()}
        onRetry={async () => (await onlock(name.trim())).ok}
      />
    {/if}
  </div>
  {#snippet actions()}
    <Button type="button" onclick={oncancel} disabled={busy.on ? BUSY : ''}>Cancel</Button>
    <!-- A refused save is settled in the panel, whose own form submits inside this one. -->
    {#if !conflict}
      <Button
        type="submit"
        class="primary"
        data-guide="lock"
        disabled={busy.on ? BUSY : !name.trim() ? INCOMPLETE : ''}
      >
        {busy.on ? 'Locking…' : 'Lock'}
      </Button>
    {/if}
  {/snippet}
</Dialog>

<style>
  .form-body {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }
  input {
    width: 100%;
    box-sizing: border-box;
  }
</style>
