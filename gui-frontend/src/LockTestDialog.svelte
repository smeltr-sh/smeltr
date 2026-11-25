<!--
  Locks the preview on screen as a test: its name, and the fields and rows the grid shows.
-->
<script lang="ts">
  import Button, { BUSY, INCOMPLETE } from './Button.svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
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
    onlock: (name: string) => Promise<void>;
    oncancel: () => void;
  } = $props();

  let name = $state('');
  const busy = new Busy();
  $effect(() => {
    if (open) name = given;
  });

  async function lock() {
    if (!name.trim() || busy.on) return;
    await busy.run(() => onlock(name.trim()));
  }
</script>

<Dialog
  {open}
  title={relocking ? 'lock this test again' : 'lock this as a test'}
  oncancel={() => busy.on || oncancel()}
  onsubmit={lock}
>
  <div class="form-body">
    <Field required label="Name">
      <input bind:value={name} placeholder="what this test checks" data-guide="lock-name" />
    </Field>
    <span class="dim">{count(fields, 'field')} on {count(rows, 'row')}</span>
  </div>
  {#snippet actions()}
    <Button type="button" onclick={oncancel} disabled={busy.on ? BUSY : ''}>Cancel</Button>
    <Button
      type="submit"
      class="primary"
      data-guide="lock"
      disabled={busy.on ? BUSY : !name.trim() ? INCOMPLETE : ''}
    >
      {busy.on ? 'Locking…' : 'Lock'}
    </Button>
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
