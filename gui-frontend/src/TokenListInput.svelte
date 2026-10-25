<!--
  A chip field for a list of names. Enter, comma or blur commits the draft;
  a chip's text renames it in place; Backspace on an empty draft removes
  the last chip. `comma` false leaves a comma in the draft, for values that
  hold one. Every edit hands the parent a new list, so a binding through a
  getter and setter sees it.

  Given `choices`, the values are picked rather than typed: the draft is a
  picker of the choices not yet chosen, a chip is not renamed, and `onnew`
  adds a last choice that has the caller make one.
-->
<script lang="ts">
  import PickOrNew from './PickOrNew.svelte';
  import RemovableChip from './RemovableChip.svelte';

  let {
    values = $bindable([]),
    placeholder = '',
    comma = true,
    choices,
    choiceLabel = (v) => v,
    label,
    newLabel,
    onnew,
    required = false,
  }: {
    values: string[];
    placeholder?: string;
    comma?: boolean;
    choices?: string[];
    /** A chosen value's text, and its choice's. */
    choiceLabel?: (v: string) => string;
    /** The picker's name, for a screen reader. */
    label?: string;
    /** The text of the choice that runs `onnew`. */
    newLabel?: string;
    onnew?: () => void;
    /** As `PasswordField`'s, said of the input a new value is typed in. */
    required?: boolean;
  } = $props();

  /** What the picker offers: each choice once, none already chosen. */
  let offered = $derived([...new Set(choices ?? [])].filter((c) => !values.includes(c)));

  let draft = $state('');
  let editingIndex = $state<number | null>(null);
  let editDraft = $state('');

  function commitDraft() {
    const v = draft.trim();
    if (v !== '') values = [...values, v];
    draft = '';
  }
  function onDraftKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter' || (comma && e.key === ',') || e.key === 'Tab') {
      // Tab with nothing typed moves focus; a comma never reaches the draft.
      if (e.key !== 'Tab' || draft.trim() !== '') e.preventDefault();
      commitDraft();
    } else if (e.key === 'Backspace' && draft === '' && values.length > 0) {
      values = values.slice(0, -1);
    }
  }

  function startEdit(i: number) {
    editingIndex = i;
    editDraft = values[i];
  }
  function commitEdit(i: number) {
    const v = editDraft.trim();
    values = v !== '' ? values.with(i, v) : values.toSpliced(i, 1);
    editingIndex = null;
  }
  function onEditKeydown(e: KeyboardEvent, i: number) {
    if (e.key === 'Enter') {
      // Inside a dialog's form, Enter would also submit it.
      e.preventDefault();
      commitEdit(i);
    } else if (e.key === 'Escape') {
      // Leaves the rename only: inside a dialog, Escape would also close it.
      e.preventDefault();
      editingIndex = null;
    }
  }

  function removeAt(i: number) {
    if (editingIndex === i) editingIndex = null;
    values = values.toSpliced(i, 1);
  }
</script>

<div class="token-field">
  {#each values as v, i (i + ':' + v)}
    {#if editingIndex === i}
      <input
        class="chip-edit"
        bind:value={editDraft}
        onblur={() => commitEdit(i)}
        onkeydown={(e) => onEditKeydown(e, i)}
      />
    {:else}
      <RemovableChip removeLabel="remove {v}" onremove={() => removeAt(i)}>
        {#if choices}
          <span class="chip-label">{choiceLabel(v)}</span>
        {:else}
          <button
            type="button"
            class="chip-label"
            onclick={() => startEdit(i)}
            aria-label="rename {v}">{v}</button
          >
        {/if}
      </RemovableChip>
    {/if}
  {/each}
  {#if choices}
    <PickOrNew
      class="chip-draft"
      value=""
      choices={offered}
      {choiceLabel}
      placeholder={values.length === 0 ? placeholder : ''}
      {newLabel}
      {onnew}
      {label}
      onpick={(v) => (values = [...values, v])}
    />
  {:else}
    <input
      class="chip-draft"
      bind:value={draft}
      placeholder={values.length === 0 ? placeholder : ''}
      aria-required={required || undefined}
      onkeydown={onDraftKeydown}
      onblur={commitDraft}
    />
  {/if}
</div>

<style>
  .token-field {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.3rem;
    background: var(--field);
    color: var(--ink);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.3rem 0.4rem;
    font-family: var(--mono);
    font-size: var(--text-label);
  }

  .chip-label {
    background: none;
    border: none;
    color: var(--ink);
    font-family: var(--mono);
    font-size: var(--text-label);
    padding: 0;
    cursor: text;
  }

  .chip-edit {
    width: 6rem;
    background: var(--panel);
    color: var(--ink);
    border: 1px solid var(--accent);
    border-radius: 999px;
    padding: 0.1rem 0.5rem;
    font-family: var(--mono);
    font-size: var(--text-label);
  }

  /* The draft is this field's input, or `PickOrNew`'s picker. */
  .token-field :global(.chip-draft) {
    flex: 1;
    min-width: 5rem;
    background: none;
    border: none;
    color: var(--ink);
    font-family: var(--mono);
    font-size: var(--text-label);
    padding: 0.15rem 0.2rem;
  }

  .token-field :global(.chip-draft:focus) {
    outline: none;
  }
</style>
