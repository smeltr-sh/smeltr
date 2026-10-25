<!--
  A named value (STYLE_GUIDE.md, "Text" and "Controls"): the label, an
  optional `?` explainer, the control, then a note or an error under it.
  A warning sits where the note would: the value saves, but something about it
  deserves a second look.
  The name and the control are one `<label>`, so a click on the name
  reaches the control. `group` makes it a `role="group"` named by its label instead,
  for a field made of several controls, where no one of them owns the name.
  `field` names the setting by its name in the file; inside a `ValidationItem`
  it shows that setting's save errors (`validation.svelte.ts`).
  Where the page is not the person's to change (`access.ts`), the control is
  shown disabled: every form on such a page is read through this.
  `bare` hides the name from sight, for a field in a table cell whose column
  head shows it; it still names the control.
-->
<script module lang="ts">
  let seq = 0;
</script>

<script lang="ts">
  import type { Snippet } from 'svelte';
  import Help from './Help.svelte';
  import Locked from './Locked.svelte';
  import { validationContext } from './validation.svelte';

  let {
    label,
    help,
    note,
    error = null,
    warning = null,
    field,
    group = false,
    bare = false,
    children,
  }: {
    label: string;
    help?: Snippet | string;
    note?: Snippet | string;
    error?: string | null;
    warning?: string | null;
    field?: string;
    group?: boolean;
    bare?: boolean;
    children: Snippet;
  } = $props();

  const nameId = `field-${++seq}`;

  const context = validationContext();
  let root: HTMLDivElement;
  const message = $derived(
    error ?? (context && field ? context.validation.message(context.id(), field) : null),
  );
  $effect(() => {
    if (!context || !field) return;
    return context.validation.mount(context.id(), field, () => {
      // A control before a button: the label's own `?` is a button too.
      const control =
        root.querySelector<HTMLElement>('input, select, textarea') ??
        root.querySelector<HTMLElement>('button:not(.ask)');
      if (control) control.focus();
      else root.scrollIntoView({ block: 'center' });
    });
  });
  // The item's id before an edit reaches the control. An edit to the id
  // field renames the item, and the item's errors go with it.
  let before = '';
  function editing() {
    if (context && field) before = context.id();
  }
  function edited() {
    if (!context || !field) return;
    const id = context.id();
    if (id !== before) context.validation.renamed(before, id);
    context.validation.edited(id, field);
  }
</script>

{#snippet name()}
  <span class="name" class:bare id={nameId}
    >{label}{#if help}<Help {label}
        >{#if typeof help === 'string'}{help}{:else}{@render help()}{/if}</Help
      >{/if}</span
  >
{/snippet}

<!-- The note and the error sit outside the `<label>`, so they describe the
     control rather than becoming part of its name. -->
<div
  class="field"
  class:invalid={!!message}
  role={group ? 'group' : undefined}
  aria-labelledby={group ? nameId : undefined}
  bind:this={root}
  oninputcapture={editing}
  onchangecapture={editing}
  oninput={edited}
  onchange={edited}
>
  {#if group}
    {@render name()}
    <Locked>{@render children()}</Locked>
  {:else}
    <!-- The lock holds the label rather than sit in it: a `<label>` may not hold a `<fieldset>`.
         The `?` is no form control, so it stays open. -->
    <Locked><label>{@render name()}{@render children()}</label></Locked>
  {/if}
  {#if message}
    <span class="error" role="alert">{message}</span>
  {:else if warning}
    <span class="warning" role="status">{warning}</span>
  {:else if note}
    <span class="note"
      >{#if typeof note === 'string'}{note}{:else}{@render note()}{/if}</span
    >
  {/if}
</div>

<style>
  .field,
  label {
    display: grid;
    /* An auto track grows to an input's default `size` and spills past a narrow field. */
    grid-template-columns: minmax(0, 1fr);
    /* Beside a taller field in a row, the control stays under its label rather than stretching. */
    align-content: start;
    gap: 0.3rem;
    min-width: 0;
  }

  .name {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    font-size: var(--text-label);
    font-weight: 600;
  }

  .name.bare {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }

  /* What stays under a field is what must be known, so it reads in the accent. */
  .note {
    color: var(--accent);
    font-size: var(--text-note);
  }

  .warning {
    color: var(--warn);
    font-size: var(--text-note);
  }

  .error {
    font-size: var(--text-note);
    white-space: pre-line;
  }

  .invalid :global(:is(input, select, textarea)) {
    border-color: var(--error);
  }
</style>
