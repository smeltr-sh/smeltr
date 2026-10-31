<!--
  Adding a source or a destination: its type is picked first (after its kind, where the catalog has kinds), then
  its fields, in a dialog. Where it goes on Add is the caller's: its own page lists it and saves it with the page,
  the Routes page saves it there and then.
-->
<script lang="ts" generics="O extends { id: string; type: T }, T extends string, K extends string">
  import { reported } from './errors.svelte';
  import type { Snippet } from 'svelte';
  import Dialog from './Dialog.svelte';
  import TypeCatalog, { type CatalogKind, type CatalogType } from './TypeCatalog.svelte';
  import Field from './Field.svelte';
  import { idProblem } from './ids';

  let {
    open,
    noun,
    unavailable,
    types,
    kinds,
    blank,
    typeLabel,
    ready = () => true,
    fields,
    onadd,
    oncancel,
  }: {
    /** Kept mounted and closed rather than removed, so the dialog closes and focus goes back to what opened it. */
    open: boolean;
    /** What is being added, in the dialog's title: `source`, `destination`. */
    noun: string;
    /** Every id the new one may not take. */
    unavailable: string[];
    types: CatalogType<T, K>[];
    kinds?: CatalogKind<K>[];
    /** A new object of a type, clear of every id in `taken`. */
    blank: (type: T, taken: string[]) => O;
    /** A type, in the title as `new <label> <noun>`. A label that names the noun itself is the title alone. */
    typeLabel: (type: T) => string;
    /** Whether a draft with a good id can be added. */
    ready?: (draft: O) => boolean;
    /** The draft's fields, under its id. */
    fields: Snippet<[O]>;
    /** Takes the new object. The dialog stays open until its owner closes it. */
    onadd: (made: O) => void | Promise<void>;
    oncancel: () => void;
  } = $props();

  let kind: K | null = $state(null);
  let draft: O | null = $state(null);
  let busy = $state(false);
  // Each opening starts at the catalog.
  $effect(() => {
    if (!open) {
      kind = null;
      draft = null;
    }
  });

  const heading = (label: string) => (label.includes(noun) ? label : `${label} ${noun}`);

  async function add() {
    if (!draft || busy || idProblem(draft.id, unavailable) || !ready(draft)) return;
    busy = true;
    try {
      await onadd($state.snapshot(draft) as O);
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }
</script>

<Dialog
  {open}
  title={draft ? `new ${heading(typeLabel(draft.type))}` : `add ${noun}`}
  wide
  {oncancel}
  onback={draft ? () => (draft = null) : kind ? () => (kind = null) : undefined}
  onsubmit={add}
>
  {#if draft}
    <div class="form-body">
      <Field required label="id" error={idProblem(draft.id, unavailable)}>
        <input type="text" class="mono" bind:value={draft.id} />
      </Field>
      {@render fields(draft)}
    </div>
  {:else}
    <TypeCatalog
      {types}
      {kinds}
      {kind}
      onkind={(k) => (kind = k)}
      onpick={(type) => (draft = blank(type, unavailable))}
    />
  {/if}
  {#snippet actions()}
    <button type="button" onclick={oncancel}>Cancel</button>
    {#if draft}
      <button
        type="submit"
        class="primary"
        disabled={busy || idProblem(draft.id, unavailable) !== null || !ready(draft)}>Add</button
      >
    {/if}
  {/snippet}
</Dialog>

<style>
  /* Its fields, stacked as a row's are. */
  .form-body {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }

  .mono {
    font-family: var(--mono);
  }
</style>
