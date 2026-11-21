<!--
  Adding a source or a destination: its type is picked first (after its kind, where the catalog has kinds), then
  its fields, in a dialog. Given one to start from, it opens on the fields, filled in as that one's. Where it goes
  on Add is the caller's: its own page lists it and saves it with the page, the Routes page saves it there and then.
  A copy given the areas it may go to picks one, and is checked against the ids there.
-->
<script lang="ts" generics="O extends { id: string; type: T }, T extends string, K extends string">
  import { Busy } from './busy.svelte';
  import Button, { BUSY, INCOMPLETE } from './Button.svelte';
  import type { Snippet } from 'svelte';
  import Dialog from './Dialog.svelte';
  import TypeCatalog, { type CatalogKind, type CatalogType } from './TypeCatalog.svelte';
  import Field from './Field.svelte';
  import { untrack } from 'svelte';
  import { freeName, idProblem } from './ids';
  import type { Area } from './areas';

  let {
    open,
    noun,
    unavailable,
    types,
    kinds,
    blank,
    typeLabel,
    typeOf = (o) => o.type,
    ready = () => true,
    fields,
    from,
    areas,
    takenIn,
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
    /** The type a draft is labelled by, when it is not the draft's own. */
    typeOf?: (draft: O) => T;
    /** Whether a draft with a good id can be added. */
    ready?: (draft: O) => boolean;
    /** The draft's fields, under its id. */
    fields: Snippet<[O, string | undefined]>;
    /** An object to copy: the dialog opens on its fields, every one as it is but the id. */
    from?: O;
    /** Where a copy may go, the page's own area first. */
    areas?: Area[];
    /** The ids a new one in another of `areas` may not take. */
    takenIn?: (area: string) => Promise<string[]>;
    /** Takes the new object, and the area picked for a copy. The dialog stays open until its owner closes it. */
    onadd: (made: O, area?: string) => void | Promise<void>;
    oncancel: () => void;
  } = $props();

  let kind: K | null = $state(null);
  let draft: O | null = $state(null);
  const busy = new Busy();
  let into = $state('');
  let there: string[] = $state([]);
  const elsewhere = $derived(!!areas && into !== '' && into !== areas[0]?.id);
  const taken = $derived(elsewhere ? there : unavailable);
  /** The id the dialog proposed for a copy, which follows the area picked until the person types their own. */
  let proposed = '';
  function propose(ids: string[]) {
    if (!draft || !from || draft.id !== proposed) return;
    draft.id = proposed = freeName(from.id, ids);
  }
  $effect(() => {
    if (!elsewhere || !takenIn) {
      untrack(() => propose(unavailable));
      return;
    }
    const asked = into;
    there = [];
    takenIn(asked)
      .then((ids) => {
        if (into !== asked) return;
        there = ids;
        propose(ids);
      })
      .catch(() => {});
  });
  // Each opening starts at the catalog, or on a copy of `from`.
  $effect(() => {
    if (!open) {
      kind = null;
      draft = null;
      into = '';
    } else if (from) {
      untrack(() => (into = areas?.[0]?.id ?? ''));
      const original = from;
      // Taken once, on opening: the copy is the person's to edit from there.
      untrack(
        () =>
          (draft = {
            ...$state.snapshot(original),
            id: (proposed = freeName(original.id, unavailable)),
          } as O),
      );
    }
  });

  const heading = (label: string) => (label.includes(noun) ? label : `${label} ${noun}`);

  async function add() {
    if (!draft || busy.on || idProblem(draft.id, taken) || !ready(draft)) return;
    await busy.run(async () => {
      await onadd($state.snapshot(draft) as O, into || undefined);
    });
  }
</script>

<Dialog
  {open}
  title={draft ? `new ${heading(typeLabel(typeOf(draft)))}` : `add ${noun}`}
  wide
  {oncancel}
  onback={from ? undefined : draft ? () => (draft = null) : kind ? () => (kind = null) : undefined}
  onsubmit={add}
>
  {#if draft}
    <div class="form-body">
      {#if from && areas && areas.length > 1}
        <Field
          label="add to"
          note={elsewhere
            ? `Saved in ${areas.find((a) => a.id === into)?.name ?? into} on Add. Unsaved changes on this page stay unsaved.`
            : undefined}
        >
          <select bind:value={into}>
            {#each areas as a (a.id)}
              <option value={a.id}>{a.name}</option>
            {/each}
          </select>
        </Field>
      {/if}
      <Field required label="id" error={idProblem(draft.id, taken)}>
        <input type="text" class="mono" bind:value={draft.id} />
      </Field>
      {@render fields(draft, into || undefined)}
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
      <Button
        type="submit"
        class="primary"
        disabled={busy.on ? BUSY : (idProblem(draft.id, taken) ?? (ready(draft) ? '' : INCOMPLETE))}
        >Add</Button
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
</style>
