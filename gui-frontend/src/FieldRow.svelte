<!--
  One declared field: its name, its type, and its own actions. A parser's fields, an object's own fields and a
  parser's suggested fields are each a list of these, so a field looks and is typed the same in all three. A datetime
  carries its format folded under the row, and an object its own fields, indented under it.

  Every row has the same tracks, so the rows of one list line up without sharing a grid.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import ChipPicker from './ChipPicker.svelte';
  import MoreIcon from './MoreIcon.svelte';
  import TypePicker from './TypePicker.svelte';
  import DatetimeFormat from './DatetimeFormat.svelte';
  import Nested from './Nested.svelte';
  import { typeKey, withTypeKey, type DataTypeSpec, type TypeKey } from './pipeline';

  let {
    ty = $bindable(),
    groups,
    number,
    disabled = false,
    label,
    name,
    actions,
    end,
    nested,
  }: {
    ty: DataTypeSpec;
    groups: TypeKey[][];
    /** A CSV column's position, counted from 1. */
    number?: number;
    disabled?: boolean;
    /** What the row's type picker and menu are named for: `<label> type`, `<label> actions`. */
    label: string;
    /** The name's box. */
    name: Snippet;
    /** The row's menu, as `.pick` buttons. Choosing one closes it. */
    actions?: Snippet;
    /** What stands in the menu's place, when the row has a single action of its own. */
    end?: Snippet;
    /** Under the row, indented: an object's own fields. */
    nested?: Snippet;
  } = $props();

  let menuOpen = $state(false);
</script>

<div class="field-row" class:numbered={number !== undefined}>
  {#if number !== undefined}<span class="number">{number}</span>{/if}
  {@render name()}
  <TypePicker
    value={typeKey(ty)}
    {groups}
    {disabled}
    ariaLabel={`${label} type`}
    onpick={(key) => (ty = withTypeKey(ty, key))}
  />
  <span class="end">
    {#if end}
      {@render end()}
    {:else if actions && !disabled}
      <ChipPicker kind="row" compact ariaLabel={`${label} actions`} bind:open={menuOpen}>
        {#snippet face()}<MoreIcon />{/snippet}
        <!-- Choosing closes the menu (`ChipPicker` leaves that to its caller). -->
        <div onclick={() => (menuOpen = false)} role="presentation">{@render actions()}</div>
      </ChipPicker>
    {/if}
  </span>
</div>
{#if ty.kind === 'datetime'}
  {@const dt = ty}
  <div class="under" class:numbered={number !== undefined}>
    <DatetimeFormat bind:format={dt.format} {disabled} />
  </div>
{/if}
{#if nested}
  <Nested numbered={number !== undefined}>{@render nested()}</Nested>
{/if}

<style>
  .field-row {
    display: grid;
    grid-template-columns: minmax(4rem, 1fr) minmax(0, 8.2rem) minmax(1.6rem, auto);
    align-items: center;
    gap: 0.4rem;
  }

  .field-row.numbered {
    grid-template-columns: 1.3rem minmax(4rem, 1fr) minmax(0, 8.2rem) minmax(1.6rem, auto);
  }

  .number {
    font-family: var(--mono);
    font-size: var(--text-note);
    color: var(--dim);
    text-align: right;
  }

  .end {
    display: flex;
    justify-content: center;
  }

  .under {
    margin-top: -0.1rem;
  }

  .under.numbered {
    margin-left: 1.7rem;
  }
</style>
