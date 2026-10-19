<!--
  The one control that picks a field's type: a parser's declared field, an object's own field, a lookup output and a
  suggested field all choose here. A native select, so the keyboard and a screen reader work as on any other; its
  groups are split by separators rather than headed, since each group's choices already say what they are.
-->
<script lang="ts">
  import type { TypeKey } from './pipeline';

  let {
    value,
    groups,
    disabled = false,
    ariaLabel = 'type',
    onpick,
  }: {
    value: TypeKey;
    groups: TypeKey[][];
    disabled?: boolean;
    ariaLabel?: string;
    onpick: (key: TypeKey) => void;
  } = $props();
</script>

<select
  class="type-picker"
  {disabled}
  aria-label={ariaLabel}
  {value}
  onchange={(e) => onpick((e.currentTarget as HTMLSelectElement).value as TypeKey)}
>
  {#each groups as group, g (g)}
    {#if g > 0}<hr />{/if}
    {#each group as key (key)}
      <option value={key}>{key}</option>
    {/each}
  {/each}
</select>

<style>
  /* Sized by its cell, not its longest choice; the closed value ellipsizes, and the open list shows it whole. */
  .type-picker {
    width: 100%;
    min-width: 0;
    box-sizing: border-box;
    font-family: var(--mono);
    overflow: hidden;
    text-overflow: ellipsis;
  }
</style>
