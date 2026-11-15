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
    auto,
    onpick,
    onauto,
  }: {
    /** `''` where the field is typed automatically. */
    value: TypeKey | '';
    groups: TypeKey[][];
    disabled?: boolean;
    ariaLabel?: string;
    /** Present where the field may be typed automatically: the type it then has, `''` where that is not known. */
    auto?: string;
    onpick: (key: TypeKey) => void;
    /** The automatic type is picked. */
    onauto?: () => void;
  } = $props();
</script>

<select
  class="type-picker"
  {disabled}
  aria-label={ariaLabel}
  title={auto === undefined
    ? undefined
    : 'auto: the type a schema the pipeline produces gives it, else the type its pattern gives it'}
  {value}
  onchange={(e) => {
    const key = (e.currentTarget as HTMLSelectElement).value;
    if (key === '') onauto?.();
    else onpick(key as TypeKey);
  }}
>
  {#if auto !== undefined}
    <option value="">{auto ? `auto · ${auto}` : 'auto'}</option>
    <hr />
  {/if}
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
