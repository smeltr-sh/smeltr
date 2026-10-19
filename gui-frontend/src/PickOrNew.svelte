<!--
  A picker whose last choice makes a new one: `+ New source…` and the like. That choice is never the value, so the
  picker is controlled: a pick goes to `onpick`, and the picker shows `value` again until the caller changes it.
-->
<script lang="ts">
  let {
    value,
    choices,
    choiceLabel = (v) => v,
    placeholder = '',
    newLabel,
    onnew,
    onpick,
    label,
    class: className,
  }: {
    value: string;
    choices: string[];
    choiceLabel?: (v: string) => string;
    /** The blank choice's text, shown while nothing is picked. */
    placeholder?: string;
    /** The text of the choice that runs `onnew`, offered only with it. */
    newLabel?: string;
    onnew?: () => void;
    onpick: (v: string) => void;
    /** The picker's name, for a screen reader, where no field label names it. */
    label?: string;
    class?: string;
  } = $props();

  /** No id is ever this, since it is not a word. */
  const NEW = '\u0000new';
</script>

<select
  class={className}
  aria-label={label}
  {value}
  onchange={(e) => {
    const el = e.currentTarget;
    const v = el.value;
    el.value = value;
    if (v === NEW) onnew?.();
    else if (v !== '') onpick(v);
  }}
>
  {#if !value}<option value="">{placeholder}</option>{/if}
  {#each choices as c}<option value={c}>{choiceLabel(c)}</option>{/each}
  {#if onnew}<option value={NEW}>{newLabel}</option>{/if}
</select>
