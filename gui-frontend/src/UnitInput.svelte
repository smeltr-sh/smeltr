<!-- A number and the unit it is counted in, inside a `Field`. `before` goes ahead of the number, such as the
     checkbox that turns it on. -->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    value = $bindable(),
    unit,
    min,
    max,
    step = 1,
    required = false,
    disabled = false,
    before,
  }: {
    value: number;
    unit: string;
    min?: number;
    max?: number;
    step?: number | 'any';
    required?: boolean;
    disabled?: boolean;
    before?: Snippet;
  } = $props();
</script>

<div class="unit">
  {@render before?.()}
  <input type="number" {min} {max} {step} bind:value {required} {disabled} />
  <span class="dim">{unit}</span>
</div>

<style>
  .unit {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  input {
    max-width: 8rem;
  }
  .dim {
    color: var(--dim);
  }
</style>
