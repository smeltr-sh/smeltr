<!--
  An `exceptions` layer's tag field, under Advanced: the group's on the Routes page, a route's in its card. One
  component, so both say and do the same.
-->
<script lang="ts">
  import Advanced from './Advanced.svelte';
  import Field from './Field.svelte';
  import Locked from './Locked.svelte';
  import { withTagField, type ExceptionsDto } from './exceptions';

  let {
    value = $bindable(),
    placeholder,
  }: {
    value: ExceptionsDto | undefined;
    /** What an empty field means: the layer beneath's tag field. */
    placeholder: string;
  } = $props();
</script>

<!-- Only the field is locked: a reader can still open Advanced to see it. -->
<Advanced modified={!!value?.tagField}>
  <Locked>
    <Field label="Tag field" optional>
      {#snippet help()}The field a tagged or delivered record carries its exceptions in. Empty means
        <code>{placeholder}</code>.{/snippet}
      <input
        class="mono"
        value={value?.tagField ?? ''}
        {placeholder}
        oninput={(e) => (value = withTagField(value, e.currentTarget.value))}
      />
    </Field>
  </Locked>
</Advanced>

<style>
  .mono {
    font-family: var(--mono);
  }
</style>
