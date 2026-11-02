<!--
  A route's or a destination's own `exceptions` layer, over the group's, folded as Advanced is: open when it
  answers anything itself. Reset to worker group clears it.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import Advanced from './Advanced.svelte';
  import ExceptionMatrix from './ExceptionMatrix.svelte';
  import TagField from './TagField.svelte';
  import { editable } from './access';
  import { overrideCount, type Answer, type ExceptionsDto } from './exceptions';

  let {
    value = $bindable(),
    name,
    help,
    rows,
    beneath,
    targets,
    tagField,
  }: {
    value: ExceptionsDto | undefined;
    /** What this answers for, for a screen reader. */
    name: string;
    help?: Snippet;
    rows: string[];
    beneath: Record<string, Answer>;
    targets: string[];
    /** The group's tag field, where this layer may name its own. */
    tagField?: string;
  } = $props();

  const readOnly = !editable();
</script>

<Advanced label="Exceptions" modified={overrideCount(value) > 0} {help}>
  <ExceptionMatrix bind:value {rows} {beneath} {targets} {name} />
  {#if tagField !== undefined}
    <TagField bind:value placeholder={tagField} />
  {/if}
  {#if !readOnly && value}
    <div>
      <button type="button" onclick={() => (value = undefined)}>Reset to worker group</button>
    </div>
  {/if}
</Advanced>
