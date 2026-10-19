<!--
  One item of a list editor (`validation.svelte.ts`): the fields inside take
  its id, and `open` is how a save error on one of them shows it.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import { provideItem, validationContext } from './validation.svelte';

  let { id, open, children }: { id: string; open: () => void; children: Snippet } = $props();

  provideItem(() => id);
  const context = validationContext();

  $effect(() => context?.validation.opener(id, open));
</script>

{@render children()}
