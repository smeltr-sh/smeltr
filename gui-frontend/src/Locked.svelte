<!--
  What a page holds that the person may not change (`access.ts`), or that the website demo does not
  do (`refusedInDemo`): every control inside is shown disabled. Adds nothing to the layout -- the children lay out as the parent's own. Below a screen
  shown in place of a page, as signing in is, it disables nothing.
-->
<script module lang="ts">
  import { getContext, setContext } from 'svelte';

  const UNLOCKED = Symbol('unlocked');

  /** Called while a component initialises: no `Locked` below it disables anything, whatever page
   * the address names. For a screen shown in place of a page, as signing in is. */
  export function neverLocked(): void {
    setContext(UNLOCKED, true);
  }
</script>

<script lang="ts">
  import type { Snippet } from 'svelte';
  import { NOT_IN_DEMO, editable } from './access';

  let {
    children,
    refusedInDemo = false,
  }: {
    children: Snippet;
    /** What is inside is a write the demo refuses, so in the demo it is shown disabled and says why. */
    refusedInDemo?: boolean;
  } = $props();

  // svelte-ignore state_referenced_locally
  const why = refusedInDemo ? NOT_IN_DEMO : '';
  const readOnly = !getContext<boolean | undefined>(UNLOCKED) && (why !== '' || !editable());
</script>

<!-- A disabled fieldset disables every control in it, whatever the caller put there. Its title is
     every control's, since none inside names its own. -->
{#if readOnly}<fieldset class="read-only" disabled title={why || undefined}>
    {@render children()}
  </fieldset>{:else}{@render children()}{/if}

<style>
  .read-only {
    display: contents;
  }
</style>
