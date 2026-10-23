<!--
  Discard and Save at the foot of a short settings panel, shown only while it holds a change. The panel is the form:
  Save submits it. A form long enough to scroll its Save away takes `SaveBar.svelte` instead.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    dirty,
    label,
    disabled = false,
    ondiscard,
    children,
  }: {
    dirty: boolean;
    /** The Save button's text. */
    label: string;
    /** Holds Save back, as while a save is in flight. */
    disabled?: boolean;
    ondiscard: () => void;
    /** What saving would do that the person should know first, beside the buttons. */
    children?: Snippet;
  } = $props();
</script>

{#if dirty}
  <div class="savebar">
    {@render children?.()}
    <div class="buttons">
      <button type="button" onclick={ondiscard}>Discard</button>
      <button class="primary" type="submit" {disabled}>{label}</button>
    </div>
  </div>
{/if}

<style>
  .savebar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.8rem;
    border-top: 1px solid var(--border);
    padding-top: 0.9rem;
  }

  .savebar :global(p) {
    margin: 0;
    font-size: var(--text-note);
  }

  .buttons {
    display: flex;
    gap: 0.5rem;
  }
</style>
