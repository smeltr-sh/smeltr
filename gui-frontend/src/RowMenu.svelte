<!--
  A row's ⋯ menu: the row's actions as `.pick` buttons, `.pick.remove` for the one that deletes. Choosing an action
  closes the menu.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import ChipPicker from './ChipPicker.svelte';
  import MoreIcon from './MoreIcon.svelte';

  let { label, children }: { label: string; children: Snippet } = $props();
  let open = $state(false);
</script>

<!-- Inline, so it sits beside a row's own buttons. -->
<div class="row-menu">
  <ChipPicker kind="row" compact ariaLabel={label} bind:open>
    {#snippet face()}<MoreIcon />{/snippet}
    <!-- Choosing closes the menu (`ChipPicker` leaves that to its caller). -->
    <div class="menu-picks" onclick={() => (open = false)} role="presentation">
      {@render children()}
    </div>
  </ChipPicker>
</div>

<style>
  .row-menu {
    display: inline-block;
    vertical-align: middle;
  }

  /* Its picks stay the panel's own children for layout. */
  .menu-picks {
    display: contents;
  }
</style>
