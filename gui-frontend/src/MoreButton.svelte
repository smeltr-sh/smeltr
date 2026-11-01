<!--
  "N more" under a list cut short: a click shows the rest. The list keeps whether it is cut.
-->
<script lang="ts">
  let { count, onclick }: { count: number; onclick: () => void } = $props();
</script>

<button
  type="button"
  class="more"
  onclick={(e) => {
    // Showing the rest is all the click does, inside a row that a click would open or close.
    e.stopPropagation();
    onclick();
  }}>{count} more</button
>

<style>
  .more {
    /* Under a grid of fields, the whole width. */
    grid-column: 1 / -1;
    justify-self: start;
    border: none;
    background: none;
    padding: 0;
    font: inherit;
    color: var(--dim);
    cursor: pointer;
    text-decoration: underline dotted;
    text-underline-offset: 0.2em;
  }

  .more:hover {
    color: var(--ink);
  }
</style>
