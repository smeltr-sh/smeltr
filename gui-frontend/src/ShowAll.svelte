<!--
  What a list cut to its first items says of the rest, and the button that shows them. As a line under a long
  list: how many it shows of how many, and Show all. Inline, in a sentence or a short list: "3 more".
-->
<script module lang="ts">
  /** Rows a long list shows before Show all: a browser takes seconds to lay out thousands. */
  export const SHOWN = 100;
</script>

<script lang="ts">
  let {
    shown,
    of,
    more,
    what,
    onshowall,
  }: {
    /** The line's form: how many it shows. */
    shown?: number;
    /** What `shown` is out of, as the line names it: "340", or "the 120 fields that hold “ip”". */
    of?: string;
    /** The inline form: how many are not shown. */
    more?: number;
    /** What the inline form's items are, singular, where the list does not say. */
    what?: string;
    onshowall: () => void;
  } = $props();
</script>

{#if more !== undefined}
  <button
    type="button"
    class="more"
    onclick={(e) => {
      // Showing the rest is all the click does, inside a row that a click would open or close.
      e.stopPropagation();
      onshowall();
    }}>{more} more{what ? ` ${more === 1 ? what : `${what}s`}` : ''}</button
  >
{:else}
  <div class="show-all">
    <span class="count">Showing {shown} of {of}</span>
    <button type="button" class="compact" onclick={onshowall}>Show all</button>
  </div>
{/if}

<style>
  .show-all {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .count {
    color: var(--dim);
    font-size: var(--text-note);
  }

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
