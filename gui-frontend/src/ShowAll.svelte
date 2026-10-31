<!--
  The line under a list cut to its first rows: how many it shows of how many, and Show all.
-->
<script module lang="ts">
  /** Rows a long list shows before Show all: a browser takes seconds to lay out thousands. */
  export const SHOWN = 100;
</script>

<script lang="ts">
  let {
    shown,
    of,
    onshowall,
  }: {
    shown: number;
    /** What `shown` is out of, as the line names it: "340", or "the 120 fields that hold “ip”". */
    of: string;
    onshowall: () => void;
  } = $props();
</script>

<div class="show-all">
  <span class="count">{shown} of {of} shown</span>
  <button type="button" class="compact" onclick={onshowall}>Show all</button>
</div>

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
</style>
