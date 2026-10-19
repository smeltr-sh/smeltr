<!--
  Rows of map objects, each a link that opens where it points: an object's name, a note in its
  level's color, and a line under them. Shared by Needs attention (`MapPage.svelte`) and a
  stack's members (`StackView.svelte`), so the two lists read alike.
-->
<script module lang="ts">
  export type MapRow = {
    key: string;
    href: string;
    subject: string;
    note?: string;
    level?: 'alarm' | 'warn' | 'dim';
    under?: string;
  };
</script>

<script lang="ts">
  let { rows, onpick }: { rows: MapRow[]; onpick?: () => void } = $props();
</script>

<ul>
  {#each rows as r (r.key)}
    <li>
      <a href={r.href} onclick={onpick}>
        <span class="mono">{r.subject}</span>
        <span class="note {r.level ?? 'dim'}">{r.note ?? ''}</span>
        {#if r.under}<span class="dim under">{r.under}</span>{/if}
      </a>
    </li>
  {/each}
</ul>

<style>
  ul {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  a {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 0.1rem 0.6rem;
    padding: 0.55rem 0.1rem;
    border-top: 1px solid var(--border);
    font-size: var(--text-note);
    color: var(--ink);
    text-decoration: none;
  }

  a:hover {
    background: color-mix(in srgb, var(--ink) 6%, var(--panel));
  }

  .mono {
    font-family: var(--mono);
  }

  .dim,
  .note.dim {
    color: var(--dim);
  }

  .note.alarm {
    color: var(--error);
  }

  .note.warn {
    color: var(--warn);
  }

  .under {
    grid-column: 1 / -1;
    font-size: var(--text-micro);
  }
</style>
