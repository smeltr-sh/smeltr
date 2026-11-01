<!--
  A column of rows, each naming a place on the page and taking the author
  there when clicked: the steps a refused save or a failed preview points at.
-->
<script lang="ts">
  import Ticked from './Ticked.svelte';

  let { links }: { links: { label: string; onClick: () => void }[] } = $props();
</script>

<div class="links">
  {#each links as link}
    <button type="button" class="link-row" onclick={link.onClick}>
      <span class="link-label"><Ticked text={link.label} /></span>
      <span class="link-go">Jump &rarr;</span>
    </button>
  {/each}
</div>

<style>
  .links {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    margin-top: 0.6rem;
  }

  .link-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.45rem 0.6rem;
    color: var(--ink);
    font: inherit;
    text-align: left;
    cursor: pointer;
  }

  .link-label {
    flex: 1;
    font-size: var(--text-note);
  }

  .link-go {
    font-size: var(--text-note);
    font-weight: 600;
    color: var(--dim);
  }
</style>
