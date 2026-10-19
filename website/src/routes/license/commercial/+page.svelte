<script lang="ts">
  import Prose from '#lib/components/Prose.svelte';
  import { blocks } from '#lib/license.ts';
  import markdown from '../../../../../COMMERCIAL-LICENSE.md?raw';
</script>

<svelte:head
  ><title>Commercial license — smeltr</title><meta
    name="description"
    content="The paid license for production use of Smeltr where the Business Source License does not allow it."
  /></svelte:head
>

<div class="marketing">
  <section class="page-hero">
    <h1>Smeltr Commercial License</h1>
  </section>
  <article>
    {#each blocks(markdown) as block, i (i)}
      {#if block.kind === 'heading'}
        <h2><Prose text={block.text} /></h2>
      {:else if block.kind === 'list'}
        <ul>
          {#each block.items as item, j (j)}
            <li><Prose text={item} /></li>
          {/each}
        </ul>
      {:else}
        <p><Prose text={block.text} /></p>
      {/if}
    {/each}
  </article>
</div>

<style>
  h2 {
    margin-top: 3rem;
  }
  p,
  ul {
    max-width: 43rem;
  }
</style>
