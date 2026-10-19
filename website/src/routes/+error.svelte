<script lang="ts">
  import { page } from '$app/state';
</script>

<svelte:head><title>{page.status} — smeltr</title></svelte:head>

<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow">{page.status}</p>
    <h1>{page.status === 404 ? 'That page is not here.' : 'Something went wrong.'}</h1>
    <p class="lede">
      {#if page.status === 404}
        The site is a preview and its pages move. The navigation above reaches everything it has.
      {:else}
        {page.error?.message ?? 'Try again, or start from the beginning.'}
      {/if}
    </p>
    <div class="actions">
      <a class="btn large primary" href="/">Back to the start</a><a class="btn large" href="/docs"
        >Read the documentation</a
      >
    </div>
  </section>
</div>
