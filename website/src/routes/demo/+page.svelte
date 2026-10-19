<script lang="ts">
  import { onMount } from 'svelte';

  // The demo is the GUI itself, served at /demo/try by the preview Worker (`preview/`), with its own guide.
  const DEMO = '/demo/try/';
  onMount(() => window.location.replace(DEMO));
</script>

<svelte:head>
  <title>Demo — smeltr</title>
  <meta name="robots" content="noindex" />
</svelte:head>

<div class="marketing">
  <section class="page-hero">
    <a class="btn large primary" href={DEMO} rel="external">Try smeltr in your browser →</a>
  </section>
</div>
