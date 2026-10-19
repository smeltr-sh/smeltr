<!--
  The website demo's strip: the shell draws it on every page of the demo build (`vite.demo.config.ts`), so no visitor
  takes the recorded fleet for a live one.
-->
<script lang="ts">
  import { onMount } from 'svelte';
  import Strip from './Strip.svelte';
  import DemoGuide from './demo/DemoGuide.svelte';
  import { WAKING_EVENT, waking } from './demo/leader';

  let asleep = $state(waking.now);
  onMount(() => {
    const heard = () => (asleep = waking.now);
    heard();
    window.addEventListener(WAKING_EVENT, heard);
    return () => window.removeEventListener(WAKING_EVENT, heard);
  });
</script>

<Strip>
  <div class="strip-row">
    <span>Demo: a recorded fleet, running in your browser. Your changes stay in this tab.</span>
    {#if asleep}
      <span class="waking" role="status"
        >Waking the demo up. Some pages wait for it, which can take up to two minutes.</span
      >
    {/if}
    <a class="get" href="https://smeltr.sh/get/" target="_blank" rel="noopener">Get smeltr</a>
    <DemoGuide />
  </div>
</Strip>

<style>
  .waking {
    font-weight: 600;
  }

  .get {
    margin-left: auto;
    font-weight: 600;
  }
</style>
