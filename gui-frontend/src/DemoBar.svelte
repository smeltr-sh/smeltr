<!--
  The website demo's strip: the shell draws it on every page of the demo build (`vite.demo.config.ts`), so no visitor
  takes the recorded fleet for a live one.
-->
<script lang="ts">
  import Strip from './Strip.svelte';
  import DemoGuide from './demo/DemoGuide.svelte';
</script>

<Strip>
  <div class="strip-row">
    <span>Demo: a recorded fleet, running in your browser. Your changes stay in this tab.</span>
    <a class="get" href="https://smeltr.sh/get/" target="_blank" rel="noopener">Get smeltr</a>
    <DemoGuide />
  </div>
</Strip>

<style>
  .get {
    margin-left: auto;
    font-weight: 600;
  }
</style>
