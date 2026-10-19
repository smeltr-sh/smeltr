<!--
  The link from an object `shared/` declares to where it is changed: the same page, in `shared/`.
  `hash` is the page's own, so the link alone changes nothing; the scope switch mounts the page afresh
  under `shared/`.
-->
<script lang="ts">
  import { setScope, SHARED_SCOPE } from './api';

  let { hash }: { hash: string } = $props();
</script>

<a href={hash} onclick={() => setScope(SHARED_SCOPE)}>edit in shared/</a>
