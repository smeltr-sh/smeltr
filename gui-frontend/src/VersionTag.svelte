<!--
  Which smeltr this leader runs, at the end of the band's tabs row on every
  screen. A release reads as `v1.2.0`; any other build shows its whole build
  string, since it has no shorter name. A newer release is offered to an
  administrator only, as a link to Admin -> System -> General, where the
  check for it is set.
-->
<script lang="ts">
  import { getSystemInfo, type SystemInfoDto } from './api';

  let { administrator }: { administrator: boolean } = $props();

  let info = $state<SystemInfoDto | null>(null);

  $effect(() => {
    getSystemInfo()
      .then((next) => (info = next))
      // A label, not a page: without an answer it stays empty, and whatever
      // the person was doing reports the failure itself.
      .catch(() => {});
  });
</script>

{#if info}
  <span class="version-tag">
    {#if info.release}
      <span>v{info.release}</span>
    {:else}
      <code>{info.smeltrVersion}</code>
    {/if}
    {#if administrator && info.newerRelease}
      <a href="#/admin/system/general">v{info.newerRelease.release} available</a>
    {/if}
  </span>
{/if}

<style>
  /* On the band, so it takes the band's ink as the band's own chips do. */
  .version-tag {
    --bg: var(--band-bg);
    --ink: var(--band-ink);
    --dim: var(--band-dim);
    --border: var(--band-line);
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-left: auto;
    /* On the tabs' line, not centered on their underline. */
    align-self: baseline;
    /* Its text ends where the identity chip's above it does, inside that chip's padding and border. */
    padding-right: calc(0.6rem + 1px);
    color: var(--dim);
    font-size: var(--text-note);
  }

  code {
    font-family: var(--mono);
  }
</style>
