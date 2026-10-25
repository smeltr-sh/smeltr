<!--
  Which smeltr this leader runs: `v1.2.0` for a release, the whole build
  string for any other build, and a newer release when one is out and
  `newerHref` says where it leads. `build` adds the build string behind a
  `?` for a release, where the number alone does not name the commit.
-->
<script lang="ts">
  import Help from './Help.svelte';
  import type { SystemInfoDto } from './api';

  let {
    info,
    build = false,
    newerHref,
  }: { info: SystemInfoDto; build?: boolean; newerHref?: string } = $props();

  // A page of this app opens in place; anywhere else, in a tab of its own.
  const outside = $derived(newerHref !== undefined && !newerHref.startsWith('#'));
</script>

<span class="version">
  {#if info.release}
    <span>v{info.release}</span>
    {#if build}
      <Help label="Build"><code>{info.smeltrVersion}</code></Help>
    {/if}
  {:else}
    <code>{info.smeltrVersion}</code>
  {/if}
  {#if newerHref && info.newerRelease}
    <a
      href={newerHref}
      target={outside ? '_blank' : undefined}
      rel={outside ? 'noopener' : undefined}>v{info.newerRelease.release} available</a
    >
  {/if}
</span>

<style>
  .version {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  code {
    font-family: var(--mono);
  }
</style>
