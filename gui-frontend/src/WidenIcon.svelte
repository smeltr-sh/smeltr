<!--
  Widen: spreads what it sits on across the page; with `open` it draws narrowing back
  (STYLE_GUIDE.md, Glyphs). Two arrows out to opposite corners, or in from them. Drawn on the logo's
  64-unit grid as one path in `currentColor`, 18px, the size a compact icon control gets.
-->
<script lang="ts">
  let { open = false }: { open?: boolean } = $props();
</script>

<svg viewBox="0 0 64 64" width="18" height="18" aria-hidden="true">
  {#if open}
    <path
      d="M28 36 V54 H22 V46 L10 58 L6 54 L18 42 H10 V36 Z M36 28 V10 H42 V18 L54 6 L58 10 L46 22 H54 V28 Z"
      fill="currentColor"
    />
  {:else}
    <path
      d="M6 58 V36 H12 V48 L26 34 L30 38 L16 52 H28 V58 Z M58 6 V28 H52 V16 L38 30 L34 26 L48 12 H36 V6 Z"
      fill="currentColor"
    />
  {/if}
</svg>
