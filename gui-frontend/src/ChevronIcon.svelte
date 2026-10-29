<!--
  Fold: points right while what it folds is closed, down while it is open (STYLE_GUIDE.md, Glyphs) -- the logo's
  64-unit grid, one path, `currentColor`. It is 1em, so it takes the size of the text it stands in, and sits on that
  text's line, as in "▸ Rotate authority".
-->
<script lang="ts">
  let { open = false }: { open?: boolean } = $props();
</script>

<svg
  viewBox="0 0 64 64"
  width="1em"
  height="1em"
  class:open
  style="vertical-align: -0.125em"
  aria-hidden="true"
>
  <path d="M20 14 L50 32 L20 50 Z" fill="currentColor" />
</svg>

<style>
  svg {
    flex: none;
    transition: transform 0.12s ease;
  }

  .open {
    transform: rotate(90deg);
  }
</style>
