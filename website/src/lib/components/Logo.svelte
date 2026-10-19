<script lang="ts">
  // The mark from `gui-frontend`'s STYLE_GUIDE.md: a pour of molten metal,
  // cut with a droplet in the current background color so it reads as
  // liquid. Both fills are theme tokens, so it re-themes wherever it lands.
  let { size = 20 }: { size?: number } = $props();
</script>

<svg viewBox="0 0 64 64" width={size} height={size} fill="none" aria-hidden="true">
  <path
    d="M32 8 C42 24 48 34 48 42 A16 16 0 1 1 16 42 C16 34 22 24 32 8 Z"
    style="fill: var(--accent)"
  />
  <path
    d="M32 22 C37 30 40 35 40 40 A8 8 0 1 1 24 40 C24 35 27 30 32 22 Z"
    style="fill: var(--bg)"
  />
</svg>
