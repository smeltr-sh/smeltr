<!--
  The logo and wordmark (STYLE_GUIDE.md, "Logo") at the head of the band, on
  every screen: the app's nav and the screens before it. It always sits on the
  band, so it takes the band's surface and ink, and the cutout shows the band.
-->
<span class="brand">
  <svg viewBox="0 0 64 64" width="22" height="22" fill="none" aria-hidden="true">
    <path
      d="M32 8 C42 24 48 34 48 42 A16 16 0 1 1 16 42 C16 34 22 24 32 8 Z"
      style="fill: var(--accent)"
    />
    <path
      d="M32 22 C37 30 40 35 40 40 A8 8 0 1 1 24 40 C24 35 27 30 32 22 Z"
      style="fill: var(--bg)"
    />
  </svg>
  smeltr
</span>

<style>
  .brand {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-family: var(--font-display);
    font-size: var(--text-title);
    font-weight: 600;
    letter-spacing: -0.025em;
    --bg: var(--band-bg);
    color: var(--band-ink);
  }
</style>
