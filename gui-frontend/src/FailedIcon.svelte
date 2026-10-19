<!--
  Failed: a row the pipeline could not process, wherever those rows are named or
  routed. One glyph, so one meaning (STYLE_GUIDE.md, Glyphs) -- the logo's
  64-unit grid, one path, `currentColor`, 18px, the size a compact icon control
  gets.

  A triangle with a bar standing in it, the conventional sign for something
  that went wrong. The triangle is a band (`evenodd`, the technique `LockIcon`
  and `PencilIcon` use) so the whole glyph stays one fill, and its base is
  heavier than its sides, as every horizontal bar here is. The bar inside
  widens toward its foot for the same reason. The caller owns the color: ink
  in a heading, `--warn` where the row's state is the point.
-->
<svg viewBox="0 0 64 64" width="18" height="18" aria-hidden="true">
  <path
    d="M32 6 L60 58 H4 Z M32 18 L50 50 H14 Z M29 24 H35 L36 38 H28 Z M28 42 H36 V49 H28 Z"
    fill="currentColor"
    fill-rule="evenodd"
  />
</svg>
