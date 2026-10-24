import type { Attachment } from 'svelte/attachments';

/** A bar held over the page at its `edge`: the window keeps what it scrolls to, as a field it focuses, out from
 * under the bar, and a pane that fits itself to the window reads the bar's height from `--clear-top` or
 * `--clear-bottom`. */
export function keepClear(edge: 'top' | 'bottom'): Attachment<HTMLElement> {
  return (bar) => {
    const root = document.documentElement.style;
    const property = edge === 'top' ? 'scroll-padding-top' : 'scroll-padding-bottom';
    const height = `--clear-${edge}`;
    const observer = new ResizeObserver(() => {
      root.setProperty(property, `${bar.offsetHeight}px`);
      root.setProperty(height, `${bar.offsetHeight}px`);
    });
    observer.observe(bar);
    return () => {
      observer.disconnect();
      root.removeProperty(property);
      root.removeProperty(height);
    };
  };
}
