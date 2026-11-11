/**
 * Gives every code block under `node` a Copy button that puts the block's text on the clipboard. The docs pages
 * write their blocks as plain `<pre>`, so one action on the docs layout reaches all of them. Without clipboard
 * access the button selects the block, so the reader can copy it by hand.
 */
export function copyButtons(node: HTMLElement) {
  for (const pre of node.querySelectorAll('pre')) {
    // A bar before the block, drawn over its top corner: inside the block the button would scroll away with a
    // wide line, and the page's own nodes stay where Svelte put them.
    const bar = document.createElement('div');
    bar.className = 'copy-bar';
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'compact';
    button.textContent = 'Copy';
    button.setAttribute('aria-label', 'Copy this code');
    // A screen reader hears the button by its fixed label, so this region tells it the code was copied.
    const live = document.createElement('span');
    live.className = 'copy-live';
    live.setAttribute('aria-live', 'polite');
    let timer = 0;
    button.addEventListener('click', async () => {
      try {
        await navigator.clipboard.writeText(pre.innerText.replace(/\n$/, ''));
      } catch {
        getSelection()?.selectAllChildren(pre);
        return;
      }
      button.textContent = live.textContent = 'Copied';
      clearTimeout(timer);
      timer = window.setTimeout(() => {
        button.textContent = 'Copy';
        live.textContent = '';
      }, 1500);
    });
    bar.append(button, live);
    pre.before(bar);
  }
}
