// A branch switch writes its choice and reloads
// immediately after -- `location.reload()` fires a real `beforeunload`,
// which the pipeline editor's own handler can block, but only after the
// write already happened. Answering "stay" then leaves the write in place
// under a page that still shows the old context. This asks first, so the
// write never happens unless the reader means it to.

import { ask } from './Confirm.svelte';
import { heldLabel, listHeld } from './edits';

/** True once it is safe to switch context: nothing currently blocks a
 * real unload, or the reader confirmed losing what does. A synthetic
 * `beforeunload` dispatch triggers every registered handler exactly as a
 * real navigation would, without one actually starting. */
export async function confirmContextSwitch(): Promise<boolean> {
  const evt = new Event('beforeunload', { cancelable: true });
  window.dispatchEvent(evt);
  if (!evt.defaultPrevented) return true;
  // Named, not counted. The handler that answers now speaks for everything
  // held (`edits.ts`), including files the reader is not looking at, so a
  // question that only said "unsaved edits" would be one they cannot weigh.
  const files = listHeld().map(heldLabel);
  const ok = await ask({
    title: 'Unsaved edits',
    body:
      files.length > 0
        ? `Switching loses edits that have not been saved yet: ${files.join(', ')}.`
        : 'Switching loses edits that have not been saved yet.',
    action: 'Switch anyway',
    danger: true,
  });
  return ok ?? false;
}
