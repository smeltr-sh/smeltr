// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRawSnippet, mount, unmount } from 'svelte';
import Locked from './Locked.svelte';
import { setScope } from './api';
import { load, signedOut } from './session';

let locked: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;

async function signInAs(areas: Record<string, string>) {
  vi.stubGlobal(
    'fetch',
    vi.fn(
      async () =>
        new Response(
          JSON.stringify({
            authenticated: true,
            userId: 'u_1',
            displayName: 'Lee Park',
            administrator: false,
            mustChangePassword: false,
            access: { administrator: false, areas },
          }),
          { headers: { 'content-type': 'application/json' } },
        ),
    ),
  );
  await load();
}

/** The controls a `Locked` holds, mounted on an authoring page. */
function controls(): HTMLElement[] {
  history.replaceState(null, '', '/#/build/sources');
  locked = mount(Locked, {
    target,
    props: {
      children: createRawSnippet(() => ({
        render: () => '<div><input type="text" /><select></select><button>Add</button></div>',
      })),
    },
  });
  return [...target.querySelectorAll<HTMLElement>('input, select, button')];
}

beforeEach(() => {
  target = document.createElement('div');
  document.body.append(target);
  setScope('payments');
});

afterEach(() => {
  if (locked) unmount(locked);
  locked = undefined;
  target.remove();
  signedOut();
  setScope('shared');
  vi.unstubAllGlobals();
});

/** Whether the browser holds `control` disabled. The test DOM does not carry a disabled fieldset's
 * state down to what it holds, so this asks what the browser does. */
const disabled = (control: HTMLElement): boolean =>
  control.matches(':disabled') || control.closest('fieldset[disabled]') !== null;

describe('what a page holds', () => {
  it('is disabled for a viewer', async () => {
    await signInAs({ shared: 'viewer', payments: 'viewer' });
    const held = controls();
    expect(held).toHaveLength(3);
    for (const c of held) expect(disabled(c)).toBe(true);
  });

  it('is open to an editor', async () => {
    await signInAs({ shared: 'viewer', payments: 'editor' });
    const held = controls();
    expect(held).toHaveLength(3);
    for (const c of held) expect(disabled(c)).toBe(false);
  });
});
