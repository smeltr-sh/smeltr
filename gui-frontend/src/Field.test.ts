// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRawSnippet, mount, unmount } from 'svelte';
import Field from './Field.svelte';
import IdentityPicker from './IdentityPicker.svelte';
import { setScope } from './api';
import { load, signedOut } from './session';

let field: ReturnType<typeof mount> | undefined;
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

function input(): HTMLInputElement {
  field = mount(Field, {
    target,
    props: {
      label: 'id',
      children: createRawSnippet(() => ({ render: () => '<input type="text" />' })),
    },
  });
  return target.querySelector('input')!;
}

beforeEach(() => {
  target = document.createElement('div');
  document.body.append(target);
  setScope('payments');
});

afterEach(() => {
  if (field) unmount(field);
  field = undefined;
  target.remove();
  signedOut();
  setScope('shared');
  vi.unstubAllGlobals();
});

// A disabled fieldset disables what it holds, in the browser; the test DOM does not apply that, so
// the tests look for the fieldset.
describe('a field where the scope is not yours to change', () => {
  it('is read-only on an authoring page for a viewer', async () => {
    await signInAs({ shared: 'viewer', payments: 'viewer' });
    history.replaceState(null, '', '/#/build/sources');
    expect(input().closest('fieldset[disabled]')).not.toBeNull();
  });

  it('keeps its name on its control, with the lock around both', async () => {
    await signInAs({ shared: 'viewer', payments: 'viewer' });
    history.replaceState(null, '', '/#/build/sources');
    const label = input().closest('label')!;
    expect(label.textContent).toContain('id');
    expect(label.querySelector('fieldset')).toBeNull();
    expect(label.closest('fieldset[disabled]')).not.toBeNull();
  });

  it('is locked once, inside a component that holds it', async () => {
    await signInAs({ shared: 'viewer', payments: 'viewer' });
    history.replaceState(null, '', '/#/build/sources');
    field = mount(IdentityPicker, {
      target,
      props: { value: undefined, identities: [], farEnd: 'source', onchange: () => {} },
    });
    expect(target.querySelectorAll('fieldset')).toHaveLength(1);
    expect(target.querySelector('select')!.closest('fieldset[disabled]')).not.toBeNull();
  });

  it('is open to an editor', async () => {
    await signInAs({ shared: 'viewer', payments: 'editor' });
    history.replaceState(null, '', '/#/build/sources');
    expect(input().closest('fieldset[disabled]')).toBeNull();
  });

  it('is open off the authoring pages, where no scope applies', async () => {
    await signInAs({ shared: 'viewer', payments: 'viewer' });
    history.replaceState(null, '', '/#/changes');
    expect(input().closest('fieldset[disabled]')).toBeNull();
  });
});
