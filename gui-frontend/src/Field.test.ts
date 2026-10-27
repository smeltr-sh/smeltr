// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRawSnippet, flushSync, mount, unmount } from 'svelte';
import Field from './Field.svelte';
import TlsFields from './TlsFields.svelte';
import { setScope } from './api';
import { load, signedOut } from './session';

let field: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;

async function signInAs(areas: Record<string, string>) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string) =>
      // The certificate picker asks for the list; everything else here is the session.
      String(url).includes('/api/certificates')
        ? new Response('[]', { headers: { 'content-type': 'application/json' } })
        : new Response(
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

function input(
  marks: { required?: boolean; optional?: boolean; help?: string } = {},
): HTMLInputElement {
  field = mount(Field, {
    target,
    props: {
      label: 'id',
      ...marks,
      children: createRawSnippet(() => ({ render: () => '<input type="text" />' })),
    },
  });
  flushSync();
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
    field = mount(TlsFields, { target, props: { value: {}, farEnd: 'sender' } });
    expect(target.querySelectorAll('fieldset')).toHaveLength(2);
    expect(target.querySelector('select')!.closest('fieldset[disabled]')).not.toBeNull();
    expect(target.querySelector('textarea')!.closest('fieldset[disabled]')).not.toBeNull();
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

describe('a required or optional field', () => {
  it('marks a required name with a star and its control with aria-required', () => {
    const control = input({ required: true, help: 'The name a route uses.' });
    expect(target.querySelector('.name.required')).not.toBeNull();
    expect(control.getAttribute('aria-required')).toBe('true');
    expect(target.querySelector('.pop')!.textContent).toBe('Required. The name a route uses.');
  });

  it("opens an optional field's help with Optional, and marks its name with nothing", () => {
    const control = input({ optional: true, help: 'The name a route uses.' });
    expect(target.querySelector('.name.required')).toBeNull();
    expect(control.hasAttribute('aria-required')).toBe(false);
    expect(target.querySelector('.pop')!.textContent).toBe('Optional. The name a route uses.');
  });

  it('leaves a field that is neither as it was', () => {
    input({ help: 'The name a route uses.' });
    expect(target.querySelector('.pop')!.textContent).toBe('The name a route uses.');
  });
});
