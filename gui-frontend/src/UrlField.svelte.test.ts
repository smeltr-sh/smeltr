// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import UrlField from './UrlField.svelte';

let field: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
const bound = $state<{ value: string | undefined }>({ value: undefined });

function show(props: { sends: boolean; optional?: boolean; instead?: string; error?: string }) {
  field = mount(UrlField, {
    target,
    props: {
      label: 'url',
      get value() {
        return bound.value;
      },
      set value(v) {
        bound.value = v;
      },
      ...props,
    },
  });
  return target.querySelector('input')!;
}

function type(input: HTMLInputElement, text: string) {
  input.value = text;
  input.dispatchEvent(new Event('input', { bubbles: true }));
  flushSync();
}

const said = () => target.querySelector('[role="status"]')?.textContent ?? null;

beforeEach(() => {
  target = document.createElement('div');
  document.body.append(target);
  bound.value = undefined;
});

afterEach(() => {
  if (field) unmount(field);
  field = undefined;
  target.remove();
});

describe('a URL field', () => {
  it('warns when a credential would cross the network unencrypted, and not otherwise', () => {
    const input = show({ sends: true });
    type(input, 'http://collector.internal');
    expect(bound.value).toBe('http://collector.internal');
    expect(said()).toBe('Its credentials will cross the network unencrypted.');
    type(input, 'https://collector.internal');
    expect(said()).toBeNull();
  });

  it('recommends where this form names a user and password', () => {
    const input = show({ sends: false, instead: 'Put them in the user and token fields.' });
    type(input, 'https://me:pw@git.example');
    expect(said()).toBe('Put them in the user and token fields.');
  });

  it('shows a refusal in place of a warning', () => {
    bound.value = 'http://collector.internal';
    show({ sends: true, error: 'no' });
    expect(target.querySelector('[role="alert"]')?.textContent).toBe('no');
    expect(said()).toBeNull();
  });

  it('unsets an optional field when cleared, and keeps an empty one otherwise', () => {
    let input = show({ sends: true, optional: true });
    type(input, ' ');
    expect(bound.value).toBeUndefined();
    unmount(field!);
    input = show({ sends: true });
    type(input, '');
    expect(bound.value).toBe('');
  });
});
