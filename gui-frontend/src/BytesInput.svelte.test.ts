// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import BytesInput from './BytesInput.svelte';
import type { ByteCount } from './bytes';

const MIB = 1024 * 1024;

let field: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
const held = $state<{ value: ByteCount | undefined }>({ value: undefined });

function show(auto = false) {
  field = mount(BytesInput, {
    target,
    props: {
      get value() {
        return held.value;
      },
      fallback: 64 * MIB,
      auto,
      onchange: (n: number | undefined) => (held.value = n),
    },
  });
  flushSync();
  return target.querySelector('input')!;
}

function type(input: HTMLInputElement, text: string) {
  input.value = text;
  input.dispatchEvent(new Event('input', { bubbles: true }));
  flushSync();
}

beforeEach(() => {
  target = document.createElement('div');
  document.body.append(target);
  held.value = undefined;
});

afterEach(() => {
  if (field) unmount(field);
  field = undefined;
  target.remove();
});

describe('a byte field', () => {
  it('shows an unset value as an empty box with the default as its hint', () => {
    const input = show();
    expect(input.value).toBe('');
    expect(input.placeholder).toBe('64 MiB');
  });

  it('hints auto for a figure the worker sizes', () => {
    const input = show(true);
    expect(input.value).toBe('');
    expect(input.placeholder).toBe('auto');
  });

  it('writes nothing until typed in, and unsets again when emptied', () => {
    const input = show(true);
    expect(held.value).toBeUndefined();
    type(input, '128 MiB');
    expect(held.value).toBe(128 * MIB);
    type(input, '');
    expect(held.value).toBeUndefined();
    expect(input.value).toBe('');
  });

  it('shows a stated value, and follows the store when it moves', () => {
    held.value = 32 * MIB;
    const input = show(true);
    expect(input.value).toBe('32 MiB');
    held.value = undefined;
    flushSync();
    expect(input.value).toBe('');
    held.value = 256 * MIB;
    flushSync();
    expect(input.value).toBe('256 MiB');
  });
});
