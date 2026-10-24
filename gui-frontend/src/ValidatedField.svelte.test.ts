// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { flushSync, mount, tick, unmount } from 'svelte';

const formatCode = vi.fn();
vi.mock('./api', () => ({ formatCode }));

const { default: ValidatedField } = await import('./ValidatedField.svelte');

let field: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;
const bound = $state({ value: '' });

function show(props: {
  autocomplete: 'condition' | 'code' | null;
  multiline?: boolean;
  errors?: string[];
  warnings?: string[];
}) {
  field = mount(ValidatedField, {
    target,
    props: {
      get value() {
        return bound.value;
      },
      set value(v) {
        bound.value = v;
      },
      ...props,
    },
  });
  return target.querySelector<HTMLInputElement | HTMLTextAreaElement>('input, textarea')!;
}

async function leave(input: HTMLElement) {
  input.dispatchEvent(new FocusEvent('blur'));
  for (let i = 0; i < 3; i++) await tick();
  flushSync();
}

beforeEach(() => {
  target = document.createElement('div');
  document.body.append(target);
  formatCode.mockReset();
});

afterEach(() => {
  if (field) unmount(field);
  field = undefined;
  target.remove();
});

describe('leaving a code field', () => {
  it('lays the code out as the leader does', async () => {
    bound.value = 'a&&b';
    formatCode.mockResolvedValue({ ok: true, errors: [], code: 'a && b' });
    await leave(show({ autocomplete: 'condition' }));
    expect(formatCode).toHaveBeenCalledWith('a&&b', 'expr');
    expect(bound.value).toBe('a && b');
  });

  it('sends a code body as a block', async () => {
    bound.value = 'x=1';
    formatCode.mockResolvedValue({ ok: true, errors: [], code: 'x = 1;' });
    await leave(show({ autocomplete: 'code', multiline: true }));
    expect(formatCode).toHaveBeenCalledWith('x=1', 'block');
    expect(bound.value).toBe('x = 1;');
  });

  it('keeps text that does not parse yet', async () => {
    bound.value = 'a &&';
    formatCode.mockResolvedValue({ ok: true, errors: [], code: null });
    await leave(show({ autocomplete: 'condition' }));
    expect(bound.value).toBe('a &&');
  });

  it('keeps a one-line field on one line', async () => {
    bound.value = 'f(x => {x})';
    formatCode.mockResolvedValue({ ok: true, errors: [], code: 'f(x => {\n  x\n})' });
    await leave(show({ autocomplete: 'condition' }));
    expect(bound.value).toBe('f(x => {x})');
  });

  it('leaves a field that is not code alone', async () => {
    bound.value = 'a&&b';
    await leave(show({ autocomplete: null }));
    expect(formatCode).not.toHaveBeenCalled();
  });
});

describe('a name box a warning names', () => {
  it('marks all of its text', () => {
    bound.value = 'x';
    const input = show({ autocomplete: null, warnings: ['`x` is written again before it is read'] });
    expect(input.classList.contains('whole-warn')).toBe(true);
  });

  it('is left alone by a warning about another name', () => {
    bound.value = 'y';
    const input = show({ autocomplete: null, warnings: ['`x` is written again before it is read'] });
    expect(input.classList.contains('whole-warn')).toBe(false);
  });

  it('shows an error that names it instead', () => {
    bound.value = 'x';
    const input = show({
      autocomplete: null,
      errors: ['unknown field `x`'],
      warnings: ['`x` is written again before it is read'],
    });
    expect(input.classList.contains('whole-warn')).toBe(false);
    expect(input.classList.contains('field-error')).toBe(true);
  });
});
