// @vitest-environment happy-dom
import { afterEach, describe, expect, it } from 'vitest';
import { mount, unmount } from 'svelte';
import StateMark, { severityState, type State } from './StateMark.svelte';

let mark: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;

function draw(props: { state: State; label?: string }): SVGSVGElement {
  target = document.createElement('div');
  document.body.append(target);
  mark = mount(StateMark, { target, props });
  return target.querySelector('svg')!;
}

afterEach(() => {
  if (mark) unmount(mark);
  mark = undefined;
  target?.remove();
});

describe('StateMark', () => {
  it('draws each state as its own shape', () => {
    const shapes = (['info', 'ok', 'warning', 'error'] as const).map((state) => {
      const svg = draw({ state });
      const d = svg.querySelector('path')!.getAttribute('d');
      unmount(mark!);
      mark = undefined;
      target.remove();
      return d;
    });
    expect(new Set(shapes).size).toBe(4);
  });

  it('is hidden from a screen reader unless it is labelled', () => {
    expect(draw({ state: 'warning' }).getAttribute('aria-hidden')).toBe('true');
    unmount(mark!);
    mark = undefined;
    target.remove();
    const labelled = draw({ state: 'error', label: 'failed' });
    expect(labelled.getAttribute('role')).toBe('img');
    expect(labelled.getAttribute('aria-label')).toBe('failed');
    expect(labelled.getAttribute('aria-hidden')).toBeNull();
  });

  it('signs a failed issue as an error', () => {
    expect(severityState('failed')).toBe('error');
    expect(severityState('warning')).toBe('warning');
    expect(severityState('info')).toBe('info');
  });
});
