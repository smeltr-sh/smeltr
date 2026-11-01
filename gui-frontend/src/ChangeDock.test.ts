// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from 'vitest';
import { mount, unmount } from 'svelte';
import ChangeDock from './ChangeDock.svelte';

let dock: ReturnType<typeof mount> | undefined;
let target: HTMLDivElement;

afterEach(() => {
  if (dock) unmount(dock);
  dock = undefined;
  target?.remove();
});

describe('ChangeDock', () => {
  it('opens the tab that lists what a count counts', () => {
    const onopen = vi.fn();
    target = document.createElement('div');
    document.body.append(target);
    dock = mount(ChangeDock, {
      target,
      props: {
        reviews: { state: 'none' },
        tests: { total: 4, passed: 3, failed: 1, couldNotRun: 0 },
        testsLoading: false,
        behind: 0,
        schemaFindings: 2,
        onopen,
        bringingCurrent: false,
        bringCurrentDisabled: false,
        onbringcurrent: () => {},
        approvable: false,
        deploying: false,
        deployDisabledReason: '',
        ondeploy: () => {},
      },
    });
    const button = (text: string) =>
      [...target.querySelectorAll('button')].find((b) => b.textContent === text)!;
    button('1 of 4 tests failing').click();
    button('2 schema findings').click();
    expect(onopen.mock.calls).toEqual([['tests'], ['schemas']]);
  });
});
