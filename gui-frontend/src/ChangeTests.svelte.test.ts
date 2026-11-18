// @vitest-environment happy-dom
import { afterEach, describe, expect, it } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import ChangeTests from './ChangeTests.svelte';

let shown: ReturnType<typeof mount> | undefined;

afterEach(async () => {
  if (shown) await unmount(shown);
  shown = undefined;
  document.body.innerHTML = '';
});

describe('a reached route', () => {
  it('goes by its name, or by its id where the leader names none', () => {
    const target = document.body.appendChild(document.createElement('div'));
    shown = mount(ChangeTests, {
      target,
      props: {
        branch: 'b',
        touchedByScope: [
          {
            area: 'firewalls',
            pipelines: [],
            routes: [
              { id: 'route-cuqg6cce', reasons: [{ type: 'sourceChanged', source: 'fortigate' }] },
              { id: 'route-unnamed', reasons: [{ type: 'edited' }] },
            ],
            routeNames: { 'route-cuqg6cce': 'fortigate → fortigate' },
          },
        ],
        testResults: [],
        testSummaryByKey: new Map(),
        testsLoading: false,
        testsError: '',
        pipelineName: (_scope: string, id: string) => id,
      },
    });
    flushSync();
    const titles = [...target.querySelectorAll('.pipeline-id')].map((e) => e.textContent);
    expect(titles).toEqual(['fortigate → fortigate', 'route-unnamed']);
  });
});
