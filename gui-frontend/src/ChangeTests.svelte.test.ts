// @vitest-environment happy-dom
import { expect, it } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import ChangeTests from './ChangeTests.svelte';

it('titles a reached route by its name, or its id where the leader names none', async () => {
  const target = document.body.appendChild(document.createElement('div'));
  const routes = ['r-1', 'r-2'].map((id) => ({ id, reasons: [{ type: 'edited' as const }] }));
  const shown = mount(ChangeTests, {
    target,
    props: {
      branch: 'b',
      touchedByScope: [
        { area: 'g', pipelines: [], routes, routeNames: { 'r-1': 'fortigate \u2192 fortigate' } },
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
  expect(titles).toEqual(['fortigate \u2192 fortigate', 'r-2']);
  await unmount(shown);
});
