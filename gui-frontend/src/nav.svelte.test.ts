// @vitest-environment happy-dom
import { afterEach, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import { announcePipelineNames, PIPELINE_NAMES_EVENT, type PipelineNames } from './nav';

let cleanup: (() => void)[] = [];
afterEach(() => {
  for (const c of cleanup) c();
  cleanup = [];
});

it('announcing names from an effect does not loop through a listener that merges them', () => {
  // The shell's listener, as `App.svelte` keeps it: it reads its own names to merge in the new.
  const shell = $state<{ names: PipelineNames }>({ names: {} });
  const listener = (e: Event) => {
    shell.names = { ...shell.names, ...(e as CustomEvent<PipelineNames>).detail };
  };
  window.addEventListener(PIPELINE_NAMES_EVENT, listener);
  cleanup.push(() => window.removeEventListener(PIPELINE_NAMES_EVENT, listener));

  cleanup.push(
    $effect.root(() => {
      $effect(() => announcePipelineNames({ p1: 'panos-traffic' }));
    }),
  );
  flushSync();

  expect(shell.names).toEqual({ p1: 'panos-traffic' });
});
