// @vitest-environment happy-dom
import { expect, it } from 'vitest';
import { createRawSnippet, flushSync, mount, unmount } from 'svelte';
import ReceiverSourcetypes from './ReceiverSourcetypes.svelte';

const help = createRawSnippet(() => ({ render: () => '<span>help</span>' }));

it('writes a blank line breaker it was given as none, before any edit, so a save does not send it', () => {
  const value = {
    sourcetypes: { blank: { lineBreaker: ' ' }, set: { lineBreaker: '(\\n)' }, none: {} },
  };
  const form = mount(ReceiverSourcetypes, { target: document.body, props: { value, help } });
  flushSync();
  unmount(form);
  expect(value.sourcetypes).toEqual({ blank: {}, set: { lineBreaker: '(\\n)' }, none: {} });
});
