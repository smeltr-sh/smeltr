// @vitest-environment happy-dom
import { afterEach, expect, it, vi } from 'vitest';
import { flushSync, mount, tick, unmount } from 'svelte';
import NewDestination from './NewDestination.svelte';
import { NO_KAFKA_FACTS, type DestinationDto } from './api';

let shown: ReturnType<typeof mount> | undefined;
// Its fields read the leader's catalogs (certificates and the like), which no test here answers.
vi.stubGlobal('fetch', () => new Promise(() => {}));

afterEach(() => {
  if (shown) unmount(shown);
  shown = undefined;
  document.body.innerHTML = '';
});

const original = (): DestinationDto => ({
  id: 'alerts',
  type: 'kafka',
  brokers: 'k1:9093,k2:9093',
  defaultTopic: 'alerts',
  compression: 'zstd',
  accepts: ['ecs'],
  exceptions: { kinds: { Refused: 'drop' } },
  properties: { 'security.protocol': 'SASL_SSL', 'sasl.username': 'smeltr' },
  credentials: { 'sasl.password': { secret: 'kafka-sasl', version: 1 } },
});

it('opens a duplicate on its fields, every one as the original but a free id, and adds that', async () => {
  const from = original();
  const onadd = vi.fn();
  const target = document.body.appendChild(document.createElement('div'));
  shown = mount(NewDestination, {
    target,
    props: {
      open: true,
      from,
      unavailable: ['alerts', 'alerts-2'],
      secrets: [],
      kafka: NO_KAFKA_FACTS,
      onadd,
      oncancel: () => {},
    },
  });
  flushSync();
  const id = document.querySelector<HTMLInputElement>('input.mono')!;
  expect(id.value).toBe('alerts-3');
  // No way back to a catalog it never showed.
  expect(document.querySelector('[aria-label="Back"]')).toBeNull();

  document.querySelector<HTMLFormElement>('form')!.requestSubmit();
  for (let i = 0; i < 3; i++) await tick();
  expect(onadd).toHaveBeenCalledWith({ ...original(), id: 'alerts-3' });
  // The copy is its own: the original is untouched.
  expect(from).toEqual(original());
});
