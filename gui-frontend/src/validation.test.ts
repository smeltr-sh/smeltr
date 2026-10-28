import { describe, expect, it, vi } from 'vitest';
import { Validation } from './validation.svelte';
import { errors } from './errors.svelte';
import type { ApiError, MutationResult } from './api';

const error = (message: string, more: Partial<ApiError> = {}): ApiError => ({
  pipeline: null,
  step: null,
  function: null,
  assignmentIndex: null,
  item: null,
  field: null,
  rule: null,
  message,
  ...more,
});
const on = (section: string, id: string, field: string | null, message: string) =>
  error(message, { item: { section, id }, field });
const refused = (...errors: ApiError[]): MutationResult => ({ ok: false, errors });

describe('a save through a validation', () => {
  it('places its own section’s field errors and leaves the rest to the error bar', async () => {
    const v = new Validation('destinations');
    const answer = refused(
      on('destinations', 'es', 'url', 'no URL'),
      on('destinations', 'es', null, 'more than one credential'),
      on('sources', 'es', 'url', 'a source’s'),
      error('a route names nothing'),
    );
    // As `apiJson` reports a refusal before the page reads it.
    errors.report(
      errors.call('PUT', '/api/areas/shared/destinations', 'bar'),
      answer.errors,
      answer.errors,
    );
    await v.save(async () => answer);
    expect(v.message('es', 'url')).toBe('no URL');
    const [entry] = errors.entries;
    expect(entry.placed?.().map((p) => p.message)).toEqual(['no URL']);
    expect(entry.errors.map((e) => e.message)).toEqual([
      'more than one credential',
      'a source’s',
      'a route names nothing',
    ]);
  });

  it('starts each save clear', async () => {
    const v = new Validation('sources');
    await v.save(async () => refused(on('sources', 'k', 'brokers', 'no brokers')));
    await v.save(async () => ({ ok: true, errors: [] }));
    expect(v.placed).toEqual([]);
  });

  it('drops an error about a field edited while the save was out', async () => {
    const v = new Validation('sources');
    let answer!: (res: MutationResult) => void;
    const saving = v.save(() => new Promise<MutationResult>((resolve) => (answer = resolve)));
    v.edited('k', 'brokers');
    answer(
      refused(
        on('sources', 'k', 'brokers', 'no brokers'),
        on('sources', 'k', 'topics', 'no topics'),
      ),
    );
    await saving;
    expect(v.message('k', 'brokers')).toBeNull();
    expect(v.message('k', 'topics')).toBe('no topics');
  });

  it('still refuses when every error it placed was edited away', async () => {
    const v = new Validation('sources');
    let answer!: (res: MutationResult) => void;
    const saving = v.save(() => new Promise<MutationResult>((resolve) => (answer = resolve)));
    v.edited('k', 'brokers');
    answer(refused(on('sources', 'k', 'brokers', 'no brokers')));
    const res = await saving;
    expect(v.placed).toEqual([]);
    expect(res.errors.map((e) => e.message)).toEqual(['no brokers']);
  });

  it('moves an item’s errors to its new id when it is renamed', async () => {
    const v = new Validation('sources');
    await v.save(async () => refused(on('sources', 'k', 'brokers', 'no brokers')));
    v.renamed('k', 'k2');
    expect(v.message('k2', 'brokers')).toBe('no brokers');
  });

  it('moves the errors of an item renamed while the save was out', async () => {
    const v = new Validation('sources');
    let answer!: (res: MutationResult) => void;
    const saving = v.save(() => new Promise<MutationResult>((resolve) => (answer = resolve)));
    v.renamed('k', 'k2');
    answer(refused(on('sources', 'k', 'brokers', 'no brokers')));
    await saving;
    expect(v.message('k2', 'brokers')).toBe('no brokers');
  });

  it('clears a field’s errors when it is edited', async () => {
    const v = new Validation('sources');
    await v.save(async () => refused(on('sources', 'k', 'brokers', 'no brokers')));
    v.edited('k', 'brokers');
    expect(v.placed).toEqual([]);
  });

  it('drops just the errors it is given', async () => {
    const v = new Validation('routes');
    const when = (rule: number) =>
      error(`rule ${rule}`, { item: { section: 'routes', id: 'r' }, field: 'deliver.when', rule });
    await v.save(async () => refused(when(0), when(1)));
    expect(v.answered).toBe(1);
    v.drop([v.placed[0]]);
    expect(v.placed.map((e) => e.message)).toEqual(['rule 1']);
  });

  it('keeps a route’s rules apart by their position', async () => {
    const v = new Validation('routes');
    const when = (rule: number, message: string) =>
      error(message, { item: { section: 'routes', id: 'r' }, field: 'deliver.when', rule });
    await v.save(async () => refused(when(0, 'first'), when(1, 'second')));
    expect(v.message('r', 'deliver.when', 1)).toBe('second');
    v.edited('r', 'deliver.when', 0);
    expect(v.message('r', 'deliver.when', 0)).toBeNull();
    expect(v.message('r', 'deliver.when', 1)).toBe('second');
  });
});

describe('an error whose field is not on screen', () => {
  it('opens its item, then what the item mounts, until the field is there', async () => {
    const v = new Validation('destinations');
    const focus = vi.fn();
    // The card mounts the disclosure; the disclosure mounts the field.
    v.opener('es', () => {
      v.opener('es', () => v.mount('es', 'bufferBytes', focus));
    });
    await v.save(async () => refused(on('destinations', 'es', 'bufferBytes', 'too small')));
    await vi.waitFor(() => expect(focus).toHaveBeenCalledOnce());
  });

  it('stops when nothing more opens', async () => {
    const v = new Validation('destinations');
    const open = vi.fn();
    v.opener('es', open);
    await v.focus(on('destinations', 'es', 'nowhere', 'x') as Parameters<Validation['focus']>[0]);
    expect(open).toHaveBeenCalledOnce();
  });
});
