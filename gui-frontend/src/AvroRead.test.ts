import { describe, expect, it } from 'vitest';
import { filledFrom, fromSchema, setFormat, type Read } from './AvroRead.svelte';
import type { CaptureStatus } from './api';

describe("a source's format", () => {
  it('drops what Avro refuses, and starts a stream registry', () => {
    const d: Read = { encoding: 'utf-16le' };
    setFormat(d, 'avro', true);
    expect(d).toEqual({
      format: 'avro',
      schemaRegistry: { url: '' },
      fields: [],
    });
  });

  it('gives an object source no registry, and drops its framing', () => {
    const d: Read = { framing: 'lines', lineBreaker: '(\\n)' };
    setFormat(d, 'avro', false);
    expect(d).toEqual({ format: 'avro', fields: [] });
  });

  it('writes nothing for text, the default, and drops what only Avro takes', () => {
    const d: Read = {
      format: 'avro',
      schemaRegistry: { url: 'https://registry.example.com' },
      fields: [{ name: 'n', type: 'i64' }],
    };
    setFormat(d, 'text', true);
    expect(d).toEqual({});
  });

  it('keeps the fields already declared when Avro is chosen again', () => {
    const d: Read = { format: 'avro', fields: [{ name: 'n', type: 'i64' }] };
    setFormat(d, 'avro', false);
    expect(d.fields).toEqual([{ name: 'n', type: 'i64' }]);
  });
});

describe('fields a fill declares', () => {
  it('are the schema’s, in its order, keeping a type already declared', () => {
    const declared = [
      { name: 'gone', type: 'str' },
      { name: 'at', type: 'str' },
    ];
    const schema = [
      { name: 'id', avroType: 'long', suggested: 'i64' },
      { name: 'at', avroType: 'long (timestamp-millis)', suggested: 'datetime' },
      { name: 'tags', avroType: 'array<string>', suggested: 'array<str>' },
    ];
    expect(fromSchema(declared, schema)).toEqual([
      { name: 'id', type: 'i64' },
      { name: 'at', type: 'str' },
      { name: 'tags', type: 'array<str>' },
    ]);
  });
});

describe('what a finished fill declares', () => {
  const status = (over: Partial<CaptureStatus>): CaptureStatus => ({
    session: 's',
    done: true,
    stopped: false,
    maxMatches: 1,
    workers: [],
    batches: [],
    ended: [],
    secondsLeft: 0,
    waitingOn: [],
    truncated: false,
    ...over,
  });
  const row = (values: unknown[], failed: number[] = []) => ({ values, failed, worker: 'w2' });

  it('is the schema’s fields, from the worker that sent the message', () => {
    const fill = filledFrom(
      [],
      status({
        avro: [{ name: 'id', avroType: 'long', suggested: 'i64' }],
        batches: [{ columns: [], rows: [row([1])] }],
      }),
    );
    expect(fill).toEqual({ fields: [{ name: 'id', type: 'i64' }], worker: 'w2' });
  });

  it('says why a message did not read', () => {
    const fill = filledFrom(
      [],
      status({ batches: [{ columns: [], rows: [row(['no schema 2'], [0])] }] }),
    );
    expect(fill).toEqual({ error: 'no schema 2' });
  });

  it('says so when no message arrived', () => {
    expect(filledFrom([], status({}))).toHaveProperty('error');
  });
});
