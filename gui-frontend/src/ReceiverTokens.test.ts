import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';
import { createRawSnippet } from 'svelte';
import ReceiverTokens, { writeTokens } from './ReceiverTokens.svelte';
import { shadowed } from './KeyedRows.svelte';

const help = createRawSnippet(() => ({ render: () => '<span>help</span>' }));

describe('a receiver’s tokens', () => {
  it('write a name to its credential, leaving out a row still being named', () => {
    const rows = [
      { key: ' collectors ', ref: 'OTEL_TOKEN', ack: false },
      { key: 'apps', ref: { secret: 'otlp-apps', version: 1 }, ack: false },
      { key: 'half', ref: undefined, ack: false },
      { key: '', ref: 'NAMELESS', ack: false },
    ];
    expect(writeTokens(rows, false)).toEqual({
      collectors: 'OTEL_TOKEN',
      apps: { secret: 'otlp-apps', version: 1 },
    });
  });

  it('write a HEC token as its credential and, only when on, its acknowledgement', () => {
    const rows = [
      { key: 'vector', ref: 'VECTOR_HEC_TOKEN', ack: true },
      { key: 'fluentbit', ref: 'FLUENTBIT_HEC_TOKEN', ack: false },
    ];
    expect(writeTokens(rows, true)).toEqual({
      vector: { token: 'VECTOR_HEC_TOKEN', acknowledgement: true },
      fluentbit: { token: 'FLUENTBIT_HEC_TOKEN' },
    });
  });

  it('keep the first row of a name given twice, and mark the later one', () => {
    const rows = [
      { key: 'ci', ref: 'FIRST', ack: false },
      { key: ' ci ', ref: 'SECOND', ack: false },
    ];
    expect(writeTokens(rows, false)).toEqual({ ci: 'FIRST' });
    expect(shadowed(rows, 0)).toBe(false);
    expect(shadowed(rows, 1)).toBe(true);
  });

  it('write nothing when no token is named, as the file omits an empty map', () => {
    expect(writeTokens([{ key: '', ref: undefined, ack: false }], false)).toBeUndefined();
  });

  it('offer acknowledgement only on a HEC receiver', () => {
    const hec = render(ReceiverTokens, {
      props: {
        value: { tokens: { vector: { token: 'VECTOR_HEC_TOKEN', acknowledgement: true } } },
        acknowledges: true,
        help,
      },
    }).body;
    expect(hec).toContain('value="vector"');
    expect(hec).toContain('acknowledgement');
    const otlp = render(ReceiverTokens, {
      props: { value: { tokens: { collectors: 'OTEL_TOKEN' } }, help },
    }).body;
    expect(otlp).toContain('value="collectors"');
    expect(otlp).not.toContain('acknowledgement');
  });
});
