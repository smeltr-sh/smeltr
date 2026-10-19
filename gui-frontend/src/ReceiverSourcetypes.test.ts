import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';
import { createRawSnippet } from 'svelte';
import ReceiverSourcetypes, {
  DEFAULT_LINE_BREAKER,
  writeSourcetypes,
} from './ReceiverSourcetypes.svelte';
import { shadowed } from './KeyedRows.svelte';

const help = createRawSnippet(() => ({ render: () => '<span>help</span>' }));

describe('a receiver’s sourcetypes', () => {
  it('write a name to its line breaker as typed, or to none when blank, leaving out a row with no name', () => {
    const rows = [
      { key: ' app:xml ', lineBreaker: '</e>(\\s*)' },
      { key: 'half', lineBreaker: ' ' },
      { key: '', lineBreaker: '([\\r\\n]+)' },
    ];
    expect(writeSourcetypes(rows)).toEqual({
      'app:xml': { lineBreaker: '</e>(\\s*)' },
      half: {},
    });
  });

  it('keep the first row of a name given twice, and mark the later one', () => {
    const rows = [
      { key: 'xml', lineBreaker: 'FIRST' },
      { key: ' xml ', lineBreaker: 'SECOND' },
    ];
    expect(writeSourcetypes(rows)).toEqual({ xml: { lineBreaker: 'FIRST' } });
    expect(shadowed(rows, 0)).toBe(false);
    expect(shadowed(rows, 1)).toBe(true);
  });

  it('write nothing when no sourcetype is named, as the file omits an empty map', () => {
    expect(writeSourcetypes([{ key: '', lineBreaker: '' }])).toBeUndefined();
  });

  it('show each sourcetype with its line breaker', () => {
    const body = render(ReceiverSourcetypes, {
      props: { value: { sourcetypes: { 'app:xml': { lineBreaker: '</e>' } } }, help },
    }).body;
    expect(body).toContain('value="app:xml"');
    expect(body).toContain('value="&lt;/e>"');
  });

  it('hint the default line breaker in an empty box', () => {
    const body = render(ReceiverSourcetypes, {
      props: { value: { sourcetypes: { plain: { lineBreaker: '' } } }, help },
    }).body;
    expect(body).toContain(`placeholder="${DEFAULT_LINE_BREAKER}"`);
  });
});
