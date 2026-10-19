import { describe, expect, it } from 'vitest';
import { columnStats, parseCsv, serializeCsv, stepsReading } from './lookups';

describe('parseCsv', () => {
  it('reads quoted fields, doubled quotes and CRLF', () => {
    const text = 'ip,note\r\n10.0.0.1,"a, b"\r\n10.0.0.2,"say ""hi"""\r\n';
    expect(parseCsv(text, ',')).toEqual([
      ['ip', 'note'],
      ['10.0.0.1', 'a, b'],
      ['10.0.0.2', 'say "hi"'],
    ]);
  });

  it('keeps a line break inside quotes and skips blank lines', () => {
    expect(parseCsv('a;b\n\n"x\ny";\n  \n', ';')).toEqual([
      ['a', 'b'],
      ['x\ny', ''],
    ]);
  });

  it('keeps a ragged row as it is', () => {
    expect(parseCsv('a,b\n1\n1,2,3', ',')).toEqual([['a', 'b'], ['1'], ['1', '2', '3']]);
  });
});

describe('serializeCsv', () => {
  it('quotes only a field that needs it', () => {
    expect(serializeCsv([['a', 'b\tc', 'say "hi"', 'x,y']], '\t')).toBe(
      'a\t"b\tc"\t"say ""hi"""\tx,y\n',
    );
  });

  it('round-trips through parseCsv', () => {
    const records = [['k', 'v'], ['', 'line\nbreak'], [''], ['a', '"q"'], ['1', '2', '3']];
    for (const d of [',', ';', '\t', '|']) {
      expect(parseCsv(serializeCsv(records, d), d)).toEqual(records);
    }
  });
});

it('counts distinct and empty cells per column', () => {
  expect(columnStats(['k', 'v'], [['a', ''], ['a', 'x'], ['b']])).toEqual([
    ['k', 2, 0],
    ['v', 2, 2],
  ]);
});

it('finds lookup steps that read a table, inside if branches too, by pipeline name', () => {
  const lookup = (id: string, table: string) => ({
    type: 'lookup',
    id,
    table,
    input: 'ip',
    keyColumn: 'net',
    outputs: [{ source: 'asn', field: 'geo.asn' }],
  });
  const pipelines = [
    {
      id: 'pipeline-a1b2c3d4',
      name: 'enrich',
      steps: [lookup('l1', 'geo'), lookup('l2', 'other')],
    },
    {
      id: 'pipeline-e5f6g7h8',
      name: 'tag',
      steps: [{ type: 'if', then: [], else: [lookup('l3', 'geo')] }],
    },
  ];
  expect(stepsReading(pipelines, 'geo')).toEqual([
    ['enrich › l1', 'ip', 'net', 'asn → geo.asn'],
    ['tag › l3', 'ip', 'net', 'asn → geo.asn'],
  ]);
});
