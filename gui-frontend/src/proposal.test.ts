import { describe, expect, it } from 'vitest';
import type { FieldShapeDto, ParserFieldProposal, ParserShapeDto } from './api';
import { productionOf, proposalRows } from './proposal';

const field = (name: string, over: Partial<FieldShapeDto> = {}): FieldShapeDto => ({
  name,
  declared: false,
  suggested: 'str',
  present: 10,
  bools: 0,
  ints: 0,
  floats: 0,
  strings: 10,
  arrays: 0,
  objects: 0,
  failed: 0,
  times: 0,
  firstSeenMs: 1_000,
  lastSeenMs: 1_000,
  ...over,
});

const shape = (over: Partial<ParserShapeDto>): ParserShapeDto => ({
  groupId: 'edge',
  source: 'hec',
  route: 'to-es',
  pipeline: 'okta',
  step: 'parse',
  rows: 10,
  fieldsDropped: 0,
  firstSeenMs: 1_000,
  lastSeenMs: 1_000,
  fields: [],
  ...over,
});

describe('productionOf', () => {
  it('is null for a step production never ran', () => {
    expect(productionOf([shape({ step: 'other' })], 'okta', 'parse')).toBeNull();
  });

  /* Two groups ran the step; a field one of them never saw was absent from that group's rows. */
  it('sums every shape of the step, each field against every row', () => {
    const p = productionOf(
      [
        shape({ fields: [field('a'), field('when', { times: 10 })], lastSeenMs: 2_000 }),
        shape({
          groupId: 'core',
          rows: 30,
          fields: [
            field('a', { present: 30, strings: 20, ints: 10, suggested: 'i64', lastSeenMs: 3_000 }),
          ],
          lastSeenMs: 3_000,
        }),
      ],
      'okta',
      'parse',
    )!;
    expect([p.rows, p.lastSeenMs]).toEqual([40, 3_000]);
    const [a, when] = p.fields;
    expect([a.present, a.rows, a.suggested]).toEqual([40, 40, 'i64']);
    expect(a.kinds).toEqual([
      ['str', 30],
      ['i64', 10],
    ]);
    expect([when.present, when.rows, when.times]).toEqual([10, 40, 10]);
  });
});

describe('proposalRows', () => {
  const proposed = (
    name: string,
    over: Partial<ParserFieldProposal> = {},
  ): ParserFieldProposal => ({
    name,
    declared: false,
    suggested: 'str',
    rows: 4,
    present: 4,
    kinds: [['str', 4]],
    ...over,
  });

  it('lists the sample, then what only production saw, each with its counts summed', () => {
    const production = productionOf(
      [shape({ fields: [field('when', { times: 10 }), field('target', { present: 2 })] })],
      'okta',
      'parse',
    );
    const rows = proposalRows([proposed('when', { times: 4 }), proposed('id')], production);
    expect(rows.map((r) => [r.name, r.present, r.times, !!r.sample, !!r.production])).toEqual([
      ['when', 14, 14, true, true],
      ['id', 4, 0, true, false],
      ['target', 2, 0, false, true],
    ]);
  });
});
