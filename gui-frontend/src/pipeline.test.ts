import { describe, expect, it } from 'vitest';
import stepReference from '../../website/src/lib/step-reference.json';
import {
  STEP_SUMMARIES,
  diffSteps,
  errorsByStep,
  proposedTypeSpec,
  newStep,
  parseInputFor,
  flatSteps,
  orderedStepErrors,
  parsePipelineDtoJson,
  stepSummary,
  stepToDto,
  stepsFromDto,
  stepsRun,
  truncateSteps,
  type Step,
  type FieldSpec,
} from './pipeline';

/** The simplest leaf step, used wherever a test only cares about tree
 * position. */
function ev(id: string): Step {
  return { type: 'eval', id, filter: '', disabled: false, assignments: [] };
}

function ifStep(id: string, condition: string, then: Step[], els: Step[]): Step {
  return { type: 'if', id, condition, then, else: els };
}

function parser(overrides: Partial<Extract<Step, { type: 'parser' }>> = {}): Step {
  return {
    type: 'parser',
    id: 'p1',
    filter: '',
    disabled: false,
    input: '_raw',
    into: '',
    kind: { type: 'json' },
    declared: [],
    ...overrides,
  };
}

/** A step through the wire format and back. */
function roundTrip(step: Step): Step {
  return parsePipelineDtoJson(JSON.stringify({ name: 'p', steps: [stepToDto(step)] })).steps[0];
}

describe('truncateSteps', () => {
  it('returns everything strictly before, or up to and including, a top-level target', () => {
    const a = ev('a');
    const b = ev('b');
    const c = ev('c');
    expect(truncateSteps([a, b, c], b, false)).toEqual([a]);
    expect(truncateSteps([a, b, c], b, true)).toEqual([a, b]);
    expect(truncateSteps([a, b], a, false)).toEqual([]);
  });

  it('returns null when the target is nowhere in the tree', () => {
    expect(truncateSteps([ev('a'), ev('b')], ev('not-in-tree'), false)).toBeNull();
  });

  it("finds a target nested in an if step's then branch, keeping the if with an emptied else", () => {
    const before = ev('before');
    const inThen1 = ev('then-1');
    const target = ev('then-target');
    const branch = ifStep('check', 'cond', [inThen1, target, ev('then-3')], [ev('else-1')]);

    const result = truncateSteps([before, branch, ev('after')], target, true);

    expect(result).toEqual([before, ifStep('check', 'cond', [inThen1, target], [])]);
  });

  it("finds a target nested in an if step's else branch, keeping the if with an emptied then", () => {
    const before = ev('before');
    const target = ev('else-target');
    const branch = ifStep('check', 'cond', [ev('then-1')], [target, ev('else-2')]);

    const result = truncateSteps([before, branch], target, false);

    expect(result).toEqual([before, ifStep('check', 'cond', [], [])]);
  });

  it('selecting the if step itself treats it as one unit, both branches intact', () => {
    const before = ev('before');
    const branch = ifStep('check', 'cond', [ev('then-1')], [ev('else-1')]);

    expect(truncateSteps([before, branch, ev('after')], branch, false)).toEqual([before]);
    expect(truncateSteps([before, branch, ev('after')], branch, true)).toEqual([before, branch]);
  });

  it('handles a target nested two ifs deep', () => {
    const target = ev('deep-target');
    const innerIf = ifStep('inner', 'b', [target], []);
    const outerIf = ifStep('outer', 'a', [innerIf], [ev('outer-else')]);

    expect(truncateSteps([outerIf], target, true)).toEqual([
      ifStep('outer', 'a', [ifStep('inner', 'b', [target], [])], []),
    ]);
  });

  it('a sibling if before the one containing the target is reproduced unchanged', () => {
    const untouchedIf = ifStep('first', 'a', [ev('a')], [ev('b')]);
    const target = ev('target');
    const targetIf = ifStep('second', 'b', [target], []);

    expect(truncateSteps([untouchedIf, targetIf], target, false)).toEqual([
      untouchedIf,
      ifStep('second', 'b', [], []),
    ]);
  });
});

describe('flatSteps', () => {
  it('lists an if before its branches, then before else', () => {
    const [a, t, e, z] = [ev('a'), ev('t'), ev('e'), ev('z')];
    const branch = ifStep('check', 'cond', [t], [e]);
    expect(flatSteps([a, branch, z])).toEqual([a, branch, t, e, z]);
  });
});

describe('stepsRun', () => {
  it('holds the steps truncateSteps keeps, as the caller holds them', () => {
    const [a, t, e, z] = [ev('a'), ev('t'), ev('e'), ev('z')];
    const branch = ifStep('check', 'cond', [t], [e]);
    const steps = [a, branch, z];
    expect(stepsRun(steps, e, true)).toEqual(new Set([a, branch, e]));
    expect(stepsRun(steps, t, false)).toEqual(new Set([a, branch]));
    expect(stepsRun(steps, branch, true)).toEqual(new Set([a, branch, t, e]));
    expect(stepsRun(steps, ev('gone'), true)).toBeNull();
  });
});

describe('proposedTypeSpec', () => {
  const field = (suggested: string, present: number, times?: number) => ({
    name: 'f',
    suggested,
    present,
    times,
  });

  it('proposes a datetime where every value read as a time', () => {
    for (const suggested of ['i64', 'f64', 'str']) {
      expect(proposedTypeSpec(field(suggested, 3, 3))).toEqual({ kind: 'datetime', format: '' });
    }
  });

  it('proposes the suggested type where any value did not', () => {
    expect(proposedTypeSpec(field('i64', 3, 2))).toEqual({ kind: 'named', name: 'i64' });
    expect(proposedTypeSpec(field('str', 3))).toEqual({ kind: 'named', name: 'str' });
    expect(proposedTypeSpec(field('array(i64)', 3))).toEqual({ kind: 'array', of: 'i64' });
    expect(proposedTypeSpec(field('struct', 3))).toBeNull();
  });
});

describe('errorsByStep', () => {
  const err = (step: string | null, message: string, fn: number | null = null) => ({
    step,
    function: fn,
    assignmentIndex: null,
    message,
  });

  it('places an error on the step whose id it names', () => {
    const a = ev('a');
    const z = ev('z');
    const by = errorsByStep([a, z], [err('z', 'boom')]);
    expect(by.get(z)).toEqual([err('z', 'boom')]);
    expect(by.has(a)).toBe(false);
  });

  it('reaches a step inside an if', () => {
    const t = ev('t');
    const by = errorsByStep([ifStep('check', 'c', [t], [])], [err('t', 'boom')]);
    expect(by.get(t)).toEqual([err('t', 'boom')]);
  });

  /* A `call` is one step here. The compiler inlines its callee before it
     numbers steps, so its `function` index runs ahead of this list. Here m2
     calls a pipeline of two steps, e1 and e2:

       index      0   1   2   3   4
       here       m1  m2  m3  m4
       compiler   m1  e1  e2  m3  m4
  */
  const call: Step = { type: 'call', id: 'm2', filter: '', disabled: false, pipeline: 'enrich' };

  it("leaves an inlined callee's error unplaced rather than on the wrong step", () => {
    const by = errorsByStep([ev('m1'), call, ev('m3'), ev('m4')], [err('e2', 'boom', 2)]);
    expect(by.size).toBe(0);
  });

  it('places an error after a call by its id, not by its index', () => {
    const m3 = ev('m3');
    const m4 = ev('m4');
    const by = errorsByStep([ev('m1'), call, m3, m4], [err('m3', 'boom', 3)]);
    expect(by.get(m3)).toEqual([err('m3', 'boom', 3)]);
    expect(by.has(m4)).toBe(false);
  });

  it('ignores an error that names no step', () => {
    expect(errorsByStep([ev('a')], [err(null, 'boom')]).size).toBe(0);
  });

  it('keeps assignmentIndex, so a step editor can put an error on its own row', () => {
    const a = ev('a');
    const withIndex = { step: 'a', function: null, assignmentIndex: 1, message: 'boom' };
    const by = errorsByStep([a], [withIndex]);
    expect(by.get(a)?.[0].assignmentIndex).toBe(1);
  });
});

describe('orderedStepErrors', () => {
  const err = (step: string | null, message: string) => ({
    step,
    function: null,
    assignmentIndex: null,
    message,
  });

  it('orders by the pipeline, not by the order errors arrived in', () => {
    const a = ev('a');
    const b = ev('b');
    const c = ev('c');
    // b's error is listed first in the response; the pipeline still puts b after a.
    const by = errorsByStep([a, b, c], [err('b', 'b broke'), err('a', 'a broke')]);
    const ordered = orderedStepErrors([a, b, c], by);
    expect(ordered.map((o) => o.step.id)).toEqual(['a', 'b']);
  });

  it('walks an if step’s branches in place', () => {
    const before = ev('before');
    const inThen = ev('in-then');
    const after = ev('after');
    const steps = [before, ifStep('check', 'c', [inThen], []), after];
    const by = errorsByStep(steps, [err('after', 'x'), err('in-then', 'y')]);
    const ordered = orderedStepErrors(steps, by);
    expect(ordered.map((o) => o.step.id)).toEqual(['in-then', 'after']);
  });

  it('puts an if’s own error before the errors in its branches', () => {
    const inThen = ev('in-then');
    const check = ifStep('check', 'c', [inThen], []);
    const by = errorsByStep([check], [err('in-then', 'y'), err('check', 'x')]);
    expect(orderedStepErrors([check], by).map((o) => o.step.id)).toEqual(['check', 'in-then']);
  });

  it('places and lists an error on a step whose id is the empty string', () => {
    const blank = ev('');
    const by = errorsByStep([blank], [err('', 'x')]);
    expect(by.get(blank)).toEqual([err('', 'x')]);
    expect(orderedStepErrors([blank], by)).toEqual([{ step: blank, messages: ['x'] }]);
  });

  it('places and lists an error on an if whose id is the empty string', () => {
    const blank = ifStep('', 'c', [], []);
    const by = errorsByStep([blank], [err('', 'x')]);
    expect(by.get(blank)).toEqual([err('', 'x')]);
    expect(orderedStepErrors([blank], by)).toEqual([{ step: blank, messages: ['x'] }]);
  });

  it('carries every message a step has, and nothing for a step with none', () => {
    const a = ev('a');
    const by = errorsByStep([a], [err('a', 'one'), err('a', 'two')]);
    expect(orderedStepErrors([a], by)).toEqual([{ step: a, messages: ['one', 'two'] }]);
    expect(orderedStepErrors([ev('clean')], by)).toEqual([]);
  });
});

describe('diffSteps', () => {
  it('nothing to report between identical steps', () => {
    expect(diffSteps([ev('a')], [ev('a')])).toEqual([]);
  });

  it('a step edited in place is modified, not removed-and-added', () => {
    const before = { ...ev('a'), assignments: [{ target: 'x', expr: '1' }] };
    const after = { ...ev('a'), assignments: [{ target: 'x', expr: '2' }] };
    expect(diffSteps([before], [after])).toEqual([{ kind: 'changed', summary: 'x = 2' }]);
  });

  it('a step only in the compare side is added, and vice versa', () => {
    expect(diffSteps([ev('a')], [ev('a'), ev('b')])).toEqual([
      { kind: 'added', summary: '(no assignments)' },
    ]);
    expect(diffSteps([ev('a'), ev('b')], [ev('a')])).toEqual([
      { kind: 'removed', summary: '(no assignments)' },
    ]);
  });

  it("a parser step is summarized with its format's name", () => {
    const before = { ...newStep('parser'), id: 'p' };
    const after = { ...before, kind: { type: 'grok' as const, patterns: [], customPatterns: [] } };
    expect(diffSteps([before], [after])).toEqual([
      { kind: 'changed', summary: '_raw as Regex/Grok' },
    ]);
  });

  it('an if step is matched by its id', () => {
    const before = ifStep('check', 'a == 1', [], []);
    const after = ifStep('check', 'a == 2', [], []);
    expect(diffSteps([before], [after])).toEqual([{ kind: 'changed', summary: 'if a == 2' }]);
  });

  it('two ifs that swap places are unchanged', () => {
    const first = ifStep('first', 'a == 1', [], []);
    const second = ifStep('second', 'a == 2', [], []);
    expect(diffSteps([first, second], [second, first])).toEqual([]);
  });
});

describe('unroll keepParentFields', () => {
  it('defaults to off and survives a round trip in both states', () => {
    const step = newStep('unroll');
    expect(step.type === 'unroll' && step.keepParentFields).toBe(false);

    for (const keep of [false, true]) {
      const back = roundTrip({
        type: 'unroll',
        id: 'u1',
        filter: '',
        disabled: false,
        field: 'events',
        keepParentFields: keep,
      });
      expect(back.type === 'unroll' && back.keepParentFields).toBe(keep);
    }
  });

  it('reads a JSON step that omits the key as off, matching the backend default', () => {
    const text = JSON.stringify({
      name: 'p',
      steps: [{ type: 'unroll', id: 'u1', field: 'events' }],
    });
    const back = parsePipelineDtoJson(text).steps[0];
    expect(back.type === 'unroll' && back.keepParentFields).toBe(false);
  });
});

describe('parser', () => {
  it('reads an omitted declared list as none', () => {
    const text = JSON.stringify({
      name: 'p',
      steps: [{ type: 'parser', id: 'p1', input: '_raw', kind: { type: 'json' } }],
    });
    const omitted = parsePipelineDtoJson(text).steps[0];
    expect(omitted.type === 'parser' && omitted.declared).toEqual([]);
  });

  it("carries a csv step's columns as its declared list, a skip column included", () => {
    const step = parser({
      kind: { type: 'csv', delimiter: ',' },
      declared: [
        { name: 'host', ty: { kind: 'named', name: 'str' } },
        { name: 'unwanted', ty: { kind: 'skip' } },
        { name: 'code', ty: { kind: 'named', name: 'i64' } },
      ],
    });

    // `skip` is a bare type string on the wire, like any named type.
    const dto = stepToDto(step) as { kind: unknown; declared: { name: string; type: unknown }[] };
    expect(dto.kind).toEqual({ type: 'csv', delimiter: ',' });
    expect(dto.declared.map((f) => f.type)).toEqual(['str', 'skip', 'i64']);

    // Position is the column, so the order has to survive the round trip.
    const back = roundTrip(step);
    expect(back.type === 'parser' && back.declared).toEqual(
      step.type === 'parser' ? step.declared : [],
    );
  });

  it('keeps a grok field type across a rename in the pattern, and drops the field the pattern lost', () => {
    // The editor's reconciliation, as `StepEditor` runs it: the captures are the names, and a type already chosen
    // survives for a name that survives. Extracted here because it is the part with a rule, not a render.
    const reconcile = (declared: FieldSpec[], caps: string[]): FieldSpec[] => {
      const chosen = new Map(declared.map((f) => [f.name, f.ty]));
      return caps.map((name) => ({
        name,
        ty: chosen.get(name) ?? { kind: 'named', name: 'str' },
      }));
    };

    const before: FieldSpec[] = [
      { name: 'status', ty: { kind: 'named', name: 'i64' } },
      { name: 'gone', ty: { kind: 'named', name: 'f64' } },
    ];

    // `status` keeps the i64 the author picked; `gone` left the pattern; `added` starts at str.
    expect(reconcile(before, ['status', 'added'])).toEqual([
      { name: 'status', ty: { kind: 'named', name: 'i64' } },
      { name: 'added', ty: { kind: 'named', name: 'str' } },
    ]);

    // Capture order is the field order, so the table follows the pattern rather than the edit history.
    expect(reconcile(before, ['added', 'status']).map((f) => f.name)).toEqual(['added', 'status']);
  });

  it('omits an empty keyValue quote, which the loader would reject as not one character', () => {
    const noQuote = stepToDto(
      parser({ kind: { type: 'keyValue', pairSep: '&', kvSep: '=', quote: '' } }),
    ) as { kind: Record<string, unknown> };
    expect('quote' in noQuote.kind).toBe(false);

    const quoted = roundTrip(
      parser({ kind: { type: 'keyValue', pairSep: '&', kvSep: '=', quote: '"' } }),
    );
    expect(quoted.type === 'parser' && quoted.kind).toEqual({
      type: 'keyValue',
      pairSep: '&',
      kvSep: '=',
      quote: '"',
    });
  });
});

describe('array-of-objects declared types', () => {
  const dto = {
    type: 'array',
    of: {
      type: 'struct',
      fields: [
        { name: 'name', type: 'str' },
        { name: 'n', type: 'i64' },
      ],
    },
  };

  it('round-trips through the pipeline JSON', () => {
    const step = parser({
      declared: [
        {
          name: 'tags',
          ty: {
            kind: 'arrayOfObjects',
            fields: [
              { name: 'name', ty: 'str' },
              { name: 'n', ty: 'i64' },
            ],
          },
        },
      ],
    });
    const out = stepToDto(step) as Record<string, unknown>;
    expect((out.declared as Record<string, unknown>[])[0].type).toEqual(dto);

    const back = roundTrip(step);
    expect(back.type === 'parser' && back.declared[0].ty).toEqual({
      kind: 'arrayOfObjects',
      fields: [
        { name: 'name', ty: 'str' },
        { name: 'n', ty: 'i64' },
      ],
    });
  });

  it('refuses a shape it cannot represent rather than flattening it', () => {
    const nested = {
      type: 'array',
      of: { type: 'struct', fields: [{ name: 'geo', type: { type: 'struct', fields: [] } }] },
    };
    const json = JSON.stringify({
      name: 'p',
      steps: [
        {
          type: 'parser',
          id: 'p1',
          input: '_raw',
          into: '',
          kind: { type: 'json' },
          declared: [{ name: 'sites', type: nested }],
        },
      ],
    });
    expect(() => parsePipelineDtoJson(json)).toThrow(
      /array of closed objects whose fields all have named types/,
    );
  });

  it('reads an element with open false as closed', () => {
    const closed = { ...dto, of: { ...dto.of, open: false } };
    const back = parsePipelineDtoJson(pipelineDeclaring(closed)).steps[0];
    expect(back.type === 'parser' && back.declared[0].ty).toEqual({
      kind: 'arrayOfObjects',
      fields: [
        { name: 'name', ty: 'str' },
        { name: 'n', ty: 'i64' },
      ],
    });
  });

  it('refuses an array of open objects rather than closing them', () => {
    const open = { ...dto, of: { ...dto.of, open: true } };
    expect(() => parsePipelineDtoJson(pipelineDeclaring(open))).toThrow(/array of closed objects/);
  });
});

/** A pipeline's JSON with one parser step declaring `sites` as `type`. */
function pipelineDeclaring(type: unknown): string {
  return JSON.stringify({
    name: 'p',
    steps: [
      {
        type: 'parser',
        id: 'p1',
        input: '_raw',
        into: '',
        kind: { type: 'json' },
        declared: [{ name: 'sites', type }],
      },
    ],
  });
}

describe('object declared types', () => {
  const fields = [
    { name: 'name', ty: 'str' as const },
    { name: 'n', ty: 'i64' as const },
  ];
  const wireFields = [
    { name: 'name', type: 'str' },
    { name: 'n', type: 'i64' },
  ];

  it('writes a closed object without open, and an open one with it', () => {
    for (const [open, dto] of [
      [false, { type: 'struct', fields: wireFields }],
      [true, { type: 'struct', fields: wireFields, open: true }],
    ] as const) {
      const step = parser({
        declared: [{ name: 'geo', ty: { kind: 'object', fields, open } }],
      });
      const out = stepToDto(step) as Record<string, unknown>;
      expect((out.declared as Record<string, unknown>[])[0].type).toEqual(dto);
    }
  });

  it('round-trips through the pipeline JSON', () => {
    for (const open of [false, true]) {
      const ty = { kind: 'object' as const, fields, open };
      const back = roundTrip(parser({ declared: [{ name: 'geo', ty }] }));
      expect(back.type === 'parser' && back.declared[0].ty).toEqual(ty);
    }
  });

  it('reads an object with no fields, open or closed', () => {
    for (const [dto, open] of [
      [{ type: 'struct', fields: [] }, false],
      [{ type: 'struct', fields: [], open: false }, false],
      [{ type: 'struct', fields: [], open: true }, true],
    ] as const) {
      const back = parsePipelineDtoJson(pipelineDeclaring(dto)).steps[0];
      expect(back.type === 'parser' && back.declared[0].ty).toEqual({
        kind: 'object',
        fields: [],
        open,
      });
    }
  });

  it('refuses a shape it cannot represent rather than flattening it', () => {
    for (const dto of [
      { type: 'struct', fields: [{ name: 'geo', type: { type: 'struct', fields: [] } }] },
      { type: 'struct', fields: [{ name: 'tags', type: { type: 'array', of: 'str' } }] },
      { type: 'struct', fields: [], open: 'yes' },
    ]) {
      expect(() => parsePipelineDtoJson(pipelineDeclaring(dto))).toThrow(
        /an object whose fields all have named types, open or closed/,
      );
    }
  });
});

describe('lookup output types', () => {
  function lookup(outputs: Extract<Step, { type: 'lookup' }>['outputs']): Step {
    return {
      type: 'lookup',
      id: 'l1',
      filter: '',
      disabled: false,
      input: 'app',
      table: 'apps',
      matchKind: 'exact',
      multiplicity: 'first',
      keyColumn: 'name',
      outputs,
    };
  }

  it("writes a type only when it is not str, in a declared field's spelling", () => {
    const dto = stepToDto(
      lookup([
        { source: 'category', field: 'cat', default: '', ty: { kind: 'named', name: 'str' } },
        { source: 'risk', field: 'risk', default: '0', ty: { kind: 'named', name: 'i64' } },
        { source: 'seen', field: 'seen', default: '', ty: { kind: 'datetime', format: '' } },
        { source: 'eol', field: 'eol', default: '', ty: { kind: 'datetime', format: '%Y-%m-%d' } },
      ]),
    ) as { outputs: Record<string, unknown>[] };
    expect(dto.outputs.map((o) => o.type)).toEqual([
      undefined,
      'i64',
      'datetime',
      { type: 'datetime', format: '%Y-%m-%d' },
    ]);
  });

  it('reads a missing type as str and every other back as written', () => {
    const step = lookup([
      { source: 'category', field: 'cat', default: '', ty: { kind: 'named', name: 'str' } },
      { source: 'risk', field: 'risk', default: '0', ty: { kind: 'named', name: 'i64' } },
      { source: 'eol', field: 'eol', default: '', ty: { kind: 'datetime', format: '%Y-%m-%d' } },
    ]);
    expect(roundTrip(step)).toEqual(step);
  });

  it('refuses an array type on an output', () => {
    const text = JSON.stringify({
      name: 'p',
      steps: [
        {
          ...(stepToDto(lookup([])) as object),
          outputs: [{ source: 'a', field: 'a', type: { type: 'array', of: 'str' } }],
        },
      ],
    });
    expect(() => parsePipelineDtoJson(text)).toThrow(/one value/);
  });
});

describe('an if’s id', () => {
  it('is read and written back, the empty string included', () => {
    for (const id of ['check', '']) {
      const step = ifStep(id, 'a', [ev('inner')], []);
      expect(stepToDto(step)).toMatchObject({ type: 'if', id });
      expect(roundTrip(step)).toEqual(step);
    }
  });

  it('comes with a new if', () => {
    expect(newStep('if').id).toMatch(/^if-/);
  });
});

describe('parseInputFor', () => {
  it('reads _raw when the record has it, or with no sample', () => {
    expect(parseInputFor({ _raw: 'x' })).toBe('_raw');
    expect(parseInputFor(undefined)).toBe('_raw');
  });

  it("reads a record without _raw's first text field", () => {
    expect(parseInputFor({ id: 1, name: 'x', note: 'y' })).toBe('name');
    expect(newStep('parser', parseInputFor({ id: 1, name: 'x' }))).toMatchObject({ input: 'name' });
  });
});

/** An `if` as a hand-edited file can hold it, with no id. */
const bareIf = { type: 'if', condition: 'a', then: [], else: [] };

describe('the JSON page', () => {
  const doc = (steps: unknown[]) => JSON.stringify({ name: 'p', steps });

  it('refuses an if with no id, as it refuses any step with none', () => {
    expect(() => parsePipelineDtoJson(doc([{ type: 'eval', assignments: [] }]))).toThrow(
      'steps[0]: expected "id" to be a string',
    );
    expect(() => parsePipelineDtoJson(doc([bareIf]))).toThrow(
      'steps[0]: expected "id" to be a string',
    );
  });

  it('refuses an if with no id inside another if', () => {
    const outer = { ...bareIf, id: 'outer', then: [bareIf] };
    expect(() => parsePipelineDtoJson(doc([outer]))).toThrow(
      'steps[0].then[0]: expected "id" to be a string',
    );
  });

  it('refuses an if whose id is not a string', () => {
    expect(() => parsePipelineDtoJson(doc([{ ...bareIf, id: 7 }]))).toThrow(
      'steps[0]: expected "id" to be a string',
    );
  });
});

describe('reading a pipeline from the leader', () => {
  it('refuses an if with no id, at any depth, as the JSON page does', () => {
    expect(() => stepsFromDto([bareIf])).toThrow('steps[0]: expected "id" to be a string');
    expect(() => stepsFromDto([{ ...bareIf, id: 'outer', else: [bareIf] }])).toThrow(
      'steps[0].else[0]: expected "id" to be a string',
    );
  });
});

describe('a parser’s splitDots', () => {
  it('is absent from the wire at the format’s default', () => {
    for (const kind of [
      { type: 'json' as const, splitDots: false },
      { type: 'keyValue' as const, pairSep: '&', kvSep: '=', quote: '', splitDots: true },
      { type: 'csv' as const, delimiter: ',', splitDots: false },
    ]) {
      const dto = stepToDto(parser({ kind })) as { kind: Record<string, unknown> };
      expect('splitDots' in dto.kind).toBe(false);
    }
  });

  it('is sent, and read back, where it differs', () => {
    const step = parser({ kind: { type: 'json', splitDots: true } });
    expect((stepToDto(step) as { kind: unknown }).kind).toEqual({ type: 'json', splitDots: true });
    const back = roundTrip(step);
    expect(back.type === 'parser' && back.kind).toEqual({ type: 'json', splitDots: true });

    const grok = parser({
      kind: { type: 'grok', patterns: [], customPatterns: [], splitDots: false },
    });
    expect((stepToDto(grok) as { kind: { splitDots?: boolean } }).kind.splitDots).toBe(false);
  });
});

describe('a Key/value parser’s nextKey', () => {
  const kv = { type: 'keyValue' as const, pairSep: ' ', kvSep: '=', quote: '"' };

  it('is absent from the wire while on', () => {
    for (const kind of [kv, { ...kv, nextKey: true }]) {
      const dto = stepToDto(parser({ kind })) as { kind: Record<string, unknown> };
      expect('nextKey' in dto.kind).toBe(false);
    }
  });

  it('is sent, and read back, when off', () => {
    const step = parser({ kind: { ...kv, nextKey: false } });
    expect((stepToDto(step) as { kind: { nextKey?: boolean } }).kind.nextKey).toBe(false);
    const back = roundTrip(step);
    expect(back.type === 'parser' && back.kind).toEqual({ ...kv, nextKey: false });
  });
});

describe('STEP_SUMMARIES', () => {
  it("says what the website's step reference says", () => {
    const website = Object.fromEntries(stepReference.steps.kinds.map((k) => [k.type, k.summary]));
    expect(STEP_SUMMARIES).toEqual(website);
  });
});

describe('a parser that writes into an object', () => {
  it('sends its object, and nothing where it writes into the record', () => {
    expect(stepToDto(parser({ into: 'app' }))).toMatchObject({ into: 'app' });
    expect(stepToDto(parser())).not.toHaveProperty('into');
    expect(stepsFromDto([stepToDto(parser({ into: 'app' }))])).toEqual([parser({ into: 'app' })]);
    expect(stepsFromDto([stepToDto(parser())])).toEqual([parser()]);
  });

  it('names its object in its summary, and not the record', () => {
    expect(stepSummary(parser({ into: 'app' }))).toBe('_raw as JSON into app');
    expect(stepSummary(parser({ into: '$' }))).toBe('_raw as JSON');
    expect(stepSummary(parser())).toBe('_raw as JSON');
  });
});
