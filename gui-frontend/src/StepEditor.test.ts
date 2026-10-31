import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render } from 'svelte/server';
import StepEditor from './StepEditor.svelte';
import { readSeparator, showSeparator } from './SeparatorInput.svelte';
import type { FieldSpec, PreviewError, Step } from './pipeline';
import { setScope } from './api';
import { load, signedOut } from './session';

/** The classes on the card tag of the step with this id. */
function cardClasses(html: string, id: string): string[] {
  const tag = html.match(new RegExp(`<div id="step-${id}"[^>]*>`))?.[0];
  expect(tag).toBeDefined();
  return tag!.match(/class="([^"]*)"/)![1].split(/\s+/);
}

describe('an if card', () => {
  const ifStep = (): Step => ({ type: 'if', id: 'if-1', condition: 'x', then: [], else: [] });

  it('has has-error when its step has an error', () => {
    const step = ifStep();
    const error: PreviewError = {
      step: 'if-1',
      function: null,
      assignmentIndex: null,
      message: 'unknown field x',
    };
    const { body } = render(StepEditor, {
      props: { steps: [step], index: 0, errorsByStep: new Map([[step, [error]]]) },
    });
    expect(cardClasses(body, 'if-1')).toContain('has-error');
  });

  it('has no has-error when its step has none', () => {
    const { body } = render(StepEditor, { props: { steps: [ifStep()], index: 0 } });
    expect(cardClasses(body, 'if-1')).not.toContain('has-error');
  });
});

describe('a parser step’s split keys at dots', () => {
  const parser = (kind: Extract<Step, { type: 'parser' }>['kind']): Step => ({
    type: 'parser',
    id: 'p-1',
    filter: '',
    disabled: false,
    input: '_raw',
    into: '$',
    kind,
    declared: [],
  });
  const render_ = (step: Step) => render(StepEditor, { props: { steps: [step], index: 0 } }).body;

  // A step opens as it was last left, which a server render reads as closed.
  beforeEach(() => vi.stubGlobal('localStorage', { getItem: () => '1', setItem: () => {} }));
  afterEach(() => vi.unstubAllGlobals());

  it('sits closed under Advanced, which says defaults, while at the default', () => {
    const body = render_(parser({ type: 'json' }));
    expect(body).toContain('declared fields');
    expect(body).toContain('defaults');
    expect(body).not.toContain('changed from defaults');
    expect(body).not.toContain('split keys at dots');
  });

  /** Whether Advanced's header names `label` at `value`. */
  const names = (body: string, label: string, value: string) =>
    new RegExp(`${label} <code[^>]*>${value}</code>`).test(body);

  it('opens Advanced, which names the setting, when changed', () => {
    const body = render_(parser({ type: 'json', splitDots: true }));
    expect(names(body, 'split keys at dots', 'on')).toBe(true);
    expect(body).toContain('Off by default for JSON');
  });

  it('names Key/value’s own default, which is on', () => {
    const body = render_(
      parser({ type: 'keyValue', pairSep: '&', kvSep: '=', quote: '', splitDots: false }),
    );
    expect(names(body, 'split keys at dots', 'off')).toBe(true);
    expect(names(body, 'quote char', '""')).toBe(true);
    expect(body).toContain('On by default for Key/value');
  });

  it('names Key/value values that stop at the first separator', () => {
    const body = render_(
      parser({ type: 'keyValue', pairSep: ' ', kvSep: '=', quote: '"', nextKey: false }),
    );
    expect(names(body, 'values run to the next key', 'off')).toBe(true);
  });

  it('is offered for CSV, off by default', () => {
    const body = render_(parser({ type: 'csv', delimiter: ',', splitDots: true }));
    expect(names(body, 'split keys at dots', 'on')).toBe(true);
    expect(body).toContain('Off by default for CSV');
  });
});

describe('a parser step’s Advanced', () => {
  beforeEach(() => vi.stubGlobal('localStorage', { getItem: () => '1', setItem: () => {} }));
  afterEach(() => vi.unstubAllGlobals());

  const parser = (over: Partial<Extract<Step, { type: 'parser' }>>): Step => ({
    type: 'parser',
    id: 'p-1',
    filter: '',
    disabled: false,
    input: '_raw',
    into: '$',
    kind: { type: 'json' },
    declared: [],
    ...over,
  });

  it('holds the input field and the filter, naming each that is set', () => {
    const body = render(StepEditor, {
      props: { steps: [parser({ input: 'msg', filter: 'x == 1' })], index: 0 },
    }).body;
    expect(body).toMatch(/input <code[^>]*>msg<\/code>/);
    expect(body).toMatch(/filter <code[^>]*>x == 1<\/code>/);
  });

  it('names the object it writes into, and not the record', () => {
    const named = (into: string) =>
      render(StepEditor, { props: { steps: [parser({ into })], index: 0 } }).body;
    expect(named('app')).toMatch(/into <code[^>]*>app<\/code>/);
    expect(named('$')).not.toMatch(/into <code/);
  });

  it('opens on an error in its filter, and counts it', () => {
    const step = parser({ filter: 'nope == 1' });
    const body = render(StepEditor, {
      props: {
        steps: [step],
        index: 0,
        errorsByStep: new Map([[step, [{ message: '`nope` is not a field' } as PreviewError]]]),
      },
    }).body;
    expect(body).toContain('1 error');
    expect(body).toContain('aria-expanded="true"><span class="caret');
  });
});

describe('a parser step’s fields', () => {
  beforeEach(() => vi.stubGlobal('localStorage', { getItem: () => '1', setItem: () => {} }));
  afterEach(() => vi.unstubAllGlobals());

  const parser = (
    kind: Extract<Step, { type: 'parser' }>['kind'],
    declared: FieldSpec[],
  ): Step => ({
    type: 'parser',
    id: 'p-1',
    filter: '',
    disabled: false,
    input: '_raw',
    into: '$',
    kind,
    declared,
  });
  const render_ = (step: Step) => render(StepEditor, { props: { steps: [step], index: 0 } }).body;

  it('offers Suggest fields for JSON, and not for CSV, whose columns are its own', () => {
    expect(render_(parser({ type: 'json' }, []))).toContain('Suggest fields');
    expect(render_(parser({ type: 'csv', delimiter: ',' }, []))).not.toContain('Suggest fields');
  });

  it('numbers CSV columns and offers skip there alone', () => {
    const declared: FieldSpec[] = [
      { name: 'a', ty: { kind: 'named', name: 'str' } },
      { name: 'b', ty: { kind: 'skip' } },
    ];
    const csv = render_(parser({ type: 'csv', delimiter: ',' }, declared));
    expect(csv).toMatch(/class="number[^"]*">2</);
    expect(csv).toContain('<option value="skip"');
    const json = render_(parser({ type: 'json' }, [declared[0]]));
    expect(json).not.toContain('<option value="skip"');
  });

  it('folds an auto datetime and shows a set format', () => {
    const auto = render_(
      parser({ type: 'json' }, [{ name: 't', ty: { kind: 'datetime', format: '' } }]),
    );
    expect(auto).toMatch(/aria-expanded="false"[^>]*>auto </);
    expect(auto).not.toContain('aria-label="format"');
    const set = render_(
      parser({ type: 'json' }, [{ name: 't', ty: { kind: 'datetime', format: '%Y' } }]),
    );
    expect(set).toContain('aria-label="format"');
  });

  it('lists an object’s fields under it', () => {
    const body = render_(
      parser({ type: 'json' }, [
        { name: 'user', ty: { kind: 'object', fields: [{ name: 'id', ty: 'i64' }], open: false } },
      ]),
    );
    expect(body).toContain('class="nested');
    expect(body).toContain('aria-label="object field name"');
  });
});

describe('a parser step’s separators', () => {
  beforeEach(() => vi.stubGlobal('localStorage', { getItem: () => '1', setItem: () => {} }));
  afterEach(() => vi.unstubAllGlobals());

  const parser = (kind: Extract<Step, { type: 'parser' }>['kind']): Step => ({
    type: 'parser',
    id: 'p-1',
    filter: '',
    disabled: false,
    input: '_raw',
    into: '$',
    kind,
    declared: [],
  });
  const render_ = (step: Step) => render(StepEditor, { props: { steps: [step], index: 0 } }).body;

  it('show a space and a tab as text, not as a blank field', () => {
    const body = render_(parser({ type: 'keyValue', pairSep: ' ', kvSep: '\t', quote: '' }));
    expect(body).toContain('value="␣"');
    expect(body).toContain('value="\\t"');
  });

  it('show a CSV tab delimiter as text', () => {
    expect(render_(parser({ type: 'csv', delimiter: '\t' }))).toContain('value="\\t"');
  });

  it('read either shown form back as its character, and keep one character', () => {
    expect(readSeparator('␣')).toBe(' ');
    expect(readSeparator('\\t')).toBe('\t');
    expect(readSeparator('␣,')).toBe(',');
    expect(readSeparator('\\t;')).toBe(';');
    expect(readSeparator(',\\t')).toBe('\t');
    expect(readSeparator('')).toBe('');
    expect(showSeparator(readSeparator('\\'))).toBe('\\');
  });

  it('read the character typed before the one already there', () => {
    expect(readSeparator(';,', 1)).toBe(';');
    expect(readSeparator('\\t␣', 2)).toBe('\t');
    expect(readSeparator(',;', 2)).toBe(';');
    expect(readSeparator(',', 0)).toBe(',');
  });
});

describe('a box that holds a name', () => {
  beforeEach(() => vi.stubGlobal('localStorage', { getItem: () => '1', setItem: () => {} }));
  afterEach(() => vi.unstubAllGlobals());

  const rename = (from: string): Step => ({
    type: 'rename',
    id: 'r-1',
    filter: '',
    disabled: false,
    pairs: [{ from, to: 'name' }],
  });

  it('refuses text that is not a path, saying why', () => {
    const body = render(StepEditor, {
      props: { steps: [rename('labels["app.kubernetes.io/name')], index: 0 },
    }).body;
    expect(body).toContain('a key in brackets is a string in quotes');
  });

  it('takes a key in brackets', () => {
    const body = render(StepEditor, {
      props: { steps: [rename('labels["app.kubernetes.io/name"]')], index: 0 },
    }).body;
    expect(body).not.toContain('field-error');
  });
});

describe('an eval assignment’s save error', () => {
  const evalStep = (): Step =>
    ({
      type: 'eval',
      id: 'e-1',
      filter: '',
      disabled: false,
      assignments: [{ target: '', expr: '' }],
    }) as Step;
  const blank = (field: string, message: string): PreviewError => ({
    step: 'e-1',
    function: null,
    assignmentIndex: 0,
    field,
    message,
  });

  // A step opens as it was last left, which a server render reads as closed.
  beforeEach(() => vi.stubGlobal('localStorage', { getItem: () => '1', setItem: () => {} }));
  afterEach(() => vi.unstubAllGlobals());

  /** Whether each of the row's two boxes is marked, in the order they sit. */
  function boxMarks(body: string): boolean[] {
    const row = body.slice(body.indexOf('row-pair'));
    return [...row.matchAll(/<input[^>]*>/g)].slice(0, 2).map((m) => m[0].includes('field-error'));
  }

  it('marks the box it names and writes its message under the step', () => {
    const step = evalStep();
    const { body } = render(StepEditor, {
      props: {
        steps: [step],
        index: 0,
        errorsByStep: new Map([[step, [blank('target', 'no target field')]]]),
      },
    });
    expect(boxMarks(body)).toEqual([true, false]);
    expect(body.slice(body.indexOf('step-errors'))).toContain('no target field');
  });

  it('writes each message once', () => {
    const step = evalStep();
    const { body } = render(StepEditor, {
      props: {
        steps: [step],
        index: 0,
        errorsByStep: new Map([
          [step, [blank('target', 'no target field'), blank('expr', 'no expression')]],
        ]),
      },
    });
    expect(boxMarks(body)).toEqual([true, true]);
    expect(body.split('no target field').length - 1).toBe(1);
    expect(body.split('no expression').length - 1).toBe(1);
  });
});

describe('a step a viewer reads', () => {
  beforeEach(async () => {
    vi.stubGlobal('localStorage', { getItem: () => '1', setItem: () => {} });
    vi.stubGlobal('location', { hash: '#/build/pipelines' });
    vi.stubGlobal(
      'fetch',
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              authenticated: true,
              userId: 'u_1',
              displayName: 'Lee Park',
              administrator: false,
              mustChangePassword: false,
              access: { administrator: false, areas: { shared: 'viewer', payments: 'viewer' } },
            }),
            { headers: { 'content-type': 'application/json' } },
          ),
      ),
    );
    await load();
    setScope('payments');
  });
  afterEach(() => {
    signedOut();
    setScope('shared');
    vi.unstubAllGlobals();
  });

  /** Each control's tag, with how many disabled fieldsets hold it. */
  function locks(body: string): { control: string; depth: number }[] {
    const found: { control: string; depth: number }[] = [];
    let depth = 0;
    for (const [tag] of body.matchAll(/<\/?(fieldset|input|select|textarea)\b[^>]*>/g)) {
      if (tag.startsWith('<fieldset')) depth++;
      else if (tag === '</fieldset>') depth--;
      else if (!tag.startsWith('</')) found.push({ control: tag, depth });
    }
    return found;
  }

  const steps: Step[] = [
    {
      type: 'eval',
      id: 'e-1',
      filter: '',
      disabled: false,
      assignments: [{ target: 'a', expr: '1' }],
    },
    { type: 'rename', id: 'r-1', filter: '', disabled: false, pairs: [{ from: 'a', to: 'b' }] },
    {
      type: 'parser',
      id: 'p-1',
      filter: '',
      disabled: false,
      input: '_raw',
      into: '$',
      kind: { type: 'keyValue', pairSep: ' ', kvSep: '=', quote: '"' },
      declared: [{ name: 'level', ty: { kind: 'named', name: 'str' } }],
    },
    { type: 'if', id: 'if-1', condition: 'x', then: [], else: [] },
  ];

  it.each(steps.map((s) => [s.type, s] as const))(
    'holds every %s control under one lock',
    (_, step) => {
      const found = locks(render(StepEditor, { props: { steps: [step], index: 0 } }).body);
      expect(found.some((f) => f.depth === 1)).toBe(true);
      for (const { control, depth } of found) {
        // A control the step's own `disabled={readOnly}` holds needs no fieldset.
        if (depth === 0) expect(control).toMatch(/\sdisabled/);
        else expect(depth, control).toBe(1);
      }
    },
  );
});
