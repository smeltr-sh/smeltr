import { describe, expect, it } from 'vitest';
import { changeText, describe as describeValue, type ScopeChange } from './changeText';
import type { FileConfigDto } from './api';

const base: FileConfigDto = {
  version: 1,
  sources: [
    {
      id: 'kafka-main',
      type: 'kafka',
      brokers: 'b1:9092,b2:9092',
      topics: ['logs'],
      groupId: 'smeltr',
    },
  ],
  destinations: [
    {
      id: 'kafka-dlq',
      type: 'kafka',
      brokers: 'b1:9092',
      defaultTopic: 'dlq',
      stallAfterSecs: 30,
    },
    {
      id: 'kafka-out',
      type: 'kafka',
      brokers: 'b1:9092',
      defaultTopic: 'logs-out',
      stallAfterSecs: 30,
    },
  ],
  pipelines: [
    {
      id: 'main',
      name: 'Main',
      steps: [
        {
          type: 'eval',
          id: 'eval-1',
          filter: null,
          disabled: false,
          assignments: [{ target: '.level', expr: '400' }],
        },
        {
          type: 'if',
          id: 'kind-check',
          condition: '.kind == "x"',
          then: [
            { type: 'drop', id: 'drop-1', filter: null, disabled: false, condition: '.noisy' },
          ],
          else: [],
        },
      ],
    },
    { id: 'metrics-pipe', name: 'Metrics passthrough', steps: [] },
  ],
  routes: [
    {
      id: 'catch-all',
      source: 'kafka-main',
      condition: null,
      pipeline: 'main',
      deliver: [{ to: ['kafka-out'] }],
    },
  ],
  lookupTables: [{ id: 'geoip', type: 'mmdb', path: 'lookups/geoip.mmdb' }],
  grokPatterns: [],
};

/** `base`, with `edit` applied to a copy of it. */
function after(edit: (config: FileConfigDto) => void): FileConfigDto {
  const copy = structuredClone(base);
  edit(copy);
  return copy;
}

function text(next: FileConfigDto, previous: FileConfigDto = base) {
  return changeText([{ scope: 'default', before: previous, after: next }]);
}

const pipeline = (config: FileConfigDto, id: string) =>
  config.pipelines.find((p) => p.id === id) as { id: string; name: string; steps: unknown[] };

describe('changeText, one object changed one way', () => {
  it('names the field and both values', () => {
    const next = after((c) => {
      (c.sources[0] as { brokers: string }).brokers = 'b1:9092,b2:9092,b3:9092';
    });
    expect(text(next)).toEqual({
      subject: 'Update source kafka-main: brokers from b1:9092,b2:9092 to b1:9092,b2:9092,b3:9092',
      body: '',
    });
  });

  it('says what was removed', () => {
    const next = after((c) => {
      c.destinations = c.destinations.filter((d) => d.id !== 'kafka-dlq');
    });
    expect(text(next).subject).toBe('Remove destination kafka-dlq');
  });

  it("says a connector's TLS edited, naming what changed", () => {
    const before = after((c) => {
      c.sources.push({
        id: 'acme',
        type: 'kafka',
        brokers: 'b',
        topics: [],
        trustedAuthorities: 'cert-v1',
      });
    });
    const next = structuredClone(before);
    (next.sources.at(-1) as { trustedAuthorities?: string }).trustedAuthorities = 'cert-v2';
    expect(text(next, before).subject).toBe(
      'Update source acme: trustedAuthorities from cert-v1 to cert-v2',
    );
  });

  it('never lets a multiline PEM value break the subject onto a second line', () => {
    const before = after((c) => {
      c.sources.push({
        id: 'acme',
        type: 'kafka',
        brokers: 'b',
        topics: [],
        trustedAuthorities:
          '-----BEGIN CERTIFICATE-----\nMIIBCjCBtAIBADAA\n-----END CERTIFICATE-----',
      });
    });
    const next = structuredClone(before);
    (next.sources.at(-1) as { trustedAuthorities?: string }).trustedAuthorities =
      '-----BEGIN CERTIFICATE-----\nMIIBDkCBtAIBADAA\n-----END CERTIFICATE-----';
    const { subject } = text(next, before);
    expect(subject.split('\n')).toHaveLength(1);
  });

  it('tells a rename from a change of behavior', () => {
    const next = after((c) => {
      pipeline(c, 'metrics-pipe').name = 'Metrics pass-through v2';
    });
    expect(text(next).subject).toBe(
      'Update pipeline Metrics passthrough: name from Metrics passthrough to Metrics pass-through v2',
    );
  });
});

describe('changeText, a pipeline is said by its name', () => {
  it('names an added pipeline, and a removed one by the name it had', () => {
    const added = after((c) => {
      c.pipelines.push({ id: 'pipeline-x7k2m9q1', name: 'Firewall', steps: [] });
    });
    expect(text(added).subject).toBe('Add pipeline Firewall');
    expect(text(base, added).subject).toBe('Remove pipeline Firewall');
  });

  it('names the pipelines a route moved between', () => {
    const next = after((c) => {
      c.routes[0].pipeline = 'metrics-pipe';
    });
    expect(text(next).subject).toBe(
      'Update route kafka-main → Main: pipeline from Main to Metrics passthrough',
    );
  });

  it('names a pipeline the change adds and a route moves onto in the same change', () => {
    const next = after((c) => {
      c.pipelines.push({ id: 'pipeline-x7k2m9q1', name: 'Firewall', steps: [] });
      c.routes[0].pipeline = 'pipeline-x7k2m9q1';
    });
    expect(text(next).body.split('\n')).toEqual([
      'Update route kafka-main → Main: pipeline from Main to Firewall',
      'Add pipeline Firewall',
    ]);
  });

  it('falls back to the id a route names when no side holds that pipeline', () => {
    const next = after((c) => {
      c.routes[0].pipeline = 'pipeline-gone0000';
    });
    expect(text(next).subject).toBe(
      'Update route kafka-main → Main: pipeline from Main to pipeline-gone0000',
    );
  });

  it('names the pipelines a call step moved between', () => {
    const before = after((c) => {
      pipeline(c, 'main').steps.push({
        type: 'call',
        id: 'call-1',
        filter: null,
        disabled: false,
        pipeline: 'metrics-pipe',
      });
      c.pipelines.push({ id: 'pipeline-x7k2m9q1', name: 'Firewall', steps: [] });
    });
    const next = structuredClone(before);
    (pipeline(next, 'main').steps[2] as { pipeline: string }).pipeline = 'pipeline-x7k2m9q1';
    expect(text(next, before).subject).toBe(
      'Update pipeline Main: call step pipeline from Metrics passthrough to Firewall',
    );
  });

  it('says two pipelines of one name by that name, each its own line', () => {
    const next = after((c) => {
      c.pipelines.push(
        { id: 'pipeline-a1a1a1a1', name: 'Parse', steps: [] },
        { id: 'pipeline-b2b2b2b2', name: 'Parse', steps: [] },
      );
    });
    expect(text(next)).toEqual({
      subject: 'Add 2 pipelines',
      body: 'Add pipeline Parse\nAdd pipeline Parse',
    });
  });
});

describe('changeText, absent and empty', () => {
  it('does not read an omitted default as nothing', () => {
    // `stallAfterSecs` absent is its default of 30 seconds, so "none"
    // would say the opposite of what is true.
    const next = after((c) => {
      delete (c.destinations[1] as { stallAfterSecs?: number }).stallAfterSecs;
    });
    expect(text(next).subject).toBe(
      'Update destination kafka-out: stallAfterSecs from 30 to unset',
    );
  });

  it('keeps a field set to nothing distinct from one never set', () => {
    expect(describeValue(undefined)).toBe('unset');
    expect(describeValue(null)).toBe('none');
  });
});

describe('changeText, a union value', () => {
  it('says what the policy does, never its JSON', () => {
    const next = after((c) => {
      c.destinations[1].exceptions = { kinds: { Refused: 'drop' } };
    });
    const { subject } = text(next);
    expect(subject).toBe('Update destination kafka-out: exceptions from unset to 1 entry');
    expect(subject).not.toContain('{');
  });

  it('keeps the destination a choice names', () => {
    expect(describeValue({ dlq: ['kafka-dlq'] })).toBe('dlq kafka-dlq');
    expect(describeValue({ dlq: ['kafka-dlq', 'es-dlq'] })).toBe('dlq kafka-dlq, es-dlq');
  });

  it('says a Kafka time as RFC 3339 in UTC', () => {
    expect(describeValue({ type: 'timestamp', millis: 1_791_984_600_000 })).toBe(
      '2026-10-14T13:30:00Z',
    );
    expect(describeValue({ type: 'timestamp', millis: 1_791_984_600_250 })).toBe(
      '2026-10-14T13:30:00.250Z',
    );
  });

  it('falls back to the tag rather than the shape', () => {
    expect(describeValue({ type: 'somethingNew', a: 1, b: 2 })).toBe('somethingNew');
  });
});

describe('changeText, pipeline steps', () => {
  it('says a step was added', () => {
    const next = after((c) => {
      pipeline(c, 'main').steps.push({
        type: 'rename',
        id: 'rename-1',
        filter: null,
        disabled: false,
        pairs: [{ from: '.a', to: '.b' }],
      });
    });
    expect(text(next).subject).toBe('Update pipeline Main: add rename step');
  });

  it('finds a step removed from inside an if', () => {
    const next = after((c) => {
      (pipeline(c, 'main').steps[1] as { then: unknown[] }).then = [];
    });
    expect(text(next).subject).toBe('Update pipeline Main: remove drop step');
  });

  it('finds a step added inside an if that has no id', () => {
    const before = after((c) => {
      delete (pipeline(c, 'main').steps[1] as { id?: string }).id;
    });
    const next = structuredClone(before);
    (pipeline(next, 'main').steps[1] as { then: unknown[] }).then.push({
      type: 'rename',
      id: 'rename-1',
      filter: null,
      disabled: false,
      pairs: [{ from: '.a', to: '.b' }],
    });
    expect(text(next, before).subject).toBe('Update pipeline Main: add rename step');
  });

  it('carries the value a step assignment moved to', () => {
    const next = after((c) => {
      (
        pipeline(c, 'main').steps[0] as { assignments: { target: string; expr: string }[] }
      ).assignments = [{ target: '.level', expr: '500' }];
    });
    expect(text(next).subject).toBe(
      'Update pipeline Main: eval step assignments from .level=400 to .level=500',
    );
  });

  it('speaks for an if only through its own condition', () => {
    const next = after((c) => {
      (pipeline(c, 'main').steps[1] as { condition: string }).condition = '.kind == "y"';
    });
    expect(text(next).subject).toBe(
      'Update pipeline Main: if step condition from .kind == "x" to .kind == "y"',
    );
  });
});

describe('changeText, more than one object', () => {
  it('counts one kind and lists every value in the body', () => {
    const next = after((c) => {
      (c.destinations[0] as { stallAfterSecs: number }).stallAfterSecs = 45;
      (c.destinations[1] as { stallAfterSecs: number }).stallAfterSecs = 45;
    });
    expect(text(next)).toEqual({
      subject: 'Update 2 destinations',
      body: [
        'Update destination kafka-dlq: stallAfterSecs from 30 to 45',
        'Update destination kafka-out: stallAfterSecs from 30 to 45',
      ].join('\n'),
    });
  });

  it('does not claim one verb for a mixed set', () => {
    const next = after((c) => {
      c.sources.push({
        id: 'kafka-eu',
        type: 'kafka',
        brokers: 'eu1:9092',
        topics: ['logs'],
        groupId: 'smeltr',
      });
      (c.sources[0] as { groupId: string }).groupId = 'smeltr-2';
    });
    expect(text(next).subject).toBe('Change 2 sources');
  });

  it('keeps the verb every edit shares, across kinds', () => {
    const next = after((c) => {
      c.sources.push({
        id: 'kafka-eu',
        type: 'kafka',
        brokers: 'eu1:9092',
        topics: ['logs'],
        groupId: 'smeltr',
      });
      c.destinations.push({
        id: 'elastic-logs',
        type: 'elasticsearch',
        url: 'https://es:9200',
        defaultIndex: 'logs',
      });
    });
    expect(text(next).subject).toBe('Add 2 objects across sources and destinations');
  });

  it('keeps both halves of a small change across two kinds', () => {
    const next = after((c) => {
      (c.sources[0] as { brokers: string }).brokers = 'b1:9092,b2:9092,b3:9092';
      c.destinations.push({
        id: 'elastic-logs',
        type: 'elasticsearch',
        url: 'https://es:9200',
        defaultIndex: 'logs',
      });
    });
    expect(text(next)).toEqual({
      subject: 'Change 2 objects across sources and destinations',
      body: [
        'Update source kafka-main: brokers from b1:9092,b2:9092 to b1:9092,b2:9092,b3:9092',
        'Add destination elastic-logs',
      ].join('\n'),
    });
  });

  it('lists every kind a messy change touched', () => {
    const next = after((c) => {
      c.sources.push({
        id: 'kafka-eu',
        type: 'kafka',
        brokers: 'eu1:9092',
        topics: ['logs'],
        groupId: 'smeltr',
      });
      (c.sources[0] as { brokers: string }).brokers = 'b1:9092,b2:9092,b3:9092';
      c.destinations.push({
        id: 'elastic-logs',
        type: 'elasticsearch',
        url: 'https://es:9200',
        defaultIndex: 'logs',
      });
      c.routes[0].condition = '__source.offset > 0';
      c.lookupTables.push({
        id: 'user-agents',
        type: 'csv',
        path: 'lookups/ua.csv',
        delimiter: ',',
        headers: ['ua'],
      });
      pipeline(c, 'main').steps.push({
        type: 'code',
        id: 'code-1',
        filter: null,
        disabled: false,
        body: '// nothing',
      });
    });
    const { subject, body } = text(next);
    expect(subject).toBe(
      'Change 6 objects across sources, destinations, routes, lookup tables and pipelines',
    );
    expect(body.split('\n')).toEqual([
      'Add source kafka-eu',
      'Update source kafka-main: brokers from b1:9092,b2:9092 to b1:9092,b2:9092,b3:9092',
      'Add destination elastic-logs',
      'Update route kafka-main → Main: condition from none to __source.offset > 0',
      'Add lookup table user-agents',
      'Update pipeline Main: add code step',
    ]);
  });
});

describe('changeText, the edges', () => {
  it('says something true when nothing it models differs', () => {
    expect(text(structuredClone(base))).toEqual({
      subject: 'Update the configuration',
      body: '',
    });
  });

  it('drops the middle of a long value, never an end', () => {
    const long = 'b1:9092,b2:9092,b3:9092,b4:9092,b5:9092,b6:9092,b7:9092';
    const next = after((c) => {
      (c.sources[0] as { brokers: string }).brokers = long;
    });
    const { subject } = text(next);
    expect(subject).toContain('…');
    expect(subject).toContain('b1:9092');
    expect(subject).toContain('b7:9092');
  });

  it('names the group when a change spans more than one', () => {
    const west = after((c) => {
      (c.sources[0] as { groupId: string }).groupId = 'west';
    });
    const east = after((c) => {
      (c.sources[0] as { groupId: string }).groupId = 'east';
    });
    const spans: ScopeChange[] = [
      { scope: 'west', before: base, after: west },
      { scope: 'east', before: base, after: east },
    ];
    const { subject, body } = changeText(spans);
    expect(subject).toBe('Update 2 objects across 2 groups');
    expect(body.split('\n')).toEqual([
      'Update source kafka-main in west: groupId from smeltr to west',
      'Update source kafka-main in east: groupId from smeltr to east',
    ]);
  });
});
