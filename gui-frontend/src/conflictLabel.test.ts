import { describe, expect, it } from 'vitest';
import type { MergeConflict } from './api';
import { conflictLabel } from './conflictLabel';

function conflict(path: string, work: string | null, id?: string): MergeConflict {
  return { path, id, base: null, main: '{}', work };
}

describe('conflictLabel', () => {
  it('names a pipeline by its name, never its id', () => {
    const c = conflict(
      'groups/west/pipelines/pipeline-a9f3.json',
      '{"id":"pipeline-a9f3","name":"Enrich","steps":[]}',
    );
    expect(conflictLabel(c)).toBe('west · Pipeline Enrich');
  });

  it('names a route by its source, never its id', () => {
    const c = conflict(
      'shared/routes.json',
      '{"id":"route-x7k2","source":"kafka-1","pipeline":"pipeline-a9f3"}',
      'route-x7k2',
    );
    expect(conflictLabel(c)).toBe('shared · Route from kafka-1');
  });

  it('tells routes from one source apart by which rows they take and where they go', () => {
    const c = conflict(
      'shared/routes.json',
      '{"id":"route-x7k2","source":"kafka-1","condition":"level = \'error\'","pipeline":"pipeline-a9f3",' +
        '"deliver":[{"to":["es"]},{"when":"x","to":["es","dlq"]}]}',
      'route-x7k2',
    );
    expect(conflictLabel(c)).toBe("shared · Route from kafka-1 if level = 'error' to es, dlq");
  });

  it('reads the next side when one does not parse', () => {
    const c: MergeConflict = {
      path: 'groups/west/pipelines/pipeline-a9f3.json',
      base: '{"id":"pipeline-a9f3","name":"Old"}',
      main: '{"id":"pipeline-a9f3","name":"Enrich"}',
      work: '{ not json',
    };
    expect(conflictLabel(c)).toBe('west · Pipeline Enrich');
  });

  it('names a file outside any bundle by its path', () => {
    expect(conflictLabel(conflict('.lfsconfig', 'x'))).toBe('.lfsconfig');
    expect(conflictLabel(conflict('sources.json', '[]'))).toBe('sources.json');
  });

  it('names a source by the id its author gave it', () => {
    const c = conflict('groups/west/sources.json', '{"id":"kafka-1"}', 'kafka-1');
    expect(conflictLabel(c)).toBe('west · Source kafka-1');
  });

  it('reads a side that still has the object when the branch removed it', () => {
    const c: MergeConflict = {
      path: 'shared/pipelines/pipeline-a9f3.json',
      base: null,
      main: '{"id":"pipeline-a9f3","name":"Enrich"}',
      work: null,
    };
    expect(conflictLabel(c)).toBe('shared · Pipeline Enrich');
  });

  it('names any other file by its path in the bundle', () => {
    expect(conflictLabel(conflict('groups/west/lookups/t.csv', 'a,b'))).toBe(
      'west · lookups/t.csv',
    );
  });
});
