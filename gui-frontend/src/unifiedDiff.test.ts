import { describe, expect, it } from 'vitest';
import { newSideLines, parseUnifiedDiff } from './unifiedDiff';

const DIFF = [
  'diff --git a/net/pipelines.json b/net/pipelines.json',
  '@@ -3,3 +3,4 @@',
  '   "a": 1,',
  '-  "b": 2',
  '+  "b": 3,',
  '+  "c": 4',
  ' }',
  '\\ No newline at end of file',
  'diff --git a/net/routes.json b/net/routes.json',
  '@@ -0,0 +1 @@',
  '+[]',
  '',
].join('\n');

describe('parseUnifiedDiff', () => {
  it('splits files at their header and numbers the new side from the hunk', () => {
    const files = parseUnifiedDiff(DIFF);
    expect(files.map((f) => f.path)).toEqual(['net/pipelines.json', 'net/routes.json']);
    expect(files[0].rows.map((r) => [r.kind, r.newLine])).toEqual([
      ['meta', undefined],
      ['hunk', undefined],
      ['same', 3],
      ['removed', undefined],
      ['added', 4],
      ['added', 5],
      ['same', 6],
      ['meta', undefined],
    ]);
    expect(files[1].rows[2]).toEqual({ kind: 'added', text: '+[]', newLine: 1 });
  });

  it('reads nothing from an empty diff', () => {
    expect(parseUnifiedDiff('')).toEqual([]);
  });
});

describe('newSideLines', () => {
  it('maps each shown new-side line to its text without the mark', () => {
    const lines = newSideLines(parseUnifiedDiff(DIFF)).get('net/pipelines.json');
    expect(lines?.get(4)).toBe('  "b": 3,');
    expect(lines?.has(2)).toBe(false);
  });
});
