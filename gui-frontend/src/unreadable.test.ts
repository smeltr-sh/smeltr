import { describe, expect, it } from 'vitest';
import type { UnreadableFileDto } from './api';
import { parseRoute, trail } from './nav';
import { aboutFile, fileHash } from './unreadable';

const file = (path: string): UnreadableFileDto => ({ path, why: 'invalid JSON', deletable: true });

describe('aboutFile', () => {
  const files = [file('groups/west/pipelines/x.json'), file('shared/system.json')];

  it('matches a load error naming the file within its bundle or by its full path', () => {
    expect(aboutFile('pipelines/x.json: invalid JSON: expected value', files)).toBe(true);
    expect(aboutFile('groups/west/pipelines/x.json: invalid JSON', files)).toBe(true);
    expect(aboutFile('shared/system.json: invalid JSON', files)).toBe(true);
  });

  it('leaves every other problem to be shown on its own', () => {
    expect(aboutFile('pipelines/y.json: invalid JSON', files)).toBe(false);
    expect(aboutFile('route "r" names unknown source "s"', files)).toBe(false);
    expect(aboutFile('x.json: invalid JSON', files)).toBe(false);
    expect(aboutFile('pipelines/x.json: invalid JSON', [])).toBe(false);
  });
});

describe('fileHash', () => {
  it('opens the file editor on that path, going back where it was sent from', () => {
    const path = 'groups/west east/pipelines/a&b.json';
    expect(parseRoute(fileHash(path, '#/build/sources'))).toEqual({
      tab: 'build',
      page: 'file',
      path,
      back: '#/build/sources',
    });
  });

  it('goes back to Build when it was given nowhere, or somewhere outside the app', () => {
    expect(parseRoute(fileHash('shared/routes.json'))).toMatchObject({ back: '#/build/routes' });
    expect(parseRoute(fileHash('shared/routes.json', 'https://example.com'))).toMatchObject({
      back: '#/build/routes',
    });
  });

  it('names the file in the trail', () => {
    expect(trail(parseRoute(fileHash('shared/system.json')))).toEqual([
      { label: 'Build', hash: '#/build/routes' },
      { label: 'shared/system.json', hash: undefined },
    ]);
  });
});
