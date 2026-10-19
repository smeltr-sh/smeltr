import { describe, expect, it } from 'vitest';
import { funnyBranchName } from './branchNames';

describe('funnyBranchName', () => {
  it('is an adjective and an animal, and a name git takes as it is', () => {
    expect(funnyBranchName(() => 0)).toBe('absurd-aardvark');
    expect(funnyBranchName(() => 0.999)).toBe('zealous-zebra');
    for (let i = 0; i < 50; i++) expect(funnyBranchName()).toMatch(/^[a-z]+-[a-z]+$/);
  });

  it('is not the same name every time', () => {
    expect(new Set(Array.from({ length: 50 }, () => funnyBranchName())).size).toBeGreaterThan(1);
  });
});
