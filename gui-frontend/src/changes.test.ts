import { describe, expect, it } from 'vitest';
import { isOpen } from './changes';
import type { BranchDto } from './api';

const branch = (change?: BranchDto['change']): BranchDto => ({ name: 'work', change }) as BranchDto;

describe('isOpen', () => {
  it('counts only an open change', () => {
    expect(isOpen(branch({ state: 'open' } as never))).toBe(true);
  });

  it('does not count a closed one, so the branch can be offered again', () => {
    expect(isOpen(branch({ state: 'closed' } as never))).toBe(false);
  });

  it('does not count a branch that never had a change', () => {
    expect(isOpen(branch(undefined))).toBe(false);
  });
});
