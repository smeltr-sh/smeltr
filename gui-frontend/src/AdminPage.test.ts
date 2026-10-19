import { describe, expect, it } from 'vitest';
import { slugify } from './AdminPage.svelte';

describe('slugify', () => {
  it('is empty for an empty name', () => {
    expect(slugify('')).toBe('');
  });

  it('turns spaces into hyphens', () => {
    expect(slugify('prod west')).toBe('prod-west');
  });

  it('turns punctuation into hyphens', () => {
    expect(slugify('prod, west!!')).toBe('prod-west');
  });

  it('trims leading and trailing junk', () => {
    expect(slugify('  --Foo Bar--  ')).toBe('foo-bar');
  });

  it('collapses repeated hyphens', () => {
    expect(slugify('foo---bar')).toBe('foo-bar');
  });

  it('leaves an already clean id untouched', () => {
    expect(slugify('prod-west')).toBe('prod-west');
  });
});
