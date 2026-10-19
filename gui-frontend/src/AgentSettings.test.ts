import { describe, expect, it } from 'vitest';
import { keepsStoredKey, missingForAgents, switched } from './AgentSettings.svelte';

describe('missingForAgents', () => {
  it('lacks nothing when every part is there', () => {
    expect(
      missingForAgents({ endpoint: 'https://api.anthropic.com', model: 'm', keyHeld: true }),
    ).toEqual([]);
  });

  it('names each missing part, in the order the form shows them', () => {
    expect(missingForAgents({ endpoint: ' ', model: '', keyHeld: false })).toEqual([
      'endpoint URL',
      'model',
      'API key',
    ]);
  });
});

describe('switched', () => {
  it("moves to the new provider's endpoint and model", () => {
    expect(
      switched('anthropic', 'openai', {
        endpoint: 'https://api.anthropic.com',
        model: 'claude-opus-5-5',
      }),
    ).toEqual({ endpoint: 'https://api.openai.com', model: '' });
    expect(switched('openai', 'anthropic', { endpoint: '', model: '' })).toEqual({
      endpoint: 'https://api.anthropic.com',
      model: 'claude-opus-5-5',
    });
  });

  it('keeps what the administrator typed', () => {
    expect(
      switched('openai-compatible', 'openai', {
        endpoint: 'https://models.example.com/v1',
        model: 'llama',
      }),
    ).toEqual({ endpoint: 'https://models.example.com/v1', model: 'llama' });
  });
});

describe('keepsStoredKey', () => {
  const stored = {
    apiKeySet: true,
    provider: 'anthropic' as const,
    endpoint: 'https://api.anthropic.com',
  };

  it('keeps it while the provider and endpoint stay the same', () => {
    expect(keepsStoredKey(stored, 'anthropic', ' https://api.anthropic.com ')).toBe(true);
  });

  it('forgets it when either changes, as the leader does', () => {
    expect(keepsStoredKey(stored, 'openai', 'https://api.anthropic.com')).toBe(false);
    expect(keepsStoredKey(stored, 'anthropic', 'https://proxy.example.com')).toBe(false);
    expect(keepsStoredKey({ ...stored, apiKeySet: false }, 'anthropic', stored.endpoint)).toBe(
      false,
    );
  });
});
