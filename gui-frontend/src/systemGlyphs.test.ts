import { describe, expect, it } from 'vitest';
import type { MapSystem } from './api';
import {
  mapGlyph,
  PIPELINE_GLYPH,
  SYSTEM_GLYPHS,
  SYSTEM_LABELS,
  UNKNOWN_SYSTEM_GLYPH,
} from './systemGlyphs';

describe('mapGlyph', () => {
  it('is the system behind a source or destination, and the block for a pipeline', () => {
    expect(mapGlyph({ kind: 'source', system: 'kafka' })).toBe(SYSTEM_GLYPHS.kafka);
    expect(mapGlyph({ kind: 'destination', system: 'elasticsearch' })).toBe(
      SYSTEM_GLYPHS.elasticsearch,
    );
    expect(mapGlyph({ kind: 'destination', system: 'splunkHec' })).toBe(SYSTEM_GLYPHS.splunkHec);
    expect(mapGlyph({ kind: 'destination', system: 'http' })).toBe(SYSTEM_GLYPHS.http);
    expect(mapGlyph({ kind: 'pipeline' })).toBe(PIPELINE_GLYPH);
  });

  it('gives HTTP a sign of its own', () => {
    const others = Object.entries(SYSTEM_GLYPHS).filter(([system]) => system !== 'http');
    expect(others.map(([, glyph]) => glyph)).not.toContain(SYSTEM_GLYPHS.http);
  });

  it('gives Iceberg a sign and a label of its own', () => {
    const iceberg = mapGlyph({ kind: 'destination', system: 'iceberg' });
    const others = Object.entries(SYSTEM_GLYPHS).filter(([system]) => system !== 'iceberg');
    expect(others.map(([, glyph]) => glyph.d)).not.toContain(iceberg.d);
    expect(iceberg).not.toBe(UNKNOWN_SYSTEM_GLYPH);
    expect(SYSTEM_LABELS.iceberg).toBe('Iceberg');
  });

  it('gives Delta a sign and a label of its own', () => {
    const delta = mapGlyph({ kind: 'destination', system: 'delta' });
    const others = Object.entries(SYSTEM_GLYPHS).filter(([system]) => system !== 'delta');
    expect(others.map(([, glyph]) => glyph.d)).not.toContain(delta.d);
    expect(delta).not.toBe(UNKNOWN_SYSTEM_GLYPH);
    expect(SYSTEM_LABELS.delta).toBe('Delta');
  });

  it('draws Event Hubs as the stream Kafka is', () => {
    expect(mapGlyph({ kind: 'source', system: 'eventHubs' })).toBe(SYSTEM_GLYPHS.kafka);
  });

  it('draws every object store as one sign, source or destination', () => {
    const pail = mapGlyph({ kind: 'destination', system: 's3' });
    expect(pail).not.toBe(UNKNOWN_SYSTEM_GLYPH);
    expect(mapGlyph({ kind: 'destination', system: 'gcs' })).toBe(pail);
    expect(mapGlyph({ kind: 'destination', system: 'azureBlob' })).toBe(pail);
    expect(mapGlyph({ kind: 'source', system: 's3Sqs' })).toBe(pail);
    expect(mapGlyph({ kind: 'source', system: 'azureBlobQueue' })).toBe(pail);
    expect(mapGlyph({ kind: 'source', system: 'gcsPubSub' })).toBe(pail);
  });

  it('draws a system the leader knows and this build does not, rather than failing', () => {
    // `MapSystem` mirrors `leader::map::System` by hand; the leader may be ahead of this build.
    const ahead = 'tape' as MapSystem;
    expect(mapGlyph({ kind: 'destination', system: ahead })).toBe(UNKNOWN_SYSTEM_GLYPH);
  });
});
