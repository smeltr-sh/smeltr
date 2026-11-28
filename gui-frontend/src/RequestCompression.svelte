<!--
  How a destination that POSTs requests compresses each body: none or gzip,
  and gzip's level. Mutates `value` in place; at none it leaves `compression`
  unset, and a change of codec drops the level.
-->
<script lang="ts" module>
  import type { HttpCompression } from './api';

  /** The settings the fields hold. */
  export type RequestCompression = { compression?: HttpCompression; compressionLevel?: number };

  /** Whether either field differs from its default, for the Advanced header's marker. */
  export function compressionModified(d: RequestCompression): boolean {
    return (d.compression ?? 'none') !== 'none' || d.compressionLevel !== undefined;
  }
</script>

<script lang="ts">
  import CompressionLevel from './CompressionLevel.svelte';
  import Field from './Field.svelte';

  let { value }: { value: RequestCompression } = $props();
</script>

<Field field="compression" label="compression">
  {#snippet help()}
    Each request body is sent gzipped, with <code>Content-Encoding: gzip</code>; the service must
    accept it.
  {/snippet}
  <select
    value={value.compression ?? 'none'}
    onchange={(e) => {
      const v = e.currentTarget.value as HttpCompression;
      if (v === 'none') delete value.compression;
      else value.compression = v;
      delete value.compressionLevel;
    }}
  >
    <option value="none">none</option>
    <option value="gzip">gzip</option>
  </select>
</Field>
<CompressionLevel {value} codec={value.compression ?? 'none'} />
