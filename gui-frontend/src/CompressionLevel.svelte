<!--
  A codec's level, as a slider over the levels it takes, its number in the
  label. Nothing for a codec that takes none. Mutates `value` in place; at the
  codec's default level it leaves `value` unset.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import { compressionLevels, type Compression, type Compressor } from './api';

  let {
    value,
    codec,
    by = 'smeltr',
  }: {
    value: { compressionLevel?: number };
    /** The destination's codec; an unset codec is already resolved to the default one. */
    codec: Compression;
    /** Whose code compresses the destination's output, which decides the levels its codec takes. */
    by?: Compressor;
  } = $props();

  const levels = $derived(compressionLevels(codec, by));
</script>

{#if levels}
  {@const level = value.compressionLevel ?? levels.default}
  <Field field="compressionLevel" label={`compression level: ${level}`}>
    <div class="level">
      <input
        type="range"
        min={levels.min}
        max={levels.max}
        step="1"
        value={level}
        oninput={(e) => {
          const v = Number(e.currentTarget.value);
          if (v === levels.default) delete value.compressionLevel;
          else value.compressionLevel = v;
        }}
      />
      <div class="ends">
        <span>fastest / least compression</span>
        <span>slowest / most compression</span>
      </div>
    </div>
  </Field>
{/if}

<style>
  .level {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    width: 100%;
    max-width: 26rem;
  }
  input[type='range'] {
    width: 100%;
    accent-color: var(--accent);
  }
  .ends {
    display: flex;
    justify-content: space-between;
    font-size: var(--text-micro);
    color: var(--dim);
  }
</style>
