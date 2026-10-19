<!--
  A step reference example from `stepExamples`: the inline lookup tables and
  called pipelines it names, each shown step as the GUI draws it (its saved
  JSON one click away), then the records in and out. One record to one record shows the change line by
  line; any other count shows the records as they are.
-->
<script lang="ts">
  import CodeExample from './CodeExample.svelte';
  import { highlight } from '#lib/highlight/highlight.ts';
  import { Decimal, stringify } from '#lib/json-text.ts';

  type Rec = Record<string, unknown>;
  let {
    example,
    pairRecords = false,
  }: {
    /** Opt in only when each output corresponds to the input at the same index. */
    pairRecords?: boolean;
    example: {
      steps: unknown[];
      pipelines?: { id: string; steps: unknown[] }[];
      lookupTables?: { id: string; headers: string[]; rows: string[][] }[];
      input: unknown[];
      output: unknown[];
      /** The pipeline's answer to a failed record, when the example shows one. */
      exceptions?: 'tag' | 'drop';
      /** Where the step's GUI screenshots are, from `stepExamples`. */
      image: string;
    };
  } = $props();

  const isRecord = (v: unknown): v is Rec =>
    v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Decimal);
  /** `v` as JSON indented two spaces; a value that fits in `fit` columns stays on one line. */
  function pretty(v: unknown, pad = '', fit = 60): string {
    const inner = pad + '  ';
    const flat = flatJson(v);
    if (pad.length + flat.length <= fit) return flat;
    if (Array.isArray(v) && v.some((x) => isRecord(x) || Array.isArray(x)))
      return `[\n${v.map((x) => inner + pretty(x, inner, fit)).join(',\n')}\n${pad}]`;
    if (isRecord(v) && Object.keys(v).length > 0)
      return `{\n${Object.entries(v)
        .map(([k, x]) => `${inner}${JSON.stringify(k)}: ${pretty(x, inner, fit)}`)
        .join(',\n')}\n${pad}}`;
    return flatJson(v);
  }
  /** `v` on one line, with a space after each colon and comma. */
  function flatJson(v: unknown): string {
    if (Array.isArray(v)) return `[${v.map(flatJson).join(', ')}]`;
    if (isRecord(v))
      return Object.keys(v).length === 0
        ? '{}'
        : `{ ${Object.entries(v)
            .map(([k, x]) => `${JSON.stringify(k)}: ${flatJson(x)}`)
            .join(', ')} }`;
    return stringify(v);
  }
  // The page names what a leading parser declares, so the steps shown start after it.
  const shown = $derived(
    example.steps.length > 1 && isRecord(example.steps[0]) && example.steps[0].type === 'parser'
      ? example.steps.slice(1)
      : example.steps,
  );
  const step = $derived(pretty(shown.length === 1 ? shown[0] : shown));
  const paired = $derived(
    (example.input.length === 1 || pairRecords) &&
      example.input.length > 0 &&
      example.input.length === example.output.length &&
      example.input.every(isRecord) &&
      example.output.every(isRecord),
  );
  // A record is drawn one field per line, as CodeExample draws one.
  const show = (v: unknown) => (typeof v === 'string' ? v : pretty(v, '', 0));
</script>

{#each example.lookupTables ?? [] as table (table.id)}
  <div class="table-scroll">
    <table>
      <caption>Lookup table <code>{table.id}</code></caption>
      <thead
        ><tr
          >{#each table.headers as h}<th scope="col">{h}</th>{/each}</tr
        ></thead
      >
      <tbody>
        {#each table.rows as row}
          <tr
            >{#each row as cell}<td><code>{cell}</code></td>{/each}</tr
          >
        {/each}
      </tbody>
    </table>
  </div>
{/each}
{#each example.pipelines ?? [] as called (called.id)}
  <p class="label">Pipeline <code>{called.id}</code></p>
  <pre><code
      >{@html highlight(pretty(called.steps.length === 1 ? called.steps[0] : called.steps))}</code
    ></pre>
{/each}
<div class="step">
  <p class="label">Step</p>
  {#each shown as s, n (n)}
    {#each ['light', 'dark'] as theme (theme)}
      <img
        class="shot-{theme}"
        srcset="/{example.image}-{n}-{theme}.png 2x"
        alt="The {isRecord(s) ? s.type : ''} step's settings in the pipeline editor"
        loading="lazy"
        decoding="async"
      />
    {/each}
  {/each}
  {#if example.exceptions}
    <p class="policy">
      The group's <strong>exception answers</strong>: every exception
      <strong>{example.exceptions}</strong>.
    </p>
  {/if}
  <details>
    <summary>Saved JSON</summary>
    <pre><code>{@html highlight(step)}</code></pre>
  </details>
</div>
{#if paired}
  {#each example.input as record, i (i)}
    {#if example.input.length > 1}<p class="label">Record {i + 1}</p>{/if}
    <CodeExample input={record as Rec} output={example.output[i] as Rec} />
  {/each}
{:else}
  <figure class="example">
    <div class="pair">
      <div>
        <p class="label">Input</p>
        {#each example.input as record, i (i)}
          <pre><code>{@html highlight(show(record))}</code></pre>
        {/each}
      </div>
      <span class="arrow" aria-hidden="true">&rarr;</span>
      <div>
        <p class="label">Output</p>
        {#each example.output as record, i (i)}
          <pre><code>{@html highlight(show(record))}</code></pre>
        {:else}
          <p class="none">No records</p>
        {/each}
      </div>
    </div>
  </figure>
{/if}

<style>
  .example {
    display: grid;
    gap: 0.9rem;
    margin: 1.25rem 0;
  }
  .label {
    margin: 0 0 0.35rem;
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--dim);
  }
  pre {
    margin: 0 0 0.5rem;
  }
  /* A name keeps its own case under an upper-case label. */
  .label code {
    text-transform: none;
  }
  .step {
    margin: 1.25rem 0 0;
  }
  .step img {
    max-width: 100%;
    height: auto;
    margin-bottom: 0.5rem;
    border-radius: 6px;
  }
  .policy {
    margin: 0 0 0.5rem;
    font-size: 0.9rem;
  }
  summary {
    cursor: pointer;
    color: var(--dim);
    font-size: 0.85rem;
  }
  .none {
    margin: 0;
    color: var(--dim);
  }
  .pair {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
    gap: 0.75rem;
    align-items: start;
  }
  .pair > div {
    min-width: 0;
  }
  .arrow {
    align-self: center;
    color: var(--dim);
    font-size: 1.3rem;
  }
  @media (max-width: 44rem) {
    .pair {
      grid-template-columns: minmax(0, 1fr);
    }
    .arrow {
      justify-self: center;
      transform: rotate(90deg);
    }
  }
</style>
