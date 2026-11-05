<!--
  A step example: the code or step settings, then the input record beside the output
  record. A line whose field the step changed, added or removed carries a
  rail and a sign on the side where it shows, so the change reads at a
  glance and both records stay real JSON. Code that changes how many records
  there are takes arrays of records, shown as they are; the lookup tables its
  code reads come first.
-->
<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import { Decimal, stringify } from '#lib/json-text.ts';

  type Rec = Record<string, unknown>;
  type State = 'same' | 'changed' | 'added' | 'removed';
  interface Line {
    pad: string;
    html: string;
    state: State;
  }

  let {
    code,
    input,
    output,
    lookupTables = [],
    label = 'Code',
  }: {
    code?: string;
    input: Rec | Rec[];
    output: Rec | Rec[];
    lookupTables?: { id: string; headers: string[]; rows: string[][] }[];
    label?: string;
  } = $props();

  const isObject = (v: unknown): v is Rec =>
    v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Decimal);

  /** The value at `path` in `rec`, or `undefined` when the record lacks it. */
  function at(rec: unknown, path: string[]): unknown {
    let v = rec;
    for (const key of path) {
      if (!isObject(v) || !(key in v)) return undefined;
      v = v[key];
    }
    return v;
  }

  /** `rec` as indented JSON, one entry per line, each leaf compared with the
   * same path in `other`. `missing` is the state of a leaf `other` lacks. */
  function lines(rec: Rec, other: Rec, missing: State): Line[] {
    const out: Line[] = [{ pad: '', html: '{', state: 'same' }];
    const walk = (obj: Rec, path: string[], depth: number) => {
      const keys = Object.keys(obj);
      keys.forEach((key, i) => {
        const pad = '  '.repeat(depth);
        const comma = i < keys.length - 1 ? ',' : '';
        const value = obj[key];
        const here = [...path, key];
        if (isObject(value) && Object.keys(value).length > 0) {
          out.push({ pad, html: highlight(`${JSON.stringify(key)}: {`), state: 'same' });
          walk(value, here, depth + 1);
          out.push({ pad, html: `}${comma}`, state: 'same' });
          return;
        }
        const theirs = at(other, here);
        const state: State =
          theirs === undefined
            ? missing
            : stringify(theirs) === stringify(value)
              ? 'same'
              : 'changed';
        const text = `${JSON.stringify(key)}: ${stringify(value)}${comma}`;
        out.push({ pad, html: highlight(text), state });
      });
    };
    walk(rec, [], 1);
    out.push({ pad: '', html: '}', state: 'same' });
    return out;
  }

  const paired = $derived(!Array.isArray(input) && !Array.isArray(output));
  let before = $derived(paired ? lines(input as Rec, output as Rec, 'removed') : []);
  let after = $derived(paired ? lines(output as Rec, input as Rec, 'added') : []);
  const records = (v: Rec | Rec[]) => (Array.isArray(v) ? v : [v]);
  /** A record on one line, as JSON with a space after each colon and comma. */
  function flat(v: unknown): string {
    if (Array.isArray(v)) return `[${v.map(flat).join(', ')}]`;
    if (isObject(v))
      return `{ ${Object.entries(v)
        .map(([k, x]) => `${JSON.stringify(k)}: ${flat(x)}`)
        .join(', ')} }`;
    return stringify(v);
  }
</script>

{#each lookupTables as table (table.id)}
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

<figure class="example">
  {#if code}
    <div>
      <p class="label">{label}</p>
      <pre><code>{@html highlight(code)}</code></pre>
    </div>
  {/if}
  {#if !paired}
    <div class="pair">
      <div>
        <p class="label">Input</p>
        {#each records(input) as record, i (i)}
          <pre class="record"><code>{@html highlight(flat(record))}</code></pre>
        {/each}
      </div>
      <span class="arrow" aria-hidden="true">&rarr;</span>
      <div>
        <p class="label">Output</p>
        {#each records(output) as record, i (i)}
          <pre class="record"><code>{@html highlight(flat(record))}</code></pre>
        {:else}
          <p class="none">No records</p>
        {/each}
      </div>
    </div>
  {:else}
    <div class="pair">
      <div>
        <p class="label">Input</p>
        <pre><code
            >{#each before as line, i (i)}<span class="line {line.state}"
                >{line.pad}<span class="text">{@html line.html}</span></span
              >{/each}</code
          ></pre>
      </div>
      <span class="arrow" aria-hidden="true">&rarr;</span>
      <div>
        <p class="label">Output</p>
        <pre><code
            >{#each after as line, i (i)}<span class="line {line.state}"
                >{line.pad}<span class="text">{@html line.html}</span></span
              >{/each}</code
          ></pre>
      </div>
    </div>
  {/if}
</figure>

<style>
  .example {
    display: grid;
    gap: 0.9rem;
    margin: 1.25rem 0;
  }
  .label {
    margin: 0 0 0.35rem;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
  }
  pre {
    margin: 0;
  }
  pre.record + pre.record {
    margin-top: 0.5rem;
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
  /* As wide as the longest line, so a line's tint runs under all of its text when the block scrolls. */
  .pair code {
    display: block;
    width: max-content;
    min-width: 100%;
  }
  .arrow {
    align-self: center;
    color: var(--dim);
    font-size: 1.3rem;
  }
  /* A changed line says so in the gutter, as the GUI's diff does: a rail
     and a sign in the state's color, over a faint tint. The text keeps
     its own token colors, which share hues with the states. */
  .line {
    display: block;
    margin-inline: -0.6rem -0.4rem;
    padding-inline: 0.25rem 0.4rem;
    border-left: 3px solid transparent;
  }
  .line::before {
    content: ' ';
    display: inline-block;
    width: 1.1em;
  }
  .line.changed {
    border-color: var(--warn);
    background: color-mix(in oklch, var(--warn) 9%, transparent);
  }
  .line.changed::before {
    content: '~';
    color: var(--warn);
  }
  .line.added {
    border-color: var(--added);
    background: color-mix(in oklch, var(--added) 8%, transparent);
  }
  .line.added::before {
    content: '+';
    color: var(--added);
  }
  .line.removed {
    border-color: var(--removed);
    background: color-mix(in oklch, var(--removed) 8%, transparent);
  }
  .line.removed::before {
    content: '\2212';
    color: var(--removed);
  }
  /* Struck from the text on, not from the indent. */
  .line.removed .text {
    text-decoration: line-through;
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
