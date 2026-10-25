<!--
  Settings, one block each: a line naming the setting, its type and its default, then what it does, then each value
  it takes when it takes a fixed set of them, or else examples of what to type.
-->
<script lang="ts">
  import Prose from './Prose.svelte';
  import type { Setting } from '#lib/connector-reference.ts';
  /** A setting as shown: a startup setting names its environment variable in place of a type. */
  type Row = Omit<Setting, 'type' | 'examples'> & {
    type?: string;
    env?: string;
    examples?: unknown[];
  };
  /** `anchored` gives each block its setting's name as its id, so a guide can link to one. Only for a page that
   * shows one table, where every name is unique. */
  let { settings, anchored = false }: { settings: Row[]; anchored?: boolean } = $props();
  /** A name in pieces that each end after a dot or an underscore, so it wraps only there. */
  const pieces = (name: string) => name.split(/(?<=[._])/);
  /** A value as a bundle would spell it, spaced so an object can wrap. */
  const shown = (value: unknown) =>
    typeof value === 'string' ? value : JSON.stringify(value).replaceAll('":', '": ');
  /** A value in a list of them: an object by the `type` that tells it from the others, its other keys left to what
   * it does. */
  const named = (value: unknown) =>
    typeof value === 'object' && value !== null && 'type' in value
      ? shown(value.type)
      : shown(value);
  type Scalar = string | number | boolean;
  const scalar = (v: unknown): v is Scalar => ['string', 'number', 'boolean'].includes(typeof v);
  /** An example as the form takes it: a list as the chips its box shows, a map as its name and value rows, anything
   * else as the bundle spells it. */
  const example = (
    value: unknown,
    type?: string,
  ): { chips: Scalar[] } | { pairs: [string, Scalar][] } | { text: string } => {
    if (Array.isArray(value) && value.every(scalar)) return { chips: value };
    if (type === 'map' && typeof value === 'object' && value !== null) {
      const pairs = Object.entries(value);
      if (pairs.every(([, v]) => scalar(v))) return { pairs: pairs as [string, Scalar][] };
    }
    // A blank string, such as a space separator, is quoted so it shows.
    if (typeof value === 'string' && value.trim() === '') return { text: JSON.stringify(value) };
    return { text: shown(value) };
  };
  /** The examples of a setting whose box is typed into. One that takes a fixed set of values, or a switch, shows
   * those instead. */
  const typed = (s: Row) => (s.values?.length || s.type === 'bool' ? [] : (s.examples ?? []));
</script>

<div class="settings">
  {#each settings as s (s.name)}
    <div class="setting" id={anchored ? s.name : undefined}>
      <p class="head">
        <code class="name"
          >{#each pieces(s.name) as p, i (i)}{p}<wbr />{/each}</code
        >
        {#if s.env}<span class="dim"
            >env <code
              >{#each pieces(s.env) as p, i (i)}{p}<wbr />{/each}</code
            ></span
          >{/if}
        {#if s.type}<span class="dim">{s.type}</span>{/if}
        {#if s.required}<span class="dim">required</span>{:else if s.default !== undefined}<span
            class="dim">default <code>{shown(s.default)}</code></span
          >{:else if s.absent}<span class="dim">default <Prose text={s.absent} /></span>{/if}
      </p>
      <p class="what"><Prose text={s.what} /></p>
      {#if typed(s).length}
        <div class="examples">
          <span class="dim">{typed(s).length > 1 ? 'Examples' : 'Example'}</span>
          <div class="list">
            {#each typed(s) as x, i (i)}
              {@const e = example(x, s.type)}
              {#if 'chips' in e}<span class="box"
                  >{#each e.chips as c, j (j)}<code class="chip">{c}</code>{/each}</span
                >{:else if 'pairs' in e}<span class="box pairs"
                  >{#each e.pairs as [k, v] (k)}<code>{k} = {v}</code>{/each}</span
                >{:else}<code>{e.text}</code>{/if}
            {/each}
          </div>
        </div>
      {/if}
      {#if s.values?.length}
        <dl class="values">
          {#each s.values as v (named(v.value))}
            <dt><code>{named(v.value)}</code></dt>
            <dd><Prose text={v.what} /></dd>
          {/each}
        </dl>
      {/if}
    </div>
  {/each}
</div>

<style>
  .settings {
    margin: 1rem 0 1.5rem;
    border-top: 1px solid var(--border);
  }
  .setting {
    padding: 0.8rem 0;
    border-bottom: 1px solid var(--border);
  }
  .setting p {
    margin: 0;
  }
  .head {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.2rem 0.8rem;
  }
  .name {
    font-weight: 700;
  }
  .dim {
    color: var(--dim);
    font-size: var(--text-note);
  }
  .what {
    margin-top: 0.3rem;
  }
  .examples {
    display: grid;
    grid-template-columns: auto 1fr;
    align-items: baseline;
    gap: 0.8rem;
    margin-top: 0.4rem;
  }
  .list {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem 1rem;
    min-width: 0;
  }
  /* A URL or a key wraps anywhere rather than widening a phone's page. */
  .list code {
    overflow-wrap: anywhere;
    /* A code body keeps its lines. */
    white-space: pre-wrap;
  }
  /* One example that is a list or a map, framed as the box the form shows it in, so two of them read apart. */
  .box {
    display: inline-flex;
    flex-wrap: wrap;
    gap: 0.3rem;
    padding: 0.2rem;
    border: 1px solid var(--border);
    border-radius: 6px;
  }
  /* As the chips of the form's list box. */
  .chip {
    padding: 0.05rem 0.55rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: color-mix(in srgb, var(--ink) 8%, transparent);
  }
  /* A map's pairs stack, as the rows of the form's map box. */
  .pairs {
    flex-direction: column;
    padding: 0.2rem 0.4rem;
  }
  .values {
    display: grid;
    /* A long value, such as a position object, wraps rather than squeezing out what it does. */
    grid-template-columns: fit-content(45%) 1fr;
    gap: 0.3rem 0.8rem;
    margin: 0.5rem 0 0 1rem;
  }
  .values dt,
  .values dd {
    margin: 0;
  }
</style>
