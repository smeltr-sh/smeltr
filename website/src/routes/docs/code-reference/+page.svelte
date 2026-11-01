<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  // The catalog is data, not markup: `tests/website-reference-tests.rs`
  // evaluates every example against the engine and fails if a documented
  // result is wrong or a function is missing.
  import {
    EXCEPTIONS as exceptions,
    GROUPS as groups,
    SIGNATURES as signatures,
    TYPES as types,
    topic,
    type Example,
    type Grouped,
    type Rejected,
  } from '#lib/reference.ts';
  import { GLOBAL_FIELDS as recordFields } from '#lib/connector-reference.ts';
  import DocsLayout from '#lib/components/DocsLayout.svelte';
  import Prose from '#lib/components/Prose.svelte';
  import language from '#lib/code-language.json';
  import CodeExample from '#lib/components/CodeExample.svelte';
  import examplesText from '#lib/code-steps-examples.json?raw';
  import { parse } from '#lib/json-text.ts';
  // Each Code step example is run against the engine by `tests/website-reference-tests.rs`.
  type Refused = { code: string; fixes: string[] };
  // Read so that a number keeps its written form: `2.0` stays `2.0`.
  const examplesData = parse(examplesText);
  const blocks = examplesData.blocks;
  const errors: Record<string, Refused[]> = examplesData.errors;
  const entries = groups
    .flatMap((group) =>
      group.functions.map((fn) => ({ id: fn.name, title: fn.name, summary: fn.summary })),
    )
    .concat(
      language.parseFunctions.map((fn) => ({
        id: `parse-${fn.name}`,
        title: fn.name,
        summary: fn.summary,
      })),
    );
  // The record comes before what you write to it: its fields, how a name reaches one, and the object it is.
  const RECORD = ['names', 'objects'];
  const recordGroups = RECORD.map((id) => groups.find((g) => g.id === id)!);
  const typeGroups = groups.filter((g) => !RECORD.includes(g.id));
  const sections = [
    { id: 'how-it-works', title: 'How it works' },
    { id: 'fields', title: 'Fields' },
    ...recordGroups.map((g) => ({ id: g.id, title: g.title })),
    { id: 'statements', title: 'Statements' },
    { id: 'types', title: 'Types' },
    { id: 'nulls', title: 'Null' },
    ...typeGroups.map((g) => ({ id: g.id, title: g.title })),
    { id: 'parse', title: 'Parse and lookup' },
    { id: 'exceptions', title: 'Exceptions and try' },
    { id: 'see-also', title: 'See also' },
  ];
  const nulls = topic('nulls');
  const exceptionTopic = topic('exceptions');
  const calls = topic('calls');
  const typeTopic = topic('types');

  /** A list split into the labeled runs the page draws. A label's items are
      contiguous, so a change of label starts a run. */
  function runs<T extends Grouped>(items: T[]): { group?: string; items: T[] }[] {
    const out: { group?: string; items: T[] }[] = [];
    for (const item of items) {
      const last = out.at(-1);
      if (last && last.group === item.group) last.items.push(item);
      else out.push({ group: item.group, items: [item] });
    }
    return out;
  }
</script>

<svelte:head>
  <title>Language reference — smeltr</title>
  <meta
    name="description"
    content="Every operator and function in Smeltr's language, grouped by type with worked examples: naming a field, comparison and logic, conversion, numbers, sequences, strings, regular expressions, arrays, dates, durations and addresses."
  />
</svelte:head>

<!-- One example: any bindings it reads, the expression, and what it gives. -->
{#snippet example(ex: Example)}
  {#each ex.given ?? [] as g, k (k)}
    <code class="given">{@html highlight(g)}</code>
  {/each}
  <code class="expr">{@html highlight(ex.expr)}</code>
  <span class="arrow" aria-hidden="true">&rarr;</span>
  {#if ex.result !== undefined}
    <code class="result" class:nul={ex.result === 'null'}>{ex.result}</code>
  {:else}
    <span class="varies">varies per batch</span>
  {/if}
  {#each ex.throws ?? [] as kind (kind)}
    <span class="throws error">throws <code>{kind}</code></span>
  {/each}
  {#if ex.note}
    <span class="exnote">// <Prose text={ex.note} /></span>
  {/if}
{/snippet}

{#snippet examples(list: Example[])}
  <ul class="examples">
    {#each list as ex, i (i)}
      <li>{@render example(ex)}</li>
    {/each}
  </ul>
{/snippet}

<!-- One Code step example: its code, and a record before and after it. -->
{#snippet codeExample(name: string)}
  <CodeExample
    code={blocks[name].code}
    input={blocks[name].input}
    output={blocks[name].output}
    lookupTables={blocks[name].lookupTables}
  />
{/snippet}

{#snippet refused(list: Refused[])}
  <ul>
    {#each list as item}
      <li>
        <code>{item.code}</code> does not compile. Write {#each item.fixes as fix, i}{#if i > 0}, or
          {/if}<code>{fix}</code>{/each}.
      </li>
    {/each}
  </ul>
{/snippet}

{#snippet rejected(list: Rejected[])}
  <div class="tscroll">
    <table class="ops rejected">
      <caption>Does not compile</caption>
      <tbody>
        {#each list as r, i (i)}
          <tr>
            <td class="ex"><code class="expr">{@html highlight(r.expr)}</code></td>
            <td class="gloss"><Prose text={r.why} /></td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/snippet}

<!-- One group of the catalog: its blurb, its operators and its functions. -->
{#snippet group(g: (typeof groups)[number])}
  <section class="group">
    <h2 id={g.id}>
      <a class="heading-link" href={`#${g.id}`}>{g.title}<span aria-hidden="true"> #</span></a>
    </h2>
    <p class="blurb"><Prose text={g.blurb} /></p>

    {#each g.notes ?? [] as note, i (i)}
      <p class="note"><Prose text={note} /></p>
    {/each}

    {#if g.id === 'objects'}
      <p>
        <code>{'{ip: str, port: i64}'}</code> is closed: converting a value to it with
        <code>as</code>
        keeps the fields it names and drops the rest. <code>{'{ip: str, ...}'}</code> is open and keeps
        them.
      </p>
      {@render codeExample('objectTypes')}
      <p>
        Writing a field by name adds it to either kind. A computed key, such as <code>o[key]</code>,
        names a field only the record knows, so it writes only to an open object: one declared with
        <code>...</code>, or one you build without declaring its type. A computed key written to a
        closed object is an error when you save.
      </p>
      {@render refused(errors.objects)}
    {/if}

    {#if g.rows}
      <div class="tscroll">
        <table class="ops">
          <tbody>
            {#each g.rows as row, i (i)}
              <tr>
                <td class="sym"><code>{row.symbol}</code></td>
                <td class="gloss"><Prose text={row.gloss} /></td>
                <td class="ex">
                  {#each row.examples as ex, j (j)}
                    <div class="exline">{@render example(ex)}</div>
                  {/each}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}

    {#if g.rejected?.length}{@render rejected(g.rejected)}{/if}

    {#each runs(g.functions) as run, r (r)}
      {#if run.group}
        <p class="grouphead">{run.group}</p>
      {/if}
      {#each run.items as fn (fn.name)}
        <article class="fn">
          <h3 id={fn.name}>
            <a class="heading-link" href={`#${fn.name}`}
              ><code>{fn.signature}</code><span aria-hidden="true"> #</span></a
            >
          </h3>
          {#if signatures[fn.name]}
            <p class="types">
              {#each signatures[fn.name] as row, i (i)}
                <span><code>{row.params}</code> &rarr; <code>{row.result}</code></span>
              {/each}
            </p>
          {/if}
          <p class="summary"><Prose text={fn.summary} /></p>
          {#each fn.notes ?? [] as note, i (i)}
            <p class="note"><Prose text={note} /></p>
          {/each}

          {#each runs(fn.examples) as run, r (r)}
            {#if run.group}
              <p class="grouphead">{run.group}</p>
            {/if}
            {@render examples(run.items)}
          {/each}

          {#if fn.specifiers?.length}
            {#if fn.specifierBlurb}
              <p class="note"><Prose text={fn.specifierBlurb} /></p>
            {/if}
            <div class="tscroll">
              <table class="ops">
                <caption>Format specifiers for <code>parseTime</code></caption>
                {#each runs(fn.specifiers) as run, r (r)}
                  <tbody>
                    {#if run.group}
                      <tr>
                        <th class="grouphead" colspan="3" scope="colgroup">{run.group}</th>
                      </tr>
                    {/if}
                    {#each run.items as row (row.specifier)}
                      <tr>
                        <td class="sym"><code>{row.specifier}</code></td>
                        <td class="gloss"><Prose text={row.matches} /></td>
                        <td class="ex">
                          {#each row.examples as ex, j (j)}
                            <div class="exline">{@render example(ex)}</div>
                          {/each}
                        </td>
                      </tr>
                    {/each}
                  </tbody>
                {/each}
              </table>
            </div>
          {/if}
        </article>
      {/each}
    {/each}
  </section>
{/snippet}

<DocsLayout {sections} {entries}>
  <section class="hero">
    <h1>Language reference</h1>
    <p class="lede">
      Smeltr's language is what you write in Eval, Code, Drop and If steps, in step filters and in
      route conditions. A statement changes the record or declares a name, and an expression
      computes a value.
    </p>
  </section>

  <section class="prelude">
    <h2 id="how-it-works">How it works</h2>
    <p>
      A name reads the field of that name on the record being processed. Operators and functions
      compute from those values. <code>host.length()</code> and <code>length(host)</code> are the
      same call. Every value has a type (see <a href="#types">Types</a>), and a pipeline compiles
      only when every type fits.
    </p>
    {@render examples(calls.examples)}
  </section>

  <section class="group">
    <h2 id="fields">Fields</h2>
    <p>
      Every record has these fields. A source's own fields are on its
      <a href="/docs/sources">source page</a>, and the fields a destination reads are on its
      <a href="/docs/destinations">destination page</a>. Smeltr never delivers a field at the
      record's root whose name starts with <code>__</code>.
    </p>
    <div class="tscroll">
      <table class="kinds">
        <thead><tr><th>Field</th><th>Type</th><th>Set by</th><th>Holds</th></tr></thead>
        <tbody>
          {#each recordFields as f (f.name)}
            <tr>
              <td><code>{f.name}</code></td>
              <td><code>{f.type}</code></td>
              <td>{f.setBy}</td>
              <td><Prose text={f.what} /></td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>

  {#each recordGroups as g (g.id)}{@render group(g)}{/each}

  <section class="group">
    <h2 id="statements">Statements</h2>
    <p class="blurb">
      A statement ends with a semicolon, and a block in braces holds statements. The language has
      these statements:
    </p>
    <div class="tscroll">
      <table class="kinds">
        <tbody>
          {#each language.statements as row (row.form)}
            <tr><td><code>{row.form}</code></td><td><Prose text={row.rule} /></td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>

  <section class="group">
    <h2 id="types">Types</h2>
    <p>
      A name's first write sets its type. A name's type is fixed for the whole pipeline. A
      declaration, <code>name: T = e</code>, states it, and
      <code>e as T</code> converts a value to another type.
    </p>
    {@render codeExample('typed')}
    <p>After that, three kinds of write treat a value of another type differently:</p>
    <div class="tscroll">
      <table class="kinds">
        <thead><tr><th>Write</th><th>What it converts</th><th>Example</th></tr></thead>
        <tbody>
          <tr
            ><td><code>x = e</code></td><td
              >Nothing. It accepts null, and an <code>i64</code> into an <code>f64</code> name.</td
            ><td><code>{'r: f64 = 1.5; r = 2;'}</code> stores <code>2.0</code></td></tr
          >
          <tr
            ><td><code>{'x: T = e'}</code></td><td
              >A field a parser did not declare, record by record. It also changes the type of
              <code>x</code>.</td
            ><td><code>{'n: i64 = extra;'}</code> turns <code>"5"</code> into <code>5</code></td
            ></tr
          >
          <tr
            ><td><code>{'e as T'}</code></td><td>Any value.</td><td
              ><code>{'"5" as i64'}</code> is <code>5</code>; <code>{'2.7 as i64'}</code> is
              <code>3</code></td
            ></tr
          >
        </tbody>
      </table>
    </div>
    {@render codeExample('convert')}
    <p>
      After a JSON or Key/value parser, you can copy a field the parser does not declare as it is:
      <code>{'copy = extra;'}</code>. Its type is only known per record, so to calculate with it,
      give it a type first, as in <code>{'extra: str = extra;'}</code>, or convert the value:
      <code>{'(extra as str) + "!"'}</code>.
    </p>
    <div class="tscroll">
      <table class="kinds">
        <thead><tr><th>Type</th><th>Holds</th><th>Example</th></tr></thead>
        <tbody>
          {#each types as t (t.name)}
            <tr>
              <td><code>{t.name}</code></td>
              <td><Prose text={t.holds} /></td>
              <td class="ex">
                {#if t.example}<div class="exline">{@render example(t.example)}</div>{/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>

    <p>Any other mismatch is an error when you save, and the error names the fix:</p>
    {@render refused(errors.conversion)}
    <p>An expression whose types do not fit on any row does not compile.</p>
    {@render rejected(typeTopic.rejected)}
  </section>

  <section class="group">
    <h2 id="nulls">Null</h2>
    <p>
      A declared field a record lacks reads as null, and a name no step declares or writes does not
      compile. Arithmetic on null gives null. A condition reads null as false: a ternary takes the
      value after <code>:</code>, an If step its else branch, and a step whose filter is null leaves
      the record unchanged. <code>??</code> gives a fallback for null. Writing
      <code>null</code> to a field removes it.
    </p>
    {@render examples(nulls.examples)}
  </section>

  {#each typeGroups as g (g.id)}{@render group(g)}{/each}

  <section class="group">
    <h2 id="parse">Parse and lookup</h2>
    <p class="blurb">
      The parse functions read text into an object, as a Parser step does, and <code>lookup</code>
      reads a lookup table, as a Lookup step does. A call stands where a statement writes its object:
      <code>{'Object.assign($, f(…))'}</code>, <code>{'$ = f(…)'}</code>, <code>{'x = f(…)'}</code>
      and <code>{'x: T = f(…)'}</code>, or a <code>let</code>. Smeltr reads everything but the text
      and the key when the pipeline compiles, so you write it as literals. A parse function converts
      a field that a declared type names to it; every other field keeps the type the format gives,
      which is <code>str</code> for all but JSON.
    </p>
    {#each language.parseFunctions as fn (fn.name)}
      <article class="fn">
        <h3 id={`parse-${fn.name}`}><code>{fn.signature}</code></h3>
        <p class="summary"><Prose text={fn.summary} /></p>
        <div class="tscroll">
          <table class="kinds">
            <thead><tr><th>Option</th><th>Holds</th><th>Default</th></tr></thead>
            <tbody>
              {#each fn.options as option (option.name)}
                <tr>
                  <td><code>{option.name}</code></td>
                  <td><Prose text={option.holds} /></td>
                  <td><Prose text={option.default} /></td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </article>
    {/each}
  </section>

  <section class="group">
    <h2 id="exceptions">Exceptions and try</h2>
    <p>
      A calculation that cannot give a value leaves its field null and marks the record with an
      exception: text that is not a number, a division by zero, an overflow. Each exception has a
      kind, such as <code>CoercionRefused</code> or <code>DivideByZero</code>. A conversion that
      fails, such as <code>{'"five" as i64'}</code>, gives null and marks the record with a
      <code>CoercionRefused</code> exception. A parser's declared field that the parser cannot read
      as its type also gives null, and marks the record with an <code>UnparsableField</code>
      exception. An exception no <code>catch</code> takes stays on the record to the end of the
      pipeline, where its
      <a href="/docs/routing/exceptions#answers">exception answers</a> apply.
    </p>
    <p>
      A function's entry says when it throws one. Each kind belongs to a category. The group's
      <a href="/docs/routing/exceptions#answers">exception answers</a> answer a kind or its category,
      and a route may override them. By default an encoding exception is ignored, since its value is kept,
      a delivery exception blocks, and every other kind is tagged.
    </p>
    {@render examples(exceptionTopic.examples)}
    <div class="tscroll">
      <table class="kinds">
        <thead><tr><th>Kind</th><th>Category</th><th>When</th></tr></thead>
        <tbody>
          {#each exceptions as x (x.kind)}
            <tr>
              <td><code>{x.kind}</code></td>
              <td>{x.category}</td>
              <td><Prose text={x.when} /></td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <p>
      A <code>try</code> block stops at a record's first exception that loses a value. The write
      that failed does not happen, so the field keeps what it held. Its <code>catch</code> then runs
      on that record, with the exception cleared.
      <code>catch (e)</code> names the exception: <code>e.kind</code> is its kind and
      <code>e.message</code> its text. When one statement leaves several, <code>e.kind</code> is the
      first and <code>e.message</code> holds all their texts, and the <code>catch</code> clears them
      all. A <code>finally</code>
      block runs on every record that entered the <code>try</code>. A <code>try</code> needs a
      <code>catch</code>, a <code>finally</code> or both.
    </p>
    {@render codeExample('tryCatch')}
    <p>
      A typed catch, such as <code>catch (e: DivideByZero | NaN)</code>, runs on the records holding
      a kind it names and clears only those kinds. A <code>try</code> can have several catches, and
      it tries them in order. A catch with no type takes every kind left, and comes last. A kind no
      catch names stays on the record, for an outer <code>try</code> or its exception answer.
      <code>throw e</code> in a catch puts the exception back on the record, which goes on to the
      next
      <code>try</code> out, or to its exception answer when there is none. A catch reads
      <code>e</code> and cannot change it. A <code>try</code> catches every kind but the encoding
      and delivery ones. An encoding exception repairs its value instead, so the block goes on and
      no
      <code>catch</code> takes it.
    </p>
  </section>

  <section class="group">
    <h2 id="see-also">See also</h2>
    <ul>
      <li><a href="/docs/steps/code">Code step</a></li>
      <li><a href="/docs/steps/eval">Eval step</a></li>
      <li><a href="/docs/routing/exceptions#answers">Exception answers</a></li>
    </ul>
  </section>
</DocsLayout>

<style>
  .hero,
  .prelude,
  .group {
    max-width: 72rem;
    margin: 0 auto;
    padding-inline: 0;
  }

  .hero {
    padding-top: 0.5rem;
    padding-bottom: 1rem;
  }

  h1 {
    font-size: clamp(1.9rem, 5vw, 2.7rem);
    margin-bottom: 0.75rem;
  }

  .lede {
    color: var(--ink);
    font-size: 1.05rem;
    margin: 0;
  }

  .prelude {
    padding-top: 2.5rem;
  }

  .prelude h2,
  .group h2 {
    font-size: 1.5rem;
    margin-bottom: 0.75rem;
  }

  .prelude h2:not(:first-child) {
    margin-top: 3rem;
  }

  .prelude h3 {
    font-size: 1.1rem;
    margin: 2rem 0 0.5rem;
  }

  .group {
    padding-top: 3rem;
  }
  .group h2 {
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.75rem;
    font-size: 1.9rem;
  }
  .heading-link {
    color: inherit;
    text-decoration: none;
    overflow-wrap: anywhere;
  }
  .heading-link span {
    color: var(--dim);
    opacity: 0;
  }
  .heading-link:hover span,
  .heading-link:focus-visible span {
    opacity: 1;
  }

  /* The container is wide so an example and its comment fit on one line; prose
     and the type list keep a comfortable measure inside it, left-aligned to the
     same edge rather than centered away from it. */
  .lede,
  .prelude p,
  .types,
  .blurb,
  .note,
  .summary {
    max-width: 52rem;
  }

  .blurb {
    color: var(--ink);
    margin: 0 0 1.5rem;
  }

  .fn {
    border-top: 1px solid var(--border);
    padding: 1.4rem 0 0.4rem;
  }

  .fn h3 {
    margin: 0 0 0.5rem;
    font-size: 1rem;
  }

  .fn h3 code {
    font-size: 0.95rem;
    color: var(--ink);
  }

  .summary {
    margin: 0 0 0.9rem;
  }

  /* The engine's rows for a call: one per accepted shape, dim, so a reader who
     knows the types passes over it. */
  .types {
    margin: -0.25rem 0 0.6rem;
    font-size: 0.8rem;
    color: var(--dim);
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 1.1rem;
  }

  .types code {
    color: var(--dim);
  }

  /* The operator table scrolls as a unit rather than widening the page: on a
     narrow screen the example column alone is wider than the viewport. */
  .tscroll {
    overflow-x: auto;
  }

  .kinds {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.875rem;
    margin: 0.5rem 0 1.25rem;
  }

  .kinds th,
  .kinds td {
    border-top: 1px solid var(--border);
    padding: 0.4rem 0.7rem 0.4rem 0;
    text-align: left;
    vertical-align: baseline;
  }

  .kinds th {
    color: var(--dim);
    font-weight: inherit;
  }

  .kinds td:first-child {
    white-space: nowrap;
  }

  /* One row per symbol per operand type. The example column carries its own
     scroll so a long expression cannot widen the table. */
  .ops {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.875rem;
    margin: 0 0 1.25rem;
  }

  .ops td {
    border-top: 1px solid var(--border);
    padding: 0.4rem 0.7rem 0.4rem 0;
    vertical-align: baseline;
  }

  /* The same label inside the specifier table, where a run is a <tbody> and
     the label is its header row. */
  .ops th.grouphead {
    border-top: 1px solid var(--border);
    padding: 0.95rem 0.7rem 0.3rem 0;
    text-align: left;
    font-weight: inherit;
  }

  .ops .sym {
    width: 3.5rem;
    white-space: nowrap;
  }

  .ops .sym code {
    color: var(--ink);
    font-size: 1rem;
  }

  .ops .gloss {
    color: var(--ink);
  }

  .ops .ex {
    max-width: 34rem;
    overflow-x: auto;
  }

  /* A second example goes on its own line, so the two cases read as a pair.
     The line wraps rather than running past the cell: an example that does not
     fit is worth two lines, never a sentence cut off at the edge. */
  .exline {
    white-space: normal;
  }

  .exline + .exline {
    margin-top: 0.25rem;
  }

  .ops caption {
    text-align: left;
    color: var(--dim);
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding-bottom: 0.3rem;
  }

  /* A wavy underline, the marker every editor uses for "this does not
     compile", drawn as the app draws it under a rejected field's text. */
  .rejected .expr {
    text-decoration: underline wavy var(--error);
    text-decoration-thickness: 1.5px;
    text-underline-offset: 4px;
  }

  /* The caption is the label for what the whole table is, so it carries the
     color rather than every row of it. */
  .rejected caption {
    color: var(--error);
  }

  .rejected .ex {
    width: 40%;
  }

  .note {
    margin: 0.9rem 0 0;
    color: var(--ink);
    font-size: 0.9rem;
    border-left: 2px solid var(--border);
    padding-left: 0.9rem;
  }

  /* A label over a run of examples, for a function with too many to read
     straight through. */
  .grouphead {
    margin: 1.1rem 0 0.4rem;
    color: var(--dim);
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .examples {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: 0.35rem;
  }

  .examples li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
    background: var(--panel);
    border-radius: var(--radius);
    padding: 0.45rem 0.7rem;
    font-size: 0.875rem;
    /* A long expression scrolls in its own row rather than widening the page. */
    overflow-x: auto;
  }

  /* `pre-wrap`, not `pre`: code keeps its spacing but may wrap at a space
     rather than overflow. `anywhere` is the last resort for a single token
     wider than the column -- a long string literal. */
  .expr {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }

  .arrow {
    color: var(--dim);
  }

  .result {
    color: var(--added);
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }

  .throws {
    font-size: 0.82rem;
  }

  .throws code {
    color: inherit;
  }

  .result.nul,
  .varies {
    color: var(--dim);
  }

  /* A binding the example reads, so an example can be pipeline code rather
     than a wall of literals. */
  .given {
    color: var(--dim);
  }

  .given::after {
    content: ';';
    padding-right: 0.35rem;
  }

  /* What this one example is here to show, next to the line it is about. */
  .exnote {
    color: var(--dim);
    font-size: 0.82rem;
  }

  .varies {
    font-size: 0.82rem;
  }

  code {
    font-size: 0.9em;
  }

  @media (max-width: 40rem) {
    .hero {
      padding-top: 0.25rem;
    }
    .prelude {
      padding-top: 1.5rem;
    }
    .ops,
    .ops tbody,
    .ops tr,
    .ops td,
    .ops th {
      display: block;
    }
    .ops caption {
      display: block;
    }
    .ops tr {
      border-top: 1px solid var(--border);
      padding: 0.75rem 0;
    }
    .ops td {
      border: 0;
      padding: 0.25rem 0;
    }
    .ops th.grouphead {
      border: 0;
      padding: 0.25rem 0 0;
    }
    .ops .sym,
    .ops .ex,
    .rejected .ex {
      width: auto;
      max-width: none;
    }
    .ops .sym {
      white-space: normal;
      overflow-wrap: anywhere;
    }
    .examples li,
    .exline {
      display: flex;
      flex-direction: column;
      align-items: stretch;
      gap: 0.35rem;
    }
    .exline + .exline {
      margin-top: 1rem;
    }
    .arrow {
      display: none;
    }
    .result::before,
    .varies::before {
      content: 'Result: ';
      color: var(--dim);
      font-family: var(--font-body);
      font-size: 0.8rem;
    }
    .given {
      overflow-wrap: anywhere;
    }
  }
</style>
