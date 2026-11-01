<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import CodeExample from '#lib/components/CodeExample.svelte';
  import examplesText from '#lib/code-steps-examples.json?raw';
  import { parse } from '#lib/json-text.ts';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { STEPS } from '#lib/step-reference.ts';
  import { kind } from '#lib/connector-reference.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  // Read so that a number keeps its written form: `2.0` stays `2.0`.
  const blocks = parse(examplesText).blocks;
  const sections = [
    { id: 'settings', title: 'Settings' },
    { id: 'tasks', title: 'Common tasks' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

{#snippet example(name: string)}
  <CodeExample
    code={blocks[name].code}
    input={blocks[name].input}
    output={blocks[name].output}
    lookupTables={blocks[name].lookupTables}
  />
{/snippet}

<GuideLayout
  title="Code step"
  description="Write a Code step in a Smeltr pipeline: worked examples of common tasks."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Code step</h1>
  <p class="lede">A Code step runs a short program on each record it processes.</p>
  <p>
    The program's statements run in order, top to bottom. It reads the record's fields by name and
    writes fields that later steps and destinations see; assigning <code>null</code> removes a
    field. Every name has one type. This page gives worked examples of common tasks, each showing a
    record before and after the step. The
    <a href="/docs/code-reference">language reference</a> gives the rules its code follows, and lists
    the operators and functions it can use.
  </p>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'code').settings} />

  <h2 id="tasks">Common tasks</h2>

  <h3 id="set-fields">Set, rename and remove fields</h3>
  <p>Assign to set a field, and assign <code>null</code> to remove one:</p>
  {@render example('assignment')}
  <p>
    <a href="/docs/code-reference#statements"><code>delete</code></a> removes fields from the
    record, and <code>move</code> renames one. Inside an
    <code>if</code>, loop or <code>try</code>, <code>delete</code> cannot remove a field bound
    outside it; assign it <code>null</code> there instead.
  </p>
  {@render example('removeMove')}

  <h3 id="conditions">Choose a value</h3>
  <p>
    <a href="/docs/code-reference#statements"><code>if</code></a> and <code>else</code> choose which
    statements run for each record. For a short value selection, write
    <code>{'severity = status >= 500 ? "error" : "normal";'}</code>.
  </p>
  {@render example('condition')}
  <p>
    <a href="/docs/code-reference#statements"><code>switch</code></a> compares one value with each
    <code>case</code>
    in order. Cases with no statements between them share the next one's, and <code>break</code> ends
    the switch:
  </p>
  {@render example('switch')}

  <h3 id="filter-records">Filter records</h3>
  <p>
    <a href="/docs/code-reference#statements"><code>drop()</code></a> discards the record. It stands
    under an <code>if</code>, never inside a loop; a loop sets a flag, and an <code>if</code> after the
    loop drops on it. The step's filter is a condition. Where it is true, the body runs on the record.
    Where it is false or null, the body does not run, and the record continues with its existing values.
  </p>
  {@render example('drop')}

  <h3 id="parse">Parse a field</h3>
  <p>
    The parse functions read text into an object, as a Parser step does, and <code>lookup</code>
    reads a lookup table, as a Lookup step does. A call stands where a statement writes its object:
    <code>{'Object.assign($, f(…))'}</code>, <code>{'$ = f(…)'}</code>, <code>{'x = f(…)'}</code>
    and <code>{'x: T = f(…)'}</code>, or a <code>let</code>. Smeltr reads everything but the text
    and the key when the pipeline compiles, so you write it as literals. Text the function cannot
    read marks the record with an <code>Unparsable</code> exception and writes nothing.
  </p>
  {@render example('parseKv')}
  <p>
    A parse function converts a field that a declared type names to it; every other field keeps the
    type the format gives, which is <code>str</code> for all but JSON. The declared type turns
    <code>status</code> into an <code>i64</code>:
  </p>
  {@render example('parseGrok')}
  <p>
    The <a href="/docs/code-reference#parse">parse functions</a> are <code>json</code>,
    <code>kv</code>, <code>cef</code>, <code>csv</code> and <code>grok</code>, each with its
    options.
  </p>

  <h3 id="lookup">Look up a table</h3>
  <p>
    <a href="/docs/code-reference#parse-lookup"><code>lookup</code></a> reads the row of a lookup
    table that a key matches. <code>fields</code>: each output named for its column,
    <code>"column"</code>, or for its column and its default,
    <code>["column", "default"]</code>. A field with no match and no default is left out.
  </p>
  {@render example('lookup')}

  <h3 id="arrays">Work with arrays</h3>
  <p>
    <a href="/docs/code-reference#filter"><code>filter</code></a> keeps the items of an array for
    which an arrow's expression is true.
    <a href="/docs/code-reference#map"><code>map</code></a> replaces each item with the arrow's
    value. The original array remains unless you assign the result back to it, and the calls chain:
    <code>values.filter(x =&gt; x &gt; 2).map(x =&gt; x * 2)</code>.
  </p>
  {@render example('arrays')}
  <p>
    <a href="/docs/code-reference#statements"><code>for...of</code></a> visits each item in an array.
  </p>
  {@render example('loop')}

  <h3 id="split">Split a record</h3>
  <p>
    <a href="/docs/code-reference#statements"><code>unroll</code></a> replaces a record with one
    record per element of an array field. With
    <code>true</code> each new record keeps the other fields. It changes how many records there are,
    so it stands outside every <code>if</code>, loop and <code>try</code>.
  </p>
  {@render example('unroll')}

  <h3 id="functions">Reuse a calculation</h3>
  <p>
    A <a href="/docs/code-reference#statements">local function</a> names a repeated calculation. The
    body's last expression, written without a semicolon, is its value, as is a <code>return</code>.
  </p>
  {@render example('fn')}

  <h3 id="bad-data">Handle bad data</h3>
  <p>
    Use <a href="/docs/code-reference#nulls"><code>items ?? []</code></a> when a missing array
    should default to an empty one. Only the selected branch can mark the record with an exception.
    For example,
    <code>d == 0 ? 0 : n / d</code>
    returns zero when <code>d</code> is zero, without a <code>DivideByZero</code>. A
    <a href="/docs/code-reference#exceptions"><code>try</code></a> answers what is left, one kind at a
    time:
  </p>
  {@render example('typedCatch')}
  <p>
    Dividing by zero stops the <code>try</code> before <code>parseInt</code> runs, so the first catch
    takes the record. A record with packets but unreadable text would run the second catch.
  </p>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/code-reference">Language reference</a></li>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/routing/exceptions#answers">Exception answers</a></li>
    <li><a href="/docs/steps/unroll">Unroll step</a></li>
  </ul>
</GuideLayout>
