<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { STEPS } from '#lib/step-reference.ts';
  import { kind } from '#lib/connector-reference.ts';
  import examplesText from '#lib/steps/eval.json?raw';
  import { stepExamples } from '#lib/step-pages.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('eval', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Eval step"
  description="Write fields from expressions with an Eval step: assignments that run in order on each record."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Eval step</h1>
  <p class="lede">
    An Eval step sets fields to the values of expressions, on every record it processes.
  </p>
  <p>
    Each assignment sets one target field to the value of one expression. The
    <a href="/docs/code-reference">language reference</a> lists the operators and functions an expression
    can use.
  </p>

  <h2 id="example">Example</h2>
  <p>
    The parser declares <code>price</code> as <code>f64</code> and <code>qty</code> as
    <code>i64</code>, so the expressions can calculate with them.
  </p>
  <StepExample example={examples.basic} />
  <p>
    <code>large</code> reads <code>total</code>, which the assignment above it wrote. The step sets
    <code>debug</code> to null, so it is gone from the output. Fields the step does not name remain.
  </p>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'eval').settings} />
  <p>In the editor, <strong>+ assignment</strong> adds a row.</p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    The assignments run in order, top to bottom. A later assignment reads the value an earlier one
    wrote. Each one replaces its whole target. When two assignments name the same target, the later
    one's value is the one that remains. Assigning <code>null</code> removes the field from the output.
  </p>
  <p>
    Each assignment is the Code statement <code>target = expression;</code>. When you need
    conditions, loops or temporary variables, use a <a href="/docs/steps/code">Code step</a>.
  </p>

  <h3 id="filter">Process selected records</h3>
  <p>When the filter is false or null, the record continues with its existing values.</p>
  <StepExample example={examples.filtered} />

  <h3 id="edges">Types and errors</h3>
  <p>
    Each assignment follows the language's rules for
    <a href="/docs/code-reference#statements">assignment</a>,
    <a href="/docs/code-reference#types">types</a>, <a href="/docs/code-reference#nulls">null</a>
    and <a href="/docs/code-reference#exceptions">exceptions</a>.
  </p>
  <p>
    An assignment with a blank target or a blank expression is an error when you save. The editor
    shows each assignment's error on its own row.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Saving reports a type error on an assignment</strong></dt>
    <dd>
      Check the type of the first value written to the target. Declare the field in the parser or
      <a href="/docs/code-reference#types">convert</a> the value to that type.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/code-reference">Language reference</a></li>
    <li><a href="/docs/steps/code">Code step</a></li>
  </ul>
</GuideLayout>
