<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { STEPS } from '#lib/step-reference.ts';
  import { kind } from '#lib/connector-reference.ts';
  import examplesText from '#lib/steps/unroll.json?raw';
  import { stepExamples } from '#lib/step-pages.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('unroll', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Unroll step"
  description="Turn one record with an array field into one record per item with an Unroll step."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Unroll step</h1>
  <p class="lede">
    An Unroll step turns one record with an array field into one record per item, each holding one
    item under the array's name.
  </p>
  <p>The field must hold an array, such as one a parser declares as an array.</p>

  <h2 id="example">Example</h2>
  <p>
    The parser declares <code>items</code> as an array of <code>str</code>. With
    <code>keepParentFields</code> off, the step does not carry over <code>order</code>:
  </p>
  <StepExample example={examples.basic} />

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'unroll').settings} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    For each item of the array, the step makes a record. The array field in that record holds the
    item itself, so later steps read it as one value of the item's type. The new records keep the
    order of the items, and the records of a batch keep their order too.
  </p>

  <h3 id="keep">Keep the parent record's fields</h3>
  <p>
    With <code>keepParentFields</code> on, each new record gets a copy of every other field of the
    original record, including <code>_raw</code>, the whole source record. A large record with a
    long array then costs memory in proportion to the record's size times the array's length. Off,
    each new record carries only its own item. In the editor, turn on
    <strong>keep the parent record's fields on every element</strong>.
  </p>
  <p>The same record with the setting on:</p>
  <StepExample example={examples.keepParent} />
  <p>The Unroll examples leave out <code>_raw</code>.</p>

  <h3 id="edges">Empty, null and missing arrays</h3>
  <p>
    A record whose array is empty, null or missing makes no records. It leaves the pipeline at this
    step:
  </p>
  <StepExample example={examples.empty} />

  <h3 id="placement">Where it can go</h3>
  <p>
    An Unroll step changes how many records there are, so it runs on every record. Its
    <code>filter</code> must stay blank, and it cannot sit inside an
    <a href="/docs/steps/if">If step</a>; saving either is an error. To work on some of the new
    records, add an If step or a <a href="/docs/steps/drop">Drop step</a> after the Unroll step.
  </p>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/steps/if">If step</a></li>
    <li><a href="/docs/steps/drop">Drop step</a></li>
  </ul>
</GuideLayout>
