<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { STEPS } from '#lib/step-reference.ts';
  import { kind } from '#lib/connector-reference.ts';
  import examplesText from '#lib/steps/drop.json?raw';
  import { stepExamples } from '#lib/step-pages.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('drop', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Drop step"
  description="Remove records from a pipeline with a Drop step and its condition."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Drop step</h1>
  <p class="lede">
    A Drop step removes each record where its condition is true, so later steps and destinations
    never see it.
  </p>
  <p>The condition is an <a href="/docs/code-reference">expression</a> read on each record.</p>

  <h2 id="example">Example</h2>
  <p>The parser declares <code>level</code> as <code>str</code>.</p>
  <StepExample example={examples.basic} />
  <p>The step drops the debug record. The other two continue in their original order.</p>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'drop').settings} />
  <p>The editor labels <code>condition</code> <strong>drop when this is true</strong>.</p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    The step reads its condition on each record. A record where it is true leaves the pipeline at
    this step. Every other record continues unchanged.
  </p>
  <p>
    Inside an <a href="/docs/steps/if">If step</a>, a Drop step removes only records on that branch.
  </p>

  <h3 id="edges">Null and missing fields</h3>
  <p>
    A condition that is null is not true, so the step keeps the record. A record without
    <code>level</code> passes the same step:
  </p>
  <StepExample example={examples.missing} />

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/steps/if">If step</a></li>
    <li><a href="/docs/code-reference">Language reference</a></li>
  </ul>
</GuideLayout>
