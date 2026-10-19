<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import examplesText from '#lib/steps/call.json?raw';
  import { kind } from '#lib/connector-reference.ts';
  import { STEPS } from '#lib/step-reference.ts';
  import { stepExamples } from '#lib/step-pages.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('call', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Call step"
  description="Run another pipeline's steps inside this one with a Call step."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Call step</h1>
  <p class="lede">
    A Call step runs another pipeline's steps at its place in this pipeline, so steps that several
    pipelines share live in one pipeline of their own.
  </p>

  <h2 id="example">Example</h2>
  <StepExample example={examples.basic} />
  <p>
    This pipeline calls <code>{examples.basic.pipelines[0].id}</code>, which holds one Eval step
    that stamps every record with the ECS version.
  </p>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'call').settings} />
  <p>
    The GUI offers the other pipelines on the Pipelines page for <strong>pipeline</strong>.
  </p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    When the configuration loads, Smeltr replaces the Call step with the steps of the pipeline it
    names. Those steps then run as if they were written here: later steps read the fields they
    write. A change to the called pipeline reaches every pipeline that calls it. Smeltr saves the
    called pipeline only when it compiles on its own, so its steps read only the fields it parses or
    writes itself.
  </p>
  <p>
    The called pipeline's steps can hold Call steps of their own. The <strong>exceptions</strong> setting
    of the record's own pipeline answers an exception in a called step.
  </p>
  <p>A disabled Call step does not look up its pipeline, so it may name one that does not exist.</p>

  <h3 id="filter">Call selected records</h3>
  <p>
    With a filter, the called steps run as the <strong>then</strong> branch of an
    <a href="/docs/steps/if">If step</a> whose condition is the filter. Records where it is false or
    null pass the Call step unchanged. A filtered Call step therefore refuses a called pipeline that
    holds an <a href="/docs/steps/unroll">Unroll step</a>, as an If step does.
  </p>
  <p>This Call step stamps only the <code>checkout</code> records:</p>
  <StepExample example={examples.filtered} />

  <h3 id="rules">Step ids and cycles</h3>
  <p>
    Every step id in a pipeline is unique, counting the ids of the steps its Call steps bring in. A
    pipeline can therefore call a given pipeline only once, and a called step may not share an id
    with a step of the caller.
  </p>
  <p>
    Saving refuses a Call step that names a pipeline that does not exist. It also refuses a pipeline
    that calls itself, directly or through other pipelines, and its error names the chain of
    pipelines.
  </p>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/steps/if">If step</a></li>
    <li><a href="/docs/steps/unroll">Unroll step</a></li>
    <li><a href="/docs/code-reference">Expression reference</a></li>
  </ul>
</GuideLayout>
