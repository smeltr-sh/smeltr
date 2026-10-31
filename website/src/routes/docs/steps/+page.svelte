<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import ConnectorIndex from '#lib/components/ConnectorIndex.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { stepPages } from '#lib/step-pages.ts';
  import { STEPS, kind } from '#lib/step-reference.ts';
  const sections = [
    { id: 'kinds', title: 'Kinds' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'settings', title: 'Settings' },
    { id: 'create', title: 'Add a step' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const entries = stepPages.map(({ href, title }) => {
    const type = href.split('/').at(-1) ?? '';
    return { id: type, type, name: title, summary: kind(STEPS, type).summary, href };
  });
</script>

<GuideLayout
  title="Pipeline steps"
  description="What a pipeline step is, every kind of step, the settings all steps share, and how to add and test one."
  {sections}
>
  <h1>Pipeline steps</h1>
  <p class="lede">
    A pipeline is a list of steps. Each step reads and changes the records that reach it, in order.
    Each kind of step has a page of its own.
  </p>

  <h2 id="kinds">Kinds</h2>
  <ConnectorIndex {entries} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Each record goes through the steps from first to last. A step sees what the steps before it
    wrote. A record a <a href="/docs/steps/drop">Drop</a> step removes goes no further, and an
    <a href="/docs/steps/unroll">Unroll</a> step turns one record into several.
  </p>
  <p>
    A step's <code>filter</code> selects the records it processes: where the filter is false or
    null, the record passes the step unchanged. An <a href="/docs/steps/if">If</a> step selects records
    for a whole list of steps, and gives the other records a list of their own. The pipeline skips a disabled
    step.
  </p>
  <p>
    A value a step cannot read or compute throws an exception on its record, such as a parse or math
    exception. The route's or the group's
    <a href="/docs/routing/exceptions#answers">exception answers</a> decide what happens to the record.
  </p>

  <h2 id="settings">Settings</h2>
  <p>A step is one entry of its pipeline's steps. Every step takes an <code>id</code>:</p>
  <SettingsTable settings={STEPS.common} />
  <p>Every step but If and Comment also takes these:</p>
  <SettingsTable settings={STEPS.shared ?? []} />

  <h2 id="create">Add a step</h2>
  <ol>
    <li>Select the worker group and work on a branch.</li>
    <li>
      Open the pipeline, click <strong>+ step</strong>, and choose the step type.
    </li>
    <li>
      Fill in its settings, choose a sample, and click <strong>Run now</strong> to preview the output.
    </li>
    <li>Save the pipeline, then <a href="/docs/review">review and deploy</a>.</li>
  </ol>

  <h2 id="verify">Verify</h2>
  <p>
    Once the preview is right, choose <strong>Lock this as a test</strong>. A regression test checks
    that later edits still produce that result, including which records the pipeline drops. After
    each save, Resources → Samples shows whether every test still gives its locked output.
  </p>
  <ProductScreenshot
    source="build-tests"
    height={1800}
    alt="Samples page with two passing firewall pipeline tests. One expects ten records in and nine out; the other expects ten in and ten out."
    caption="A passing test can include dropped records: the first test expects 10 inputs and 9 outputs."
  />

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>A step changes nothing</strong></dt>
    <dd>Check that it is not disabled and that its filter is true on the sample's records.</dd>
    <dt><strong>Saving refuses the pipeline</strong></dt>
    <dd>
      The message names the step by its id and what it refuses, such as an If condition that is not
      a bool.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/code-reference">Expression reference</a></li>
    <li><a href="/docs/first-pipeline">Build a first pipeline</a></li>
    <li><a href="/docs/routing/exceptions#answers">Exception answers</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
